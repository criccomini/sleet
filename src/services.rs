//! The per-database services: GC, compactor coordinators, and
//! compaction workers, each a thin wrapper over `slatedb::admin::Admin`.
//!
//! Every runner takes a `CancellationToken` and blocks until cancelled
//! or failed. Safety is SlateDB's: GC deletes are idempotent,
//! coordinators are fenced by `compactor_epoch`, and workers claim jobs
//! by CAS. sleet only decides where these loops run.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use slatedb::admin::{Admin, AdminBuilder};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, GarbageCollectorScheduleOptions, SizeTieredCompactionSchedulerOptions,
};
use slatedb::{CloseReason, ErrorKind};
use tokio_util::sync::CancellationToken;

use crate::config::{
    CompressionCodec, ResolvedCoordinator, ResolvedGc, ResolvedGcDirectory, ResolvedServices,
    ResolvedWorkers,
};
use crate::registry;

/// A service task failure.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// The database URL was rejected.
    #[error(transparent)]
    Url(#[from] registry::UrlError),
    /// The database's object store could not be opened.
    #[error("failed to open database store: {0}")]
    Store(#[from] object_store::Error),
    /// The underlying SlateDB service failed.
    #[error(transparent)]
    SlateDb(#[from] slatedb::Error),
    /// The mirror task failed.
    #[error(transparent)]
    Mirror(#[from] crate::mirror::MirrorError),
}

impl ServiceError {
    /// Whether this failure is a `compactor_epoch` fence: another node
    /// started a coordinator for the same database, i.e. the two
    /// disagree about ownership.
    pub fn is_fenced(&self) -> bool {
        match self {
            ServiceError::SlateDb(e) => {
                matches!(e.kind(), ErrorKind::Closed(CloseReason::Fenced))
            }
            _ => false,
        }
    }
}

/// One managed database: its store and `slatedb::Admin`.
pub struct DatabaseHandle {
    /// The database URL the handle was opened with.
    pub url: String,
    /// The SlateDB admin API over the database's store.
    pub admin: Admin,
    /// The database's object store, for raw object reads and writes
    /// (the mirror byte-copies data objects and manifests).
    pub store: Arc<dyn ObjectStore>,
    /// The database root's path within the store.
    pub path: StorePath,
}

impl DatabaseHandle {
    /// Open a database by canonical URL. Credentials come from the
    /// environment, per `object_store`.
    pub fn open(url: &str) -> Result<Self, ServiceError> {
        let canonical = registry::canonicalize_database_url(url)?;
        let parsed = url::Url::parse(&canonical).expect("canonical URL reparses");
        let (store, path) = crate::store::parse_url(&parsed)?;
        Ok(Self::from_parts(url, store.into(), path))
    }

    /// A handle over an existing store, for tests and embedding.
    pub fn from_parts(url: &str, store: Arc<dyn ObjectStore>, path: StorePath) -> Self {
        let admin = AdminBuilder::new(path.clone(), store.clone()).build();
        Self {
            url: url.to_string(),
            admin,
            store,
            path,
        }
    }
}

/// Run long-running GC for one database until cancelled.
pub async fn run_gc(
    db: &DatabaseHandle,
    resolved: &ResolvedGc,
    token: CancellationToken,
) -> Result<(), ServiceError> {
    let options = gc_options(resolved);
    db.admin.run_gc_with_options(token, options).await?;
    Ok(())
}

/// Run the standalone compaction coordinator (no embedded worker) for
/// one database until cancelled or fenced.
pub async fn run_coordinator(
    db: &DatabaseHandle,
    resolved: &ResolvedCoordinator,
    token: CancellationToken,
) -> Result<(), ServiceError> {
    let options = coordinator_options(resolved);
    db.admin.run_compactor_with_options(token, options).await?;
    Ok(())
}

/// Run a compaction worker for one database until cancelled. The
/// worker polls `.compactions` on `compactions_poll_interval` and
/// executes what it claims, up to `max_concurrent_compactions` jobs.
pub async fn run_workers(
    db: &DatabaseHandle,
    resolved: &ResolvedWorkers,
    token: CancellationToken,
) -> Result<(), ServiceError> {
    let options = worker_options(resolved);
    db.admin
        .run_compaction_worker_with_options(token, options)
        .await?;
    Ok(())
}

/// Compaction queue depth for one database, from `.compactions`.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueueDepth {
    /// Jobs waiting for a worker (`Submitted` or `Scheduled`).
    pub claimable: usize,
    /// Jobs a worker is executing (`Running`).
    pub running: usize,
}

/// Read a database's compaction queue depth from `.compactions`.
pub async fn queue_depth(admin: &Admin) -> Result<QueueDepth, slatedb::Error> {
    use slatedb::compactor::CompactionStatus;
    let mut depth = QueueDepth::default();
    let Some(compactions) = admin.read_compactions(None).await? else {
        return Ok(depth);
    };
    for compaction in compactions.recent_compactions() {
        match compaction.status() {
            CompactionStatus::Submitted | CompactionStatus::Scheduled => depth.claimable += 1,
            CompactionStatus::Running => depth.running += 1,
            _ => {}
        }
    }
    Ok(depth)
}

/// Run the resolved services for one `(database, service)` assignment.
/// Mirror assignments carry a target and run through
/// `crate::mirror::run_mirror` instead; the daemon dispatches them
/// before reaching here.
pub async fn run_service(
    db: &DatabaseHandle,
    service: crate::config::Service,
    resolved: &ResolvedServices,
    token: CancellationToken,
) -> Result<(), ServiceError> {
    use crate::config::Service;
    match service {
        Service::Gc => run_gc(db, &resolved.gc, token).await,
        Service::CompactorCoordinator => run_coordinator(db, &resolved.coordinator, token).await,
        Service::CompactionWorkers => run_workers(db, &resolved.workers, token).await,
        Service::Mirror => unreachable!("mirror assignments dispatch to mirror::run_mirror"),
    }
}

fn gc_directory(dir: &ResolvedGcDirectory) -> Option<GarbageCollectorDirectoryOptions> {
    dir.enabled.then_some(GarbageCollectorDirectoryOptions {
        interval: Some(dir.interval),
        min_age: dir.min_age,
        dry_run: dir.dry_run,
    })
}

/// The SlateDB GC options for a resolved sleet GC config.
pub fn gc_options(resolved: &ResolvedGc) -> GarbageCollectorOptions {
    GarbageCollectorOptions {
        manifest_options: gc_directory(&resolved.manifest),
        wal_options: gc_directory(&resolved.wal),
        wal_fence_options: gc_directory(&resolved.wal_fence),
        compacted_options: gc_directory(&resolved.compacted),
        compactions_options: gc_directory(&resolved.compactions),
        detach_options: resolved
            .detach
            .enabled
            .then_some(GarbageCollectorScheduleOptions {
                interval: Some(resolved.detach.interval),
            }),
        metric_level: None,
    }
}

/// The SlateDB coordinator options for a resolved sleet config. The
/// embedded worker is always disabled: execution belongs to the
/// `compaction-workers` service.
pub fn coordinator_options(resolved: &ResolvedCoordinator) -> CompactorOptions {
    let scheduler = SizeTieredCompactionSchedulerOptions {
        min_compaction_sources: resolved.scheduler.min_compaction_sources as usize,
        max_compaction_sources: resolved.scheduler.max_compaction_sources as usize,
        include_size_threshold: resolved.scheduler.include_size_threshold,
    };
    CompactorOptions {
        poll_interval: resolved.poll_interval,
        manifest_update_timeout: resolved.manifest_update_timeout,
        max_concurrent_compactions: resolved.max_concurrent_compactions as usize,
        scheduler_options: scheduler.into(),
        worker: None,
        metric_level: None,
        commit_compacted_interval: resolved.commit_compacted_interval,
        worker_heartbeat_timeout: resolved.worker_heartbeat_timeout,
    }
}

/// The SlateDB worker options for a resolved sleet config.
pub fn worker_options(resolved: &ResolvedWorkers) -> CompactionWorkerOptions {
    CompactionWorkerOptions {
        max_concurrent_compactions: resolved.max_concurrent_compactions as usize,
        compactions_poll_interval: resolved.compactions_poll_interval,
        heartbeat_bytes: resolved.heartbeat_bytes,
        heartbeat_min_interval: resolved.heartbeat_min_interval,
        max_sst_size: resolved.max_sst_size as usize,
        max_fetch_tasks: resolved.max_fetch_tasks as usize,
        bytes_to_fetch: resolved.bytes_to_fetch as usize,
        max_subcompactions: resolved.max_subcompactions as usize,
        min_filter_keys: resolved.min_filter_keys,
        compression_codec: resolved.compression_codec.map(codec),
        metric_level: None,
    }
}

fn codec(codec: CompressionCodec) -> slatedb::config::CompressionCodec {
    use slatedb::config::CompressionCodec as Slate;
    match codec {
        CompressionCodec::Snappy => Slate::Snappy,
        CompressionCodec::Zlib => Slate::Zlib,
        CompressionCodec::Lz4 => Slate::Lz4,
        CompressionCodec::Zstd => Slate::Zstd,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::ResolvedServices;

    #[test]
    fn options_map_from_resolved_defaults() {
        let resolved = ResolvedServices::default();

        let gc = gc_options(&resolved.gc);
        let manifest = gc.manifest_options.unwrap();
        assert_eq!(manifest.interval, Some(Duration::from_secs(60)));
        assert_eq!(manifest.min_age, Duration::from_secs(300));
        assert!(!manifest.dry_run);
        assert!(gc.wal_fence_options.unwrap().dry_run);

        let coordinator = coordinator_options(&resolved.coordinator);
        assert!(coordinator.worker.is_none(), "no embedded worker, ever");
        assert_eq!(coordinator.poll_interval, Duration::from_secs(5));
        assert_eq!(
            coordinator.scheduler_options.get("max_compaction_sources"),
            Some(&"8".to_string())
        );

        let worker = worker_options(&resolved.workers);
        assert_eq!(worker.compactions_poll_interval, Duration::from_secs(5));
        assert_eq!(worker.heartbeat_bytes, 5 * 1024 * 1024);
        assert!(worker.compression_codec.is_none());
    }

    #[test]
    fn disabled_gc_directories_map_to_none() {
        let mut resolved = ResolvedServices::default();
        resolved.gc.wal.enabled = false;
        resolved.gc.detach.enabled = false;
        let gc = gc_options(&resolved.gc);
        assert!(gc.wal_options.is_none());
        assert!(gc.detach_options.is_none());
        assert!(gc.manifest_options.is_some());
    }
}
