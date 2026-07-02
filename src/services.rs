//! The per-database services: GC, compactor coordinators, and
//! compaction workers, each a thin wrapper over `slatedb::admin::Admin`.
//!
//! Every runner takes a `CancellationToken` and blocks until cancelled
//! or failed. Safety is SlateDB's: GC deletes are idempotent,
//! coordinators are fenced by `compactor_epoch`, and workers claim jobs
//! by CAS — sleet only decides where these loops run.

use std::sync::Arc;
use std::time::Duration;

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

/// While a database is idle, worker polling backs off exponentially
/// from `compactions_poll_interval` up to this ceiling.
const IDLE_POLL_MAX: Duration = Duration::from_secs(300);

/// While the worker runs, sleet checks the queue on this cadence and
/// stops the worker after two consecutive empty checks.
const IDLE_CHECK_MIN: Duration = Duration::from_secs(30);

/// A service task failure.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("invalid database URL: {0}")]
    Url(#[from] registry::UrlError),
    #[error("failed to open database store: {0}")]
    Store(#[from] object_store::Error),
    #[error(transparent)]
    SlateDb(#[from] slatedb::Error),
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
    pub url: String,
    pub admin: Admin,
}

impl DatabaseHandle {
    /// Open a database by canonical URL. Credentials come from the
    /// environment, per `object_store`.
    pub fn open(url: &str) -> Result<Self, ServiceError> {
        let canonical = registry::canonicalize_url(url)?;
        let parsed = url::Url::parse(&canonical).expect("canonical URL reparses");
        let (store, path) = object_store::parse_url(&parsed)?;
        Ok(Self::from_parts(url, store.into(), path))
    }

    /// A handle over an existing store, for tests and embedding.
    pub fn from_parts(url: &str, store: Arc<dyn ObjectStore>, path: StorePath) -> Self {
        let admin = AdminBuilder::new(path, store).build();
        Self {
            url: url.to_string(),
            admin,
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

/// Run a compaction worker for one database until cancelled, polling
/// `.compactions` with exponential backoff while the database is idle:
/// the worker itself only runs while there is work, so mostly-idle
/// fleets cost one queue read per backed-off interval.
pub async fn run_workers(
    db: &DatabaseHandle,
    resolved: &ResolvedWorkers,
    token: CancellationToken,
) -> Result<(), ServiceError> {
    let base = resolved
        .compactions_poll_interval
        .max(Duration::from_millis(100));
    let max_idle = IDLE_POLL_MAX.max(base);
    let mut idle_poll = base;
    loop {
        if token.is_cancelled() {
            return Ok(());
        }
        let depth = queue_depth(&db.admin).await?;
        if depth.claimable > 0 || depth.running > 0 {
            idle_poll = base;
            run_worker_until_drained(db, resolved, &token).await?;
        } else {
            tokio::select! {
                _ = token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(idle_poll) => {}
            }
            idle_poll = (idle_poll * 2).min(max_idle);
        }
    }
}

/// Run the SlateDB worker until the parent is cancelled or the queue
/// has drained (two consecutive empty checks).
async fn run_worker_until_drained(
    db: &DatabaseHandle,
    resolved: &ResolvedWorkers,
    token: &CancellationToken,
) -> Result<(), ServiceError> {
    let worker_token = token.child_token();
    let options = worker_options(resolved);
    let check_every = (resolved.compactions_poll_interval * 6).max(IDLE_CHECK_MIN);
    let run = db
        .admin
        .run_compaction_worker_with_options(worker_token.clone(), options);
    tokio::pin!(run);
    let mut empty_checks = 0;
    loop {
        tokio::select! {
            result = &mut run => return result.map_err(Into::into),
            _ = tokio::time::sleep(check_every) => {
                match queue_depth(&db.admin).await {
                    Ok(depth) if depth.claimable == 0 && depth.running == 0 => {
                        empty_checks += 1;
                        if empty_checks >= 2 {
                            worker_token.cancel();
                        }
                    }
                    Ok(_) => empty_checks = 0,
                    // Transient read failure: keep the worker running.
                    Err(_) => {}
                }
            }
        }
    }
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
