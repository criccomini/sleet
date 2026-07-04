//! One-shot subcommands: `register`, `status`, and the `mirror`
//! family. All read (and for `register`, write) only the fleet root
//! and object storage; nodes serve no API.

use std::collections::{BTreeMap, HashSet};

use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};

use crate::config::Service;
use crate::heartbeat::Heartbeat;
use crate::mirror::{self, AppliedTarget};
use crate::placement;
use crate::registry;
use crate::response::{
    DatabaseStatus, MirrorPrefixesResponse, MirrorRestoreResponse, MirrorStatus,
    MirrorSyncResponse, MirrorVerifyResponse, NodeStatus, PrefixFormat, QueueStatus,
    RegisterResponse, RestorePointStatus, ServicePlacement, StatusResponse,
};
use crate::root::{ConfigPoller, FleetRoot, HeartbeatEntry, node_view};
use crate::services::{self, DatabaseHandle};

/// A one-shot subcommand failure.
#[derive(Debug, thiserror::Error)]
pub enum OpsError {
    /// A database URL was rejected.
    #[error(transparent)]
    Url(#[from] registry::UrlError),
    /// An object-store read or write failed.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    /// The database is not in the registry.
    #[error("{url} is not registered; `sleet register` it first")]
    NotRegistered {
        /// The canonical database URL.
        url: String,
    },
    /// No enabled target of that name applies to the database.
    #[error("no enabled mirror target {target:?} applies to {url}")]
    NoSuchTarget {
        /// The target name asked for.
        target: String,
        /// The canonical database URL.
        url: String,
    },
    /// A database store could not be opened.
    #[error(transparent)]
    Service(#[from] services::ServiceError),
    /// The mirror operation failed.
    #[error(transparent)]
    Mirror(#[from] mirror::MirrorError),
}

/// Register a database: canonicalize its URL and create-only PUT its
/// registry file, so registering never overwrites operator edits.
pub async fn register(root: &FleetRoot, url: &str) -> Result<RegisterResponse, OpsError> {
    let canonical = registry::canonicalize_database_url(url)?;
    let path = root.database_path(&canonical);
    let file = format!("dbs/{}", registry::file_name(&canonical));
    let options = PutOptions::from(PutMode::Create);
    let created = match root
        .store()
        .put_opts(&path, PutPayload::default(), options)
        .await
    {
        Ok(_) => true,
        Err(object_store::Error::AlreadyExists { .. }) => false,
        Err(e) => return Err(e.into()),
    };
    Ok(RegisterResponse {
        url: canonical,
        file,
        created,
    })
}

/// Derive fleet status from the tree: node liveness, roles, and
/// versions from `nodes/`, intent from `sleet.toml` and `dbs/`, and
/// placement by computing the same rendezvous ranking the nodes do.
/// With `compactions`, also read each database's `.compactions` depth.
/// With `mirrors`, also read each `(database, target)`'s source and
/// destination heads and report lag, destination collisions, and
/// databases with mirror enabled but no applicable target.
pub async fn status(
    root: &FleetRoot,
    compactions: bool,
    mirrors: bool,
) -> Result<StatusResponse, OpsError> {
    let mut poller = ConfigPoller::default();
    let state = poller.poll(root).await;
    let mut warnings = state.warnings.clone();
    let entries = root.list_heartbeats().await?;
    let timeout = state.config.node.heartbeat_timeout.0;

    let nodes = node_statuses(root, &entries, timeout).await;
    let live = node_view(&entries, timeout);

    let mut databases = Vec::new();
    let mut mirror_statuses = Vec::new();
    let mut destinations: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut unoffered: HashSet<Service> = HashSet::new();
    for (url, db) in &state.databases {
        let resolved = state.config.resolve(Some(db));
        let mut placements = Vec::new();
        for &service in &resolved.services {
            let candidates: Vec<&str> = live
                .iter()
                .filter(|n| n.services.contains(&service))
                .map(|n| n.node_id.as_str())
                .collect();
            if service == Service::Mirror {
                let applied = mirror::applied_targets(url, &resolved.mirror);
                if mirrors && applied.is_empty() {
                    warnings.push(format!("{url} has mirror enabled but no applicable target"));
                }
                for target in applied {
                    let owner = placement::owner_target(url, &target.name, &candidates);
                    if owner.is_none() {
                        unoffered.insert(service);
                    }
                    placements.push(ServicePlacement {
                        service,
                        nodes: owner.into_iter().map(String::from).collect(),
                    });
                    if mirrors {
                        destinations
                            .entry(target.destination.clone())
                            .or_default()
                            .push(format!("{url} target {}", target.name));
                        mirror_statuses.push(mirror_status(url, &target).await);
                    }
                }
                continue;
            }
            let count = match service {
                Service::CompactionWorkers => resolved.workers.count as usize,
                _ => 1,
            };
            let owners = placement::owners(url, service, count, &candidates);
            if owners.is_empty() {
                unoffered.insert(service);
            }
            placements.push(ServicePlacement {
                service,
                nodes: owners.into_iter().map(String::from).collect(),
            });
        }
        let queue = if compactions {
            match queue_status(url).await {
                Ok(queue) => Some(queue),
                Err(e) => {
                    warnings.push(format!("failed to read compactions for {url}: {e}"));
                    None
                }
            }
        } else {
            None
        };
        databases.push(DatabaseStatus {
            url: url.clone(),
            services: placements,
            queue,
        });
    }
    for (destination, sources) in destinations {
        if sources.len() > 1 {
            warnings.push(format!(
                "mirror destinations collide: {} all map to {destination}",
                sources.join(" and ")
            ));
        }
        // A destination can never itself be a registered database
        // (DESIGN-MIRROR §2): sleet services at the destination would
        // violate the mirror's single-writer invariant.
        if state.databases.contains_key(&destination) {
            warnings.push(format!(
                "mirror destination {destination} is itself a registered database; \
                 its services will fork the mirror's history"
            ));
        }
    }
    for service in unoffered {
        warnings.push(format!("no live node offers {}", service.as_str()));
    }
    warnings.sort();
    Ok(StatusResponse {
        nodes,
        databases,
        mirrors: mirror_statuses,
        warnings,
    })
}

/// One `(database, target)`'s lag, from the source and destination
/// heads. Read failures land in the `error` field rather than failing
/// the whole status.
async fn mirror_status(url: &str, target: &AppliedTarget) -> MirrorStatus {
    let mut status = MirrorStatus {
        database: url.to_string(),
        target: target.name.clone(),
        destination: target.destination.clone(),
        source_manifest_id: None,
        target_manifest_id: None,
        manifests_behind: None,
        wal_behind: None,
        seconds_behind: None,
        error: None,
    };
    match mirror_lag(url, target, &mut status).await {
        Ok(()) => {}
        Err(e) => status.error = Some(e.to_string()),
    }
    status
}

async fn mirror_lag(
    url: &str,
    target: &AppliedTarget,
    status: &mut MirrorStatus,
) -> Result<(), OpsError> {
    use crate::mirror::layout;
    use slatedb::seq_tracker::FindOption;
    let source = DatabaseHandle::open(url)?;
    let dest = DatabaseHandle::open(&target.destination)?;
    let source_head = source
        .admin
        .read_manifest(None)
        .await
        .map_err(mirror::MirrorError::from)?;
    let Some(source_head) = source_head else {
        return Err(mirror::MirrorError::NotADatabase {
            url: url.to_string(),
        }
        .into());
    };
    status.source_manifest_id = Some(source_head.id());
    let dest_head = dest
        .admin
        .read_manifest(None)
        .await
        .map_err(mirror::MirrorError::from)?;
    let Some(dest_head) = dest_head else {
        // Nothing mirrored yet: fully behind.
        status.manifests_behind = Some(source_head.id());
        return Ok(());
    };
    status.target_manifest_id = Some(dest_head.id());
    status.manifests_behind = Some(source_head.id().saturating_sub(dest_head.id()));
    let source_wal = layout::list_wals(&source).await?.last().map(|(id, _)| *id);
    let dest_wal = layout::list_wals(&dest).await?.last().map(|(id, _)| *id);
    if let Some(source_wal) = source_wal {
        status.wal_behind = Some(source_wal.saturating_sub(dest_wal.unwrap_or(0)));
    }
    // Source and target sequence numbers mapped through the source's
    // sequence tracker.
    let tracker = source_head.sequence_tracker();
    if let (Some(source_ts), Some(dest_ts)) = (
        tracker.find_ts(source_head.last_l0_seq(), FindOption::RoundDown),
        tracker.find_ts(dest_head.last_l0_seq(), FindOption::RoundDown),
    ) {
        status.seconds_behind = Some((source_ts - dest_ts).num_seconds().max(0) as u64);
    }
    Ok(())
}

/// Every fleet member with a heartbeat object, dead or alive, with
/// versions from the youngest body per node.
async fn node_statuses(
    root: &FleetRoot,
    entries: &[HeartbeatEntry],
    timeout: std::time::Duration,
) -> Vec<NodeStatus> {
    let mut youngest: BTreeMap<&str, &HeartbeatEntry> = BTreeMap::new();
    for entry in entries {
        match youngest.get(entry.node_id.as_str()) {
            Some(existing) if existing.age <= entry.age => {}
            _ => {
                youngest.insert(&entry.node_id, entry);
            }
        }
    }
    let mut nodes = Vec::new();
    for entry in youngest.into_values() {
        let body: Option<Heartbeat> = match root.store().get(&entry.location).await {
            Ok(get) => get
                .bytes()
                .await
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok()),
            Err(_) => None,
        };
        nodes.push(NodeStatus {
            node_id: entry.node_id.clone(),
            live: entry.age < timeout,
            heartbeat_age: std::time::Duration::from_secs(entry.age.as_secs()).into(),
            services: entry.services.clone(),
            sleet_version: body.as_ref().map(|b| b.sleet_version.clone()),
            slatedb_version: body.as_ref().map(|b| b.slatedb_version.clone()),
        });
    }
    nodes
}

/// Resolve one database's applied mirror target from the fleet root:
/// the registry entry must exist and an enabled target of that name
/// must apply.
async fn applied_target(
    root: &FleetRoot,
    db_url: &str,
    target_name: &str,
) -> Result<(String, AppliedTarget), OpsError> {
    let canonical = registry::canonicalize_database_url(db_url)?;
    let mut poller = ConfigPoller::default();
    let state = poller.poll(root).await;
    let db = state
        .databases
        .get(&canonical)
        .ok_or_else(|| OpsError::NotRegistered {
            url: canonical.clone(),
        })?;
    let resolved = state.config.resolve(Some(db));
    let applied = mirror::applied_targets(&canonical, &resolved.mirror)
        .into_iter()
        .find(|t| t.name == target_name)
        .ok_or_else(|| OpsError::NoSuchTarget {
            target: target_name.to_string(),
            url: canonical.clone(),
        })?;
    Ok((canonical, applied))
}

/// `sleet mirror sync`: one pass regardless of mode, plus the prune
/// that follows it when retention is set.
pub async fn mirror_sync(
    root: &FleetRoot,
    db_url: &str,
    target_name: &str,
    rclone: Option<&str>,
) -> Result<MirrorSyncResponse, OpsError> {
    let (canonical, target) = applied_target(root, db_url, target_name).await?;
    let source = DatabaseHandle::open(&canonical)?;
    let dest = DatabaseHandle::open(&target.destination)?;
    let (outcome, pruned) = mirror::sync_once(&source, &dest, &target, rclone).await?;
    Ok(MirrorSyncResponse {
        database: canonical,
        target: target.name,
        destination: target.destination,
        head: outcome.head,
        committed: outcome.committed,
        manifests_committed: outcome.manifests_committed,
        objects_copied: outcome.copied.objects,
        bytes_copied: outcome.copied.bytes,
        pruned_manifests: pruned.deleted_manifests,
        pruned_objects: pruned.deleted_objects,
    })
}

/// `sleet mirror verify`: existence and size for every restore point's
/// closure at the destination; `Depth::Bytes` also compares content.
pub async fn mirror_verify(
    root: &FleetRoot,
    db_url: &str,
    target_name: &str,
    depth: mirror::Depth,
) -> Result<MirrorVerifyResponse, OpsError> {
    let (canonical, target) = applied_target(root, db_url, target_name).await?;
    let source = DatabaseHandle::open(&canonical)?;
    let dest = DatabaseHandle::open(&target.destination)?;
    let outcome = mirror::verify(&source, &dest, target.settings.keep, depth).await?;
    Ok(MirrorVerifyResponse {
        database: canonical,
        target: target.name,
        destination: target.destination,
        deep: depth == mirror::Depth::Bytes,
        ok: outcome.ok(),
        points: outcome
            .points
            .into_iter()
            .map(|p| RestorePointStatus {
                manifest_id: p.manifest_id,
                objects: p.objects,
                problems: p.problems,
            })
            .collect(),
    })
}

/// `sleet mirror restore`: copy one restore point's closure from a
/// backup into an empty destination root and commit it.
pub async fn mirror_restore(
    backup_url: &str,
    dest_url: &str,
    at: mirror::RestorePoint,
) -> Result<MirrorRestoreResponse, OpsError> {
    let backup = DatabaseHandle::open(backup_url)?;
    let dest = DatabaseHandle::open(dest_url)?;
    let outcome = mirror::restore(&backup, &dest, at).await?;
    Ok(MirrorRestoreResponse {
        backup: backup.url,
        destination: dest.url,
        manifest_id: outcome.manifest_id,
        manifests_committed: outcome.manifests_committed,
        objects_copied: outcome.copied_objects,
        bytes_copied: outcome.copied_bytes,
    })
}

/// `sleet mirror prefixes`: the anchored key-prefix filter lists for
/// configuring external replication over one database's data
/// directories.
pub async fn mirror_prefixes(
    root: &FleetRoot,
    db_url: &str,
    target_name: &str,
    format: PrefixFormat,
) -> Result<MirrorPrefixesResponse, OpsError> {
    use crate::mirror::layout::{COMPACTED_DIR, WAL_DIR};
    let (canonical, target) = applied_target(root, db_url, target_name).await?;
    let (source_bucket, source_path) = bucket_and_path(&canonical)?;
    let (dest_bucket, dest_path) = bucket_and_path(&target.destination)?;
    let dir_prefixes = |path: &str| {
        [WAL_DIR, COMPACTED_DIR]
            .iter()
            .map(|dir| {
                if path.is_empty() {
                    format!("{dir}/")
                } else {
                    format!("{path}/{dir}/")
                }
            })
            .collect::<Vec<String>>()
    };
    let prefixes = dir_prefixes(&source_path);
    let destination_prefixes = dir_prefixes(&dest_path);
    let configuration =
        prefix_configuration(format, target_name, &source_bucket, &dest_bucket, &prefixes);
    Ok(MirrorPrefixesResponse {
        database: canonical,
        target: target.name,
        destination: target.destination,
        format,
        source_bucket,
        destination_bucket: dest_bucket,
        prefixes,
        destination_prefixes,
        configuration,
    })
}

/// A URL's bucket (or container) and root-relative path.
fn bucket_and_path(url: &str) -> Result<(String, String), OpsError> {
    let canonical = registry::canonicalize_url(url)?;
    let parsed = url::Url::parse(&canonical).expect("canonical URL reparses");
    Ok((
        parsed.host_str().unwrap_or_default().to_string(),
        parsed.path().trim_matches('/').to_string(),
    ))
}

/// The service-native configuration snippet for one format. These are
/// skeletons carrying the filter lists; account-specific fields (roles,
/// rule ids) are left as placeholders.
fn prefix_configuration(
    format: PrefixFormat,
    target_name: &str,
    source_bucket: &str,
    dest_bucket: &str,
    prefixes: &[String],
) -> serde_json::Value {
    use serde_json::json;
    match format {
        PrefixFormat::S3 => json!({
            "Rules": prefixes
                .iter()
                .enumerate()
                .map(|(i, prefix)| {
                    json!({
                        "ID": format!("sleet-{target_name}-{i}"),
                        "Status": "Enabled",
                        "Priority": i + 1,
                        "Filter": { "Prefix": prefix },
                        "Destination": { "Bucket": format!("arn:aws:s3:::{dest_bucket}") },
                        "DeleteMarkerReplication": { "Status": "Disabled" }
                    })
                })
                .collect::<Vec<_>>()
        }),
        PrefixFormat::Sts => json!({
            "transferSpec": {
                "gcsDataSource": { "bucketName": source_bucket },
                "gcsDataSink": { "bucketName": dest_bucket },
                "objectConditions": { "includePrefixes": prefixes }
            }
        }),
        PrefixFormat::Azure => json!({
            "rules": [{
                "ruleId": format!("sleet-{target_name}"),
                "sourceContainer": source_bucket,
                "destinationContainer": dest_bucket,
                "filters": { "prefixMatch": prefixes }
            }]
        }),
    }
}

/// One database's compaction queue depth, read via `slatedb::Admin`.
async fn queue_status(url: &str) -> Result<QueueStatus, String> {
    let db = DatabaseHandle::open(url).map_err(|e| e.to_string())?;
    let depth = services::queue_depth(&db.admin)
        .await
        .map_err(|e| e.to_string())?;
    Ok(QueueStatus {
        claimable: depth.claimable as u64,
        running: depth.running as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Service;
    use crate::heartbeat;
    use crate::testing::{TestClock, TestStore};
    use chrono::Utc;
    use object_store::ObjectStoreExt;
    use object_store::path::Path as StorePath;
    use std::time::Duration;

    /// Status lists dead and live nodes with the youngest body's
    /// versions; unreadable bodies yield absent versions.
    #[tokio::test]
    async fn status_reports_dead_nodes_and_unreadable_bodies() {
        let clock = TestClock::new(Utc::now());
        let store = TestStore::in_memory_at(clock.clone());
        let root = FleetRoot::from_parts(store, StorePath::from("fleet"), "memory:///f")
            .with_clock(clock.clone());

        // "old" heartbeats, then advance past heartbeat_timeout (30s).
        let dead = Heartbeat::new("dead", "0.14.1", vec![]);
        root.store()
            .put(
                &root.node_path(&heartbeat::object_name("dead", &Service::ALL)),
                serde_json::to_vec(&dead).unwrap().into(),
            )
            .await
            .unwrap();
        clock.advance(Duration::from_secs(120));
        root.store()
            .put(
                &root.node_path(&heartbeat::object_name("garbled", &[Service::Gc])),
                "not json".into(),
            )
            .await
            .unwrap();

        let status = status(&root, false, false).await.unwrap();
        assert_eq!(status.nodes.len(), 2);
        let dead = status.nodes.iter().find(|n| n.node_id == "dead").unwrap();
        assert!(!dead.live);
        assert_eq!(dead.heartbeat_age.0, Duration::from_secs(120));
        assert_eq!(
            dead.sleet_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        let garbled = status
            .nodes
            .iter()
            .find(|n| n.node_id == "garbled")
            .unwrap();
        assert!(garbled.live);
        assert_eq!(garbled.sleet_version, None);
        assert_eq!(garbled.slatedb_version, None);
        assert_eq!(garbled.services, vec![Service::Gc]);
    }
}
