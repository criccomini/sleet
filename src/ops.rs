//! One-shot subcommands: `register` and `status`. Both read (and for
//! `register`, write) only the fleet root and object storage; nodes
//! serve no API.

use std::collections::{BTreeMap, HashSet};

use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};

use crate::config::Service;
use crate::heartbeat::Heartbeat;
use crate::placement;
use crate::registry;
use crate::response::{
    DatabaseStatus, NodeStatus, QueueStatus, RegisterResponse, ServicePlacement, StatusResponse,
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
pub async fn status(root: &FleetRoot, compactions: bool) -> Result<StatusResponse, OpsError> {
    let mut poller = ConfigPoller::default();
    let state = poller.poll(root).await;
    let mut warnings = state.warnings.clone();
    let entries = root.list_heartbeats().await?;
    let timeout = state.config.node.heartbeat_timeout.0;

    let nodes = node_statuses(root, &entries, timeout).await;
    let live = node_view(&entries, timeout);

    let mut databases = Vec::new();
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
    for service in unoffered {
        warnings.push(format!("no live node offers {}", service.as_str()));
    }
    warnings.sort();
    Ok(StatusResponse {
        nodes,
        databases,
        warnings,
    })
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

        let status = status(&root, false).await.unwrap();
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
