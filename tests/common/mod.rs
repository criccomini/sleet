//! Shared multi-node test harness: real daemons over one in-memory
//! store, observable through heartbeat bodies and `sleet status`.
//!
//! Registered databases are plain URLs, not real SlateDB databases:
//! their service tasks fail and back off, which is fine — ownership,
//! liveness, and reconciliation are what these tests exercise.

// Each test binary compiles this module separately and uses a subset.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use sleet::config::Service;
use sleet::daemon::{self, NodeOptions};
use sleet::heartbeat::{self, Heartbeat};
use sleet::root::FleetRoot;
use sleet::testing::TestStore;
use sleet::{ops, registry};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Fast intervals so tests converge in seconds.
pub const FAST: &str = "[node]\nheartbeat_interval = \"200ms\"\n\
                        heartbeat_timeout = \"1s\"\nconfig_poll = \"400ms\"\n";

pub struct Cluster {
    /// The shared backing store all nodes ultimately write to.
    pub inner: Arc<InMemory>,
    /// The default instrumented store and root, used by observers and
    /// by nodes spawned without an override.
    pub store: Arc<TestStore>,
    pub root: FleetRoot,
    nodes: Vec<(String, CancellationToken, JoinHandle<()>)>,
}

impl Cluster {
    pub async fn new() -> Self {
        let inner = Arc::new(InMemory::new());
        let store = TestStore::new(inner.clone());
        let root =
            FleetRoot::from_parts(store.clone(), StorePath::from("fleet"), "memory:///fleet");
        root.store()
            .put(&root.config_path(), FAST.into())
            .await
            .unwrap();
        Self {
            inner,
            store,
            root,
            nodes: Vec::new(),
        }
    }

    /// A fresh instrumented view over the same backing store, for
    /// per-node fault injection or clocks.
    pub fn node_root(&self) -> (Arc<TestStore>, FleetRoot) {
        let store = TestStore::new(self.inner.clone());
        let root =
            FleetRoot::from_parts(store.clone(), StorePath::from("fleet"), "memory:///fleet");
        (store, root)
    }

    pub fn spawn(&mut self, node_id: &str, services: &[Service]) {
        let root = self.root.clone();
        self.spawn_on(node_id, services, root);
    }

    /// Spawn a node over its own root (own store wrapper and clock).
    pub fn spawn_on(&mut self, node_id: &str, services: &[Service], root: FleetRoot) {
        let token = CancellationToken::new();
        let handle = tokio::spawn({
            let options = NodeOptions {
                node_id: node_id.into(),
                services: services.to_vec(),
                max_compaction_jobs: 1,
            };
            let token = token.clone();
            async move {
                daemon::run(root, options, token).await.unwrap();
            }
        });
        self.nodes.push((node_id.into(), token, handle));
    }

    /// Kill a node without any cleanup: its heartbeat object stays
    /// behind and goes stale.
    pub fn kill(&mut self, node_id: &str) {
        let i = self
            .nodes
            .iter()
            .position(|(id, ..)| id == node_id)
            .unwrap();
        let (_, _, handle) = self.nodes.remove(i);
        handle.abort();
    }

    /// Stop a node cleanly (it deletes its heartbeat).
    pub async fn stop(&mut self, node_id: &str) {
        let i = self
            .nodes
            .iter()
            .position(|(id, ..)| id == node_id)
            .unwrap();
        let (_, token, handle) = self.nodes.remove(i);
        token.cancel();
        handle.await.unwrap();
    }

    /// Whether any node's daemon task died (e.g. panicked).
    pub fn any_node_died(&self) -> bool {
        self.nodes.iter().any(|(_, _, handle)| handle.is_finished())
    }

    pub async fn shutdown(mut self) {
        for (_, token, _) in &self.nodes {
            token.cancel();
        }
        for (_, _, handle) in self.nodes.drain(..) {
            let _ = handle.await;
        }
    }

    pub async fn register(&self, url: &str) {
        ops::register(&self.root, url).await.unwrap();
    }

    /// The youngest heartbeat body for a node, if readable.
    pub async fn body(&self, node_id: &str, services: &[Service]) -> Option<Heartbeat> {
        let path = self
            .root
            .node_path(&heartbeat::object_name(node_id, services));
        let get = self.root.store().get(&path).await.ok()?;
        serde_json::from_slice(&get.bytes().await.ok()?).ok()
    }

    /// Total supervised tasks a node reports across services.
    pub async fn task_count(&self, node_id: &str, services: &[Service]) -> u64 {
        self.body(node_id, services)
            .await
            .map(|b| b.services.iter().map(|s| s.running + s.backoff).sum())
            .unwrap_or(0)
    }

    /// The registry file path for a database URL.
    pub fn db_file(&self, url: &str) -> StorePath {
        self.root
            .database_path(&registry::canonicalize_url(url).unwrap())
    }
}

/// Poll an async condition every 100ms for up to 30s.
pub async fn poll_until<T, F, Fut>(what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Expected single-owner pair count per node, straight from the pure
/// ranking.
pub fn expected_pairs(node: &str, ids: &[&str], dbs: &[String]) -> u64 {
    let mut count = 0;
    for db in dbs {
        for service in Service::ALL {
            if sleet::placement::owners(db, service, 1, ids)[0] == node {
                count += 1;
            }
        }
    }
    count
}
