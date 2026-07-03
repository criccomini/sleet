//! The adapter mapping the coordination spec's actions onto real sleet
//! primitives: an in-memory fleet root with a simulated clock, real
//! heartbeat objects, and the daemon's real ownership decision
//! function.
//!
//! What is verified: `Recompute` and `HandleFence` return the decision
//! the spec computed from its `RANK` and freshness state, and the
//! adapter returns the decision `daemon::owned_assignments` makes from
//! the real store (heartbeat names, `node_view` liveness, self
//! inclusion, the frozen rendezvous hash, config resolution). The
//! runner fails the test when any action's return diverges, so every
//! generated interleaving cross-checks the model against the code.
//!
//! The runner also dispatches actions whose `require` guard is false
//! in the model; those are model no-ops returning nil, so the adapter
//! mirrors the spec's guards and budgets, and returns `None` without
//! touching the store for a disabled action.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use fizzbee_mbt::config::TestOptions;
use fizzbee_mbt::error::MbtError;
use fizzbee_mbt::traits::*;
use fizzbee_mbt::types::Arg;
use fizzbee_mbt::value::Value;
use object_store::ObjectStoreExt;
use sleet::config::Service;
use sleet::heartbeat::{self, Heartbeat};
use sleet::root::{Clock, ConfigPoller, FleetRoot, FleetState};
use sleet::testing::{TestClock, TestStore};
use sleet::{daemon, ops, placement};
use tokio::sync::Mutex;

use super::traits::*;

/// Spec constants, mirrored: node ids by role index, database URLs by
/// spec name, and the spec's `RANK`, which must equal the real frozen
/// ranking of these URLs over these nodes (checked at init).
const NODES: [&str; 3] = ["n1", "n2", "n3"];
const DBS: [(&str, &str); 2] = [("db1", "memory:///dbs/db1"), ("db2", "memory:///dbs/db2")];
const RANK: [(&str, [&str; 3]); 2] = [("db1", ["n3", "n2", "n1"]), ("db2", ["n1", "n2", "n3"])];

/// One fleet under test plus the mirrored model bookkeeping the spec's
/// guards need (`alive`, `fresh`, `fenced`, `runners`, fault budgets).
struct Harness {
    clock: Arc<TestClock>,
    store: Arc<TestStore>,
    root: FleetRoot,
    state: FleetState,
    alive: BTreeMap<String, bool>,
    fresh: BTreeMap<String, bool>,
    runners: BTreeMap<String, Vec<String>>,
    fenced: BTreeMap<String, Vec<String>>,
    crashes_left: u32,
    restarts_left: u32,
    suspects_left: u32,
}

impl Harness {
    async fn new() -> Result<Self, MbtError> {
        let clock = TestClock::new(Utc::now());
        let store = TestStore::in_memory_at(clock.clone());
        let root = FleetRoot::from_parts(
            store.clone(),
            object_store::path::Path::from("fleet"),
            "memory:///f",
        )
        .with_clock(clock.clone());
        store
            .put(
                &root.config_path(),
                "[database]\nservices = [\"gc\"]\n".into(),
            )
            .await
            .map_err(MbtError::from_err)?;
        for (_, url) in DBS {
            ops::register(&root, url)
                .await
                .map_err(MbtError::from_err)?;
        }
        let mut harness = Self {
            clock,
            store,
            root,
            state: FleetState::default(),
            alive: NODES.iter().map(|n| (n.to_string(), true)).collect(),
            fresh: NODES.iter().map(|n| (n.to_string(), true)).collect(),
            runners: DBS.iter().map(|(d, _)| (d.to_string(), vec![])).collect(),
            fenced: DBS.iter().map(|(d, _)| (d.to_string(), vec![])).collect(),
            crashes_left: 1,
            restarts_left: 1,
            suspects_left: 2,
        };
        // All nodes start fresh, matching the spec's Init.
        for node in NODES {
            harness.put_heartbeat(node).await?;
        }
        harness.state = ConfigPoller::default().poll(&harness.root).await;
        if !harness.state.warnings.is_empty() {
            return Err(MbtError::other(format!(
                "registry warnings: {:?}",
                harness.state.warnings
            )));
        }
        // The spec's RANK must match the real frozen ranking, or every
        // return comparison below is meaningless.
        for (db, rank) in RANK {
            let url = db_url(db);
            let real = placement::owners(url, Service::Gc, NODES.len(), &NODES);
            if real != rank {
                return Err(MbtError::other(format!(
                    "spec RANK for {db} is {rank:?} but the frozen hash ranks {url} as \
                     {real:?}; update RANK in specs/coordination.fizz and here"
                )));
            }
        }
        Ok(harness)
    }

    async fn put_heartbeat(&self, node: &str) -> Result<(), MbtError> {
        let name = heartbeat::object_name(node, &[Service::Gc]);
        let body = serde_json::to_vec(&Heartbeat::new(node, "0.0.0", vec![]))
            .expect("heartbeat serializes");
        self.store
            .put(&self.root.node_path(&name), body.into())
            .await
            .map_err(MbtError::from_err)?;
        Ok(())
    }

    /// Backdate a node's heartbeat by exactly `heartbeat_timeout`, so
    /// the real liveness rule reads it as stale.
    fn backdate_heartbeat(&self, node: &str) {
        let name = heartbeat::object_name(node, &[Service::Gc]);
        let timeout = self.state.config.node.heartbeat_timeout.0;
        let stale = self.clock.now() - chrono::Duration::from_std(timeout).expect("timeout fits");
        self.store.set_modified(&self.root.node_path(&name), stale);
    }

    /// The real decision: which of the spec's databases does `node`
    /// own, per `daemon::owned_assignments` over the real store.
    async fn real_decision(&self, node: &str) -> Result<BTreeMap<String, bool>, MbtError> {
        let entries = self
            .root
            .list_heartbeats()
            .await
            .map_err(MbtError::from_err)?;
        let owned = daemon::owned_assignments(node, &[Service::Gc], &entries, &self.state);
        Ok(DBS
            .iter()
            .map(|(db, url)| {
                let key = (url.to_string(), Service::Gc, None);
                (db.to_string(), owned.contains_key(&key))
            })
            .collect())
    }
}

fn db_url(db: &str) -> &'static str {
    DBS.iter()
        .find(|(name, _)| *name == db)
        .map(|(_, url)| *url)
        .unwrap_or_else(|| panic!("unknown database {db:?}"))
}

fn str_arg<'a>(args: &'a [Arg], name: &str) -> Result<&'a str, MbtError> {
    match args.iter().find(|a| a.name == name).map(|a| &a.value) {
        Some(Value::Str(s)) => Ok(s),
        other => Err(MbtError::other(format!(
            "expected str arg {name:?}, got {other:?}"
        ))),
    }
}

pub struct NodeRoleAdapter {
    node_id: String,
    harness: Arc<Mutex<Harness>>,
}

#[async_trait]
impl NodeRole for NodeRoleAdapter {
    /// Beat: refresh this node's heartbeat object.
    async fn action_beat(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness.lock().await;
        if !h.alive[&self.node_id] {
            return Ok(Value::None);
        }
        h.put_heartbeat(&self.node_id).await?;
        h.fresh.insert(self.node_id.clone(), true);
        Ok(Value::Str("beat".to_string()))
    }

    /// Recompute: the real ownership decision per database, returned
    /// for comparison with the model's; runners/fenced bookkeeping
    /// follows the spec's transition using the real decisions.
    async fn action_recompute(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness.lock().await;
        if !h.alive[&self.node_id] {
            return Ok(Value::None);
        }
        let decisions = h.real_decision(&self.node_id).await?;
        // Comma-joined owned databases in DBS order, the spec's return
        // encoding.
        let returned = DBS
            .iter()
            .map(|(db, _)| *db)
            .filter(|db| decisions[*db])
            .collect::<Vec<_>>()
            .join(",");
        for (db, mine) in &decisions {
            let in_runners = h.runners[db].contains(&self.node_id);
            let in_fenced = h.fenced[db].contains(&self.node_id);
            if *mine && !in_runners {
                let rivals = h.runners[db].clone();
                h.runners
                    .get_mut(db)
                    .expect("known db")
                    .push(self.node_id.clone());
                let fenced = h.fenced.get_mut(db).expect("known db");
                for m in rivals {
                    if !fenced.contains(&m) {
                        fenced.push(m);
                    }
                }
            } else if !mine && in_runners && !in_fenced {
                h.runners
                    .get_mut(db)
                    .expect("known db")
                    .retain(|m| m != &self.node_id);
            }
        }
        Ok(Value::Str(returned))
    }

    /// HandleFence: the fenced task's rerun-or-stand-down decision for
    /// one database, decided by the same real decision function.
    async fn action_handlefence(&self, args: &[Arg]) -> Result<Value, MbtError> {
        let db = str_arg(args, "db")?.to_string();
        let mut h = self.harness.lock().await;
        if !h.alive[&self.node_id] || !h.fenced[&db].contains(&self.node_id) {
            return Ok(Value::None);
        }
        h.fenced
            .get_mut(&db)
            .expect("known db")
            .retain(|m| m != &self.node_id);
        let rerun = h.real_decision(&self.node_id).await?[&db];
        if rerun {
            let rivals: Vec<String> = h.runners[&db]
                .iter()
                .filter(|m| *m != &self.node_id)
                .cloned()
                .collect();
            let fenced = h.fenced.get_mut(&db).expect("known db");
            for m in rivals {
                if !fenced.contains(&m) {
                    fenced.push(m);
                }
            }
        } else {
            h.runners
                .get_mut(&db)
                .expect("known db")
                .retain(|m| m != &self.node_id);
        }
        Ok(Value::Str(
            if rerun { "rerun" } else { "stand-down" }.to_string(),
        ))
    }

    /// Crash: tasks vanish, the heartbeat lingers until expiry, so the
    /// store is untouched.
    async fn action_crash(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness.lock().await;
        if !h.alive[&self.node_id] || h.crashes_left == 0 {
            return Ok(Value::None);
        }
        h.crashes_left -= 1;
        h.alive.insert(self.node_id.clone(), false);
        for db in h.runners.values_mut() {
            db.retain(|m| m != &self.node_id);
        }
        for db in h.fenced.values_mut() {
            db.retain(|m| m != &self.node_id);
        }
        Ok(Value::Str("crashed".to_string()))
    }

    /// Restart: the node acts again; its next Beat refreshes it.
    async fn action_restart(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness.lock().await;
        if h.alive[&self.node_id] || h.restarts_left == 0 {
            return Ok(Value::None);
        }
        h.restarts_left -= 1;
        h.alive.insert(self.node_id.clone(), true);
        Ok(Value::Str("restarted".to_string()))
    }
}

impl Role for NodeRoleAdapter {}

pub struct CoordinationModelAdapter {
    harness: Option<Arc<Mutex<Harness>>>,
    node_roles: Vec<Arc<NodeRoleAdapter>>,
}

#[async_trait]
impl CoordinationModel for CoordinationModelAdapter {
    type R0 = NodeRoleAdapter;
    fn get_node_roles(&self) -> Result<Vec<Arc<Self::R0>>, MbtError> {
        Ok(self.node_roles.clone())
    }

    /// ExpireCrashed: a crashed node's heartbeat reads as stale, via a
    /// real backdated `LastModified`.
    async fn action_expirecrashed(&self, args: &[Arg]) -> Result<Value, MbtError> {
        let node = str_arg(args, "n")?.to_string();
        let harness = self.harness.as_ref().expect("initialized");
        let mut h = harness.lock().await;
        if h.alive[&node] || !h.fresh[&node] {
            return Ok(Value::None);
        }
        h.backdate_heartbeat(&node);
        h.fresh.insert(node, false);
        Ok(Value::Str("expired".to_string()))
    }

    /// Suspect: transient false staleness of a live node, same
    /// backdating; the node itself must still count itself live.
    async fn action_suspect(&self, args: &[Arg]) -> Result<Value, MbtError> {
        let node = str_arg(args, "n")?.to_string();
        let harness = self.harness.as_ref().expect("initialized");
        let mut h = harness.lock().await;
        if h.suspects_left == 0 || !h.alive[&node] || !h.fresh[&node] {
            return Ok(Value::None);
        }
        h.suspects_left -= 1;
        h.backdate_heartbeat(&node);
        h.fresh.insert(node, false);
        Ok(Value::Str("suspected".to_string()))
    }
}

#[async_trait]
impl Model for CoordinationModelAdapter {
    async fn init(&mut self) -> Result<(), MbtError> {
        let harness = Arc::new(Mutex::new(Harness::new().await?));
        self.node_roles = NODES
            .iter()
            .map(|node| {
                Arc::new(NodeRoleAdapter {
                    node_id: node.to_string(),
                    harness: harness.clone(),
                })
            })
            .collect();
        self.harness = Some(harness);
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), MbtError> {
        self.node_roles = Vec::new();
        self.harness = None;
        Ok(())
    }
}

pub fn new_coordination_model() -> CoordinationModelAdapter {
    CoordinationModelAdapter {
        harness: None,
        node_roles: Vec::new(),
    }
}

/// Test volume: sequential only (one shared harness per run), enough
/// runs and depth to cover the interesting interleavings.
pub fn get_test_options() -> TestOptions {
    TestOptions {
        max_seq_runs: Some(500),
        max_parallel_runs: Some(0),
        max_actions: Some(15),
    }
}
