//! Deterministic simulation tests: virtual time end to end.
//!
//! Everything runs in a paused-time, single-threaded tokio runtime.
//! The daemons' sleeps advance virtual time; heartbeat `LastModified`
//! comes from a `TestClock` the driver advances in lockstep; the store
//! is in-memory. A seeded schedule of crashes, restarts, and registry
//! churn drives the fleet, and after quiescence the invariants must
//! hold: every pair owned by exactly its ranked node, nothing crashed,
//! and the outcome reproduces from the seed. Separate cadence tests pin
//! the daemon's timing (heartbeat interval, config poll, failover
//! latency) against exact virtual time.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use object_store::ObjectStoreExt;
use object_store::path::Path as StorePath;
use sleet::config::Service;
use sleet::daemon::{self, NodeOptions};
use sleet::heartbeat::{self, Heartbeat};
use sleet::root::FleetRoot;
use sleet::testing::{Op, TestClock, TestStore};
use sleet::{ops, placement};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const FAST: &str = "[node]\nheartbeat_interval = \"200ms\"\n\
                    heartbeat_timeout = \"1s\"\nconfig_poll = \"400ms\"\n";

/// One simulated fleet under virtual time.
struct Sim {
    clock: Arc<TestClock>,
    store: Arc<TestStore>,
    root: FleetRoot,
    nodes: BTreeMap<String, (CancellationToken, JoinHandle<()>)>,
    rng: u64,
}

impl Sim {
    async fn new(seed: u64) -> Self {
        let clock = TestClock::new(Utc::now());
        let store = TestStore::in_memory_at(clock.clone());
        let root = FleetRoot::from_parts(store.clone(), StorePath::from("fleet"), "memory:///f")
            .with_clock(clock.clone());
        store.put(&root.config_path(), FAST.into()).await.unwrap();
        Self {
            clock,
            store,
            root,
            nodes: BTreeMap::new(),
            rng: seed.max(1),
        }
    }

    fn roll(&mut self, bound: u64) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng % bound
    }

    fn spawn(&mut self, node_id: &str) {
        let token = CancellationToken::new();
        let handle = tokio::spawn({
            let root = self.root.clone();
            let options = NodeOptions {
                node_id: node_id.into(),
                services: vec![Service::Gc],
                max_compaction_jobs: 1,
                ..NodeOptions::default()
            };
            let token = token.clone();
            async move {
                daemon::run(root, options, token).await.unwrap();
            }
        });
        self.nodes.insert(node_id.into(), (token, handle));
    }

    fn crash(&mut self, node_id: &str) {
        if let Some((_, handle)) = self.nodes.remove(node_id) {
            handle.abort();
        }
    }

    /// Advance virtual time: tokio timers and the heartbeat clock move
    /// together.
    async fn tick(&self, by: Duration) {
        self.clock.advance(by);
        tokio::time::sleep(by).await;
    }

    async fn task_count(&self, node_id: &str) -> u64 {
        let path = self
            .root
            .node_path(&heartbeat::object_name(node_id, &[Service::Gc]));
        let Ok(get) = self.root.store().get(&path).await else {
            return 0;
        };
        let Ok(bytes) = get.bytes().await else {
            return 0;
        };
        serde_json::from_slice::<Heartbeat>(&bytes)
            .map(|b| b.services.iter().map(|s| s.running + s.backoff).sum())
            .unwrap_or(0)
    }

    async fn shutdown(mut self) {
        for (token, _) in self.nodes.values() {
            token.cancel();
        }
        for (_, (_, handle)) in std::mem::take(&mut self.nodes) {
            let _ = handle.await;
        }
    }
}

/// Run one seeded schedule of crashes, restarts, and registrations,
/// then quiesce and return the final ownership map.
async fn run_schedule(seed: u64) -> BTreeMap<String, u64> {
    let mut sim = Sim::new(seed).await;
    let all_ids = ["d1", "d2", "d3", "d4"];
    for id in &all_ids[..3] {
        sim.spawn(id);
    }
    let mut dbs: Vec<String> = (0..5).map(|i| format!("memory:///dbs/sim{i}")).collect();
    for db in &dbs {
        ops::register(&sim.root, db).await.unwrap();
    }

    // 60 steps x 100ms = 6s of virtual churn.
    for step in 0..60u64 {
        sim.tick(Duration::from_millis(100)).await;
        if step % 5 != 0 {
            continue;
        }
        match sim.roll(6) {
            0 => {
                // Crash a random running node (keep at least one).
                if sim.nodes.len() > 1 {
                    let victims: Vec<String> = sim.nodes.keys().cloned().collect();
                    let victim = victims[sim.roll(victims.len() as u64) as usize].clone();
                    sim.crash(&victim);
                }
            }
            1 => {
                // (Re)start a node that isn't running.
                let stopped: Vec<&str> = all_ids
                    .iter()
                    .copied()
                    .filter(|id| !sim.nodes.contains_key(*id))
                    .collect();
                if !stopped.is_empty() {
                    let id = stopped[sim.roll(stopped.len() as u64) as usize];
                    sim.spawn(id);
                }
            }
            2 => {
                // Register another database.
                let db = format!("memory:///dbs/sim{}", dbs.len());
                ops::register(&sim.root, &db).await.unwrap();
                dbs.push(db);
            }
            _ => {}
        }
    }

    // Quiesce: no more faults or churn; several timeouts of calm.
    for _ in 0..50 {
        sim.tick(Duration::from_millis(100)).await;
    }

    // Invariants: nothing died on its own, and every database's gc pair
    // is owned by exactly its ranked node among the live set.
    let live: Vec<String> = sim.nodes.keys().cloned().collect();
    let live_refs: Vec<&str> = live.iter().map(String::as_str).collect();
    assert!(
        sim.nodes.values().all(|(_, handle)| !handle.is_finished()),
        "seed {seed}: a daemon died"
    );
    let mut counts = BTreeMap::new();
    for id in &live {
        let want: u64 = dbs
            .iter()
            .filter(|db| placement::owners(db, Service::Gc, 1, &live_refs)[0] == id)
            .count() as u64;
        let got = sim.task_count(id).await;
        assert_eq!(got, want, "seed {seed}: node {id} owns {got}, want {want}");
        counts.insert(id.clone(), got);
    }
    let total: u64 = counts.values().sum();
    assert_eq!(
        total,
        dbs.len() as u64,
        "seed {seed}: every pair owned once"
    );

    sim.shutdown().await;
    counts
}

/// Seeded churn converges to exact ranked ownership, for several seeds,
/// and the outcome reproduces from the seed.
#[tokio::test(start_paused = true)]
async fn seeded_churn_converges_and_reproduces() {
    for seed in [7, 1234, 987654321] {
        let first = run_schedule(seed).await;
        let second = run_schedule(seed).await;
        assert_eq!(first, second, "seed {seed} must reproduce");
    }
}

/// Heartbeat and config-poll cadences, pinned in virtual time: over 10
/// virtual seconds a 200ms heartbeat means ~50 PUTs, and a 400ms
/// config_poll means ~25 config GETs.
#[tokio::test(start_paused = true)]
async fn cadences_follow_virtual_time() {
    let mut sim = Sim::new(1).await;
    sim.spawn("d1");
    // Setup writes one config PUT before the daemon starts.
    let baseline_puts = sim.store.counters().count(Op::Put);
    for _ in 0..100 {
        sim.tick(Duration::from_millis(100)).await;
    }
    let puts = sim.store.counters().count(Op::Put) - baseline_puts;
    assert!(
        (45..=55).contains(&puts),
        "expected ~50 heartbeat PUTs in 10s at 200ms, got {puts}"
    );
    let gets = sim.store.counters().count(Op::Get);
    assert!(
        (20..=30).contains(&gets),
        "expected ~25 config GETs in 10s at 400ms, got {gets}"
    );
    sim.shutdown().await;
}

/// Failover latency, measured in virtual time: after an unclean crash
/// the survivor owns the pair within heartbeat_timeout plus a couple of
/// intervals, the design's stated bound.
#[tokio::test(start_paused = true)]
async fn failover_latency_is_bounded_in_virtual_time() {
    let mut sim = Sim::new(2).await;
    let db = "memory:///dbs/latency";
    ops::register(&sim.root, db).await.unwrap();
    sim.spawn("d1");
    sim.spawn("d2");

    let owner = placement::owners(db, Service::Gc, 1, &["d1", "d2"])[0].to_string();
    let survivor = if owner == "d1" { "d2" } else { "d1" };
    // Converge first.
    for _ in 0..30 {
        sim.tick(Duration::from_millis(100)).await;
        if sim.task_count(&owner).await == 1 && sim.task_count(survivor).await == 0 {
            break;
        }
    }
    assert_eq!(sim.task_count(&owner).await, 1);

    sim.crash(&owner);
    let mut elapsed = Duration::ZERO;
    while sim.task_count(survivor).await != 1 {
        sim.tick(Duration::from_millis(100)).await;
        elapsed += Duration::from_millis(100);
        assert!(
            elapsed < Duration::from_secs(10),
            "survivor never took over"
        );
    }
    // heartbeat_timeout (1s) + a couple of 200ms ticks + the heartbeat
    // the survivor must publish. Generous but still a real bound.
    assert!(
        elapsed <= Duration::from_secs(2),
        "failover took {elapsed:?}, bound is ~heartbeat_timeout + ticks"
    );
    sim.shutdown().await;
}

/// A continuous mirror over a source with no manifest yet polls with
/// idle backoff: over 1000 virtual seconds of a 1s poll, wakeups
/// follow 1, 2, 4, ... capped at 300s, so only a handful of source
/// LISTs instead of ~1000.
#[tokio::test(start_paused = true)]
async fn mirror_polling_backs_off_while_idle() {
    use sleet::config::ResolvedMirrorTarget;
    use sleet::mirror::{self, AppliedTarget};
    use sleet::services::DatabaseHandle;

    let source_store = TestStore::in_memory();
    let dest_store = TestStore::in_memory();
    let target = AppliedTarget {
        name: "dr".into(),
        destination: "memory:///dst".into(),
        settings: ResolvedMirrorTarget {
            poll: Duration::from_secs(1),
            ..ResolvedMirrorTarget::default()
        },
    };
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let source_store = source_store.clone();
        let dest_store = dest_store.clone();
        let target = target.clone();
        let token = token.clone();
        async move {
            let source =
                DatabaseHandle::from_parts("memory:///src", source_store, StorePath::from("src"));
            let dest =
                DatabaseHandle::from_parts("memory:///dst", dest_store, StorePath::from("dst"));
            let jobs = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
            mirror::run_mirror(&source, &dest, &target, jobs, None, token).await
        }
    });

    tokio::time::sleep(Duration::from_secs(1000)).await;
    token.cancel();
    task.await.unwrap().unwrap();

    let lists = source_store.counters().count(Op::List);
    assert!(lists >= 5, "mirror never polled: {lists} LISTs");
    assert!(
        lists <= 30,
        "mirror polled without backing off: {lists} LISTs in 1000s"
    );
    // An idle mirror touches the destination not at all.
    assert_eq!(dest_store.counters().count(Op::Put), 0);
}
