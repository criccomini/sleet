//! Chaos tests: multi-node fleets under injected store faults, an
//! asymmetric partition, and reader clock skew. Every run asserts the
//! design's invariants (no panics, ownership converges after faults
//! stop, and duplication is the worst case) rather than specific
//! interleavings.

mod common;

use std::time::Duration;

use common::{Cluster, expected_pairs, poll_until};
use sleet::config::Service;
use sleet::testing::{Op, TestClock, TestStore};
use sleet::{ops, placement};

/// A fleet running under a 20% fault rate on every store operation
/// keeps going, and once the faults stop it converges to exactly the
/// ranked placement.
#[tokio::test(flavor = "multi_thread")]
async fn faulted_fleet_converges_after_healing() {
    let mut cluster = Cluster::new().await;
    let ids = ["n1", "n2", "n3"];
    let dbs: Vec<String> = (0..6).map(|i| format!("memory:///dbs/chaos{i}")).collect();
    for db in &dbs {
        cluster.register(db).await;
    }
    for id in ids {
        cluster.spawn(id, &Service::ALL);
    }
    // Wait for every node's first heartbeat before injecting faults: a
    // fault on a node's *initial* config read would leave it on
    // built-in defaults (10s heartbeat), which isn't the scenario;
    // the design assumes a node reads its config once before faults.
    for id in ids {
        poll_until("node heartbeats before faults", || async {
            cluster.body(id, &Service::ALL).await.map(|_| ())
        })
        .await;
    }

    // Fault every operation type nodes use, deterministically seeded.
    cluster.store.fail_probability(0.2, 42);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!cluster.any_node_died(), "faults must never kill a node");
    cluster.store.heal();

    for id in ids {
        let want = expected_pairs(id, &ids, &dbs);
        poll_until("post-heal convergence", || async {
            (cluster.task_count(id, &Service::ALL).await == want).then_some(())
        })
        .await;
    }
    assert!(!cluster.any_node_died());
    cluster.shutdown().await;
}

/// An asymmetric partition: one node loses access to the fleet root
/// while its peers stay healthy. The fleet declares it dead and takes
/// over (a double-run at worst, which is safe); when the partition
/// heals the node rejoins and placement converges back.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_node_is_taken_over_and_rejoins() {
    let mut cluster = Cluster::new().await;
    let dbs: Vec<String> = (0..4).map(|i| format!("memory:///dbs/part{i}")).collect();
    for db in &dbs {
        cluster.register(db).await;
    }
    let (a_store, a_root) = cluster.node_root();
    cluster.spawn_on("a", &[Service::Gc], a_root);
    cluster.spawn("b", &[Service::Gc]);

    let ids = ["a", "b"];
    for id in ids {
        let want: u64 = dbs
            .iter()
            .filter(|db| placement::owners(db, Service::Gc, 1, &ids)[0] == id)
            .count() as u64;
        poll_until("initial convergence", || async {
            (cluster.task_count(id, &[Service::Gc]).await == want).then_some(())
        })
        .await;
    }

    // Partition `a` from the fleet root only. Its heartbeats stop, so
    // `b` must take over every database within heartbeat_timeout.
    for op in [Op::Get, Op::Put, Op::List, Op::Delete] {
        a_store.fail_all(op);
    }
    poll_until("survivor owns everything", || async {
        (cluster.task_count("b", &[Service::Gc]).await == dbs.len() as u64).then_some(())
    })
    .await;
    assert!(!cluster.any_node_died(), "a partitioned node keeps running");

    // Heal: `a` re-heartbeats, takes back its share, `b` stands down.
    a_store.heal();
    for id in ids {
        let want: u64 = dbs
            .iter()
            .filter(|db| placement::owners(db, Service::Gc, 1, &ids)[0] == id)
            .count() as u64;
        poll_until("post-heal convergence", || async {
            (cluster.task_count(id, &[Service::Gc]).await == want).then_some(())
        })
        .await;
    }
    cluster.shutdown().await;
}

/// A reader whose clock is skewed far past `heartbeat_timeout` declares
/// every peer dead and takes over the whole fleet. That is the design's
/// stated worst case, a double-run, and it must be stable and safe:
/// the unskewed node keeps its ranked share, the skewed node runs
/// everything, and nobody crashes.
#[tokio::test(flavor = "multi_thread")]
async fn skewed_reader_takes_over_everything_safely() {
    let mut cluster = Cluster::new().await;
    let dbs: Vec<String> = (0..4).map(|i| format!("memory:///dbs/skew{i}")).collect();
    for db in &dbs {
        cluster.register(db).await;
    }

    // `fast` reads heartbeat ages through a clock 30s in the future:
    // every peer looks long dead.
    let clock = TestClock::new(chrono::Utc::now() + chrono::Duration::seconds(30));
    let (_, fast_root) = cluster.node_root();
    let fast_root = fast_root.with_clock(clock);
    cluster.spawn_on("fast", &[Service::Gc], fast_root);
    cluster.spawn("sane", &[Service::Gc]);

    let ids = ["fast", "sane"];
    // The skewed node owns everything (its view: it is alone). The sane
    // node sees both live and keeps exactly its ranked share.
    let sane_share: u64 = dbs
        .iter()
        .filter(|db| placement::owners(db, Service::Gc, 1, &ids)[0] == "sane")
        .count() as u64;
    poll_until("skewed node runs everything", || async {
        (cluster.task_count("fast", &[Service::Gc]).await == dbs.len() as u64).then_some(())
    })
    .await;
    poll_until("sane node keeps its share", || async {
        (cluster.task_count("sane", &[Service::Gc]).await == sane_share).then_some(())
    })
    .await;

    // The overlap is stable, not a crash loop: hold, then require the
    // same steady state again (poll-based: parallel test load can
    // starve heartbeats transiently, which is itself a legal skew).
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!cluster.any_node_died());
    poll_until("overlap steady after hold", || async {
        (cluster.task_count("fast", &[Service::Gc]).await == dbs.len() as u64
            && cluster.task_count("sane", &[Service::Gc]).await == sane_share)
            .then_some(())
    })
    .await;

    // The fleet stays observable throughout.
    let status = ops::status(&cluster.root, false).await.unwrap();
    assert_eq!(status.databases.len(), dbs.len());

    let _ = TestStore::in_memory; // keep the import used on all cfgs
    cluster.shutdown().await;
}
