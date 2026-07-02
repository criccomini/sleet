//! Multi-node integration tests over one shared in-memory store: the
//! design's failover, partitioning, and propagation claims.

mod common;

use std::time::Duration;

use common::{Cluster, expected_pairs, poll_until};
use object_store::ObjectStoreExt;
use sleet::config::Service;
use sleet::{ops, placement};

/// The design's core failover claim: a node that dies without cleanup
/// is declared dead within `heartbeat_timeout`, its pairs move to the
/// survivor, and its heartbeat is housekept after 10x the timeout.
#[tokio::test(flavor = "multi_thread")]
async fn failover_on_unclean_death() {
    let mut cluster = Cluster::new().await;
    let db = "memory:///dbs/failover";
    cluster.register(db).await;
    cluster.spawn("n1", &[Service::Gc]);
    cluster.spawn("n2", &[Service::Gc]);

    let owner = placement::owners(db, Service::Gc, 1, &["n1", "n2"])[0].to_string();
    let survivor = if owner == "n1" { "n2" } else { "n1" };

    // Converged state: the ranked owner runs the pair and nobody else
    // does. (A brief startup double-run is legal while views converge,
    // so poll for the converged state rather than asserting instantly.)
    poll_until("owner runs the gc task, survivor idle", || async {
        (cluster.task_count(&owner, &[Service::Gc]).await == 1
            && cluster.task_count(survivor, &[Service::Gc]).await == 0)
            .then_some(())
    })
    .await;

    // Kill it with no cleanup; the survivor takes over once the stale
    // heartbeat ages past heartbeat_timeout (2s, from common::FAST).
    cluster.kill(&owner);
    let takeover = tokio::time::Instant::now();
    poll_until("survivor takes over", || async {
        (cluster.task_count(survivor, &[Service::Gc]).await == 1).then_some(())
    })
    .await;
    assert!(
        takeover.elapsed() < Duration::from_secs(10),
        "takeover should happen within a few heartbeat timeouts"
    );

    // Status agrees: placement moved to the survivor.
    let status = ops::status(&cluster.root, false).await.unwrap();
    assert_eq!(status.databases[0].services[0].nodes, vec![survivor]);

    // Housekeeping deletes the dead heartbeat after 10x the timeout.
    poll_until("dead heartbeat housekept", || async {
        let status = ops::status(&cluster.root, false).await.unwrap();
        (!status.nodes.iter().any(|n| n.node_id == owner)).then_some(())
    })
    .await;

    cluster.shutdown().await;
}

/// Placement partitions: every pair runs on exactly its ranked owner,
/// nowhere else, and every node's reported task count matches the
/// ranking's assignment count.
#[tokio::test(flavor = "multi_thread")]
async fn placement_partitions_across_nodes() {
    let mut cluster = Cluster::new().await;
    let ids = ["n1", "n2", "n3"];
    let dbs: Vec<String> = (0..9).map(|i| format!("memory:///dbs/part{i}")).collect();
    for db in &dbs {
        cluster.register(db).await;
    }
    for id in ids {
        cluster.spawn(id, &Service::ALL);
    }

    for id in ids {
        let want = expected_pairs(id, &ids, &dbs);
        poll_until("node runs exactly its share", || async {
            (cluster.task_count(id, &Service::ALL).await == want).then_some(())
        })
        .await;
    }

    // Status placement equals the ranking for every pair, one owner each.
    let status = ops::status(&cluster.root, false).await.unwrap();
    assert_eq!(status.databases.len(), dbs.len());
    for db in &status.databases {
        for entry in &db.services {
            let want = placement::owners(&db.url, entry.service, 1, &ids);
            assert_eq!(entry.nodes, want, "{} {:?}", db.url, entry.service);
        }
    }

    cluster.shutdown().await;
}

/// Registry edits propagate within one config_poll: services change,
/// disable, and unregister all reconcile running tasks.
#[tokio::test(flavor = "multi_thread")]
async fn config_changes_propagate() {
    let mut cluster = Cluster::new().await;
    let db = "memory:///dbs/prop";
    cluster.register(db).await;
    let file = cluster.db_file(db);
    cluster
        .root
        .store()
        .put(&file, "services = [\"gc\"]".into())
        .await
        .unwrap();
    cluster.spawn("n1", &Service::ALL);

    poll_until("gc task only", || async {
        let body = cluster.body("n1", &Service::ALL).await?;
        let gc = body.services.iter().find(|s| s.service == Service::Gc)?;
        let total: u64 = body.services.iter().map(|s| s.running + s.backoff).sum();
        (gc.running + gc.backoff == 1 && total == 1).then_some(())
    })
    .await;

    // Add the coordinator: one more task within a poll.
    cluster
        .root
        .store()
        .put(
            &file,
            "services = [\"gc\", \"compactor-coordinator\"]".into(),
        )
        .await
        .unwrap();
    poll_until("coordinator task added", || async {
        (cluster.task_count("n1", &Service::ALL).await == 2).then_some(())
    })
    .await;

    // Disable: tasks stop, database stays registered.
    cluster
        .root
        .store()
        .put(&file, "services = []".into())
        .await
        .unwrap();
    poll_until("all tasks stopped", || async {
        (cluster.task_count("n1", &Service::ALL).await == 0).then_some(())
    })
    .await;
    let status = ops::status(&cluster.root, false).await.unwrap();
    assert_eq!(status.databases.len(), 1);
    assert!(status.databases[0].services.is_empty());

    // Delete: unregistered entirely.
    cluster.root.store().delete(&file).await.unwrap();
    poll_until("database unregistered", || async {
        let status = ops::status(&cluster.root, false).await.unwrap();
        status.databases.is_empty().then_some(())
    })
    .await;

    cluster.shutdown().await;
}

/// Roles split placement: single-slot services land on the one node
/// offering them, and `count = 2` workers span both worker nodes.
#[tokio::test(flavor = "multi_thread")]
async fn roles_split_and_worker_count_spans_nodes() {
    let mut cluster = Cluster::new().await;
    let db = "memory:///dbs/roles";
    cluster.register(db).await;
    cluster
        .root
        .store()
        .put(
            &cluster.db_file(db),
            "[compaction-workers]\ncount = 2".into(),
        )
        .await
        .unwrap();

    let control = [Service::Gc, Service::CompactorCoordinator];
    let workers = [Service::CompactionWorkers];
    cluster.spawn("small", &control);
    cluster.spawn("big1", &workers);
    cluster.spawn("big2", &workers);

    poll_until("control tasks on the small node", || async {
        (cluster.task_count("small", &control).await == 2).then_some(())
    })
    .await;
    for big in ["big1", "big2"] {
        poll_until("worker task on each big node", || async {
            (cluster.task_count(big, &workers).await == 1).then_some(())
        })
        .await;
    }

    let status = ops::status(&cluster.root, false).await.unwrap();
    for entry in &status.databases[0].services {
        match entry.service {
            Service::CompactionWorkers => {
                let mut got = entry.nodes.clone();
                got.sort();
                assert_eq!(got, vec!["big1", "big2"]);
            }
            _ => assert_eq!(entry.nodes, vec!["small"]),
        }
    }

    cluster.shutdown().await;
}

/// A role change renames the heartbeat: the node deletes its old name
/// at startup and the fleet converges on the new offering.
#[tokio::test(flavor = "multi_thread")]
async fn role_change_replaces_the_heartbeat_name() {
    let mut cluster = Cluster::new().await;
    cluster.spawn("n1", &Service::ALL);
    poll_until("all-services heartbeat exists", || async {
        cluster.body("n1", &Service::ALL).await.map(|_| ())
    })
    .await;

    // Unclean restart with a narrower role.
    cluster.kill("n1");
    cluster.spawn("n1", &[Service::Gc]);

    poll_until("old name deleted, new name live", || async {
        let old = cluster.body("n1", &Service::ALL).await;
        let new = cluster.body("n1", &[Service::Gc]).await;
        (old.is_none() && new.is_some()).then_some(())
    })
    .await;
    let status = ops::status(&cluster.root, false).await.unwrap();
    let node = status.nodes.iter().find(|n| n.node_id == "n1").unwrap();
    assert_eq!(node.services, vec![Service::Gc]);

    cluster.shutdown().await;
}

/// Clean shutdown of one node hands its pairs to the rest immediately
/// (no heartbeat_timeout wait), per the deleted-heartbeat rule.
#[tokio::test(flavor = "multi_thread")]
async fn clean_shutdown_hands_off_immediately() {
    let mut cluster = Cluster::new().await;
    let db = "memory:///dbs/handoff";
    cluster.register(db).await;
    cluster.spawn("n1", &[Service::Gc]);
    cluster.spawn("n2", &[Service::Gc]);

    let owner = placement::owners(db, Service::Gc, 1, &["n1", "n2"])[0].to_string();
    let survivor = if owner == "n1" { "n2" } else { "n1" };
    poll_until("owner runs the pair", || async {
        (cluster.task_count(&owner, &[Service::Gc]).await == 1).then_some(())
    })
    .await;

    cluster.stop(&owner).await;
    // The heartbeat is gone the moment stop returns.
    assert!(cluster.body(&owner, &[Service::Gc]).await.is_none());
    poll_until("survivor picks the pair up", || async {
        (cluster.task_count(survivor, &[Service::Gc]).await == 1).then_some(())
    })
    .await;

    cluster.shutdown().await;
}
