//! End-to-end fleet tests over local object stores: register a real
//! SlateDB database, run the daemon, and observe it compact.

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use sleet::config::Service;
use sleet::daemon::{self, NodeOptions};
use sleet::root::FleetRoot;
use sleet::services::DatabaseHandle;
use sleet::{ops, registry};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn register_and_status_roundtrip() {
    let root = FleetRoot::from_parts(
        Arc::new(InMemory::new()),
        StorePath::from("fleet"),
        "memory:///fleet",
    );

    let first = ops::register(&root, "s3://Bucket/db/").await.unwrap();
    assert!(first.created);
    assert_eq!(first.url, "s3://bucket/db");
    assert_eq!(
        first.file,
        format!("dbs/{}", registry::file_name("s3://bucket/db"))
    );

    let again = ops::register(&root, "s3://bucket/db").await.unwrap();
    assert!(!again.created, "create-only PUT never overwrites");

    let status = ops::status(&root, false).await.unwrap();
    assert!(status.nodes.is_empty());
    assert_eq!(status.databases.len(), 1);
    assert_eq!(status.databases[0].url, "s3://bucket/db");
    // Nothing offers any service: placement is empty and reported.
    assert!(
        status.databases[0]
            .services
            .iter()
            .all(|s| s.nodes.is_empty())
    );
    assert!(
        status
            .warnings
            .iter()
            .any(|w| w.contains("no live node offers")),
        "{:?}",
        status.warnings
    );
}

/// The full loop against a real SlateDB database: write enough to leave
/// several L0 SSTs (embedded compactor disabled), register the
/// database, run a sleet node, and wait for sleet's coordinator and
/// worker to compact it.
#[tokio::test(flavor = "multi_thread")]
async fn daemon_compacts_a_real_database() {
    let dir = tempfile::tempdir().unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    let db_url = format!("file://{}/db1", dir.path().display());
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    std::fs::create_dir_all(dir.path().join("db1")).unwrap();

    // Write four L0 SSTs with the embedded compactor and GC disabled:
    // background maintenance belongs to sleet.
    {
        let (store, path) = object_store::parse_url(&url::Url::parse(&db_url).unwrap()).unwrap();
        let settings = slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        };
        let db = slatedb::Db::builder(path, Arc::from(store))
            .with_settings(settings)
            .build()
            .await
            .unwrap();
        for sst in 0..4 {
            for key in 0..64 {
                db.put(
                    format!("key-{sst}-{key}").as_bytes(),
                    vec![sst as u8; 1024].as_slice(),
                )
                .await
                .unwrap();
            }
            // A plain flush() only flushes the WAL; force memtable
            // flushes so each round leaves an L0 SST to compact.
            db.flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();
        }
        db.close().await.unwrap();
    }

    // Precondition: the writes above left L0 SSTs behind.
    {
        let db = DatabaseHandle::open(&db_url).unwrap();
        let manifest = db.admin.read_manifest(None).await.unwrap().unwrap();
        assert!(
            manifest.l0().len() >= 2,
            "expected L0 SSTs to compact, found {}",
            manifest.l0().len()
        );
    }

    let root = FleetRoot::open(&fleet_url).unwrap();
    // Fast intervals so the test converges quickly.
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"500ms\"\nheartbeat_timeout = \"2s\"\nconfig_poll = \"1s\"\n".into(),
        )
        .await
        .unwrap();
    ops::register(&root, &db_url).await.unwrap();
    // Per-database overrides: compact eagerly, poll fast.
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&db_url).unwrap()),
            "[compactor-coordinator]\npoll_interval = \"500ms\"\n\
             [compactor-coordinator.scheduler]\nmin_compaction_sources = 2\n\
             [compaction-workers]\ncompactions_poll_interval = \"250ms\"\n"
                .into(),
        )
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let node = tokio::spawn(daemon::run(
        root.clone(),
        NodeOptions {
            node_id: "n1".into(),
            services: Service::ALL.to_vec(),
            max_compaction_jobs: 2,
        },
        shutdown.clone(),
    ));

    // The node appears in status and owns every service.
    let status = poll_until("node is live in status", || async {
        let status = ops::status(&root, false).await.unwrap();
        status
            .nodes
            .iter()
            .any(|n| n.node_id == "n1" && n.live)
            .then_some(status)
    })
    .await;
    let db_status = &status.databases[0];
    for placement in &db_status.services {
        assert_eq!(placement.nodes, vec!["n1".to_string()], "{:?}", placement);
    }

    // sleet's coordinator schedules and its worker executes: the
    // database ends up with a compacted sorted run in the manifest.
    poll_until("database is compacted", || async {
        let db = DatabaseHandle::open(&db_url).unwrap();
        let manifest = db.admin.read_manifest(None).await.ok().flatten()?;
        (!manifest.compacted().is_empty()).then_some(())
    })
    .await;

    // Queue depth is readable through status --queues.
    let status = ops::status(&root, true).await.unwrap();
    assert!(status.databases[0].queue.is_some());

    // Clean shutdown deletes the heartbeat, handing assignments off.
    shutdown.cancel();
    node.await.unwrap().unwrap();
    let status = ops::status(&root, false).await.unwrap();
    assert!(status.nodes.is_empty(), "{:?}", status.nodes);
}

/// Poll an async condition every 250ms for up to 60s.
async fn poll_until<T, F, Fut>(what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Create a real SlateDB database at `file://<dir>/<name>` with several
/// L0 SSTs and no embedded compactor or GC: background maintenance
/// belongs to sleet.
async fn seed_database(dir: &std::path::Path, name: &str) -> String {
    let db_url = format!("file://{}/{name}", dir.display());
    std::fs::create_dir_all(dir.join(name)).unwrap();
    let (store, path) = object_store::parse_url(&url::Url::parse(&db_url).unwrap()).unwrap();
    let settings = slatedb::config::Settings {
        compactor_options: None,
        garbage_collector_options: None,
        ..Default::default()
    };
    let db = slatedb::Db::builder(path, Arc::from(store))
        .with_settings(settings)
        .build()
        .await
        .unwrap();
    for sst in 0..4u8 {
        for key in 0..64 {
            db.put(
                format!("key-{sst}-{key}").as_bytes(),
                vec![sst; 1024].as_slice(),
            )
            .await
            .unwrap();
        }
        // A plain flush() only flushes the WAL; force memtable flushes
        // so each round leaves an L0 SST to compact.
        db.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    db.close().await.unwrap();
    db_url
}

/// The design's GC claim, observed: after sleet compacts, the
/// superseded L0 SSTs become garbage and sleet's GC actually removes
/// them from the store once they age past `min_age`.
#[tokio::test(flavor = "multi_thread")]
async fn gc_deletes_superseded_ssts() {
    let dir = tempfile::tempdir().unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    let db_url = seed_database(dir.path(), "db1").await;

    let list_ssts = || {
        let mut names = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(dir.path().join("db1/compacted")).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            if name.ends_with(".sst") {
                names.insert(name);
            }
        }
        names
    };
    let seeded = list_ssts();
    assert!(seeded.len() >= 4, "seed left {} SSTs", seeded.len());

    let root = FleetRoot::open(&fleet_url).unwrap();
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"500ms\"\nheartbeat_timeout = \"2s\"\nconfig_poll = \"1s\"\n".into(),
        )
        .await
        .unwrap();
    ops::register(&root, &db_url).await.unwrap();
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&db_url).unwrap()),
            "[gc.compacted]\ninterval = \"500ms\"\nmin_age = \"1s\"\n\
             [gc.manifest]\ninterval = \"500ms\"\nmin_age = \"1s\"\n\
             [compactor-coordinator]\npoll_interval = \"500ms\"\n\
             [compactor-coordinator.scheduler]\nmin_compaction_sources = 2\n\
             [compaction-workers]\ncompactions_poll_interval = \"250ms\"\n"
                .into(),
        )
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let node = tokio::spawn(daemon::run(
        root.clone(),
        NodeOptions {
            node_id: "n1".into(),
            services: Service::ALL.to_vec(),
            max_compaction_jobs: 2,
        },
        shutdown.clone(),
    ));

    // Compaction first, then GC removes the superseded inputs.
    poll_until("compacted sorted run exists", || async {
        let db = DatabaseHandle::open(&db_url).unwrap();
        let manifest = db.admin.read_manifest(None).await.ok().flatten()?;
        (!manifest.compacted().is_empty()).then_some(())
    })
    .await;

    // SlateDB's commit protocol checkpoints the pre-commit manifest for
    // 15 minutes, pinning the superseded L0s, and GC honors the pin.
    // Release it so deletion is observable within the test; in
    // production GC just lags a compaction by that checkpoint.
    {
        let db = DatabaseHandle::open(&db_url).unwrap();
        for checkpoint in db.admin.list_checkpoints(None).await.unwrap() {
            db.admin.delete_checkpoint(checkpoint.id).await.unwrap();
        }
    }

    // Compaction adds output SSTs, so total counts can't prove
    // deletion; watch the seeded input SSTs disappear. (The newest
    // seeded L0 is protected by the newest-L0 cutoff, so expect the
    // older ones to go.)
    poll_until("superseded SSTs deleted", || async {
        let remaining = list_ssts().intersection(&seeded).count();
        (remaining < seeded.len()).then_some(())
    })
    .await;

    shutdown.cancel();
    node.await.unwrap().unwrap();
}

/// Two coordinators on one database self-resolve: the newer fences the
/// older, `compactor_epoch` only advances, and the survivor keeps
/// running. This is the safety half of the fenced-coordinators design.
#[tokio::test(flavor = "multi_thread")]
async fn coordinator_duel_self_resolves() {
    use sleet::config::ResolvedServices;
    use sleet::services::run_coordinator;

    let dir = tempfile::tempdir().unwrap();
    let db_url = seed_database(dir.path(), "db1").await;
    let mut resolved = ResolvedServices::default().coordinator;
    resolved.poll_interval = Duration::from_millis(250);
    resolved.scheduler.min_compaction_sources = 2;

    let token_a = CancellationToken::new();
    let a = tokio::spawn({
        let db_url = db_url.clone();
        let resolved = resolved;
        let token = token_a.clone();
        async move {
            let db = DatabaseHandle::open(&db_url).unwrap();
            run_coordinator(&db, &resolved, token).await
        }
    });

    async fn epoch(db_url: &str) -> u64 {
        let db = DatabaseHandle::open(db_url).unwrap();
        let m = db.admin.read_manifest(None).await.unwrap().unwrap();
        m.compactor_epoch()
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let epoch_a = epoch(&db_url).await;

    let token_b = CancellationToken::new();
    let b = tokio::spawn({
        let db_url = db_url.clone();
        let resolved = resolved;
        let token = token_b.clone();
        async move {
            let db = DatabaseHandle::open(&db_url).unwrap();
            run_coordinator(&db, &resolved, token).await
        }
    });

    // A is fenced; the exchange is bounded and B survives.
    let a_result = tokio::time::timeout(Duration::from_secs(30), a)
        .await
        .expect("fenced coordinator exits")
        .unwrap();
    assert!(
        a_result.as_ref().is_err_and(|e| e.is_fenced()),
        "{a_result:?}"
    );
    assert!(!b.is_finished(), "the fencing coordinator keeps running");

    // The epoch advanced monotonically, once per coordinator start.
    let epoch_b = epoch(&db_url).await;
    assert!(
        epoch_b > epoch_a,
        "epoch must advance: {epoch_a} -> {epoch_b}"
    );

    token_b.cancel();
    b.await.unwrap().unwrap();
}

/// The node-wide jobs semaphore gates workers: with no permit a worker
/// never claims scheduled jobs, and after the queue drains the permit
/// is released (the drained-stop path).
#[tokio::test(flavor = "multi_thread")]
async fn worker_semaphore_gates_and_releases() {
    use sleet::config::ResolvedServices;
    use sleet::services::{queue_depth, run_coordinator, run_workers};
    use tokio::sync::Semaphore;

    let dir = tempfile::tempdir().unwrap();
    let db_url = seed_database(dir.path(), "db1").await;

    // Schedule compactions, then stop the coordinator so jobs stay
    // claimable.
    let mut coordinator = ResolvedServices::default().coordinator;
    coordinator.poll_interval = Duration::from_millis(250);
    coordinator.scheduler.min_compaction_sources = 2;
    let token = CancellationToken::new();
    let c = tokio::spawn({
        let db_url = db_url.clone();
        let token = token.clone();
        async move {
            let db = DatabaseHandle::open(&db_url).unwrap();
            run_coordinator(&db, &coordinator, token).await
        }
    });
    poll_until("jobs scheduled", || async {
        let db = DatabaseHandle::open(&db_url).unwrap();
        let depth = queue_depth(&db.admin).await.ok()?;
        (depth.claimable > 0).then_some(())
    })
    .await;
    token.cancel();
    c.await.unwrap().unwrap();

    // No permits: the worker sees the work but cannot claim it.
    let jobs = Arc::new(Semaphore::new(0));
    let mut workers = ResolvedServices::default().workers;
    workers.compactions_poll_interval = Duration::from_millis(250);
    let worker_token = CancellationToken::new();
    let w = tokio::spawn({
        let db_url = db_url.clone();
        let jobs = jobs.clone();
        let token = worker_token.clone();
        async move {
            let db = DatabaseHandle::open(&db_url).unwrap();
            run_workers(&db, &workers, jobs, token).await
        }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    {
        let db = DatabaseHandle::open(&db_url).unwrap();
        let depth = queue_depth(&db.admin).await.unwrap();
        assert!(
            depth.claimable > 0,
            "worker must not claim without a permit: {depth:?}"
        );
        assert_eq!(depth.running, 0);
    }

    // Grant a permit: the job is claimed and executed, and once the
    // queue drains for two checks the worker stops and releases it.
    jobs.add_permits(1);
    poll_until("job claimed and executed", || async {
        let db = DatabaseHandle::open(&db_url).unwrap();
        let depth = queue_depth(&db.admin).await.ok()?;
        (depth.claimable == 0).then_some(())
    })
    .await;
    poll_until("drained worker releases the permit", || async {
        (jobs.available_permits() == 1).then_some(())
    })
    .await;

    worker_token.cancel();
    w.await.unwrap().unwrap();
}
