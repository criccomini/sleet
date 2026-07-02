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
