//! Consumer-level tests for the supported Rust API.

use std::time::Duration;

use sleet::{
    CancellationToken, Error, Fleet, MirrorSyncOptions, NodeOptions, RestorePoint, StatusOptions,
    mirror_restore,
};

#[tokio::test]
async fn registration_status_and_mirror_errors_use_the_facade() {
    let fleet = Fleet::open("memory:///fleet").unwrap();
    assert_eq!(fleet.url(), "memory:///fleet");

    let registered = fleet.register("s3://Bucket/db/").await.unwrap();
    assert!(registered.created);
    assert_eq!(registered.url, "s3://bucket/db");

    let status = fleet
        .status(
            StatusOptions::default()
                .with_compactions(false)
                .with_mirrors(false),
        )
        .await
        .unwrap();
    assert_eq!(status.databases.len(), 1);
    assert_eq!(status.databases[0].url, "s3://bucket/db");

    let error = fleet
        .sync_mirror("s3://bucket/db", "missing", MirrorSyncOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NoSuchMirrorTarget { .. }));

    let error = fleet
        .run_node(NodeOptions::new("bad id"), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidNodeId(_)));

    let error = fleet
        .run_node(
            NodeOptions::new("duplicate").with_services([sleet::Service::Gc, sleet::Service::Gc]),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::DuplicateService { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn node_runs_until_cancellation_and_deletes_its_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    let fleet_path = dir.path().join("fleet");
    std::fs::create_dir_all(&fleet_path).unwrap();
    let fleet = Fleet::open(&format!("file://{}", fleet_path.display())).unwrap();

    let shutdown = CancellationToken::new();
    let task = tokio::spawn({
        let fleet = fleet.clone();
        let shutdown = shutdown.clone();
        async move {
            fleet
                .run_node(NodeOptions::new("api-node").with_services([]), shutdown)
                .await
        }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = fleet.status(StatusOptions::default()).await.unwrap();
        if status.nodes.iter().any(|node| node.node_id == "api-node") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node heartbeat did not appear"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    shutdown.cancel();
    task.await.unwrap().unwrap();
    let status = fleet.status(StatusOptions::default()).await.unwrap();
    assert!(status.nodes.is_empty());
}

#[tokio::test]
async fn restore_uses_environment_backed_urls() {
    let dir = tempfile::tempdir().unwrap();
    let backup_path = dir.path().join("backup");
    let dest_path = dir.path().join("dest");
    std::fs::create_dir_all(&backup_path).unwrap();
    std::fs::create_dir_all(&dest_path).unwrap();
    let backup_url = format!("file://{}", backup_path.display());
    let dest_url = format!("file://{}", dest_path.display());

    seed_database(&backup_url).await;
    let restored = mirror_restore(&backup_url, &dest_url, RestorePoint::Latest)
        .await
        .unwrap();
    assert!(restored.manifests_committed > 0);
    assert_eq!(restored.backup, backup_url);
    assert_eq!(restored.destination, dest_url);
}

async fn seed_database(url: &str) {
    let parsed = url::Url::parse(url).unwrap();
    let (store, path) = object_store::parse_url(&parsed).unwrap();
    let db = slatedb::Db::builder(path, store.into())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    db.put(b"key", b"value").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
}
