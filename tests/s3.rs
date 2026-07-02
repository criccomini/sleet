//! Real S3 semantics via MinIO: conditional PUTs, ETags, and LIST
//! pagination, the behaviors `file://` and `memory://` don't exercise.
//! The test owns no infrastructure: it connects to the MinIO endpoint
//! in `SLEET_S3_ENDPOINT` (CI provides one as a service container; the
//! workflow names the image to run locally) and skips with a note when
//! the variable is unset. When the variable is set, an unreachable
//! MinIO is a failure, not a skip.

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use sleet::ops;
use sleet::root::{ConfigPoller, FleetRoot};
use sleet::testing::{Op, TestStore};

fn minio_store(endpoint: &str) -> Arc<dyn object_store::ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("sleet")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .build()
            .expect("s3 store builds"),
    )
}

/// Register (conditional create), ETag-cached polling, and
/// >1000-entry LIST pagination against real S3 semantics.
#[tokio::test(flavor = "multi_thread")]
async fn s3_semantics_via_minio() {
    let Ok(endpoint) = std::env::var("SLEET_S3_ENDPOINT") else {
        eprintln!("note: SLEET_S3_ENDPOINT unset; skipping MinIO test");
        return;
    };
    let store = TestStore::new(minio_store(&endpoint));
    // A fresh prefix per run: local MinIO containers outlive test runs,
    // and the register assertions need an empty registry.
    let prefix = format!(
        "fleet-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let url = format!("s3://sleet/{prefix}");
    let root = FleetRoot::from_parts(store.clone(), StorePath::from(prefix), &url);

    // Absorb MinIO startup lag, then fail: with an endpoint configured,
    // unreachable means broken, never skip.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match ops::status(&root, false).await {
            Ok(_) => break,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => panic!("minio at {endpoint} never became ready: {e}"),
        }
    }

    // Conditional create: the second register must see AlreadyExists.
    let first = ops::register(&root, "s3://data/db").await.unwrap();
    assert!(first.created);
    let again = ops::register(&root, "s3://data/db").await.unwrap();
    assert!(
        !again.created,
        "S3 If-None-Match must reject the second PUT"
    );

    // ETag caching against real S3 ETags: a config body is fetched
    // once, then served from cache until it changes. The +1 allows the
    // unconditional sleet.toml read every poll makes.
    root.store()
        .put(
            &root.database_path("s3://data/db"),
            "services = [\"gc\"]".into(),
        )
        .await
        .unwrap();
    let mut poller = ConfigPoller::default();
    poller.poll(&root).await;
    let gets = store.counters().count(Op::Get);
    poller.poll(&root).await;
    let second = store.counters().count(Op::Get);
    assert!(
        second <= gets + 1,
        "unchanged override re-fetched: {gets} -> {second}"
    );

    // LIST pagination: more than one page of registry entries.
    use futures::StreamExt;
    futures::stream::iter(0..1100)
        .for_each_concurrent(64, |i| {
            let root = root.clone();
            async move {
                root.store()
                    .put(
                        &root.database_path(&format!("s3://data/many-{i:04}")),
                        object_store::PutPayload::default(),
                    )
                    .await
                    .unwrap();
            }
        })
        .await;
    let state = poller.poll(&root).await;
    assert_eq!(
        state.databases.len(),
        1101,
        "pagination must see every entry"
    );
    assert!(state.warnings.is_empty(), "{:?}", state.warnings);
}
