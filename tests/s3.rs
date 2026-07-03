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
        match ops::status(&root, false, false).await {
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

/// The mirror's commit protocol against real S3 semantics: a pass
/// seeds a destination prefix with create-if-absent manifest commits,
/// verify passes, a racing duplicate commit is harmless, and a forked
/// destination manifest is detected as divergence.
#[tokio::test(flavor = "multi_thread")]
async fn s3_mirror_pass_and_divergence_via_minio() {
    use sleet::config::ResolvedMirrorTarget;
    use sleet::mirror;
    use sleet::services::DatabaseHandle;

    let Ok(endpoint) = std::env::var("SLEET_S3_ENDPOINT") else {
        eprintln!("note: SLEET_S3_ENDPOINT unset; skipping MinIO mirror test");
        return;
    };
    let store = minio_store(&endpoint);
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let src_path = format!("mirror-{run}/src");
    let dst_path = format!("mirror-{run}/dst");
    let source = DatabaseHandle::from_parts(
        &format!("s3://sleet/{src_path}"),
        store.clone(),
        StorePath::from(src_path.clone()),
    );
    let dest = DatabaseHandle::from_parts(
        &format!("s3://sleet/{dst_path}"),
        store.clone(),
        StorePath::from(dst_path.clone()),
    );

    // A real database in MinIO.
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    for key in 0..64 {
        writer
            .put(format!("k-{key}").as_bytes(), vec![7u8; 1024].as_slice())
            .await
            .unwrap();
    }
    writer
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    let settings = ResolvedMirrorTarget::default();
    let outcome = mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert!(outcome.committed);
    // Converge the unpin manifest; the watermark reaches the head.
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    let src_head = source.admin.read_manifest(None).await.unwrap().unwrap();
    let dst_head = dest.admin.read_manifest(None).await.unwrap().unwrap();
    assert_eq!(src_head.id(), dst_head.id());
    let verified = mirror::verify(&source, &dest, None).await.unwrap();
    assert!(verified.ok(), "{:?}", verified.points);

    // A forked destination: a real writer opens the target and commits
    // its own manifests. However the fork's and source's manifest id
    // ranges interleave, If-None-Match plus the byte comparison must
    // call it divergence rather than mix the histories.
    let fork = slatedb::Db::builder(dest.path.clone(), dest.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    for i in 0..6u8 {
        fork.put(format!("fork-{i}").as_bytes(), b"x")
            .await
            .unwrap();
        fork.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    }
    fork.close().await.unwrap();
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    writer.put(b"more", b"data").await.unwrap();
    writer
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    writer.close().await.unwrap();
    let err = mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, mirror::MirrorError::Diverged { .. }),
        "{err:?}"
    );
}
