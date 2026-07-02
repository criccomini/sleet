//! Real S3 semantics via MinIO in Docker: conditional PUTs, ETags, and
//! LIST pagination, the behaviors `file://` and `memory://` don't
//! exercise. Skips (with a note) when Docker isn't available.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use sleet::root::{ConfigPoller, FleetRoot};
use sleet::testing::{Op, TestStore};
use sleet::{ops, registry};

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .is_ok_and(|out| out.status.success())
}

struct Minio {
    container: String,
    port: u16,
}

impl Minio {
    fn start() -> Self {
        // Let the OS pick a free port, then hand it to Docker.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "-p",
                &format!("127.0.0.1:{port}:9000"),
                "minio/minio",
                "server",
                "/data",
            ])
            .output()
            .expect("docker run");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let container = String::from_utf8(output.stdout).unwrap().trim().to_string();
        // The FS backend treats a directory as a bucket.
        let mkdir = Command::new("docker")
            .args(["exec", &container, "mkdir", "-p", "/data/sleet"])
            .status()
            .expect("docker exec");
        assert!(mkdir.success());
        Self { container, port }
    }

    fn store(&self) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name("sleet")
                .with_endpoint(format!("http://127.0.0.1:{}", self.port))
                .with_allow_http(true)
                .with_access_key_id("minioadmin")
                .with_secret_access_key("minioadmin")
                .with_region("us-east-1")
                .build()
                .expect("s3 store builds"),
        )
    }
}

impl Drop for Minio {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
    }
}

/// Register (conditional create), alias detection, ETag-cached polling,
/// and >1000-entry LIST pagination against real S3 semantics.
#[tokio::test(flavor = "multi_thread")]
async fn s3_semantics_via_minio() {
    if !docker_available() {
        eprintln!("note: docker unavailable; skipping MinIO test");
        return;
    }
    let minio = Minio::start();
    let store = TestStore::new(minio.store());
    let root = FleetRoot::from_parts(store.clone(), StorePath::from("fleet"), "s3://sleet/fleet");

    // Wait for MinIO to accept requests.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match ops::status(&root, false).await {
            Ok(_) => break,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => panic!("minio never became ready: {e}"),
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
    // once, then served from cache until it changes.
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
    let _ = registry::file_name("s3://data/db");
}
