//! Real GCS semantics via fake-gcs-server: create-if-absent commits
//! ride `x-goog-if-generation-match` rather than S3's If-None-Match,
//! and cross-store copies are the mirror's stated DR shape. The test
//! owns no infrastructure: it connects to the endpoint in
//! `SLEET_GCS_ENDPOINT` (CI runs fake-gcs-server; the workflow names
//! the image and flags to run locally) and skips with a note when the
//! variable is unset. The cross-store test additionally needs
//! `SLEET_S3_ENDPOINT` (the MinIO the s3 tests use).
//!
//! Run locally (the tustvold fork; upstream fsouza rejects
//! object_store's XML-API uploads with "invalid uploadType"):
//!   docker run -d -p 4443:4443 tustvold/fake-gcs-server \
//!     -scheme http -backend memory -public-host 127.0.0.1:4443
//!   SLEET_GCS_ENDPOINT=http://127.0.0.1:4443 cargo test --test gcs

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStoreExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as StorePath;
use sleet::config::ResolvedMirrorTarget;
use sleet::mirror;
use sleet::services::DatabaseHandle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn gcs_store(endpoint: &str) -> Arc<dyn object_store::ObjectStore> {
    // A static dummy bearer: fake-gcs-server ignores auth, and
    // object_store's upload path fetches real credentials even with
    // skip_signature set, which would reach for this machine's
    // Google credentials and hang the test in a token retry loop.
    let credential = object_store::gcp::GcpCredential {
        bearer: "fake".to_string(),
    };
    Arc::new(
        GoogleCloudStorageBuilder::new()
            .with_bucket_name("sleet")
            .with_base_url(endpoint)
            .with_credentials(Arc::new(object_store::StaticCredentialProvider::new(
                credential,
            )))
            .with_client_options(object_store::ClientOptions::new().with_allow_http(true))
            .build()
            .expect("gcs store builds"),
    )
}

/// Create the test bucket, idempotently, over fake-gcs-server's
/// unauthenticated JSON API. object_store has no bucket-creation API,
/// so this is one raw HTTP POST; 409 means an earlier run made it.
async fn ensure_bucket(endpoint: &str) {
    let host = endpoint
        .strip_prefix("http://")
        .expect("fake-gcs-server endpoint is http://");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match try_create_bucket(host).await {
            Ok(()) => return,
            Err(e) if tokio::time::Instant::now() < deadline => {
                eprintln!("waiting for fake-gcs-server: {e}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => panic!("fake-gcs-server at {endpoint} never became ready: {e}"),
        }
    }
}

async fn try_create_bucket(host: &str) -> Result<(), String> {
    let mut stream = tokio::net::TcpStream::connect(host)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let body = r#"{"name":"sleet"}"#;
    let request = format!(
        "POST /storage/v1/b?project=sleet HTTP/1.1\r\nHost: {host}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(|e| format!("read: {e}"))?;
    let status = response
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    match status.as_str() {
        "200" | "409" => Ok(()),
        other => Err(format!("bucket create returned {other}")),
    }
}

async fn seed(source: &DatabaseHandle, keys: u32) {
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    for key in 0..keys {
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
}

fn run_id() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// The completeness oracle: the destination's latest manifest holds
/// its own objects and, for every live checkpoint entry whose
/// checkpoint still exists at the source, the pinned manifest and its
/// objects (RFC 0002 §3).
async fn assert_closure_complete(source: &DatabaseHandle, dest: &DatabaseHandle) {
    use sleet::mirror::layout;
    let head = dest
        .admin
        .read_manifest(None)
        .await
        .unwrap()
        .expect("destination head");
    let src_cps: std::collections::BTreeSet<uuid::Uuid> = source
        .admin
        .read_manifest(None)
        .await
        .unwrap()
        .expect("source head")
        .checkpoints()
        .iter()
        .map(|cp| cp.id)
        .collect();
    let now = chrono::Utc::now();
    let mut members = vec![head.id()];
    for cp in head.checkpoints() {
        if layout::checkpoint_live(cp, now)
            && src_cps.contains(&cp.id)
            && cp.manifest_id != head.id()
        {
            members.push(cp.manifest_id);
        }
    }
    for id in members {
        let manifest = dest
            .admin
            .read_manifest(Some(id))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("pinned manifest {id} missing at the destination"));
        for rel in layout::manifest_objects(&manifest).rel_names() {
            assert!(
                dest.store
                    .head(&layout::object_path(dest, &rel))
                    .await
                    .is_ok(),
                "{rel} missing at the destination (member {id})"
            );
        }
    }
}

/// The mirror's commit protocol against GCS semantics: a pass seeds a
/// destination with create-if-absent commits (generation match zero),
/// byte-verifies, and a forked destination is detected as divergence.
#[tokio::test(flavor = "multi_thread")]
async fn gcs_mirror_pass_and_divergence() {
    let Ok(endpoint) = std::env::var("SLEET_GCS_ENDPOINT") else {
        eprintln!("note: SLEET_GCS_ENDPOINT unset; skipping fake-gcs-server test");
        return;
    };
    ensure_bucket(&endpoint).await;
    let store = gcs_store(&endpoint);
    let run = run_id();
    let src_path = format!("mirror-{run}/src");
    let dst_path = format!("mirror-{run}/dst");
    let source = DatabaseHandle::from_parts(
        &format!("gs://sleet/{src_path}"),
        store.clone(),
        StorePath::from(src_path.clone()),
    );
    let dest = DatabaseHandle::from_parts(
        &format!("gs://sleet/{dst_path}"),
        store.clone(),
        StorePath::from(dst_path.clone()),
    );
    seed(&source, 64).await;

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
    assert_closure_complete(&source, &dest).await;

    // A forked destination: a real writer opens the target and commits
    // its own manifests; generation-match plus the byte comparison must
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
    seed(&source, 1).await;
    let err = mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, mirror::MirrorError::Diverged { .. }),
        "{err:?}"
    );
}

/// The DR shape the design leads with: an S3 source (MinIO) mirrored
/// into a GCS destination (fake-gcs-server), and restored back out of
/// the GCS backup into a fresh S3 root that opens and reads.
#[tokio::test(flavor = "multi_thread")]
async fn cross_store_mirror_s3_to_gcs() {
    let (Ok(gcs_endpoint), Ok(s3_endpoint)) = (
        std::env::var("SLEET_GCS_ENDPOINT"),
        std::env::var("SLEET_S3_ENDPOINT"),
    ) else {
        eprintln!("note: SLEET_GCS_ENDPOINT or SLEET_S3_ENDPOINT unset; skipping cross-store test");
        return;
    };
    ensure_bucket(&gcs_endpoint).await;
    let s3 = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("sleet")
            .with_endpoint(&s3_endpoint)
            .with_allow_http(true)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .build()
            .expect("s3 store builds"),
    ) as Arc<dyn object_store::ObjectStore>;
    let gcs = gcs_store(&gcs_endpoint);
    let run = run_id();
    let src_path = format!("cross-{run}/src");
    let dst_path = format!("cross-{run}/dst");
    let restore_path = format!("cross-{run}/restore");
    let source = DatabaseHandle::from_parts(
        &format!("s3://sleet/{src_path}"),
        s3.clone(),
        StorePath::from(src_path.clone()),
    );
    let dest = DatabaseHandle::from_parts(
        &format!("gs://sleet/{dst_path}"),
        gcs.clone(),
        StorePath::from(dst_path.clone()),
    );
    seed(&source, 64).await;

    let settings = ResolvedMirrorTarget::default();
    let outcome = mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert!(outcome.committed);
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert_closure_complete(&source, &dest).await;

    // Restore back out of the GCS backup into a fresh S3 root, open
    // the result, and scan every key.
    let scratch = DatabaseHandle::from_parts(
        &format!("s3://sleet/{restore_path}"),
        s3.clone(),
        StorePath::from(restore_path.clone()),
    );
    mirror::restore(&dest, &scratch, mirror::RestorePoint::Latest)
        .await
        .unwrap();
    let db = slatedb::Db::builder(scratch.path.clone(), scratch.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let mut keys = 0u64;
    let mut scan = db.scan(..).await.unwrap();
    while let Some(_kv) = scan.next().await.unwrap() {
        keys += 1;
    }
    drop(scan);
    db.close().await.unwrap();
    assert_eq!(keys, 64);
}
