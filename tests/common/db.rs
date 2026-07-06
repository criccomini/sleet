//! Real-SlateDB helpers shared by the mirror integration binaries
//! (mirror, gcs, s3): database handles, a seed writer, and the
//! closure-completeness oracle.

use std::sync::Arc;

use object_store::ObjectStoreExt;
use object_store::path::Path as StorePath;
use sleet::mirror::layout;
use sleet::services::DatabaseHandle;

/// A database handle at `path` under `store`.
pub fn handle(store: Arc<dyn object_store::ObjectStore>, url: &str, path: &str) -> DatabaseHandle {
    DatabaseHandle::from_parts(url, store, StorePath::from(path))
}

/// Write `rounds` batches of 32 keys into a real SlateDB database at
/// the handle's root, each batch left as an L0 SST; background
/// maintenance is disabled, as under sleet.
pub async fn seed(db: &DatabaseHandle, rounds: u8) {
    let settings = slatedb::config::Settings {
        compactor_options: None,
        garbage_collector_options: None,
        ..Default::default()
    };
    let writer = slatedb::Db::builder(db.path.clone(), db.store.clone())
        .with_settings(settings)
        .build()
        .await
        .unwrap();
    for sst in 0..rounds {
        for key in 0..32 {
            writer
                .put(
                    format!("key-{sst}-{key}").as_bytes(),
                    vec![sst; 512].as_slice(),
                )
                .await
                .unwrap();
        }
        writer
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();
    }
    writer.close().await.unwrap();
}

/// The completeness invariant (RFC 0002 §3) as a test oracle: the
/// destination's latest manifest must hold its own objects and, for
/// every live checkpoint entry whose checkpoint still exists at the
/// source, the pinned manifest and its objects. Entries of checkpoints
/// retired at the source may dangle (§3). Returns the first problem
/// found.
pub async fn closure_problems(source: &DatabaseHandle, dest: &DatabaseHandle) -> Option<String> {
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
        let Some(manifest) = dest.admin.read_manifest(Some(id)).await.unwrap() else {
            return Some(format!("pinned manifest {id} missing at the destination"));
        };
        for rel in layout::manifest_objects(&manifest).rel_names() {
            if dest
                .store
                .head(&layout::object_path(dest, &rel))
                .await
                .is_err()
            {
                return Some(format!("{rel} missing at the destination (member {id})"));
            }
        }
    }
    None
}

/// Panic on the first completeness problem at the destination.
pub async fn assert_closure_complete(source: &DatabaseHandle, dest: &DatabaseHandle) {
    if let Some(problem) = closure_problems(source, dest).await {
        panic!("destination closure incomplete: {problem}");
    }
}
