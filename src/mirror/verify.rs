//! On-demand verification (DESIGN-MIRROR §10): a commit proves its
//! closure by induction, and a deletion outside prune silently breaks
//! it, so `sleet mirror verify` re-checks existence and size for every
//! restore point's closure. Sizes rather than ETags: multipart ETags do
//! not survive cross-store copies.

use std::collections::BTreeMap;

use chrono::Utc;
use object_store::ObjectStoreExt;

use super::MirrorError;
use super::layout::{self, object_path};
use crate::services::DatabaseHandle;

/// One restore point's verification result.
#[derive(Clone, Debug)]
pub struct VerifiedPoint {
    /// The restore point's manifest id.
    pub manifest_id: u64,
    /// Objects checked in its closure (manifests and data objects).
    pub objects: u64,
    /// What is missing or mismatched; empty means the point verifies.
    pub problems: Vec<String>,
}

/// The whole target's verification result.
#[derive(Clone, Debug, Default)]
pub struct VerifyOutcome {
    /// Every restore point, ascending by manifest id.
    pub points: Vec<VerifiedPoint>,
}

impl VerifyOutcome {
    /// Whether every restore point verified.
    pub fn ok(&self) -> bool {
        self.points.iter().all(|p| p.problems.is_empty())
    }
}

/// Verify every restore point's closure at the target: each closure
/// manifest and data object must exist, and where the source still has
/// the same object, the sizes must match. `keep` bounds which manifests
/// count as restore points; without retention every target manifest is
/// one.
pub async fn verify(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    keep: Option<std::time::Duration>,
) -> Result<VerifyOutcome, MirrorError> {
    let now = Utc::now();
    let manifests = layout::list_manifests(dest).await?;
    let Some(&(latest, _)) = manifests.last() else {
        return Ok(VerifyOutcome::default());
    };
    let by_id: BTreeMap<u64, u64> = manifests
        .iter()
        .map(|(id, meta)| (*id, meta.size))
        .collect();
    let restore_points: Vec<u64> = manifests
        .iter()
        .filter(|(id, meta)| {
            *id == latest
                || keep.is_none_or(|k| {
                    now - meta.last_modified < chrono::Duration::from_std(k).expect("keep fits")
                })
        })
        .map(|(id, _)| *id)
        .collect();

    // Target data listings replace per-object HEADs: existence and
    // size for every listed object in one request per thousand.
    let compacted: BTreeMap<String, u64> = layout::list_compacted(dest)
        .await?
        .into_iter()
        .map(|(ulid, meta)| (ulid, meta.size))
        .collect();
    let wals: BTreeMap<u64, u64> = layout::list_wals(dest)
        .await?
        .into_iter()
        .map(|(id, meta)| (id, meta.size))
        .collect();

    // Source sizes, HEADed once per unique object; None caches "gone
    // at the source", which skips the size comparison.
    let mut source_sizes: BTreeMap<String, Option<u64>> = BTreeMap::new();

    let mut outcome = VerifyOutcome::default();
    let mut decoded: BTreeMap<u64, Option<slatedb::VersionedManifest>> = BTreeMap::new();
    for id in restore_points {
        let mut point = VerifiedPoint {
            manifest_id: id,
            objects: 0,
            problems: Vec::new(),
        };
        // The restore point's closure: itself plus what its live
        // checkpoints pin, one level.
        let mut members = vec![id];
        match read_cached(dest, id, &mut decoded).await? {
            None => point.problems.push(format!("manifest {id} is unreadable")),
            Some(manifest) => {
                for cp in manifest.checkpoints() {
                    if layout::checkpoint_live(cp, now) && cp.manifest_id != id {
                        members.push(cp.manifest_id);
                    }
                }
            }
        }
        for member in members {
            point.objects += 1;
            let rel = layout::manifest_rel(member);
            let Some(&target_size) = by_id.get(&member) else {
                point.problems.push(format!("{rel}: missing at the target"));
                continue;
            };
            check_size(source, &rel, target_size, &mut source_sizes, &mut point).await;
            let Some(manifest) = read_cached(dest, member, &mut decoded).await? else {
                point.problems.push(format!("{rel}: unreadable"));
                continue;
            };
            let objects = layout::manifest_objects(manifest);
            for ulid in &objects.compacted {
                point.objects += 1;
                let rel = layout::compacted_rel(ulid);
                match compacted.get(ulid) {
                    None => point.problems.push(format!("{rel}: missing at the target")),
                    Some(&size) => {
                        check_size(source, &rel, size, &mut source_sizes, &mut point).await;
                    }
                }
            }
            for wal in &objects.wal {
                point.objects += 1;
                let rel = layout::wal_rel(*wal);
                match wals.get(wal) {
                    None => point.problems.push(format!("{rel}: missing at the target")),
                    Some(&size) => {
                        check_size(source, &rel, size, &mut source_sizes, &mut point).await;
                    }
                }
            }
        }
        point.problems.sort();
        point.problems.dedup();
        outcome.points.push(point);
    }
    Ok(outcome)
}

/// Compare one object's target size against the source, where the
/// source still has it.
async fn check_size(
    source: &DatabaseHandle,
    rel: &str,
    target_size: u64,
    cache: &mut BTreeMap<String, Option<u64>>,
    point: &mut VerifiedPoint,
) {
    if !cache.contains_key(rel) {
        let size = match source.store.head(&object_path(source, rel)).await {
            Ok(meta) => Some(meta.size),
            Err(_) => None,
        };
        cache.insert(rel.to_string(), size);
    }
    if let Some(Some(source_size)) = cache.get(rel)
        && *source_size != target_size
    {
        point.problems.push(format!(
            "{rel}: size mismatch (source {source_size}, target {target_size})"
        ));
    }
}

async fn read_cached<'a>(
    dest: &DatabaseHandle,
    id: u64,
    cache: &'a mut BTreeMap<u64, Option<slatedb::VersionedManifest>>,
) -> Result<Option<&'a slatedb::VersionedManifest>, MirrorError> {
    match cache.entry(id) {
        std::collections::btree_map::Entry::Occupied(entry) => Ok(entry.into_mut().as_ref()),
        std::collections::btree_map::Entry::Vacant(entry) => {
            let manifest = dest.admin.read_manifest(Some(id)).await.unwrap_or(None);
            Ok(entry.insert(manifest).as_ref())
        }
    }
}
