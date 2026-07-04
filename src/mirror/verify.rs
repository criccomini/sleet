//! On-demand verification (DESIGN-MIRROR §10): a commit proves its
//! closure by induction, and a deletion outside prune silently breaks
//! it, so `sleet mirror verify` re-checks existence and size for every
//! restore point's closure. Sizes rather than ETags: multipart ETags do
//! not survive cross-store copies. `Depth::Bytes` re-reads both stores
//! and compares content, catching same-size corruption sizes cannot.

use std::collections::BTreeMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::ObjectStoreExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::MirrorError;
use super::layout::{self, object_path};
use crate::services::DatabaseHandle;

/// How deeply each closure object is checked against the source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Depth {
    /// Existence at the target, size against the source.
    #[default]
    Sizes,
    /// Sizes, plus a re-read of both stores comparing bytes.
    Bytes,
}

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

/// Current verify record format version.
pub const RECORD_VERSION: u32 = 1;

/// Problems a record carries at most; the full list comes from
/// re-running `sleet mirror verify`.
const RECORD_PROBLEMS_MAX: usize = 10;

/// One periodic verification outcome: `verify/<db>.<target>.json` at
/// the fleet root, written by the owning daemon task on the target's
/// `verify_interval` and read by `sleet status --mirrors`. The record
/// is observability-only, like a heartbeat body: readers ignore
/// unknown fields, `version` bumps only on incompatible change, and a
/// record for a retired target is inert.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(title = "sleet verify record")]
pub struct VerifyRecord {
    /// Record format version; bumped only on incompatible change.
    pub version: u32,

    /// The node that ran the verification.
    pub node_id: String,

    /// The source database's canonical URL.
    pub database: String,

    /// The target's name.
    pub target: String,

    /// The destination root verified.
    pub destination: String,

    /// When the verification finished.
    pub verified_at: DateTime<Utc>,

    /// Whether every restore point verified.
    pub ok: bool,

    /// Restore points checked.
    pub points: u64,

    /// Objects checked across every restore point's closure.
    pub objects: u64,

    /// Total problems found.
    pub problems: u64,

    /// The first few problems.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample: Vec<String>,
}

impl VerifyRecord {
    /// A current-version record for one outcome, stamped `verified_at`
    /// now.
    pub fn new(
        node_id: &str,
        database: &str,
        target: &str,
        destination: &str,
        outcome: &VerifyOutcome,
    ) -> Self {
        let problems: Vec<&String> = outcome.points.iter().flat_map(|p| &p.problems).collect();
        Self {
            version: RECORD_VERSION,
            node_id: node_id.to_string(),
            database: database.to_string(),
            target: target.to_string(),
            destination: destination.to_string(),
            verified_at: Utc::now(),
            ok: outcome.ok(),
            points: outcome.points.len() as u64,
            objects: outcome.points.iter().map(|p| p.objects).sum(),
            problems: problems.len() as u64,
            sample: problems
                .into_iter()
                .take(RECORD_PROBLEMS_MAX)
                .cloned()
                .collect(),
        }
    }
}

/// The verify record JSON Schema, pretty-printed.
pub fn record_schema_json() -> String {
    crate::schema_pretty::<VerifyRecord>()
}

/// Verify every restore point's closure at the target: each closure
/// manifest and data object must exist, and where the source still has
/// the same object, the sizes (and at `Depth::Bytes` the contents)
/// must match. `keep` bounds which manifests count as restore points;
/// without retention every target manifest is one.
pub async fn verify(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    keep: Option<std::time::Duration>,
    depth: Depth,
) -> Result<VerifyOutcome, MirrorError> {
    let now = Utc::now();
    let manifests = layout::list_manifests(dest).await?;
    let Some(&(latest, _)) = manifests.last() else {
        return Ok(VerifyOutcome::default());
    };
    // Checkpoints already retired at the source resolve nowhere at the
    // source either (§4): a target manifest committed as closure
    // support immutably carries such entries, their pinned manifests
    // were never promised to the target, and restore refuses those
    // points (§7). Verify judges an entry only while its checkpoint
    // still exists at the source; expiry is handled separately.
    let source_head =
        source
            .admin
            .read_manifest(None)
            .await?
            .ok_or_else(|| MirrorError::NotADatabase {
                url: source.url.clone(),
            })?;
    let source_cps: std::collections::BTreeSet<uuid::Uuid> =
        source_head.checkpoints().iter().map(|cp| cp.id).collect();
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

    // Source comparisons, made once per unique object; restore points
    // share support, so repeats serve from the cache.
    let mut compared: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let mut outcome = VerifyOutcome::default();
    let mut decoded: BTreeMap<u64, Option<slatedb::VersionedManifest>> = BTreeMap::new();
    for id in restore_points {
        let mut point = VerifiedPoint {
            manifest_id: id,
            objects: 0,
            problems: Vec::new(),
        };
        // The restore point's closure: itself plus what its live,
        // still-source-known checkpoints pin, one level.
        let mut members = vec![id];
        match read_cached(dest, id, &mut decoded).await? {
            None => point.problems.push(format!("manifest {id} is unreadable")),
            Some(manifest) => {
                for cp in manifest.checkpoints() {
                    if layout::checkpoint_live(cp, now)
                        && source_cps.contains(&cp.id)
                        && cp.manifest_id != id
                    {
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
            compare(
                source,
                dest,
                &rel,
                target_size,
                depth,
                &mut compared,
                &mut point,
            )
            .await;
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
                        compare(source, dest, &rel, size, depth, &mut compared, &mut point).await;
                    }
                }
            }
            for wal in &objects.wal {
                point.objects += 1;
                let rel = layout::wal_rel(*wal);
                match wals.get(wal) {
                    None => point.problems.push(format!("{rel}: missing at the target")),
                    Some(&size) => {
                        compare(source, dest, &rel, size, depth, &mut compared, &mut point).await;
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

/// Compare one object's target copy against the source, where the
/// source still has it: sizes always, bytes at `Depth::Bytes`. Cached
/// per name; restore points share support objects.
async fn compare(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    rel: &str,
    target_size: u64,
    depth: Depth,
    cache: &mut BTreeMap<String, Vec<String>>,
    point: &mut VerifiedPoint,
) {
    if !cache.contains_key(rel) {
        let problems = source_problems(source, dest, rel, target_size, depth).await;
        cache.insert(rel.to_string(), problems);
    }
    point
        .problems
        .extend(cache.get(rel).cloned().unwrap_or_default());
}

/// The problems one target object shows against the source. A source
/// that no longer holds the object (GC took it; the target is the only
/// copy) compares clean, matching what a commit could have proven.
async fn source_problems(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    rel: &str,
    target_size: u64,
    depth: Depth,
) -> Vec<String> {
    let source_size = match source.store.head(&object_path(source, rel)).await {
        Ok(meta) => meta.size,
        Err(_) => return Vec::new(),
    };
    if source_size != target_size {
        return vec![format!(
            "{rel}: size mismatch (source {source_size}, target {target_size})"
        )];
    }
    if depth == Depth::Sizes {
        return Vec::new();
    }
    match first_difference(source, dest, rel).await {
        Ok(None) => Vec::new(),
        Ok(Some(offset)) => vec![format!("{rel}: content mismatch at byte {offset}")],
        Err(e) => vec![format!("{rel}: unreadable for byte comparison: {e}")],
    }
}

/// The first offset where the source and target bytes of `rel` differ.
/// Source read failures end the comparison with no finding (the object
/// left the source mid-check, same as the shallow path); target read
/// failures are the caller's problem to report.
async fn first_difference(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    rel: &str,
) -> Result<Option<u64>, MirrorError> {
    let src = match source.store.get(&object_path(source, rel)).await {
        Ok(get) => get,
        Err(_) => return Ok(None),
    };
    let dst = dest.store.get(&object_path(dest, rel)).await?;
    let mut a = src.into_stream();
    let mut b = dst.into_stream();
    let mut ahead = Bytes::new();
    let mut bhead = Bytes::new();
    let mut offset = 0u64;
    loop {
        while ahead.is_empty() {
            match a.next().await {
                Some(Ok(chunk)) => ahead = chunk,
                Some(Err(_)) => return Ok(None),
                None => break,
            }
        }
        while bhead.is_empty() {
            match b.next().await {
                Some(chunk) => bhead = chunk?,
                None => break,
            }
        }
        match (ahead.is_empty(), bhead.is_empty()) {
            (true, true) => return Ok(None),
            (true, false) | (false, true) => return Ok(Some(offset)),
            (false, false) => {}
        }
        let n = ahead.len().min(bhead.len());
        if let Some(i) = ahead[..n].iter().zip(&bhead[..n]).position(|(x, y)| x != y) {
            return Ok(Some(offset + i as u64));
        }
        offset += n as u64;
        ahead = ahead.slice(n..);
        bhead = bhead.slice(n..);
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
