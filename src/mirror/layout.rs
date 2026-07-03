//! The frozen SlateDB layout facts the mirror relies on: object names
//! under a database root, manifest closure enumeration, and the listing
//! helpers passes and prune share.
//!
//! Names are FROZEN, like a wire format, and match SlateDB's
//! `PathResolver` and `ManifestStore`: `manifest/<id:020>.manifest`,
//! `wal/<id:020>.sst`, `compacted/<ulid>.sst`. The corpus test pins
//! them; changing either breaks every existing mirror target.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use object_store::path::Path as StorePath;
use slatedb::{Checkpoint, VersionedManifest};

use super::MirrorError;
use crate::services::DatabaseHandle;

/// The manifest directory under a database root.
pub const MANIFEST_DIR: &str = "manifest";
/// The WAL SST directory under a database root.
pub const WAL_DIR: &str = "wal";
/// The compacted SST directory under a database root.
pub const COMPACTED_DIR: &str = "compacted";

/// The relative name of a manifest object, e.g.
/// `manifest/00000000000000000042.manifest`.
pub fn manifest_rel(id: u64) -> String {
    format!("{MANIFEST_DIR}/{id:020}.manifest")
}

/// The relative name of a WAL SST, e.g. `wal/00000000000000000007.sst`.
pub fn wal_rel(id: u64) -> String {
    format!("{WAL_DIR}/{id:020}.sst")
}

/// The relative name of a compacted SST from its ULID string, e.g.
/// `compacted/01J79C21YKR31J2BS1EFXJZ7MR.sst`.
pub fn compacted_rel(ulid: &str) -> String {
    format!("{COMPACTED_DIR}/{ulid}.sst")
}

/// The store path of a relative object name under a database root.
pub fn object_path(db: &DatabaseHandle, rel: &str) -> StorePath {
    StorePath::from(format!("{}/{}", db.path, rel))
}

/// The manifest id an object name encodes, if it is a manifest.
pub fn parse_manifest_name(name: &str) -> Option<u64> {
    name.strip_suffix(".manifest")?.parse().ok()
}

/// The WAL id an object name encodes, if it is a WAL SST.
pub fn parse_wal_name(name: &str) -> Option<u64> {
    name.strip_suffix(".sst")?.parse().ok()
}

/// The ULID string an object name encodes, if it is a compacted SST.
pub fn parse_compacted_name(name: &str) -> Option<String> {
    let ulid = name.strip_suffix(".sst")?;
    (ulid.len() == 26 && ulid.chars().all(|c| c.is_ascii_alphanumeric())).then(|| ulid.to_string())
}

/// LIST one directory under a database root, treating a missing prefix
/// as empty.
async fn list_dir(db: &DatabaseHandle, dir: &str) -> Result<Vec<ObjectMeta>, MirrorError> {
    let prefix = StorePath::from(format!("{}/{dir}", db.path));
    match db.store.list(Some(&prefix)).try_collect().await {
        Ok(metas) => Ok(metas),
        Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

/// Every manifest at a database root, ascending by id.
pub async fn list_manifests(db: &DatabaseHandle) -> Result<Vec<(u64, ObjectMeta)>, MirrorError> {
    let mut out: Vec<(u64, ObjectMeta)> = list_dir(db, MANIFEST_DIR)
        .await?
        .into_iter()
        .filter_map(|meta| {
            let id = meta.location.filename().and_then(parse_manifest_name)?;
            Some((id, meta))
        })
        .collect();
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

/// The highest manifest id at a database root, from one LIST.
pub async fn max_manifest_id(db: &DatabaseHandle) -> Result<Option<u64>, MirrorError> {
    Ok(list_manifests(db).await?.last().map(|(id, _)| *id))
}

/// Every WAL SST at a database root, ascending by id.
pub async fn list_wals(db: &DatabaseHandle) -> Result<Vec<(u64, ObjectMeta)>, MirrorError> {
    let mut out: Vec<(u64, ObjectMeta)> = list_dir(db, WAL_DIR)
        .await?
        .into_iter()
        .filter_map(|meta| {
            let id = meta.location.filename().and_then(parse_wal_name)?;
            Some((id, meta))
        })
        .collect();
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

/// Every compacted SST at a database root, by ULID string.
pub async fn list_compacted(db: &DatabaseHandle) -> Result<Vec<(String, ObjectMeta)>, MirrorError> {
    Ok(list_dir(db, COMPACTED_DIR)
        .await?
        .into_iter()
        .filter_map(|meta| {
            let ulid = meta.location.filename().and_then(parse_compacted_name)?;
            Some((ulid, meta))
        })
        .collect())
}

/// The data objects one manifest references: its own trees' SSTs and
/// the WAL window `(replay_after_wal_id, next_wal_sst_id)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ManifestObjects {
    /// Compacted SST ULID strings, across the root tree and every
    /// RFC-0024 segment.
    pub compacted: BTreeSet<String>,
    /// WAL SST ids the manifest's recorded state replays.
    pub wal: BTreeSet<u64>,
}

impl ManifestObjects {
    /// Objects in `self` but not `other`.
    pub fn difference(&self, other: &ManifestObjects) -> ManifestObjects {
        ManifestObjects {
            compacted: self
                .compacted
                .difference(&other.compacted)
                .cloned()
                .collect(),
            wal: self.wal.difference(&other.wal).copied().collect(),
        }
    }

    /// Union `other` into `self`.
    pub fn extend(&mut self, other: &ManifestObjects) {
        self.compacted.extend(other.compacted.iter().cloned());
        self.wal.extend(other.wal.iter().copied());
    }

    /// Whether both sets are empty.
    pub fn is_empty(&self) -> bool {
        self.compacted.is_empty() && self.wal.is_empty()
    }

    /// Total object count.
    pub fn len(&self) -> usize {
        self.compacted.len() + self.wal.len()
    }

    /// Every object as a relative name under the database root.
    pub fn rel_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.wal.iter().map(|&id| wal_rel(id)).collect();
        names.extend(self.compacted.iter().map(|u| compacted_rel(u)));
        names
    }
}

/// The data objects a manifest references, from its public accessors.
pub fn manifest_objects(m: &VersionedManifest) -> ManifestObjects {
    use slatedb::manifest::{SsTableId, SsTableView};
    let mut objects = ManifestObjects::default();
    let mut add = |view: &SsTableView| match &view.sst.id {
        SsTableId::Compacted(ulid) => {
            objects.compacted.insert(ulid.to_string());
        }
        SsTableId::Wal(id) => {
            objects.wal.insert(*id);
        }
    };
    for (l0, runs) in std::iter::once((m.l0(), m.compacted()))
        .chain(m.segments().iter().map(|s| (s.l0(), s.compacted())))
    {
        l0.iter().for_each(&mut add);
        runs.iter()
            .flat_map(|run| run.sst_views.iter())
            .for_each(&mut add);
    }
    for wal_id in (m.replay_after_wal_id() + 1)..m.next_wal_sst_id() {
        objects.wal.insert(wal_id);
    }
    objects
}

/// Whether a checkpoint is live (unexpired) at `now`.
pub fn checkpoint_live(cp: &Checkpoint, now: DateTime<Utc>) -> bool {
    cp.expire_time.is_none_or(|t| t > now)
}

/// Refuse excluded sources (DESIGN-MIRROR §2): clones and databases
/// with a separate WAL object store.
pub fn check_source(url: &str, m: &VersionedManifest) -> Result<(), MirrorError> {
    if !m.external_dbs().is_empty() {
        return Err(MirrorError::ExcludedSource {
            url: url.to_string(),
            reason: "it is a clone (external_dbs is set)".to_string(),
        });
    }
    if m.wal_object_store_uri().is_some() {
        return Err(MirrorError::ExcludedSource {
            url: url.to_string(),
            reason: "it uses a separate WAL object store".to_string(),
        });
    }
    Ok(())
}

/// A per-pass cache of decoded manifests; ids are immutable so entries
/// never invalidate.
#[derive(Default)]
pub struct ManifestCache {
    by_id: BTreeMap<u64, VersionedManifest>,
}

impl ManifestCache {
    /// Read manifest `id` from `db`, serving repeats from the cache.
    /// `Ok(None)` means the manifest does not exist (GC took it).
    pub async fn read(
        &mut self,
        db: &DatabaseHandle,
        id: u64,
    ) -> Result<Option<&VersionedManifest>, MirrorError> {
        if !self.by_id.contains_key(&id) {
            match db.admin.read_manifest(Some(id)).await? {
                Some(manifest) => {
                    self.by_id.insert(id, manifest);
                }
                None => return Ok(None),
            }
        }
        Ok(self.by_id.get(&id))
    }
}

/// The closure of one head manifest (DESIGN-MIRROR §3): the head's own
/// data objects plus, for each live checkpoint in its list, the pinned
/// manifest and that manifest's data objects. One level only.
pub struct Closure {
    /// The head manifest's id.
    pub head: u64,
    /// Every manifest in the closure (the head and each pinned
    /// manifest), with its data objects.
    pub manifests: BTreeMap<u64, ManifestObjects>,
}

impl Closure {
    /// The union of every member manifest's data objects.
    pub fn objects(&self) -> ManifestObjects {
        let mut all = ManifestObjects::default();
        for objects in self.manifests.values() {
            all.extend(objects);
        }
        all
    }
}

/// Enumerate the closure of `head` against the source. `Ok(None)` means
/// a live checkpoint's pinned manifest is already gone at the source
/// (the checkpoint was deleted since `head` was read); the caller
/// re-reads the head and retries.
pub async fn closure(
    db: &DatabaseHandle,
    head: &VersionedManifest,
    now: DateTime<Utc>,
    cache: &mut ManifestCache,
) -> Result<Option<Closure>, MirrorError> {
    let mut manifests = BTreeMap::new();
    manifests.insert(head.id(), manifest_objects(head));
    for cp in head.checkpoints() {
        if !checkpoint_live(cp, now) || cp.manifest_id == head.id() {
            continue;
        }
        match cache.read(db, cp.manifest_id).await? {
            Some(pinned) => {
                manifests.insert(cp.manifest_id, manifest_objects(pinned));
            }
            None => return Ok(None),
        }
    }
    Ok(Some(Closure {
        head: head.id(),
        manifests,
    }))
}
