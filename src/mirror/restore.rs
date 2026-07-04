//! `sleet mirror restore` (DESIGN-MIRROR §7): a one-shot pass with a
//! chosen restore point as `L`, copying its closure to an empty
//! destination and committing it. The destination is then an ordinary
//! database at that point. `drill` (§10) is restore's proof: restore
//! into a scratch root, open it, and scan every key.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{ObjectStoreExt, PutMode, PutOptions};
use slatedb::seq_tracker::FindOption;

use super::MirrorError;
use super::copier::Copier;
use super::layout::{self, ManifestObjects, object_path};
use crate::config::{CopierKind, ResolvedMirrorTarget};
use crate::services::DatabaseHandle;

/// Which restore point to restore.
#[derive(Clone, Debug)]
pub enum RestorePoint {
    /// The backup's latest manifest.
    Latest,
    /// A manifest id.
    Manifest(u64),
    /// A wall-clock time, mapped through the sequence tracker.
    Time(DateTime<Utc>),
}

impl RestorePoint {
    /// Parse `--at`: a manifest id or an RFC 3339 timestamp.
    pub fn parse(at: &str) -> Result<Self, String> {
        if let Ok(id) = at.parse::<u64>() {
            return Ok(RestorePoint::Manifest(id));
        }
        DateTime::parse_from_rfc3339(at)
            .map(|t| RestorePoint::Time(t.with_timezone(&Utc)))
            .map_err(|_| format!("--at {at:?} is neither a manifest id nor an RFC 3339 timestamp"))
    }
}

/// What one restore did.
#[derive(Clone, Copy, Debug, Default)]
pub struct RestoreOutcome {
    /// The restore point committed as the destination's head.
    pub manifest_id: u64,
    /// Manifests committed (the closure's, ascending, the point last).
    pub manifests_committed: u64,
    /// Data objects copied.
    pub copied_objects: u64,
    /// Data bytes copied.
    pub copied_bytes: u64,
}

/// Restore `backup` at `point` into the empty root `dest`.
pub async fn restore(
    backup: &DatabaseHandle,
    dest: &DatabaseHandle,
    point: RestorePoint,
) -> Result<RestoreOutcome, MirrorError> {
    // The destination must be empty; restore refuses anything else and
    // never deletes.
    let mut existing = dest.store.list(Some(&dest.path));
    if existing.next().await.transpose()?.is_some() {
        return Err(MirrorError::DestinationNotEmpty {
            url: dest.url.clone(),
        });
    }

    let manifests = layout::list_manifests(backup).await?;
    if manifests.is_empty() {
        return Err(MirrorError::NotADatabase {
            url: backup.url.clone(),
        });
    }
    let chosen = resolve_point(backup, &manifests, &point).await?;
    let manifest = backup
        .admin
        .read_manifest(Some(chosen))
        .await?
        .ok_or_else(|| MirrorError::NoRestorePoint {
            at: format!("{point:?}"),
            reason: format!("manifest {chosen} vanished from the backup"),
        })?;

    // The closure, read wholly from the backup. A support manifest's
    // own live entries may dangle; restore fails rather than commit an
    // incomplete closure.
    let now = Utc::now();
    let mut members: BTreeMap<u64, ManifestObjects> = BTreeMap::new();
    members.insert(chosen, layout::manifest_objects(&manifest));
    for cp in manifest.checkpoints() {
        if !layout::checkpoint_live(cp, now) || cp.manifest_id == chosen {
            continue;
        }
        match backup.admin.read_manifest(Some(cp.manifest_id)).await? {
            Some(pinned) => {
                members.insert(cp.manifest_id, layout::manifest_objects(&pinned));
            }
            None => {
                return Err(MirrorError::NoRestorePoint {
                    at: format!("{point:?}"),
                    reason: format!(
                        "manifest {chosen} is closure support, not a restore point: its live \
                         checkpoint pins manifest {}, which the backup no longer has",
                        cp.manifest_id
                    ),
                });
            }
        }
    }

    // Copy the closure's data objects, then commit the manifests in
    // ascending id order, the restore point last.
    let mut objects = ManifestObjects::default();
    for member in members.values() {
        objects.extend(member);
    }
    let settings = ResolvedMirrorTarget {
        copier: CopierKind::Builtin,
        ..ResolvedMirrorTarget::default()
    };
    let copier = Copier::new(&settings, None, backup, dest);
    let copied = copier.copy(&objects.rel_names()).await?;

    let mut committed = 0;
    for &id in members.keys() {
        let rel = layout::manifest_rel(id);
        let bytes = backup
            .store
            .get(&object_path(backup, &rel))
            .await?
            .bytes()
            .await?;
        dest.store
            .put_opts(
                &object_path(dest, &rel),
                bytes.into(),
                PutOptions::from(PutMode::Create),
            )
            .await?;
        committed += 1;
    }
    Ok(RestoreOutcome {
        manifest_id: chosen,
        manifests_committed: committed,
        copied_objects: copied.objects,
        copied_bytes: copied.bytes,
    })
}

/// What one drill proved.
#[derive(Clone, Copy, Debug, Default)]
pub struct DrillOutcome {
    /// The restore exercised.
    pub restored: RestoreOutcome,
    /// Keys the full scan read back.
    pub keys: u64,
    /// Key and value bytes the full scan read back.
    pub bytes: u64,
}

/// Restore `backup` at `point` into the empty `scratch` root, open the
/// result as an ordinary database (compactor and GC off), and scan
/// every key: the end-to-end proof that the point restores and reads.
/// The caller owns the scratch root's cleanup.
pub async fn drill(
    backup: &DatabaseHandle,
    scratch: &DatabaseHandle,
    point: RestorePoint,
) -> Result<DrillOutcome, MirrorError> {
    let restored = restore(backup, scratch, point).await?;
    let db = slatedb::Db::builder(scratch.path.clone(), scratch.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await?;
    let mut keys = 0u64;
    let mut bytes = 0u64;
    let mut scan = db.scan(..).await?;
    while let Some(kv) = scan.next().await? {
        keys += 1;
        bytes += (kv.key.len() + kv.value.len()) as u64;
    }
    drop(scan);
    db.close().await?;
    Ok(DrillOutcome {
        restored,
        keys,
        bytes,
    })
}

/// Resolve `--at` to a manifest id. Restore points map to wall-clock
/// time by the manifest's sequence tracker: each candidate manifest's
/// recorded state maps to the newest tracked time at or below it, and
/// the newest manifest within `--at` wins. The tracker samples one
/// entry per interval (60s stock), so resolution is bounded by that
/// granularity.
async fn resolve_point(
    backup: &DatabaseHandle,
    manifests: &[(u64, object_store::ObjectMeta)],
    point: &RestorePoint,
) -> Result<u64, MirrorError> {
    let latest = manifests.last().expect("nonempty").0;
    match point {
        RestorePoint::Latest => Ok(latest),
        RestorePoint::Manifest(id) => {
            if manifests.iter().any(|(m, _)| m == id) {
                Ok(*id)
            } else {
                Err(MirrorError::NoRestorePoint {
                    at: id.to_string(),
                    reason: format!("the backup has no manifest {id}"),
                })
            }
        }
        RestorePoint::Time(ts) => {
            let head = backup
                .admin
                .read_manifest(Some(latest))
                .await?
                .ok_or_else(|| MirrorError::NotADatabase {
                    url: backup.url.clone(),
                })?;
            for (id, _) in manifests.iter().rev() {
                let Some(m) = backup.admin.read_manifest(Some(*id)).await? else {
                    continue;
                };
                // A manifest whose state precedes every tracked entry
                // maps nowhere; skip it rather than guess.
                let Some(m_ts) = head
                    .sequence_tracker()
                    .find_ts(m.last_l0_seq(), FindOption::RoundDown)
                else {
                    continue;
                };
                if m_ts <= *ts {
                    return Ok(*id);
                }
            }
            Err(MirrorError::NoRestorePoint {
                at: ts.to_rfc3339(),
                reason: "the timestamp predates the backup's tracked history".to_string(),
            })
        }
    }
}
