//! Restore-point retention (RFC 0002 §7): the only deletion that
//! may run against a target.
//!
//! With `retention.keep` set the pruner keeps two tiers: restore points
//! (the latest manifest and every manifest younger than `keep`) and
//! closure support (for each restore point, the manifests its live
//! checkpoints pin). Everything else is deleted: the manifests, then
//! data objects unreferenced by any kept manifest and older than
//! `min_age`. Two guards hold data-object deletion back from in-flight
//! passes: the source's latest closure is spared (listing the target
//! before reading the source makes the guard exact), and while any
//! checkpoint named for the target exists nothing newer than the
//! oldest one's `create_time` less `min_age` is deleted.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use object_store::ObjectStoreExt;
use tracing::{debug, info, warn};

use super::MirrorError;
use super::layout::{self, ManifestCache, ManifestObjects, object_path};
use super::pass::pin_name;
use crate::config::ResolvedMirrorTarget;
use crate::services::DatabaseHandle;

/// What one prune deleted and kept.
#[derive(Clone, Copy, Debug, Default)]
pub struct PruneReport {
    /// Manifests kept: restore points plus closure support.
    pub kept_manifests: u64,
    /// Manifests deleted.
    pub deleted_manifests: u64,
    /// Data objects deleted.
    pub deleted_objects: u64,
    /// False when the source was unreachable and data-object deletion
    /// was skipped entirely.
    pub data_deletion_ran: bool,
}

/// Prune one target. A no-op unless `retention.keep` is set.
pub async fn prune(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target_name: &str,
    settings: &ResolvedMirrorTarget,
) -> Result<PruneReport, MirrorError> {
    prune_at(source, dest, target_name, settings, Utc::now()).await
}

/// The prune pass with an injected `now`, so tests control ages.
pub async fn prune_at(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target_name: &str,
    settings: &ResolvedMirrorTarget,
    now: DateTime<Utc>,
) -> Result<PruneReport, MirrorError> {
    let Some(keep) = settings.keep else {
        return Ok(PruneReport::default());
    };
    let keep = chrono::Duration::from_std(keep).expect("keep fits");
    let min_age = chrono::Duration::from_std(settings.min_age).expect("min_age fits");

    // List the target before reading the source: any object at the
    // target at list time that a later pass commits a reference to was
    // already in the source's closure at read time.
    let manifests = layout::list_manifests(dest).await?;
    let Some(&(latest, _)) = manifests.last() else {
        return Ok(PruneReport::default());
    };
    let compacted = layout::list_compacted(dest).await?;
    let wals = layout::list_wals(dest).await?;

    // Guard 1: the source's latest closure, counting checkpoint entries
    // whether or not they have expired (retiring them is source GC's
    // job). An unreachable source stops data-object deletion entirely.
    let spared = source_closure(source).await;
    // Guard 2: while any checkpoint named for the target exists, delete
    // no data object newer than the oldest one's create_time less
    // min_age (clock slack).
    let floor = match &spared {
        Ok(_) => match source
            .admin
            .list_checkpoints(Some(&pin_name(target_name)))
            .await
        {
            Ok(pins) => pins.iter().map(|cp| cp.create_time - min_age).min(),
            Err(e) => {
                warn!(target = target_name, "cannot list pin checkpoints: {e}");
                None
            }
        },
        Err(_) => None,
    };
    if let Err(e) = &spared {
        warn!(
            target = target_name,
            "source unreachable ({e}); skipping data-object deletion"
        );
    }

    // Restore points: the latest manifest and every manifest younger
    // than keep.
    let restore_points: Vec<u64> = manifests
        .iter()
        .filter(|(id, meta)| *id == latest || now - meta.last_modified < keep)
        .map(|(id, _)| *id)
        .collect();

    // Support, judged per restore point: the manifests each one's live
    // checkpoints pin. One level, matching the closure.
    let dest_ids: BTreeSet<u64> = manifests.iter().map(|(id, _)| *id).collect();
    let mut kept: BTreeSet<u64> = BTreeSet::new();
    let mut decoded = ManifestCache::default();
    for &id in &restore_points {
        // A restore point is kept whether or not it decodes: deleting
        // an unreadable latest manifest would destroy the watermark.
        kept.insert(id);
        let Some(manifest) = decoded.read(dest, id).await? else {
            continue;
        };
        let pins: Vec<u64> = manifest
            .checkpoints()
            .iter()
            .filter(|cp| layout::checkpoint_live(cp, now))
            .map(|cp| cp.manifest_id)
            .collect();
        for pinned in pins {
            // Support manifests are kept for their objects; their own
            // checkpoint entries may dangle, so a missing pinned
            // manifest is skipped, not chased.
            if dest_ids.contains(&pinned) {
                kept.insert(pinned);
            }
        }
    }

    // The objects every kept manifest references. An unreadable kept
    // manifest leaves its objects unenumerated, so data-object
    // deletion is skipped entirely rather than deleting what it might
    // reference.
    let mut kept_objects = ManifestObjects::default();
    let mut kept_decoded = true;
    for &id in kept.clone().iter() {
        match decoded.read(dest, id).await? {
            Some(manifest) => kept_objects.extend(&layout::manifest_objects(manifest)),
            None => {
                warn!(
                    target = target_name,
                    manifest = id,
                    "kept manifest unreadable"
                );
                kept_decoded = false;
            }
        }
    }
    // The WAL tail above the latest manifest is never pruned.
    let tail_floor = decoded
        .read(dest, latest)
        .await?
        .map(|m| m.next_wal_sst_id())
        .unwrap_or(0);

    let mut report = PruneReport {
        kept_manifests: kept.len() as u64,
        data_deletion_ran: spared.is_ok() && kept_decoded,
        ..Default::default()
    };

    // Delete the manifests first, then data objects.
    for (id, _) in &manifests {
        if kept.contains(id) {
            continue;
        }
        match dest
            .store
            .delete(&object_path(dest, &layout::manifest_rel(*id)))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => report.deleted_manifests += 1,
            Err(e) => return Err(e.into()),
        }
    }

    let Ok(spared) = spared else {
        info!(
            target = target_name,
            deleted_manifests = report.deleted_manifests,
            "pruned manifests only (source unreachable)"
        );
        return Ok(report);
    };
    if !kept_decoded {
        info!(
            target = target_name,
            deleted_manifests = report.deleted_manifests,
            "pruned manifests only (a kept manifest is unreadable)"
        );
        return Ok(report);
    }

    let guarded =
        |modified: DateTime<Utc>| now - modified <= min_age || floor.is_some_and(|f| modified >= f);
    for (ulid, meta) in &compacted {
        if kept_objects.compacted.contains(ulid)
            || spared.compacted.contains(ulid)
            || guarded(meta.last_modified)
        {
            continue;
        }
        match dest.store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => report.deleted_objects += 1,
            Err(e) => return Err(e.into()),
        }
        debug!(object = %meta.location, "pruned");
    }
    for (id, meta) in &wals {
        if *id >= tail_floor
            || kept_objects.wal.contains(id)
            || spared.wal.contains(id)
            || guarded(meta.last_modified)
        {
            continue;
        }
        match dest.store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => report.deleted_objects += 1,
            Err(e) => return Err(e.into()),
        }
        debug!(object = %meta.location, "pruned");
    }
    info!(
        target = target_name,
        kept = report.kept_manifests,
        deleted_manifests = report.deleted_manifests,
        deleted_objects = report.deleted_objects,
        "pruned"
    );
    Ok(report)
}

/// The source's latest closure: its latest manifest's objects plus the
/// objects of every manifest a checkpoint entry pins, expired entries
/// included. A pinned manifest source GC already deleted resolves
/// nowhere and is skipped.
async fn source_closure(source: &DatabaseHandle) -> Result<ManifestObjects, MirrorError> {
    let latest =
        source
            .admin
            .read_manifest(None)
            .await?
            .ok_or_else(|| MirrorError::NotADatabase {
                url: source.url.clone(),
            })?;
    let mut objects = layout::manifest_objects(&latest);
    let mut seen = BTreeSet::new();
    for cp in latest.checkpoints() {
        if cp.manifest_id == latest.id() || !seen.insert(cp.manifest_id) {
            continue;
        }
        if let Some(pinned) = source.admin.read_manifest(Some(cp.manifest_id)).await? {
            objects.extend(&layout::manifest_objects(&pinned));
        }
    }
    Ok(objects)
}
