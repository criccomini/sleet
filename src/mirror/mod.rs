//! The mirror service (RFC 0002): replicate a SlateDB database
//! into another object-store root by copying its immutable objects to
//! the same relative names and committing manifests as the atomic
//! step.
//!
//! `layout` holds the frozen SlateDB layout facts and closure
//! enumeration, `pass` the sync pass and WAL tail, `copier` the
//! builtin/rclone/external data movers, `prune` restore-point
//! retention, and `restore` the backup restore. This module maps
//! targets to destinations and runs the continuous and periodic mode
//! loops.

pub mod copier;
pub mod layout;
pub mod pass;
pub mod prune;
pub mod restore;

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::{MirrorMode, ResolvedMirror, ResolvedMirrorTarget};
use crate::registry;
use crate::services::DatabaseHandle;

pub use pass::{PassOutcome, sync_pass};
pub use prune::{PruneReport, prune};
pub use restore::{RestoreOutcome, RestorePoint, restore};

/// While a continuous mirror is idle, polling backs off exponentially
/// from the target's `poll` up to this ceiling.
const IDLE_POLL_MAX: Duration = Duration::from_secs(300);

/// A caught-up continuous mirror with retention set still prunes this
/// often, so restore points age out without waiting for a commit.
const IDLE_PRUNE_EVERY: Duration = Duration::from_secs(600);

/// A mirror task failure.
#[derive(Debug, thiserror::Error)]
pub enum MirrorError {
    /// A URL was rejected.
    #[error(transparent)]
    Url(#[from] registry::UrlError),
    /// An object-store operation failed.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    /// A SlateDB read or checkpoint operation failed.
    #[error(transparent)]
    SlateDb(#[from] slatedb::Error),
    /// The source has no manifest.
    #[error("{url} is not a SlateDB database (no manifest)")]
    NotADatabase {
        /// The URL read.
        url: String,
    },
    /// The source cannot be mirrored (RFC 0002 §2).
    #[error("{url} cannot be a mirror source: {reason}")]
    ExcludedSource {
        /// The source URL.
        url: String,
        /// Why it is excluded.
        reason: String,
    },
    /// The destination's history is ahead of the source's: something
    /// else has been committing manifests there.
    #[error(
        "destination {destination} has diverged: its manifest {dest_id} is ahead of the \
         source's {source_id}; it is not a mirror of this source"
    )]
    Diverged {
        /// The destination root.
        destination: String,
        /// The destination's latest manifest id.
        dest_id: u64,
        /// The source's latest manifest id.
        source_id: u64,
    },
    /// The pass kept restarting without completing.
    #[error("mirror pass for target {target} kept restarting: {reason}")]
    Stalled {
        /// The target name.
        target: String,
        /// The last restart's reason.
        reason: String,
    },
    /// rclone could not be run or exited nonzero.
    #[error("rclone: {0}")]
    Rclone(String),
    /// The restore destination already holds objects.
    #[error("destination {url} is not empty; restore never deletes, pick a fresh root")]
    DestinationNotEmpty {
        /// The destination root.
        url: String,
    },
    /// `--at` does not name a usable restore point.
    #[error("no restore point for --at {at}: {reason}")]
    NoRestorePoint {
        /// The `--at` value as given.
        at: String,
        /// Why it does not resolve.
        reason: String,
    },
}

/// One mirror target applied to one database: the computed destination
/// plus the resolved settings.
#[derive(Clone, Debug)]
pub struct AppliedTarget {
    /// The target's name, its identity for placement and the source
    /// pin checkpoint.
    pub name: String,
    /// The destination root for this database.
    pub destination: String,
    /// The resolved target settings.
    pub settings: ResolvedMirrorTarget,
}

/// The enabled targets that apply to a database, with their computed
/// destinations (RFC 0002 §9). A target with `source_prefix` maps
/// every database under the prefix; one without is an exact
/// destination. A database no target applies to does not mirror.
pub fn applied_targets(db_url: &str, mirror: &ResolvedMirror) -> Vec<AppliedTarget> {
    mirror
        .targets
        .iter()
        .filter_map(|(name, settings)| {
            let destination = destination_for(db_url, settings)?;
            Some(AppliedTarget {
                name: name.clone(),
                destination,
                settings: settings.clone(),
            })
        })
        .collect()
}

/// The destination an enabled target sends a database to, if it
/// applies. Prefixes match at path-segment boundaries, and stripping a
/// fixed prefix cannot send two databases to the same place.
fn destination_for(db_url: &str, settings: &ResolvedMirrorTarget) -> Option<String> {
    if settings.disabled {
        return None;
    }
    let url = registry::canonicalize_url(settings.url.as_deref()?).ok()?;
    let destination = match &settings.source_prefix {
        None => url,
        Some(prefix) => {
            let prefix = registry::canonicalize_url(prefix).ok()?;
            let rest = db_url.strip_prefix(&prefix)?;
            if !rest.is_empty() && !rest.starts_with('/') {
                return None;
            }
            format!("{url}{rest}")
        }
    };
    // A destination equal to the source would "mirror" a database onto
    // itself; never apply.
    (destination != db_url).then_some(destination)
}

/// Run one `(database, target)` mirror assignment until cancelled: the
/// continuous or periodic loop per the target's mode. `jobs` is the
/// node-wide `--max-mirror-jobs` cap; a permit is held while a pass
/// or prune runs, not while polling idle.
pub async fn run_mirror(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target: &AppliedTarget,
    jobs: Arc<tokio::sync::Semaphore>,
    rclone: Option<String>,
    token: CancellationToken,
) -> Result<(), MirrorError> {
    info!(
        database = %source.url,
        target = %target.name,
        destination = %dest.url,
        mode = ?target.settings.mode,
        "mirror task starting"
    );
    match target.settings.mode {
        MirrorMode::Continuous => {
            run_continuous(source, dest, target, jobs, rclone.as_deref(), token).await
        }
        MirrorMode::Periodic => {
            run_periodic(source, dest, target, jobs, rclone.as_deref(), token).await
        }
    }
}

/// One pass plus a prune when retention is set: what the one-shot
/// `sleet mirror sync` runs, and the periodic loop's unit of work.
pub async fn sync_once(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target: &AppliedTarget,
    rclone: Option<&str>,
) -> Result<(PassOutcome, PruneReport), MirrorError> {
    let outcome = sync_pass(source, dest, &target.name, &target.settings, rclone).await?;
    let report = prune(source, dest, &target.name, &target.settings).await?;
    Ok((outcome, report))
}

async fn run_continuous(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target: &AppliedTarget,
    jobs: Arc<tokio::sync::Semaphore>,
    rclone: Option<&str>,
    token: CancellationToken,
) -> Result<(), MirrorError> {
    let settings = &target.settings;
    let max_idle = IDLE_POLL_MAX.max(settings.poll);
    let mut idle = settings.poll;
    // The committed watermark, cached so a caught-up mirror costs one
    // source LIST per poll; recovered from the target by the pass
    // whenever a pass runs.
    let mut watermark: Option<u64> = None;
    let mut tail: Option<pass::Tail> = None;
    let mut last_prune = Instant::now();
    loop {
        if token.is_cancelled() {
            return Ok(());
        }
        let mut active = false;
        let source_head = layout::max_manifest_id(source).await?;
        if source_head.is_some() && source_head != watermark {
            let _permit = tokio::select! {
                _ = token.cancelled() => return Ok(()),
                permit = jobs.clone().acquire_owned() => permit.expect("semaphore never closes"),
            };
            let outcome = sync_pass(source, dest, &target.name, settings, rclone).await?;
            active |= outcome.committed;
            watermark = Some(outcome.head);
            match &mut tail {
                Some(tail) => tail.advance_floor(outcome.next_wal_sst_id),
                None => tail = Some(pass::Tail::start(dest, outcome.next_wal_sst_id).await?),
            }
            if outcome.committed || last_prune.elapsed() >= IDLE_PRUNE_EVERY {
                prune(source, dest, &target.name, settings).await?;
                last_prune = Instant::now();
            }
        } else if settings.keep.is_some() && last_prune.elapsed() >= IDLE_PRUNE_EVERY {
            let _permit = tokio::select! {
                _ = token.cancelled() => return Ok(()),
                permit = jobs.clone().acquire_owned() => permit.expect("semaphore never closes"),
            };
            prune(source, dest, &target.name, settings).await?;
            last_prune = Instant::now();
        }
        if let Some(tail) = &mut tail {
            active |= tail.step(source, dest).await? > 0;
        }
        idle = if active {
            settings.poll
        } else {
            (idle * 2).min(max_idle)
        };
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            _ = tokio::time::sleep(idle) => {}
        }
    }
}

async fn run_periodic(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target: &AppliedTarget,
    jobs: Arc<tokio::sync::Semaphore>,
    rclone: Option<&str>,
    token: CancellationToken,
) -> Result<(), MirrorError> {
    let settings = &target.settings;
    let interval = chrono::Duration::from_std(settings.interval).expect("interval fits");
    // How often a due-but-idle target re-checks the source; a fresh
    // commit resets the wait to the full interval.
    let check = (settings.interval / 20).clamp(Duration::from_secs(60), Duration::from_secs(3600));
    loop {
        if token.is_cancelled() {
            return Ok(());
        }
        // Stateless scheduling: a pass runs when the target's latest
        // manifest's LastModified is older than the interval.
        let latest = layout::list_manifests(dest).await?.pop();
        let age = latest.map(|(_, meta)| chrono::Utc::now() - meta.last_modified);
        let sleep = match age {
            Some(age) if age < interval => (interval - age)
                .to_std()
                .unwrap_or(settings.interval)
                .min(settings.interval),
            _ => {
                let _permit = tokio::select! {
                    _ = token.cancelled() => return Ok(()),
                    permit = jobs.clone().acquire_owned() => permit.expect("semaphore never closes"),
                };
                let (outcome, _) = sync_once(source, dest, target, rclone).await?;
                if outcome.committed {
                    settings.interval
                } else {
                    check.min(settings.interval)
                }
            }
        };
        tokio::select! {
            _ = token.cancelled() => return Ok(()),
            _ = tokio::time::sleep(sleep) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ResolvedMirrorTarget;

    fn target(url: &str, prefix: Option<&str>) -> ResolvedMirrorTarget {
        ResolvedMirrorTarget {
            url: Some(url.to_string()),
            source_prefix: prefix.map(String::from),
            ..ResolvedMirrorTarget::default()
        }
    }

    /// §9: a prefix target maps every database under the prefix to the
    /// destination plus the stripped path, matching at path-segment
    /// boundaries.
    #[test]
    fn prefix_targets_map_and_scope() {
        let t = target("s3://dr-bucket/mirrors", Some("s3://user-data"));
        assert_eq!(
            destination_for("s3://user-data/tenant1/db1", &t).as_deref(),
            Some("s3://dr-bucket/mirrors/tenant1/db1")
        );
        // Segment boundary: s3://user-data does not capture
        // s3://user-database/x.
        assert_eq!(destination_for("s3://user-database/x", &t), None);
        // The prefix itself maps to the bare destination.
        assert_eq!(
            destination_for("s3://user-data", &t).as_deref(),
            Some("s3://dr-bucket/mirrors")
        );
    }

    /// An exact target (no prefix) applies to any database in its
    /// scope; a disabled target applies to none; a self-destination
    /// never applies.
    #[test]
    fn exact_disabled_and_self_targets() {
        let t = target("s3://backup/db1", None);
        assert_eq!(
            destination_for("s3://data/db1", &t).as_deref(),
            Some("s3://backup/db1")
        );
        let off = ResolvedMirrorTarget {
            disabled: true,
            ..t.clone()
        };
        assert_eq!(destination_for("s3://data/db1", &off), None);
        assert_eq!(destination_for("s3://backup/db1", &t), None, "self");
        let no_url = ResolvedMirrorTarget {
            url: None,
            ..ResolvedMirrorTarget::default()
        };
        assert_eq!(destination_for("s3://data/db1", &no_url), None);
    }

    /// applied_targets filters and maps the whole resolved table.
    #[test]
    fn applied_targets_filter_and_map() {
        let mut mirror = ResolvedMirror::default();
        mirror
            .targets
            .insert("dr".into(), target("s3://dr/mirrors", Some("s3://data")));
        mirror.targets.insert(
            "off".into(),
            ResolvedMirrorTarget {
                disabled: true,
                ..target("s3://elsewhere", None)
            },
        );
        mirror
            .targets
            .insert("other".into(), target("s3://x", Some("s3://other-bucket")));
        let applied = applied_targets("s3://data/db1", &mirror);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].name, "dr");
        assert_eq!(applied[0].destination, "s3://dr/mirrors/db1");
    }
}
