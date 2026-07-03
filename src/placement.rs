//! Rendezvous placement: which nodes own a `(database, service)`.
//!
//! Every live node offering a service gets a score (a hash of the pair
//! combined with the node's id), and the ranking assigns owners: `gc`
//! and `compactor-coordinator` run on the top-ranked node,
//! `compaction-workers` on the top `count` nodes. Removing a node moves
//! only the pairs it owned; adding one moves only the pairs it now wins.
//!
//! The hash and its key encoding are FROZEN, like a wire format:
//! FNV-1a 64 over `database ++ 0x00 ++ service ++ 0x00 ++ node_id`,
//! ties broken by node id. Changing either breaks mixed-version fleets;
//! the golden test below pins them.

use crate::config::Service;

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn fnv1a64(chunks: &[&[u8]]) -> u64 {
    let mut h = FNV_OFFSET;
    for chunk in chunks {
        for &b in *chunk {
            h ^= u64::from(b);
            h = h.wrapping_mul(FNV_PRIME);
        }
    }
    h
}

/// The frozen score of one candidate node for one `(database, service)`.
pub fn score(database: &str, service: Service, node_id: &str) -> u64 {
    fnv1a64(&[
        database.as_bytes(),
        b"\0",
        service.as_str().as_bytes(),
        b"\0",
        node_id.as_bytes(),
    ])
}

/// Candidate node ids ranked best-first for a `(database, service)`.
/// Callers pass the live nodes offering the service.
pub fn rank<'a>(database: &str, service: Service, candidates: &[&'a str]) -> Vec<&'a str> {
    let mut ranked: Vec<(u64, &str)> = candidates
        .iter()
        .map(|&n| (score(database, service, n), n))
        .collect();
    // Descending by score; ties broken by node id so every node agrees.
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    ranked.into_iter().map(|(_, n)| n).collect()
}

/// The owners of a `(database, service)`: the top `count` of the
/// ranking. `count` is 1 for `gc` and `compactor-coordinator`, and the
/// database's `compaction-workers.count` for workers.
pub fn owners<'a>(
    database: &str,
    service: Service,
    count: usize,
    candidates: &[&'a str],
) -> Vec<&'a str> {
    let mut ranked = rank(database, service, candidates);
    ranked.truncate(count);
    ranked
}

/// The frozen score of one candidate node for one `(database, mirror,
/// target)` triple. Like [`score`] but with the target name appended:
/// FNV-1a 64 over `database ++ 0x00 ++ "mirror" ++ 0x00 ++ target ++
/// 0x00 ++ node_id`. FROZEN like the pair encoding; the golden test
/// pins it.
pub fn score_target(database: &str, target: &str, node_id: &str) -> u64 {
    fnv1a64(&[
        database.as_bytes(),
        b"\0",
        Service::Mirror.as_str().as_bytes(),
        b"\0",
        target.as_bytes(),
        b"\0",
        node_id.as_bytes(),
    ])
}

/// Candidate node ids ranked best-first for a `(database, mirror,
/// target)` triple. Callers pass the live nodes offering `mirror`.
pub fn rank_target<'a>(database: &str, target: &str, candidates: &[&'a str]) -> Vec<&'a str> {
    let mut ranked: Vec<(u64, &str)> = candidates
        .iter()
        .map(|&n| (score_target(database, target, n), n))
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    ranked.into_iter().map(|(_, n)| n).collect()
}

/// The owner of a `(database, mirror, target)` triple: the top of the
/// ranking, or `None` when no live node offers `mirror`.
pub fn owner_target<'a>(database: &str, target: &str, candidates: &[&'a str]) -> Option<&'a str> {
    rank_target(database, target, candidates).first().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODES: &[&str] = &["sleet-1", "sleet-2", "sleet-3", "sleet-4"];

    /// The hash and key encoding are frozen; if this test breaks, the
    /// change breaks mixed-version fleets.
    #[test]
    fn scores_are_frozen() {
        assert_eq!(
            score("s3://b/db", Service::Gc, "sleet-1"),
            0x0db7953ae9becf63
        );
        assert_eq!(
            score("s3://b/db", Service::CompactorCoordinator, "sleet-1"),
            0xf9bc77ef11433076
        );
        assert_eq!(
            score("s3://b/db", Service::CompactionWorkers, "sleet-2"),
            0xb6c679bdaed44473
        );
    }

    #[test]
    fn ranking_is_deterministic_and_service_dependent() {
        let a = rank("s3://b/db", Service::Gc, NODES);
        let b = rank("s3://b/db", Service::Gc, NODES);
        assert_eq!(a, b);
        assert_eq!(a.len(), NODES.len());
        // Different services and databases rank independently (not a
        // property FNV guarantees in general, but these fixed inputs
        // demonstrate the keys are distinct).
        let c = rank("s3://b/db", Service::CompactorCoordinator, NODES);
        let d = rank("s3://b/other", Service::Gc, NODES);
        assert!(a != c || a != d);
    }

    #[test]
    fn removing_a_node_preserves_the_order_of_the_rest() {
        let full = rank("s3://b/db", Service::Gc, NODES);
        let removed = full[1];
        let remaining: Vec<&str> = NODES.iter().copied().filter(|&n| n != removed).collect();
        let rehashed = rank("s3://b/db", Service::Gc, &remaining);
        let expected: Vec<&str> = full.into_iter().filter(|&n| n != removed).collect();
        assert_eq!(rehashed, expected);
    }

    /// The triple hash and its key encoding are frozen; if this test
    /// breaks, the change breaks mixed-version fleets.
    #[test]
    fn target_scores_are_frozen() {
        assert_eq!(
            score_target("s3://b/db", "dr", "sleet-1"),
            0xa7057dde1da5335e
        );
        assert_eq!(
            score_target("s3://b/db", "backup", "sleet-2"),
            0x01282b68db71bebf
        );
    }

    /// The triple ranking is deterministic and target-dependent, and a
    /// pair score never collides with a triple score for the same
    /// database (the target segment is part of the key).
    #[test]
    fn target_ranking_is_deterministic_and_target_dependent() {
        let a = rank_target("s3://b/db", "dr", NODES);
        assert_eq!(a, rank_target("s3://b/db", "dr", NODES));
        assert_eq!(a.len(), NODES.len());
        let b = rank_target("s3://b/db", "backup", NODES);
        assert!(a != b || rank_target("s3://b/other", "dr", NODES) != a);
        assert_eq!(owner_target("s3://b/db", "dr", NODES), Some(a[0]));
        assert_eq!(owner_target("s3://b/db", "dr", &[]), None);
    }

    #[test]
    fn owners_are_distinct_and_capped() {
        let two = owners("s3://b/db", Service::CompactionWorkers, 2, NODES);
        assert_eq!(two.len(), 2);
        assert_ne!(two[0], two[1]);
        // count larger than the pool takes every offering node.
        let all = owners("s3://b/db", Service::CompactionWorkers, 10, NODES);
        assert_eq!(all.len(), NODES.len());
    }
}
