//! Property-based tests: registry naming, placement invariants, and
//! config resolution as field-wise last-writer-wins.

use std::collections::BTreeSet;
use std::time::Duration;

use proptest::prelude::*;
use sleet::config::{
    DatabaseConfig, GcDirectoryOverrides, GcOverrides, HumanDuration, Service, SleetConfig,
    WorkersOverrides,
};
use sleet::{placement, registry};

/// A database URL from parts the schemes accept; paths may contain any
/// non-`/` unicode.
fn url_strategy() -> impl Strategy<Value = String> {
    let scheme = prop::sample::select(vec!["s3", "gs", "az", "file", "memory"]);
    let bucket = "[a-z][a-z0-9-]{0,20}";
    let segment = "[^/\u{0}]{1,12}";
    (
        scheme,
        bucket.prop_map(String::from),
        prop::collection::vec(segment, 0..4),
    )
        .prop_map(|(scheme, bucket, segments)| {
            let path = segments.join("/");
            format!("{scheme}://{bucket}/{path}")
        })
}

fn node_ids() -> impl Strategy<Value = Vec<String>> {
    prop::collection::btree_set("[a-z0-9-]{1,12}", 1..8).prop_map(|set| set.into_iter().collect())
}

fn service() -> impl Strategy<Value = Service> {
    prop::sample::select(Service::ALL.to_vec())
}

proptest! {
    /// Canonicalization is idempotent, and registry names round-trip
    /// the canonical URL without ever containing `/`.
    #[test]
    fn registry_names_roundtrip(url in url_strategy()) {
        // Arbitrary URLs may be rejected (bad length etc.); accepted
        // ones must round-trip.
        if let Ok(canonical) = registry::canonicalize_url(&url) {
            prop_assert_eq!(registry::canonicalize_url(&canonical).unwrap(), canonical.clone());
            let name = registry::file_name(&canonical);
            prop_assert!(!name.contains('/'), "{}", name);
            prop_assert!(name.len() <= 1024);
            prop_assert_eq!(registry::parse_file_name(&name), Some(canonical));
        }
    }

    /// The ranking is a permutation of the candidates; owners are its
    /// prefix; every node computes the same answer.
    #[test]
    fn ranking_is_a_deterministic_permutation(
        db in url_strategy(),
        service in service(),
        nodes in node_ids(),
        count in 1usize..6,
    ) {
        let refs: Vec<&str> = nodes.iter().map(String::as_str).collect();
        let ranked = placement::rank(&db, service, &refs);
        prop_assert_eq!(
            ranked.iter().copied().collect::<BTreeSet<_>>(),
            refs.iter().copied().collect::<BTreeSet<_>>()
        );
        let owners = placement::owners(&db, service, count, &refs);
        prop_assert_eq!(&owners[..], &ranked[..count.min(ranked.len())]);
        // Determinism: same inputs, same answer, any evaluation order.
        prop_assert_eq!(placement::rank(&db, service, &refs), ranked);
    }

    /// Removing a node moves only that node's pairs: the relative order
    /// of the remaining nodes is unchanged.
    #[test]
    fn removal_is_minimally_disruptive(
        db in url_strategy(),
        service in service(),
        nodes in node_ids(),
        pick in any::<prop::sample::Index>(),
    ) {
        let refs: Vec<&str> = nodes.iter().map(String::as_str).collect();
        let full = placement::rank(&db, service, &refs);
        let removed = refs[pick.index(refs.len())];
        let remaining: Vec<&str> = refs.iter().copied().filter(|&n| n != removed).collect();
        let expected: Vec<&str> = full.into_iter().filter(|&n| n != removed).collect();
        prop_assert_eq!(placement::rank(&db, service, &remaining), expected);
    }

    /// Config resolution is field-wise last-writer-wins across the
    /// three layers: built-ins, `[database]`, and the registry file.
    #[test]
    fn resolution_is_fieldwise_lww(
        fleet_count in prop::option::of(1u32..100),
        db_count in prop::option::of(1u32..100),
        fleet_services in prop::option::of(prop::collection::vec(service(), 0..3)),
        db_services in prop::option::of(prop::collection::vec(service(), 0..3)),
        db_min_age in prop::option::of(1u64..10_000),
    ) {
        let fleet = SleetConfig {
            database: DatabaseConfig {
                services: fleet_services.clone(),
                compaction_workers: fleet_count.map(|count| WorkersOverrides {
                    count: Some(count),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let db = DatabaseConfig {
            services: db_services.clone(),
            compaction_workers: db_count.map(|count| WorkersOverrides {
                count: Some(count),
                ..Default::default()
            }),
            gc: db_min_age.map(|secs| GcOverrides {
                manifest: Some(GcDirectoryOverrides {
                    min_age: Some(HumanDuration(Duration::from_secs(secs))),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = fleet.resolve(Some(&db));

        let expected_count = db_count.or(fleet_count).unwrap_or(1);
        prop_assert_eq!(resolved.workers.count, expected_count);

        let expected_services = db_services.or(fleet_services).unwrap_or_else(|| {
            vec![
                Service::Gc,
                Service::CompactorCoordinator,
                Service::CompactionWorkers,
            ]
        });
        prop_assert_eq!(resolved.services, expected_services);

        let expected_min_age = db_min_age.map_or(300, |s| s);
        prop_assert_eq!(
            resolved.gc.manifest.min_age,
            Duration::from_secs(expected_min_age)
        );
        // A field set only in one layer never disturbs the others:
        // built-ins hold for everything unset.
        prop_assert_eq!(resolved.coordinator.poll_interval, Duration::from_secs(5));
    }
}
