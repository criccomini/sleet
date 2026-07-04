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

        let expected_services =
            db_services.or(fleet_services).unwrap_or_else(|| Service::ALL.to_vec());
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

proptest! {
    /// Mirror target layering: per-field last-writer-wins by target
    /// name, except `url` and `source_prefix` travel together (a layer
    /// that sets either overrides both), and `disabled` is an ordinary
    /// overridable field.
    #[test]
    fn mirror_targets_layer_per_field(
        fleet_url in prop::option::of(url_strategy()),
        fleet_prefix in prop::option::of(url_strategy()),
        db_url in prop::option::of(url_strategy()),
        db_prefix in prop::option::of(url_strategy()),
        fleet_poll_secs in prop::option::of(1u64..3600),
        db_disabled in prop::option::of(proptest::bool::ANY),
    ) {
        use sleet::config::{MirrorOverrides, MirrorTargetOverrides};
        let target = |url: &Option<String>, prefix: &Option<String>| MirrorTargetOverrides {
            url: url.clone(),
            source_prefix: prefix.clone(),
            ..Default::default()
        };
        let fleet = SleetConfig {
            database: DatabaseConfig {
                mirror: Some(MirrorOverrides {
                    targets: [(
                        "dr".to_string(),
                        MirrorTargetOverrides {
                            poll: fleet_poll_secs
                                .map(|s| HumanDuration(Duration::from_secs(s))),
                            ..target(&fleet_url, &fleet_prefix)
                        },
                    )]
                    .into(),
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let db = DatabaseConfig {
            mirror: Some(MirrorOverrides {
                targets: [(
                    "dr".to_string(),
                    MirrorTargetOverrides {
                        disabled: db_disabled,
                        ..target(&db_url, &db_prefix)
                    },
                )]
                .into(),
            }),
            ..Default::default()
        };
        let resolved = fleet.resolve(Some(&db));
        let t = &resolved.mirror.targets["dr"];
        // url and source_prefix travel together across layers.
        let (want_url, want_prefix) = if db_url.is_some() || db_prefix.is_some() {
            (db_url.clone(), db_prefix.clone())
        } else if fleet_url.is_some() || fleet_prefix.is_some() {
            (fleet_url.clone(), fleet_prefix.clone())
        } else {
            (None, None)
        };
        prop_assert_eq!(&t.url, &want_url);
        prop_assert_eq!(&t.source_prefix, &want_prefix);
        // Independent fields fall through per field.
        let want_poll = fleet_poll_secs.map_or(Duration::from_secs(10), Duration::from_secs);
        prop_assert_eq!(t.poll, want_poll);
        prop_assert_eq!(t.disabled, db_disabled.unwrap_or(false));
    }

    /// Prefix mapping is injective: distinct databases under the same
    /// prefix map to distinct destinations, and databases outside the
    /// prefix never apply.
    #[test]
    fn prefix_mapping_is_injective_and_scoped(
        segs in prop::collection::btree_set("[a-z0-9]{1,10}", 2..6),
        outside in "[a-z0-9]{1,10}",
    ) {
        use sleet::config::ResolvedMirrorTarget;
        use sleet::mirror;
        let target = ResolvedMirrorTarget {
            url: Some("s3://dr/mirrors".to_string()),
            source_prefix: Some("s3://data/tenants".to_string()),
            ..Default::default()
        };
        let mirror_config = sleet::config::ResolvedMirror {
            targets: [("dr".to_string(), target)].into(),
        };
        let mut destinations = BTreeSet::new();
        for seg in &segs {
            let db = format!("s3://data/tenants/{seg}");
            let applied = mirror::applied_targets(&db, &mirror_config);
            prop_assert_eq!(applied.len(), 1, "{}", db);
            prop_assert!(
                destinations.insert(applied[0].destination.clone()),
                "stripping a fixed prefix cannot send two databases to the same place"
            );
        }
        // Segment boundaries: a sibling of the prefix never matches.
        let sibling = format!("s3://data/tenants{outside}");
        prop_assert!(mirror::applied_targets(&sibling, &mirror_config).is_empty());
        // Unrelated buckets never match.
        prop_assert!(
            mirror::applied_targets("s3://other/tenants/x", &mirror_config).is_empty()
        );
    }
}

/// A random schedule of source writes, WAL-only flushes, checkpoint
/// churn, real GC passes, sync passes, aggressive prunes, and tail
/// steps must always leave a mirror that converges and verifies: the
/// closure enumeration and both prune guards hold across generated
/// histories, not just the handcrafted ones. Proptest shrinks any
/// failure to a minimal schedule.
#[derive(Clone, Debug)]
enum MirrorOp {
    /// Put a batch and flush the memtable (new L0, manifest commit).
    WriteFlush(u8),
    /// Put and flush the WAL only (tail material).
    WalOnly(u8),
    /// Create a named operator checkpoint.
    Checkpoint,
    /// Delete the oldest operator checkpoint, if any.
    DropCheckpoint,
    /// One real GC pass over the source with a zero age floor.
    Gc,
    /// One sync pass.
    Sync,
    /// One prune with retention aged to a millisecond.
    Prune,
    /// One WAL tail step.
    Tail,
}

fn mirror_op() -> impl Strategy<Value = MirrorOp> {
    prop_oneof![
        3 => (0u8..3).prop_map(MirrorOp::WriteFlush),
        2 => (0u8..3).prop_map(MirrorOp::WalOnly),
        1 => Just(MirrorOp::Checkpoint),
        1 => Just(MirrorOp::DropCheckpoint),
        2 => Just(MirrorOp::Gc),
        3 => Just(MirrorOp::Sync),
        2 => Just(MirrorOp::Prune),
        2 => Just(MirrorOp::Tail),
    ]
}

async fn run_mirror_schedule(ops: Vec<MirrorOp>) -> Result<(), String> {
    use object_store::path::Path as StorePath;
    use sleet::config::{ResolvedGc, ResolvedMirrorTarget};
    use sleet::mirror::{self, pass};
    use sleet::services::{self, DatabaseHandle};
    use std::sync::Arc;
    use std::time::Duration;

    let source = DatabaseHandle::from_parts(
        "memory:///src",
        Arc::new(object_store::memory::InMemory::new()),
        StorePath::from("src"),
    );
    let dest = DatabaseHandle::from_parts(
        "memory:///dst",
        Arc::new(object_store::memory::InMemory::new()),
        StorePath::from("dst"),
    );
    let settings = ResolvedMirrorTarget {
        keep: Some(Duration::from_millis(1)),
        min_age: Duration::ZERO,
        ..ResolvedMirrorTarget::default()
    };
    let mut gc = ResolvedGc::default();
    for dir in [
        &mut gc.manifest,
        &mut gc.wal,
        &mut gc.compacted,
        &mut gc.compactions,
    ] {
        dir.min_age = Duration::ZERO;
    }
    gc.wal_fence.enabled = false;
    gc.detach.enabled = false;

    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .map_err(|e| format!("open writer: {e}"))?;
    let mut batch = 0u32;
    let mut checkpoints: Vec<uuid::Uuid> = Vec::new();
    let mut tail: Option<pass::Tail> = None;
    fn err<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> String {
        move |e| format!("{what}: {e}")
    }

    for op in &ops {
        match op {
            MirrorOp::WriteFlush(n) => {
                for i in 0..=*n {
                    writer
                        .put(format!("k-{batch}-{i}").as_bytes(), vec![*n; 64].as_slice())
                        .await
                        .map_err(err("put"))?;
                }
                batch += 1;
                writer
                    .flush_with_options(slatedb::config::FlushOptions {
                        flush_type: slatedb::config::FlushType::MemTable,
                    })
                    .await
                    .map_err(err("memtable flush"))?;
            }
            MirrorOp::WalOnly(n) => {
                for i in 0..=*n {
                    writer
                        .put(format!("w-{batch}-{i}").as_bytes(), b"wal".as_slice())
                        .await
                        .map_err(err("wal put"))?;
                }
                batch += 1;
                writer.flush().await.map_err(err("wal flush"))?;
            }
            MirrorOp::Checkpoint => {
                let result = source
                    .admin
                    .create_detached_checkpoint(&slatedb::config::CheckpointOptions {
                        lifetime: None,
                        source: None,
                        name: Some(format!("op-{batch}")),
                    })
                    .await
                    .map_err(err("checkpoint"))?;
                checkpoints.push(result.id);
            }
            MirrorOp::DropCheckpoint => {
                if !checkpoints.is_empty() {
                    let id = checkpoints.remove(0);
                    source
                        .admin
                        .delete_checkpoint(id)
                        .await
                        .map_err(err("delete checkpoint"))?;
                }
            }
            MirrorOp::Gc => {
                source
                    .admin
                    .run_gc_once(services::gc_options(&gc))
                    .await
                    .map_err(err("gc"))?;
            }
            MirrorOp::Sync => {
                let outcome = mirror::sync_pass(&source, &dest, "dr", &settings, None)
                    .await
                    .map_err(err("sync"))?;
                match &mut tail {
                    Some(t) => t.advance_floor(outcome.next_wal_sst_id),
                    None => {
                        tail = Some(
                            pass::Tail::start(&dest, outcome.next_wal_sst_id)
                                .await
                                .map_err(err("tail start"))?,
                        )
                    }
                }
            }
            MirrorOp::Prune => {
                mirror::prune(&source, &dest, "dr", &settings)
                    .await
                    .map_err(err("prune"))?;
            }
            MirrorOp::Tail => {
                if let Some(t) = &mut tail {
                    t.step(&source, &dest).await.map_err(err("tail"))?;
                }
            }
        }
    }
    writer.close().await.map_err(err("close"))?;

    // Converge (the unpin dance takes two quiet passes) and check the
    // oracle: the watermark reaches the head and the whole target
    // verifies, restore points and closures intact.
    for _ in 0..3 {
        mirror::sync_pass(&source, &dest, "dr", &settings, None)
            .await
            .map_err(err("converging sync"))?;
    }
    let src_head = source
        .admin
        .read_manifest(None)
        .await
        .map_err(err("src head"))?
        .ok_or("source has no manifest")?;
    let dst_head = dest
        .admin
        .read_manifest(None)
        .await
        .map_err(err("dst head"))?
        .ok_or("destination has no manifest")?;
    if src_head.id() != dst_head.id() {
        return Err(format!(
            "not converged: source {} target {}",
            src_head.id(),
            dst_head.id()
        ));
    }
    let verified = mirror::verify(&source, &dest, None, mirror::Depth::Bytes)
        .await
        .map_err(err("verify"))?;
    if !verified.ok() {
        return Err(format!("verification failed: {:#?}", verified.points));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 24,
        ..ProptestConfig::default()
    })]

    #[test]
    fn random_mirror_schedules_converge_and_verify(
        ops in prop::collection::vec(mirror_op(), 1..14),
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_mirror_schedule(ops));
        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}

proptest! {
    /// The frozen layout name formats are inverse to their parsers,
    /// and never cross-parse as another directory's names.
    #[test]
    fn layout_names_roundtrip_and_never_cross_parse(
        id in any::<u64>(),
        ulid in "[0-9A-HJKMNP-TV-Z]{26}",
    ) {
        use sleet::mirror::layout;
        let base = |rel: &str| rel.rsplit('/').next().unwrap().to_string();

        let manifest = base(&layout::manifest_rel(id));
        prop_assert_eq!(layout::parse_manifest_name(&manifest), Some(id));
        prop_assert_eq!(layout::parse_wal_name(&manifest), None);
        prop_assert_eq!(layout::parse_compacted_name(&manifest), None);

        let wal = base(&layout::wal_rel(id));
        prop_assert_eq!(layout::parse_wal_name(&wal), Some(id));
        prop_assert_eq!(layout::parse_manifest_name(&wal), None);
        // A WAL id is never 26 digits, so it cannot read as a ULID.
        prop_assert_eq!(layout::parse_compacted_name(&wal), None);

        let compacted = base(&layout::compacted_rel(&ulid));
        prop_assert_eq!(layout::parse_compacted_name(&compacted), Some(ulid));
        prop_assert_eq!(layout::parse_manifest_name(&compacted), None);
        prop_assert_eq!(layout::parse_wal_name(&compacted), None);
    }

    /// `ManifestObjects::difference` and `extend` obey the set laws the
    /// pass's diff-against-watermark subtraction relies on: the
    /// difference is disjoint from the subtrahend, within the minuend,
    /// and extending the subtrahend by it covers the minuend.
    #[test]
    fn manifest_objects_difference_is_a_set_difference(
        a_compacted in prop::collection::btree_set("[0-9A-HJKMNP-TV-Z]{26}", 0..8),
        b_compacted in prop::collection::btree_set("[0-9A-HJKMNP-TV-Z]{26}", 0..8),
        a_wal in prop::collection::btree_set(any::<u32>(), 0..8),
        b_wal in prop::collection::btree_set(any::<u32>(), 0..8),
    ) {
        use sleet::mirror::layout::ManifestObjects;
        let a = ManifestObjects {
            compacted: a_compacted,
            wal: a_wal.iter().map(|&w| w as u64).collect(),
        };
        let mut b = ManifestObjects {
            compacted: b_compacted,
            wal: b_wal.iter().map(|&w| w as u64).collect(),
        };
        let d = a.difference(&b);
        prop_assert!(d.compacted.is_disjoint(&b.compacted));
        prop_assert!(d.wal.is_disjoint(&b.wal));
        prop_assert!(d.compacted.is_subset(&a.compacted));
        prop_assert!(d.wal.is_subset(&a.wal));
        prop_assert_eq!(d.len(), d.compacted.len() + d.wal.len());
        b.extend(&d);
        prop_assert!(a.compacted.is_subset(&b.compacted));
        prop_assert!(a.wal.is_subset(&b.wal));
    }

    /// `--at` parsing: manifest ids and RFC 3339 timestamps resolve to
    /// their restore point, and non-timestamp text is refused.
    #[test]
    fn restore_points_parse_ids_and_timestamps(
        id in any::<u64>(),
        secs in 0i64..4_102_444_800,
        garbage in "[a-z ]{1,20}",
    ) {
        use sleet::mirror::RestorePoint;
        prop_assert!(matches!(
            RestorePoint::parse(&id.to_string()),
            Ok(RestorePoint::Manifest(parsed)) if parsed == id
        ));
        let ts = chrono::DateTime::from_timestamp(secs, 0).unwrap();
        prop_assert!(matches!(
            RestorePoint::parse(&ts.to_rfc3339()),
            Ok(RestorePoint::Time(parsed)) if parsed == ts
        ));
        prop_assert!(RestorePoint::parse(&garbage).is_err());
    }

    /// Verify record names are injective per `(database, target)`:
    /// distinct pairs never collide at the fleet root. Target names
    /// carry no `.`, so the encoded database and the target split
    /// unambiguously.
    #[test]
    fn verify_paths_are_injective(
        db_a in url_strategy(),
        db_b in url_strategy(),
        target_a in "[a-z0-9_-]{1,12}",
        target_b in "[a-z0-9_-]{1,12}",
    ) {
        use object_store::path::Path as StorePath;
        use sleet::root::FleetRoot;
        let root = FleetRoot::from_parts(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            StorePath::from("fleet"),
            "memory:///f",
        );
        let path_a = root.verify_path(&db_a, &target_a);
        let path_b = root.verify_path(&db_b, &target_b);
        prop_assert!(path_a.as_ref().starts_with("fleet/verify/"));
        if (&db_a, &target_a) != (&db_b, &target_b) {
            prop_assert_ne!(path_a, path_b);
        } else {
            prop_assert_eq!(path_a, path_b);
        }
    }
}
