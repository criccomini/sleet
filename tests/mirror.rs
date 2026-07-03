//! Mirror integration tests against real SlateDB databases: the sync
//! pass invariants (DESIGN-MIRROR §3-4), the WAL tail, retention and
//! both prune guards (§7), copiers (§8), verify (§10), restore, and
//! failover by opening the target as an ordinary database (§3).

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use sleet::config::{CopierKind, ResolvedMirrorTarget};
use sleet::mirror::{self, RestorePoint, layout, pass};
use sleet::root::Clock;
use sleet::services::DatabaseHandle;
use sleet::testing::{Op, TestClock, TestStore};

/// A source database handle over its own in-memory store.
fn handle(store: Arc<dyn object_store::ObjectStore>, url: &str, path: &str) -> DatabaseHandle {
    DatabaseHandle::from_parts(url, store, StorePath::from(path))
}

/// Write `rounds` batches into a real SlateDB database at the handle's
/// root, each left as an L0 SST; background maintenance is disabled,
/// as under sleet.
async fn seed(db: &DatabaseHandle, rounds: u8) {
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

fn target_settings() -> ResolvedMirrorTarget {
    ResolvedMirrorTarget::default()
}

async fn names(db: &DatabaseHandle, dir: &str) -> Vec<String> {
    use futures::TryStreamExt;
    let prefix = StorePath::from(format!("{}/{dir}", db.path));
    let metas: Vec<object_store::ObjectMeta> =
        match db.store.list(Some(&prefix)).try_collect().await {
            Ok(metas) => metas,
            Err(object_store::Error::NotFound { .. }) => Vec::new(),
            Err(e) => panic!("{e}"),
        };
    let mut names: Vec<String> = metas
        .iter()
        .filter_map(|m| m.location.filename().map(String::from))
        .collect();
    names.sort();
    names
}

/// The full closure of the source's latest manifest exists at the
/// target under the same relative names, and the target's latest
/// manifest byte-equals the source's.
async fn assert_mirrored(source: &DatabaseHandle, dest: &DatabaseHandle) {
    let src = source.admin.read_manifest(None).await.unwrap().unwrap();
    let dst = dest.admin.read_manifest(None).await.unwrap().unwrap();
    assert_eq!(src.id(), dst.id(), "watermark at the source head");
    let src_bytes = source
        .store
        .get(&layout::object_path(
            source,
            &layout::manifest_rel(src.id()),
        ))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let dst_bytes = dest
        .store
        .get(&layout::object_path(dest, &layout::manifest_rel(dst.id())))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(src_bytes, dst_bytes, "manifests byte-copied");
    let objects = layout::manifest_objects(&src);
    for rel in objects.rel_names() {
        dest.store
            .get(&layout::object_path(dest, &rel))
            .await
            .unwrap_or_else(|e| panic!("{rel} missing at target: {e}"));
    }
}

/// No pin checkpoint outlives its pass (§4 step 6), except expired
/// leftovers a crashed pass abandons to source GC.
async fn assert_no_live_pins(source: &DatabaseHandle, target_name: &str) {
    let pins = source
        .admin
        .list_checkpoints(Some(&pass::pin_name(target_name)))
        .await
        .unwrap();
    for pin in pins {
        assert!(
            pin.expire_time.is_some_and(|t| t <= chrono::Utc::now()),
            "live pin left behind: {pin:?}"
        );
    }
}

/// Seeding: one pass brings an empty target to the source's head with
/// the whole closure present; a second pass is a no-op; and the pass
/// leaves no pin behind.
#[tokio::test(flavor = "multi_thread")]
async fn seeding_pass_copies_the_full_closure() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 3).await;

    let outcome = mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    assert!(outcome.committed);
    assert!(outcome.copied.objects > 0);
    assert_no_live_pins(&source, "dr").await;

    // §4 step 6: the unpin wrote one more source manifest; the next
    // pass commits it through the pinless path (no new pin manifests,
    // no data objects) and converges the watermark to the source head.
    let src_head_before = layout::max_manifest_id(&source).await.unwrap().unwrap();
    let converge = mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    assert!(converge.committed);
    assert_eq!(converge.copied.objects, 0, "checkpoint-only change");
    assert_eq!(
        layout::max_manifest_id(&source).await.unwrap().unwrap(),
        src_head_before,
        "a pinless pass writes nothing at the source"
    );
    assert_mirrored(&source, &dest).await;

    // Caught up: nothing to commit.
    let noop = mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    assert!(!noop.committed);
}

/// Incremental sync: after more writes at the source, a pass copies
/// only the delta, and the closure invariant holds at the new head.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_pass_copies_only_the_delta() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest_store = TestStore::in_memory();
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    seed(&source, 2).await;
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let puts_before = dest_store.counters().count(Op::Put);
    seed(&source, 2).await;
    let outcome = mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    assert!(outcome.committed);
    // Converge the unpin manifest, then the watermark sits at the head.
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    assert_mirrored(&source, &dest).await;
    // The delta is bounded: the two new rounds' SSTs and WALs plus the
    // closure's manifests, nowhere near a reseed of everything.
    let puts = dest_store.counters().count(Op::Put) - puts_before;
    assert!(
        puts > 0 && puts <= 40,
        "expected a bounded delta of PUTs, got {puts}"
    );
    assert_no_live_pins(&source, "dr").await;
}

/// §5: a named operator checkpoint at the source arrives in the next
/// committed manifest, and its pinned manifest (closure support)
/// exists at the target.
#[tokio::test(flavor = "multi_thread")]
async fn operator_checkpoints_travel_with_their_support() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 2).await;
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let snap = source
        .admin
        .create_detached_checkpoint(&slatedb::config::CheckpointOptions {
            lifetime: None,
            source: None,
            name: Some("nightly".into()),
        })
        .await
        .unwrap();
    seed(&source, 1).await;
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let dst_head = dest.admin.read_manifest(None).await.unwrap().unwrap();
    let arrived = dst_head
        .checkpoints()
        .iter()
        .find(|cp| cp.name.as_deref() == Some("nightly"))
        .expect("named checkpoint arrives at the target");
    assert_eq!(arrived.manifest_id, snap.manifest_id);
    // The pinned manifest itself was committed (ascending order lands
    // every referenced manifest before its referencer).
    let pinned = dest
        .admin
        .read_manifest(Some(snap.manifest_id))
        .await
        .unwrap();
    assert!(pinned.is_some(), "support manifest present at the target");
}

/// §4 step 7: the WAL tail copies new WAL SSTs in id order as they
/// appear, and a later pass HEAD-hits them instead of recopying.
#[tokio::test(flavor = "multi_thread")]
async fn wal_tail_copies_in_order_and_passes_head_hit() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 1).await;
    let outcome = mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let mut tail = pass::Tail::start(&dest, outcome.next_wal_sst_id)
        .await
        .unwrap();
    assert_eq!(tail.step(&source, &dest).await.unwrap(), 0, "caught up");

    // New WAL-only writes (flushed, not memtable-flushed).
    let settings = slatedb::config::Settings {
        compactor_options: None,
        garbage_collector_options: None,
        ..Default::default()
    };
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(settings)
        .build()
        .await
        .unwrap();
    for i in 0..3 {
        writer
            .put(format!("tail-{i}").as_bytes(), b"v")
            .await
            .unwrap();
        writer.flush().await.unwrap();
    }
    writer.close().await.unwrap();

    let copied = tail.step(&source, &dest).await.unwrap();
    assert!(copied >= 3, "tail copied the new WAL SSTs, got {copied}");
    // Gapless from the tail's floor to the source's newest WAL. (A
    // GC-less source also retains WALs the manifest no longer needs;
    // the closure legitimately skips those.)
    let source_max: u64 = names(&source, layout::WAL_DIR)
        .await
        .iter()
        .filter_map(|n| n.strip_suffix(".sst").and_then(|s| s.parse().ok()))
        .max()
        .unwrap();
    let tailed: Vec<u64> = names(&dest, layout::WAL_DIR)
        .await
        .iter()
        .filter_map(|n| n.strip_suffix(".sst").and_then(|s| s.parse().ok()))
        .filter(|id| *id >= outcome.next_wal_sst_id)
        .collect();
    assert_eq!(
        tailed,
        (outcome.next_wal_sst_id..=source_max).collect::<Vec<u64>>(),
        "the target never has a WAL gap"
    );

    // The next pass sees the tail's WALs as HEAD hits: it commits the
    // new manifests without recopying them.
    let dest_store = TestStore::new(dest.store.clone());
    let counted_dest = handle(dest_store.clone(), "memory:///dst", "dst");
    mirror::sync_pass(&source, &counted_dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    let wal_puts = dest_store.counters().count(Op::Put);
    // Mostly manifests plus at most a couple of closure objects the
    // tail could not have seen; never a recopy of the tailed WALs.
    assert!(
        wal_puts <= 10,
        "pass should commit manifests, not recopy the tail: {wal_puts} PUTs"
    );
}

/// §7: prune keeps restore points and their closure support, deletes
/// superseded manifests and unreferenced data objects past min_age,
/// and never touches the WAL tail above the latest manifest.
#[tokio::test(flavor = "multi_thread")]
async fn prune_keeps_restore_points_support_and_tail() {
    let clock = TestClock::new(chrono::Utc::now());
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest_store = TestStore::in_memory_at(clock.clone());
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");

    let mut settings = ResolvedMirrorTarget {
        keep: Some(Duration::from_secs(3600)),
        min_age: Duration::from_secs(300),
        ..target_settings()
    };

    // Epoch 1: seed and sync; these become old restore points.
    seed(&source, 2).await;
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    let old_manifests = names(&dest, layout::MANIFEST_DIR).await;
    assert!(old_manifests.len() >= 2);

    // Epoch 2: two hours later, more churn at the source and a fresh
    // sync; the old restore points age out of keep.
    clock.advance(Duration::from_secs(2 * 3600));
    seed(&source, 2).await;
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();

    let now = clock.now();
    let report = mirror::prune::prune_at(&source, &dest, "dr", &settings, now)
        .await
        .unwrap();
    assert!(report.data_deletion_ran);
    assert!(report.deleted_manifests > 0, "{report:?}");

    // Every remaining manifest is younger than keep or the latest, and
    // the latest head still verifies completely.
    let remaining = layout::list_manifests(&dest).await.unwrap();
    let latest = remaining.last().unwrap().0;
    for (id, meta) in &remaining {
        assert!(
            *id == latest || now - meta.last_modified < chrono::Duration::hours(1),
            "manifest {id} should have been pruned"
        );
    }
    assert_mirrored(&source, &dest).await;

    // The WAL tail above the latest manifest is never pruned: fake a
    // tailed WAL above the head and prune again.
    let head = dest.admin.read_manifest(None).await.unwrap().unwrap();
    let tail_rel = layout::wal_rel(head.next_wal_sst_id() + 5);
    dest.store
        .put(&layout::object_path(&dest, &tail_rel), "tail".into())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(3 * 3600));
    settings.min_age = Duration::from_secs(1);
    mirror::prune::prune_at(&source, &dest, "dr", &settings, clock.now())
        .await
        .unwrap();
    dest.store
        .get(&layout::object_path(&dest, &tail_rel))
        .await
        .expect("the WAL tail above the latest manifest survives");
}

/// §7 guard 1: an object at the target that the source's latest
/// closure still references is spared even when no kept target
/// manifest references it (early delivery); an object in neither is
/// deleted once past min_age. And with the source unreachable, no
/// data object is deleted at all.
#[tokio::test(flavor = "multi_thread")]
async fn prune_spares_the_source_closure_and_stops_without_the_source() {
    let clock = TestClock::new(chrono::Utc::now());
    let source_store = TestStore::in_memory();
    let source = handle(source_store.clone(), "memory:///src", "src");
    let dest_store = TestStore::in_memory_at(clock.clone());
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    let settings = ResolvedMirrorTarget {
        keep: Some(Duration::from_secs(60)),
        min_age: Duration::from_secs(30),
        ..target_settings()
    };

    seed(&source, 2).await;
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();

    // More churn at the source, NOT yet synced: its new SSTs are in
    // the source's closure but referenced by no target manifest.
    seed(&source, 1).await;
    let src_head = source.admin.read_manifest(None).await.unwrap().unwrap();
    let at_dest = names(&dest, layout::COMPACTED_DIR).await;
    let new_at_source: Vec<String> = layout::manifest_objects(&src_head)
        .compacted
        .iter()
        .filter(|u| !at_dest.contains(&format!("{u}.sst")))
        .cloned()
        .collect();
    let early = new_at_source.first().expect("source grew a new SST");
    let early_rel = layout::compacted_rel(early);
    let body = source
        .store
        .get(&layout::object_path(&source, &early_rel))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    dest.store
        .put(&layout::object_path(&dest, &early_rel), body.into())
        .await
        .unwrap();
    // Garbage: an object in neither the target's kept set nor the
    // source's closure.
    let garbage_rel = layout::compacted_rel("00000000000000000GARBAGE00");
    dest.store
        .put(&layout::object_path(&dest, &garbage_rel), "junk".into())
        .await
        .unwrap();

    clock.advance(Duration::from_secs(3600));
    let report = mirror::prune::prune_at(&source, &dest, "dr", &settings, clock.now())
        .await
        .unwrap();
    assert!(report.data_deletion_ran);
    dest.store
        .get(&layout::object_path(&dest, &early_rel))
        .await
        .expect("early-delivered object spared by the source closure");
    assert!(
        dest.store
            .get(&layout::object_path(&dest, &garbage_rel))
            .await
            .is_err(),
        "garbage object deleted"
    );

    // Unreachable source: manifests may go, data objects must not.
    dest.store
        .put(&layout::object_path(&dest, &garbage_rel), "junk".into())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(3600));
    source_store.fail_all(Op::Get);
    source_store.fail_all(Op::List);
    let report = mirror::prune::prune_at(&source, &dest, "dr", &settings, clock.now())
        .await
        .unwrap();
    source_store.heal();
    assert!(!report.data_deletion_ran);
    assert_eq!(report.deleted_objects, 0);
    dest.store
        .get(&layout::object_path(&dest, &garbage_rel))
        .await
        .expect("no data deletion with the source unreachable");
}

/// §7 guard 2: while a checkpoint named for the target exists, nothing
/// newer than its create_time minus min_age is deleted, even when
/// otherwise unreferenced and past min_age.
#[tokio::test(flavor = "multi_thread")]
async fn prune_honors_the_pin_floor() {
    let clock = TestClock::new(chrono::Utc::now());
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest_store = TestStore::in_memory_at(clock.clone());
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    let settings = ResolvedMirrorTarget {
        keep: Some(Duration::from_secs(60)),
        min_age: Duration::from_secs(300),
        ..target_settings()
    };
    seed(&source, 1).await;
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();

    // A pass's pin stands (as if a pass were mid-copy)...
    let pin = source
        .admin
        .create_detached_checkpoint(&slatedb::config::CheckpointOptions {
            lifetime: Some(Duration::from_secs(3600)),
            source: None,
            name: Some(pass::pin_name("dr")),
        })
        .await
        .unwrap();
    // ... and staged objects land after it (their LastModified is
    // above the floor), then age far past min_age.
    let staged_rel = layout::compacted_rel("00000000000000000STAGED000");
    dest.store
        .put(&layout::object_path(&dest, &staged_rel), "staged".into())
        .await
        .unwrap();
    clock.advance(Duration::from_secs(6 * 3600));
    let report = mirror::prune::prune_at(&source, &dest, "dr", &settings, clock.now())
        .await
        .unwrap();
    assert!(report.data_deletion_ran);
    dest.store
        .get(&layout::object_path(&dest, &staged_rel))
        .await
        .expect("staged object survives while the pin stands");

    // Unpin: the floor lifts and the next prune reclaims it.
    source.admin.delete_checkpoint(pin.id).await.unwrap();
    clock.advance(Duration::from_secs(3600));
    mirror::prune::prune_at(&source, &dest, "dr", &settings, clock.now())
        .await
        .unwrap();
    assert!(
        dest.store
            .get(&layout::object_path(&dest, &staged_rel))
            .await
            .is_err(),
        "orphan reclaimed after the pin is gone"
    );
}

/// §10: verify passes on an intact target, then catches a data-object
/// deletion and a size mismatch.
#[tokio::test(flavor = "multi_thread")]
async fn verify_catches_missing_and_mismatched_objects() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 2).await;
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let outcome = mirror::verify(&source, &dest, None).await.unwrap();
    assert!(outcome.ok(), "{:?}", outcome.points);

    // Delete one referenced SST behind the mirror's back.
    let head = dest.admin.read_manifest(None).await.unwrap().unwrap();
    let victim = layout::manifest_objects(&head)
        .compacted
        .iter()
        .next()
        .cloned()
        .expect("head references an SST");
    let victim_rel = layout::compacted_rel(&victim);
    dest.store
        .delete(&layout::object_path(&dest, &victim_rel))
        .await
        .unwrap();
    let outcome = mirror::verify(&source, &dest, None).await.unwrap();
    assert!(!outcome.ok());
    assert!(
        outcome
            .points
            .iter()
            .flat_map(|p| &p.problems)
            .any(|p| p.contains(&victim_rel) && p.contains("missing")),
        "{:?}",
        outcome.points
    );

    // Restore it with the wrong bytes: size mismatch.
    dest.store
        .put(&layout::object_path(&dest, &victim_rel), "short".into())
        .await
        .unwrap();
    let outcome = mirror::verify(&source, &dest, None).await.unwrap();
    assert!(!outcome.ok());
    assert!(
        outcome
            .points
            .iter()
            .flat_map(|p| &p.problems)
            .any(|p| p.contains("size mismatch")),
        "{:?}",
        outcome.points
    );
}

/// §7: restore copies a chosen restore point's closure into an empty
/// root, which then opens as an ordinary database at that point;
/// non-empty destinations are refused.
#[tokio::test(flavor = "multi_thread")]
async fn restore_rebuilds_a_point_and_refuses_nonempty_roots() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let backup = handle(Arc::new(InMemory::new()), "memory:///bak", "bak");
    seed(&source, 2).await;
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();
    let early_head = backup
        .admin
        .read_manifest(None)
        .await
        .unwrap()
        .unwrap()
        .id();
    seed(&source, 2).await;
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();

    // Restore the earlier point.
    let dest = handle(Arc::new(InMemory::new()), "memory:///restored", "restored");
    let outcome = mirror::restore(&backup, &dest, RestorePoint::Manifest(early_head))
        .await
        .unwrap();
    assert_eq!(outcome.manifest_id, early_head);
    let restored_head = dest.admin.read_manifest(None).await.unwrap().unwrap();
    assert_eq!(restored_head.id(), early_head);

    // The destination is an ordinary database at that point: it opens
    // and serves the first epoch's keys.
    let db = slatedb::Db::builder(dest.path.clone(), dest.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let value = db.get(b"key-0-0").await.unwrap();
    assert!(value.is_some(), "restored data readable");
    db.close().await.unwrap();

    // Non-empty destination: refused, nothing deleted.
    let err = mirror::restore(&backup, &dest, RestorePoint::Latest)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        mirror::MirrorError::DestinationNotEmpty { .. }
    ));

    // Unknown restore point: refused.
    let empty = handle(Arc::new(InMemory::new()), "memory:///e", "e");
    let err = mirror::restore(&backup, &empty, RestorePoint::Manifest(999_999))
        .await
        .unwrap_err();
    assert!(matches!(err, mirror::MirrorError::NoRestorePoint { .. }));
}

/// §3/§11.5: failover is opening the target as an ordinary database.
/// The first writer replays the copied WAL tail exactly like one
/// recovering from a crash, so even unflushed tailed writes survive.
#[tokio::test(flavor = "multi_thread")]
async fn target_opens_live_and_replays_the_tailed_wal() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 2).await;
    let outcome = mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    // WAL-only writes after the pass, shipped by the tail.
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    writer.put(b"tail-only", b"survives").await.unwrap();
    writer.flush().await.unwrap();
    writer.close().await.unwrap();
    let mut tail = pass::Tail::start(&dest, outcome.next_wal_sst_id)
        .await
        .unwrap();
    tail.step(&source, &dest).await.unwrap();

    // Failover: open the destination as an ordinary database.
    let failover = slatedb::Db::builder(dest.path.clone(), dest.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    assert_eq!(
        failover.get(b"key-0-0").await.unwrap().as_deref(),
        Some(vec![0u8; 512].as_slice()),
        "manifest-referenced data survives"
    );
    assert_eq!(
        failover.get(b"tail-only").await.unwrap().as_deref(),
        Some(b"survives".as_slice()),
        "the copied WAL tail replays on open"
    );
    // The target goes live for real: it commits its own manifests,
    // forking its history from the source's.
    for i in 0..6u8 {
        failover
            .put(format!("failover-{i}").as_bytes(), b"fork")
            .await
            .unwrap();
        failover
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();
    }
    failover.close().await.unwrap();

    // The source moves on too; the next pass must refuse the forked
    // target rather than interleave the two histories.
    seed(&source, 1).await;
    let err = mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, mirror::MirrorError::Diverged { .. }),
        "{err:?}"
    );
}

/// Racing mirror tasks (open question 2): two concurrent passes over
/// the same pair both terminate, the target converges to the source
/// head, and no live pin outlasts them.
#[tokio::test(flavor = "multi_thread")]
async fn racing_passes_converge_safely() {
    let source_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dest_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let source = handle(source_store.clone(), "memory:///src", "src");
    seed(&source, 3).await;

    let mut racers = Vec::new();
    for _ in 0..2 {
        let source = handle(source_store.clone(), "memory:///src", "src");
        let dest = handle(dest_store.clone(), "memory:///dst", "dst");
        racers.push(tokio::spawn(async move {
            mirror::sync_pass(&source, &dest, "dr", &target_settings(), None).await
        }));
    }
    for racer in racers {
        racer.await.unwrap().expect("racing passes are safe");
    }
    let dest = handle(dest_store, "memory:///dst", "dst");
    // Converge the unpin manifests, then check the invariant.
    for _ in 0..3 {
        mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
            .await
            .unwrap();
    }
    assert_mirrored(&source, &dest).await;
    assert_no_live_pins(&source, "dr").await;
}

/// §8: the external copier backfills only what replication has not
/// delivered, and sleet still commits the manifests.
#[tokio::test(flavor = "multi_thread")]
async fn external_copier_backfills_only_misses() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest_store = TestStore::in_memory();
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    seed(&source, 2).await;

    // "Replication" delivers half the compacted SSTs ahead of sleet.
    let head = source.admin.read_manifest(None).await.unwrap().unwrap();
    let objects = layout::manifest_objects(&head);
    let delivered: Vec<String> = objects.compacted.iter().take(1).cloned().collect();
    for ulid in &delivered {
        let rel = layout::compacted_rel(ulid);
        let body = source
            .store
            .get(&layout::object_path(&source, &rel))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        dest.store
            .put(&layout::object_path(&dest, &rel), body.into())
            .await
            .unwrap();
    }

    let settings = ResolvedMirrorTarget {
        copier: CopierKind::External,
        ..target_settings()
    };
    let outcome = mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert!(outcome.committed);
    // Converge the unpin manifest, then the watermark sits at the head.
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert_mirrored(&source, &dest).await;
    // The delivered SST was not recopied: fewer data PUTs than objects
    // in the closure.
    let total = layout::manifest_objects(&dest.admin.read_manifest(None).await.unwrap().unwrap())
        .len() as u64;
    assert!(
        outcome.copied.objects < total,
        "backfill ({}) should be less than the closure ({total})",
        outcome.copied.objects
    );
}

/// Chaos: under a 20% fault rate on both stores the pass either
/// completes or fails cleanly, and after healing it converges with a
/// complete closure and no live pins.
#[tokio::test(flavor = "multi_thread")]
async fn faulted_passes_converge_after_healing() {
    let source_store = TestStore::in_memory();
    let dest_store = TestStore::in_memory();
    let source = handle(source_store.clone(), "memory:///src", "src");
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    seed(&source, 3).await;

    source_store.fail_probability(0.2, 7);
    dest_store.fail_probability(0.2, 8);
    let settings = ResolvedMirrorTarget {
        checkpoint_lifetime: Duration::from_secs(2),
        ..target_settings()
    };
    for _ in 0..20 {
        // Failures are fine; wedging or panicking is not.
        let _ = mirror::sync_pass(&source, &dest, "dr", &settings, None).await;
    }
    source_store.heal();
    dest_store.heal();

    for _ in 0..4 {
        mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
            .await
            .unwrap();
    }
    assert_mirrored(&source, &dest).await;
    let outcome = mirror::verify(&source, &dest, None).await.unwrap();
    assert!(outcome.ok(), "{:?}", outcome.points);
    // Expired or deleted pins only; a healed fleet leaves none live
    // past their lifetime.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_no_live_pins(&source, "dr").await;
}
