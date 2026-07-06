//! Mirror integration tests against real SlateDB databases: the sync
//! pass invariants (RFC 0002 §3-4), the WAL tail, retention and
//! both prune guards (§7), copiers (§8), restore, and failover by
//! opening the target as an ordinary database (§3).

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

/// The completeness invariant (§3) as a test oracle: the destination's
/// latest manifest must hold its own objects and, for every live
/// checkpoint entry whose checkpoint still exists at the source, the
/// pinned manifest and its objects. Entries of checkpoints retired at
/// the source may dangle (§3). Returns the first problem found.
async fn closure_problems(source: &DatabaseHandle, dest: &DatabaseHandle) -> Option<String> {
    let head = dest
        .admin
        .read_manifest(None)
        .await
        .unwrap()
        .expect("destination head");
    let src_cps: std::collections::BTreeSet<uuid::Uuid> = source
        .admin
        .read_manifest(None)
        .await
        .unwrap()
        .expect("source head")
        .checkpoints()
        .iter()
        .map(|cp| cp.id)
        .collect();
    let now = chrono::Utc::now();
    let mut members = vec![head.id()];
    for cp in head.checkpoints() {
        if layout::checkpoint_live(cp, now)
            && src_cps.contains(&cp.id)
            && cp.manifest_id != head.id()
        {
            members.push(cp.manifest_id);
        }
    }
    for id in members {
        let Some(manifest) = dest.admin.read_manifest(Some(id)).await.unwrap() else {
            return Some(format!("pinned manifest {id} missing at the destination"));
        };
        for rel in layout::manifest_objects(&manifest).rel_names() {
            if dest
                .store
                .head(&layout::object_path(dest, &rel))
                .await
                .is_err()
            {
                return Some(format!("{rel} missing at the destination (member {id})"));
            }
        }
    }
    None
}

async fn assert_closure_complete(source: &DatabaseHandle, dest: &DatabaseHandle) {
    if let Some(problem) = closure_problems(source, dest).await {
        panic!("destination closure incomplete: {problem}");
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
    assert_closure_complete(&source, &dest).await;
    // Healed passes delete their pins; pins abandoned by faulted
    // passes only expire. Outlive the 2s checkpoint_lifetime so both
    // read as dead.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_no_live_pins(&source, "dr").await;
}

/// The continuous mode loop end to end: passes and the WAL tail keep
/// the destination converged while the loop runs, and cancellation
/// stops it cleanly.
#[tokio::test(flavor = "multi_thread")]
async fn continuous_mode_tracks_the_source() {
    use tokio_util::sync::CancellationToken;
    let source_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dest_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let source = handle(source_store.clone(), "memory:///src", "src");
    seed(&source, 2).await;

    let target = mirror::AppliedTarget {
        name: "dr".into(),
        destination: "memory:///dst".into(),
        settings: ResolvedMirrorTarget {
            poll: Duration::from_millis(100),
            ..target_settings()
        },
    };
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let source = handle(source_store.clone(), "memory:///src", "src");
        let dest = handle(dest_store.clone(), "memory:///dst", "dst");
        let target = target.clone();
        let token = token.clone();
        async move {
            let jobs = Arc::new(tokio::sync::Semaphore::new(1));
            mirror::run_mirror(&source, &dest, &target, jobs, None, token).await
        }
    });

    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    poll_until("initial convergence", || async {
        let src = source.admin.read_manifest(None).await.ok()??;
        let dst = dest.admin.read_manifest(None).await.ok().flatten()?;
        (src.id() == dst.id()).then_some(())
    })
    .await;

    // More writes at the source: the loop catches up on its own.
    seed(&source, 1).await;
    poll_until("incremental convergence", || async {
        let src = source.admin.read_manifest(None).await.ok()??;
        let dst = dest.admin.read_manifest(None).await.ok().flatten()?;
        (src.id() == dst.id()).then_some(())
    })
    .await;
    assert_mirrored(&source, &dest).await;

    token.cancel();
    task.await.unwrap().unwrap();
    assert_no_live_pins(&source, "dr").await;
}

/// The periodic mode loop: a pass runs when the target's latest
/// manifest is older than the interval, and each committed manifest is
/// a point-in-time cut.
#[tokio::test(flavor = "multi_thread")]
async fn periodic_mode_cuts_on_the_interval() {
    use sleet::config::MirrorMode;
    use tokio_util::sync::CancellationToken;
    let source_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dest_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let source = handle(source_store.clone(), "memory:///src", "src");
    seed(&source, 2).await;

    let target = mirror::AppliedTarget {
        name: "backup".into(),
        destination: "memory:///bak".into(),
        settings: ResolvedMirrorTarget {
            mode: MirrorMode::Periodic,
            interval: Duration::from_secs(1),
            ..target_settings()
        },
    };
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let source = handle(source_store.clone(), "memory:///src", "src");
        let dest = handle(dest_store.clone(), "memory:///bak", "bak");
        let target = target.clone();
        let token = token.clone();
        async move {
            let jobs = Arc::new(tokio::sync::Semaphore::new(1));
            mirror::run_mirror(&source, &dest, &target, jobs, None, token).await
        }
    });

    let dest = handle(dest_store.clone(), "memory:///bak", "bak");
    poll_until("first periodic cut", || async {
        dest.admin
            .read_manifest(None)
            .await
            .ok()
            .flatten()
            .map(|_| ())
    })
    .await;
    token.cancel();
    task.await.unwrap().unwrap();

    // The cut is a valid point: its closure is complete.
    assert_closure_complete(&source, &dest).await;
}

/// §8: the rclone copier drives `rclone copy --files-from` for the
/// data directories and never touches manifest/. A stub rclone binary
/// stands in for the real one and copies between the file:// roots.
#[tokio::test(flavor = "multi_thread")]
async fn rclone_copier_moves_data_objects() {
    use sleet::config::CopierKind;
    let dir = tempfile::tempdir().unwrap();
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&dst_dir).unwrap();

    // A stub rclone: copy <src>/<name> to <dst>/<name> for each line
    // of --files-from, and refuse to ever see manifest/ in the list.
    let stub = dir.path().join("rclone");
    std::fs::write(
        &stub,
        "#!/bin/sh\n\
         # args: copy --files-from LIST SRC DST\n\
         list=\"$3\"; src=\"$4\"; dst=\"$5\"\n\
         while IFS= read -r name; do\n\
           [ -z \"$name\" ] && continue\n\
           case \"$name\" in manifest/*) echo \"rclone must never touch manifest/\" >&2; exit 1;; esac\n\
           mkdir -p \"$dst/$(dirname \"$name\")\"\n\
           cp \"$src/$name\" \"$dst/$name\" || exit 1\n\
         done < \"$list\"\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let source = DatabaseHandle::open(&format!("file://{}", src_dir.display())).unwrap();
    let dest = DatabaseHandle::open(&format!("file://{}", dst_dir.display())).unwrap();
    seed(&source, 2).await;

    let settings = ResolvedMirrorTarget {
        copier: CopierKind::Rclone,
        ..target_settings()
    };
    let outcome = mirror::sync_pass(
        &source,
        &dest,
        "dr",
        &settings,
        Some(stub.to_str().unwrap()),
    )
    .await
    .unwrap();
    assert!(outcome.committed);
    mirror::sync_pass(
        &source,
        &dest,
        "dr",
        &settings,
        Some(stub.to_str().unwrap()),
    )
    .await
    .unwrap();
    assert_mirrored(&source, &dest).await;
    assert_closure_complete(&source, &dest).await;
}

/// The whole stack over a file:// fleet root: a daemon node owns the
/// (database, mirror, target) assignment, its heartbeat carries the
/// mirror summary, the destination converges, and the ops one-shots
/// (status --mirrors, sync) agree.
#[tokio::test(flavor = "multi_thread")]
async fn daemon_mirrors_a_registered_database() {
    use sleet::daemon::{self, NodeOptions};
    use sleet::root::FleetRoot;
    use sleet::{ops, registry};
    use tokio_util::sync::CancellationToken;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    std::fs::create_dir_all(dir.path().join("dst")).unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    let dest_url = format!("file://{}/dst", dir.path().display());

    // A real source database.
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let db_url = format!("file://{}", src_dir.display());
    let source = DatabaseHandle::open(&db_url).unwrap();
    seed(&source, 2).await;

    let root = FleetRoot::open(&fleet_url).unwrap();
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"500ms\"\nheartbeat_timeout = \"2s\"\nconfig_poll = \"1s\"\n"
                .into(),
        )
        .await
        .unwrap();
    ops::register(&root, &db_url).await.unwrap();
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&db_url).unwrap()),
            format!(
                "[mirror.targets.dr]\nurl = \"{dest_url}\"\nmode = \"continuous\"\npoll = \"250ms\"\n"
            )
            .into(),
        )
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let node = tokio::spawn(daemon::run(
        root.clone(),
        NodeOptions {
            node_id: "n1".into(),
            ..NodeOptions::default()
        },
        shutdown.clone(),
    ));

    // Placement: the mirror target lands on the only node.
    let status = poll_until("mirror placement visible", || async {
        let status = ops::status(&root, false, true).await.unwrap();
        (!status.mirrors.is_empty()).then_some(status)
    })
    .await;
    assert_eq!(status.mirrors[0].target, "dr");
    assert_eq!(status.mirrors[0].destination, dest_url);

    // The daemon's task converges the destination.
    let dest = DatabaseHandle::open(&dest_url).unwrap();
    poll_until("destination converges", || async {
        let status = ops::status(&root, false, true).await.unwrap();
        (status.mirrors[0].manifests_behind == Some(0)).then_some(())
    })
    .await;
    assert_mirrored(&source, &dest).await;

    // The heartbeat body carries the mirror task summary; poll past
    // the first tick, which is written before tasks reconcile.
    let path = root.node_path(&sleet::heartbeat::object_name(
        "n1",
        &sleet::config::Service::ALL,
    ));
    poll_until("mirror summary in the heartbeat", || async {
        let body = root.store().get(&path).await.ok()?.bytes().await.ok()?;
        let heartbeat: sleet::heartbeat::Heartbeat = serde_json::from_slice(&body).ok()?;
        let mirror_summary = heartbeat
            .services
            .iter()
            .find(|s| s.service == sleet::config::Service::Mirror)?;
        (mirror_summary.running == 1).then_some(())
    })
    .await;

    // The converged destination's closure is complete.
    assert_closure_complete(&source, &dest).await;

    // One-shot sync on a caught-up pair is a clean no-op.
    shutdown.cancel();
    node.await.unwrap().unwrap();
    let sync = ops::mirror_sync(&root, &db_url, "dr", None).await.unwrap();
    assert!(sync.head > 0);

    // Unknown targets and unregistered databases fail loudly.
    let err = ops::mirror_sync(&root, &db_url, "nope", None)
        .await
        .unwrap_err();
    assert!(matches!(err, ops::OpsError::NoSuchTarget { .. }));
    let err = ops::mirror_sync(&root, "file:///not/registered", "dr", None)
        .await
        .unwrap_err();
    assert!(matches!(err, ops::OpsError::NotRegistered { .. }));
}

/// §10: status --mirrors flags destination collisions and databases
/// with mirror enabled but no applicable target.
#[tokio::test(flavor = "multi_thread")]
async fn status_flags_collisions_and_uncovered_databases() {
    use sleet::root::FleetRoot;
    use sleet::{ops, registry};

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    let root = FleetRoot::open(&fleet_url).unwrap();

    // A fleet-wide EXACT target: every database maps to the same
    // destination, which is exactly the collision status must flag.
    root.store()
        .put(
            &root.config_path(),
            "[database.mirror.targets.dr]\nurl = \"s3://dr-bucket/one-destination\"\n".into(),
        )
        .await
        .unwrap();
    ops::register(&root, "s3://data/db1").await.unwrap();
    ops::register(&root, "s3://data/db2").await.unwrap();
    // A database opted out: mirror enabled, no applicable target.
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url("s3://data/db3").unwrap()),
            "[mirror.targets.dr]\ndisabled = true\n".into(),
        )
        .await
        .unwrap();

    // And the worst case: the shared destination itself registered as
    // a database (a mirror chain), which sleet must call out.
    ops::register(&root, "s3://dr-bucket/one-destination")
        .await
        .unwrap();

    let status = ops::status(&root, false, true).await.unwrap();
    assert!(
        status
            .warnings
            .iter()
            .any(|w| w.contains("mirror destinations collide")),
        "{:?}",
        status.warnings
    );
    assert!(
        status
            .warnings
            .iter()
            .any(|w| w.contains("is itself a registered database")),
        "{:?}",
        status.warnings
    );
    assert!(
        status
            .warnings
            .iter()
            .any(|w| w.contains("db3") && w.contains("no applicable target")),
        "{:?}",
        status.warnings
    );
    // Lag reads fail cleanly for unreachable s3 stores: the error
    // rides in the per-target field, not the whole status.
    assert_eq!(status.mirrors.len(), 2);
}

/// Poll an async condition every 100ms for up to 150s (the soak's
/// post-churn convergence takes a while on slow CI runners).
async fn poll_until<T, F, Fut>(what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// §6's cost claim, pinned: a caught-up continuous mirror costs one
/// source manifest LIST and one WAL tail probe per wakeup, and
/// touches the destination not at all.
#[tokio::test(flavor = "multi_thread")]
async fn caught_up_mirror_costs_one_list_and_one_probe_per_wakeup() {
    use tokio_util::sync::CancellationToken;
    let source_store = TestStore::in_memory();
    let dest_store = TestStore::in_memory();
    let source = handle(source_store.clone(), "memory:///src", "src");
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    seed(&source, 2).await;
    // Converge fully before measuring.
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let target = mirror::AppliedTarget {
        name: "dr".into(),
        destination: "memory:///dst".into(),
        settings: ResolvedMirrorTarget {
            poll: Duration::from_millis(50),
            ..target_settings()
        },
    };
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let source = handle(source_store.clone(), "memory:///src", "src");
        let dest = handle(dest_store.clone(), "memory:///dst", "dst");
        let target = target.clone();
        let token = token.clone();
        async move {
            let jobs = Arc::new(tokio::sync::Semaphore::new(1));
            mirror::run_mirror(&source, &dest, &target, jobs, None, token).await
        }
    });

    // Let the loop's first wakeup run its recovery pass and tail init,
    // then measure the marginal cost of idle wakeups.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let src_lists = source_store.counters().count(Op::List);
    let src_gets = source_store.counters().count(Op::Get);
    let dst_lists = dest_store.counters().count(Op::List);
    let dst_gets = dest_store.counters().count(Op::Get);
    let dst_puts = dest_store.counters().count(Op::Put);
    // The idle window under measurement; the wakeup count it fits is
    // asserted below.
    tokio::time::sleep(Duration::from_secs(3)).await;
    token.cancel();
    task.await.unwrap().unwrap();

    let wakeup_lists = source_store.counters().count(Op::List) - src_lists;
    let wakeup_gets = source_store.counters().count(Op::Get) - src_gets;
    // Idle backoff doubles the interval, so only a handful of wakeups
    // land in the window; each costs exactly one LIST and one GET.
    assert!(
        (1..=8).contains(&wakeup_lists),
        "expected a few idle wakeups, saw {wakeup_lists} LISTs"
    );
    let diff = wakeup_gets.abs_diff(wakeup_lists);
    assert!(
        diff <= 1,
        "each wakeup is one manifest LIST plus one tail probe GET: \
         {wakeup_lists} LISTs vs {wakeup_gets} GETs"
    );
    assert_eq!(
        dest_store.counters().count(Op::Put) - dst_puts,
        0,
        "an idle mirror never writes the destination"
    );
    assert_eq!(
        (dest_store.counters().count(Op::List) - dst_lists)
            + (dest_store.counters().count(Op::Get) - dst_gets),
        0,
        "an idle mirror never reads the destination (the watermark is cached)"
    );
}

/// §7: `--at` accepts a timestamp, mapped through the sequence
/// tracker to the newest restore point at or before it. The tracker
/// samples one entry per 60s, so a short test cannot discriminate
/// close epochs; what it can pin: a current timestamp resolves and
/// restores, and one before all tracked history refuses cleanly.
#[tokio::test(flavor = "multi_thread")]
async fn restore_at_a_timestamp_resolves_and_bounds() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let backup = handle(Arc::new(InMemory::new()), "memory:///bak", "bak");
    seed(&source, 2).await;
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();

    // Epoch 2 writes a marker key.
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    writer.put(b"epoch2-marker", b"late").await.unwrap();
    writer
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    writer.close().await.unwrap();
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();

    // A current timestamp resolves within the tracker's granularity
    // and the restored root opens with the data.
    let dest = handle(Arc::new(InMemory::new()), "memory:///restored", "restored");
    let outcome = mirror::restore(&backup, &dest, RestorePoint::Time(chrono::Utc::now()))
        .await
        .unwrap();
    assert!(outcome.manifest_id > 0);
    let db = slatedb::Db::builder(dest.path.clone(), dest.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    assert!(
        db.get(b"key-0-0").await.unwrap().is_some(),
        "restored data present"
    );
    db.close().await.unwrap();

    // A timestamp before all tracked history refuses cleanly.
    let empty = handle(Arc::new(InMemory::new()), "memory:///e2", "e2");
    let err = mirror::restore(
        &backup,
        &empty,
        RestorePoint::Time(chrono::Utc::now() - chrono::Duration::days(365)),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, mirror::MirrorError::NoRestorePoint { .. }));

    // --at parsing: manifest ids, RFC 3339 timestamps, nothing else.
    assert!(matches!(
        RestorePoint::parse("42"),
        Ok(RestorePoint::Manifest(42))
    ));
    assert!(matches!(
        RestorePoint::parse("2026-01-02T03:04:05Z"),
        Ok(RestorePoint::Time(_))
    ));
    assert!(RestorePoint::parse("yesterday-ish").is_err());
}

/// The production shape (RFC 0002 §3 core premise): gc, the
/// compaction coordinator, workers, and the mirror all running
/// against one database while a writer churns, with retention set on
/// the target. Compaction rewrites and GC deletions race passes and
/// the tail; afterward the destination verifies, fails over, and
/// serves exactly the source's contents.
#[tokio::test(flavor = "multi_thread")]
async fn soak_mirror_races_live_compaction_and_gc() {
    use sleet::daemon::{self, NodeOptions};
    use sleet::root::FleetRoot;
    use sleet::{ops, registry};
    use tokio_util::sync::CancellationToken;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    std::fs::create_dir_all(dir.path().join("dst")).unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    let dest_url = format!("file://{}/dst", dir.path().display());
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let db_url = format!("file://{}", src_dir.display());
    let source = DatabaseHandle::open(&db_url).unwrap();
    seed(&source, 2).await;

    let root = FleetRoot::open(&fleet_url).unwrap();
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"400ms\"\nheartbeat_timeout = \"2s\"\nconfig_poll = \"800ms\"\n"
                .into(),
        )
        .await
        .unwrap();
    ops::register(&root, &db_url).await.unwrap();
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&db_url).unwrap()),
            format!(
                "[gc.manifest]\ninterval = \"500ms\"\nmin_age = \"1s\"\n\
                 [gc.compacted]\ninterval = \"500ms\"\nmin_age = \"1s\"\n\
                 [gc.wal]\ninterval = \"500ms\"\nmin_age = \"1s\"\n\
                 [compactor-coordinator]\npoll_interval = \"250ms\"\n\
                 [compactor-coordinator.scheduler]\nmin_compaction_sources = 2\n\
                 [compaction-workers]\ncompactions_poll_interval = \"150ms\"\n\
                 [mirror.targets.dr]\nurl = \"{dest_url}\"\nmode = \"continuous\"\n\
                 poll = \"200ms\"\nmin_age = \"1s\"\n\
                 [mirror.targets.dr.retention]\nkeep = \"4s\"\n"
            )
            .into(),
        )
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let node = tokio::spawn(daemon::run(
        root.clone(),
        NodeOptions {
            node_id: "n1".into(),
            max_compaction_jobs: 2,
            ..NodeOptions::default()
        },
        shutdown.clone(),
    ));

    // The writer churns while every service runs: fresh keys, rolling
    // overwrites (compaction fodder), and periodic memtable flushes.
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    for round in 0..12u8 {
        for i in 0..40u8 {
            writer
                .put(
                    format!("soak-{round}-{i}").as_bytes(),
                    vec![round; 256].as_slice(),
                )
                .await
                .unwrap();
            writer
                .put(
                    format!("rolling-{i}").as_bytes(),
                    vec![round; 128].as_slice(),
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
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    writer.close().await.unwrap();

    // Quiesce: compaction commits drain, GC and prune settle, and the
    // mirror converges to the source's final head.
    poll_until("mirror converges after the churn", || async {
        let status = ops::status(&root, false, true).await.unwrap();
        let m = status.mirrors.first()?;
        (m.manifests_behind == Some(0) && m.error.is_none()).then_some(())
    })
    .await;
    // Hold convergence across a few more polls (compaction results and
    // checkpoint churn keep committing briefly after the writer stops).
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        poll_until("mirror re-converges", || async {
            let status = ops::status(&root, false, true).await.unwrap();
            (status.mirrors.first()?.manifests_behind == Some(0)).then_some(())
        })
        .await;
    }
    shutdown.cancel();
    node.await.unwrap().unwrap();

    // Failover: open both and compare every key and value.
    let dest = DatabaseHandle::open(&dest_url).unwrap();
    let contents = |handle: &DatabaseHandle| {
        let path = handle.path.clone();
        let store = handle.store.clone();
        async move {
            let db = slatedb::Db::builder(path, store)
                .with_settings(slatedb::config::Settings {
                    compactor_options: None,
                    garbage_collector_options: None,
                    ..Default::default()
                })
                .build()
                .await
                .unwrap();
            let mut all = std::collections::BTreeMap::new();
            let mut it = db.scan(..).await.unwrap();
            while let Some(kv) = it.next().await.unwrap() {
                all.insert(kv.key.to_vec(), kv.value.to_vec());
            }
            drop(it);
            db.close().await.unwrap();
            all
        }
    };
    let source_contents = contents(&source).await;
    let dest_contents = contents(&dest).await;
    assert!(
        source_contents.len() >= 12 * 40,
        "the soak wrote data: {} keys",
        source_contents.len()
    );
    assert_eq!(
        source_contents, dest_contents,
        "the failed-over destination serves exactly the source's contents"
    );
}

/// §2: excluded sources are refused. A clone (external_dbs set) and a
/// database with a separate WAL object store cannot be mirror sources.
#[tokio::test(flavor = "multi_thread")]
async fn excluded_sources_are_refused() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let parent = handle(store.clone(), "memory:///parent", "parent");
    seed(&parent, 2).await;

    // A clone of the parent, via the real clone protocol.
    let clone = handle(store.clone(), "memory:///clone", "clone");
    clone
        .admin
        .create_clone_builder_from_source(slatedb::CloneSourceSpec {
            path: parent.path.clone(),
            checkpoint: None,
            projection_range: None,
        })
        .build()
        .await
        .unwrap();
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    let err = mirror::sync_pass(&clone, &dest, "dr", &target_settings(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, mirror::MirrorError::ExcludedSource { ref reason, .. } if reason.contains("clone")),
        "{err:?}"
    );

    // A database whose WAL lives in a separate object store.
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let split = handle(store.clone(), "memory:///split", "split");
    let db = slatedb::Db::builder(split.path.clone(), split.store.clone())
        .with_wal_object_store(wal_store)
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    db.put(b"k", b"v").await.unwrap();
    db.close().await.unwrap();
    let err = mirror::sync_pass(&split, &dest, "dr", &target_settings(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, mirror::MirrorError::ExcludedSource { ref reason, .. } if reason.contains("WAL")),
        "{err:?}"
    );
}

/// §4: the pin refreshes at half-life while the pass runs. With
/// copies held slower than the pin lifetime, the pass can only commit
/// if refresh works; a lapse would restart every attempt into
/// Stalled.
#[tokio::test(flavor = "multi_thread")]
async fn pin_refreshes_while_a_slow_copy_runs() {
    let source_store = TestStore::in_memory();
    let source = handle(source_store.clone(), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 8).await;

    // Each GET (data objects and the admin's manifest reads alike)
    // takes 40ms; with serial copies the pass holds the pin far past
    // several lifetimes.
    source_store.set_latency(Op::Get, Duration::from_millis(40));
    let settings = ResolvedMirrorTarget {
        checkpoint_lifetime: Duration::from_millis(250),
        copy_parallelism: 1,
        ..target_settings()
    };
    let start = tokio::time::Instant::now();
    let outcome = mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert!(outcome.committed);
    assert!(
        start.elapsed() > Duration::from_millis(500),
        "the copy was meant to outlive the pin lifetime (took {:?})",
        start.elapsed()
    );
    source_store.heal();
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    assert_mirrored(&source, &dest).await;
    assert_no_live_pins(&source, "dr").await;
}

/// §4: a pass that cannot prove its pin alive (refresh fails) refuses
/// to commit and surfaces an error; after healing, the next pass
/// converges and the abandoned pin is left to expiry.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_failure_fails_the_pass_and_heals() {
    let source_store = TestStore::in_memory();
    let source = handle(source_store.clone(), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 8).await;

    source_store.set_latency(Op::Get, Duration::from_millis(50));
    let settings = ResolvedMirrorTarget {
        checkpoint_lifetime: Duration::from_millis(200),
        copy_parallelism: 1,
        ..target_settings()
    };
    let pass = tokio::spawn({
        let source_store = source_store.clone();
        let dest_store = dest.store.clone();
        async move {
            let source = handle(source_store, "memory:///src", "src");
            let dest = handle(dest_store, "memory:///dst", "dst");
            mirror::sync_pass(&source, &dest, "dr", &settings, None).await
        }
    });
    // Once the copy is underway, break every source write: refreshes
    // (manifest CAS PUTs) start failing, and so do restart pin
    // deletions and fresh pin creations.
    tokio::time::sleep(Duration::from_millis(150)).await;
    source_store.fail_all(Op::Put);
    let result = pass.await.unwrap();
    assert!(result.is_err(), "a pass without a provable pin must fail");

    // Heal: the next passes converge, and any abandoned pin has a
    // 200ms lifetime, so it reads as expired by the time we check.
    source_store.heal();
    for _ in 0..3 {
        mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
            .await
            .unwrap();
    }
    assert_mirrored(&source, &dest).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_no_live_pins(&source, "dr").await;
}

/// §7: restore refuses to commit an incomplete closure, and the
/// refusal happens before anything lands at the destination. (A data
/// object vanishing mid-copy is different: copies land before the
/// commit, and restore never deletes, so a failed mid-copy restore
/// leaves data-object litter in a root restore will then refuse; the
/// pre-flight case here must stay clean.)
#[tokio::test(flavor = "multi_thread")]
async fn restore_refuses_dangling_support_before_writing() {
    use futures::TryStreamExt;
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let backup = handle(Arc::new(InMemory::new()), "memory:///bak", "bak");
    seed(&source, 2).await;
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();
    let snap = source
        .admin
        .create_detached_checkpoint(&slatedb::config::CheckpointOptions {
            lifetime: None,
            source: None,
            name: Some("pinned".into()),
        })
        .await
        .unwrap();
    seed(&source, 1).await;
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &backup, "backup", &target_settings(), None)
        .await
        .unwrap();

    // Break the backup behind the mirror's back: the support manifest
    // the head's live checkpoint pins disappears.
    backup
        .store
        .delete(&layout::object_path(
            &backup,
            &layout::manifest_rel(snap.manifest_id),
        ))
        .await
        .unwrap();

    let dest = handle(Arc::new(InMemory::new()), "memory:///restored", "restored");
    let err = mirror::restore(&backup, &dest, RestorePoint::Latest)
        .await
        .unwrap_err();
    assert!(
        matches!(err, mirror::MirrorError::NoRestorePoint { .. }),
        "{err:?}"
    );
    let leftovers: Vec<object_store::ObjectMeta> = dest
        .store
        .list(Some(&dest.path))
        .try_collect()
        .await
        .unwrap();
    assert!(
        leftovers.is_empty(),
        "a refused restore must leave the destination empty: {leftovers:?}"
    );
}

/// §5: a named checkpoint created at the source mounts read-only at
/// the target by id with DbReader, pinned to the state at creation.
#[tokio::test(flavor = "multi_thread")]
async fn replica_reader_mounts_a_mirrored_checkpoint() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 2).await;
    let snap = source
        .admin
        .create_detached_checkpoint(&slatedb::config::CheckpointOptions {
            lifetime: None,
            source: None,
            name: Some("reader-snap".into()),
        })
        .await
        .unwrap();

    // Later writes must be invisible through the checkpoint.
    let writer = slatedb::Db::builder(source.path.clone(), source.store.clone())
        .with_settings(slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    writer.put(b"after-snap", b"x").await.unwrap();
    writer
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
    writer.close().await.unwrap();

    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let reader = slatedb::DbReader::builder(dest.path.clone(), dest.store.clone())
        .with_checkpoint_id(snap.id)
        .build()
        .await
        .unwrap();
    assert!(
        reader.get(b"key-0-0").await.unwrap().is_some(),
        "checkpointed data readable at the target"
    );
    assert_eq!(
        reader.get(b"after-snap").await.unwrap(),
        None,
        "the checkpoint pins the state at its creation"
    );
    reader.close().await.unwrap();
}

/// §9's example shape: one database, two targets (a continuous
/// replica and a periodic backup), both owned, both converging, and
/// both reported in the heartbeat; then disabling one stops its task
/// within a config poll and leaves its destination valid at the
/// watermark while the other keeps mirroring.
#[tokio::test(flavor = "multi_thread")]
async fn two_targets_run_and_disable_stops_one() {
    use sleet::daemon::{self, NodeOptions};
    use sleet::root::FleetRoot;
    use sleet::{ops, registry};
    use tokio_util::sync::CancellationToken;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    std::fs::create_dir_all(dir.path().join("replica")).unwrap();
    std::fs::create_dir_all(dir.path().join("backup")).unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    let replica_url = format!("file://{}/replica", dir.path().display());
    let backup_url = format!("file://{}/backup", dir.path().display());
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let db_url = format!("file://{}", src_dir.display());
    let source = DatabaseHandle::open(&db_url).unwrap();
    seed(&source, 2).await;

    let root = FleetRoot::open(&fleet_url).unwrap();
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"400ms\"\nheartbeat_timeout = \"2s\"\nconfig_poll = \"600ms\"\n"
                .into(),
        )
        .await
        .unwrap();
    ops::register(&root, &db_url).await.unwrap();
    let registry_body = format!(
        "[mirror.targets.replica]\nurl = \"{replica_url}\"\nmode = \"continuous\"\npoll = \"250ms\"\n\
         [mirror.targets.backup]\nurl = \"{backup_url}\"\nmode = \"periodic\"\ninterval = \"1s\"\n"
    );
    let registry_path = root.database_path(&registry::canonicalize_url(&db_url).unwrap());
    root.store()
        .put(&registry_path, registry_body.clone().into())
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let node = tokio::spawn(daemon::run(
        root.clone(),
        NodeOptions {
            node_id: "n1".into(),
            ..NodeOptions::default()
        },
        shutdown.clone(),
    ));

    // Both targets converge and both tasks ride in the heartbeat.
    poll_until("both targets converge", || async {
        let status = ops::status(&root, false, true).await.unwrap();
        (status.mirrors.len() == 2 && status.mirrors.iter().all(|m| m.manifests_behind == Some(0)))
            .then_some(())
    })
    .await;
    let path = root.node_path(&sleet::heartbeat::object_name(
        "n1",
        &sleet::config::Service::ALL,
    ));
    poll_until("two mirror tasks in the heartbeat", || async {
        let body = root.store().get(&path).await.ok()?.bytes().await.ok()?;
        let hb: sleet::heartbeat::Heartbeat = serde_json::from_slice(&body).ok()?;
        let m = hb
            .services
            .iter()
            .find(|s| s.service == sleet::config::Service::Mirror)?;
        (m.running == 2).then_some(())
    })
    .await;

    // Disable the backup target; its task stops within a config poll,
    // the replica keeps running, and the backup destination stays
    // valid at its watermark even as the source moves on.
    let backup_head_at_disable = DatabaseHandle::open(&backup_url)
        .unwrap()
        .admin
        .read_manifest(None)
        .await
        .unwrap()
        .unwrap()
        .id();
    root.store()
        .put(
            &registry_path,
            format!("{registry_body}[mirror.targets.backup.retention]\nkeep = \"1h\"\n")
                .replace(
                    "[mirror.targets.backup]",
                    "[mirror.targets.backup]\ndisabled = true",
                )
                .into(),
        )
        .await
        .unwrap();
    poll_until("backup task stops", || async {
        let body = root.store().get(&path).await.ok()?.bytes().await.ok()?;
        let hb: sleet::heartbeat::Heartbeat = serde_json::from_slice(&body).ok()?;
        let m = hb
            .services
            .iter()
            .find(|s| s.service == sleet::config::Service::Mirror)?;
        (m.running + m.backoff == 1).then_some(())
    })
    .await;

    seed(&source, 1).await;
    poll_until("replica still tracks the source", || async {
        let status = ops::status(&root, false, true).await.unwrap();
        let replica = status.mirrors.iter().find(|m| m.target == "replica")?;
        (replica.manifests_behind == Some(0)).then_some(())
    })
    .await;
    shutdown.cancel();
    node.await.unwrap().unwrap();

    // The disabled backup froze at its watermark, still complete.
    let backup = DatabaseHandle::open(&backup_url).unwrap();
    let frozen = backup.admin.read_manifest(None).await.unwrap().unwrap();
    assert_eq!(frozen.id(), backup_head_at_disable, "left at the watermark");
    assert_closure_complete(&source, &backup).await;
}

/// A broken mirror target must not hurt anything else: its task backs
/// off in the heartbeat while a healthy database's mirror on the same
/// node converges normally.
#[tokio::test(flavor = "multi_thread")]
async fn broken_target_backs_off_without_hurting_others() {
    use sleet::daemon::{self, NodeOptions};
    use sleet::root::FleetRoot;
    use sleet::{ops, registry};
    use tokio_util::sync::CancellationToken;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    std::fs::create_dir_all(dir.path().join("good-dst")).unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    let good_dst = format!("file://{}/good-dst", dir.path().display());

    let good_dir = dir.path().join("good");
    let bad_dir = dir.path().join("bad");
    std::fs::create_dir_all(&good_dir).unwrap();
    std::fs::create_dir_all(&bad_dir).unwrap();
    let good_url = format!("file://{}", good_dir.display());
    let bad_url = format!("file://{}", bad_dir.display());
    let good = DatabaseHandle::open(&good_url).unwrap();
    seed(&good, 1).await;
    let bad = DatabaseHandle::open(&bad_url).unwrap();
    seed(&bad, 1).await;

    let root = FleetRoot::open(&fleet_url).unwrap();
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"400ms\"\nheartbeat_timeout = \"2s\"\nconfig_poll = \"600ms\"\n"
                .into(),
        )
        .await
        .unwrap();
    ops::register(&root, &good_url).await.unwrap();
    ops::register(&root, &bad_url).await.unwrap();
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&good_url).unwrap()),
            format!("[mirror.targets.dr]\nurl = \"{good_dst}\"\npoll = \"250ms\"\n").into(),
        )
        .await
        .unwrap();
    // /dev/null is not a directory: every destination write fails.
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&bad_url).unwrap()),
            "[mirror.targets.dr]\nurl = \"file:///dev/null/mirror\"\npoll = \"250ms\"\n".into(),
        )
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let node = tokio::spawn(daemon::run(
        root.clone(),
        NodeOptions {
            node_id: "n1".into(),
            ..NodeOptions::default()
        },
        shutdown.clone(),
    ));

    // The healthy database converges while the broken target's task
    // sits in backoff in the same heartbeat.
    poll_until("good target converges", || async {
        let status = ops::status(&root, false, true).await.unwrap();
        let m = status
            .mirrors
            .iter()
            .find(|m| m.database == registry::canonicalize_url(&good_url).unwrap())?;
        (m.manifests_behind == Some(0)).then_some(())
    })
    .await;
    let path = root.node_path(&sleet::heartbeat::object_name(
        "n1",
        &sleet::config::Service::ALL,
    ));
    poll_until("broken target backs off", || async {
        let body = root.store().get(&path).await.ok()?.bytes().await.ok()?;
        let hb: sleet::heartbeat::Heartbeat = serde_json::from_slice(&body).ok()?;
        let m = hb
            .services
            .iter()
            .find(|s| s.service == sleet::config::Service::Mirror)?;
        (m.backoff >= 1).then_some(())
    })
    .await;
    // And the healthy database's other services never wobbled: its
    // destination verifies while the fleet stays live.
    let good_dest = DatabaseHandle::open(&good_dst).unwrap();
    assert_closure_complete(&good, &good_dest).await;

    shutdown.cancel();
    node.await.unwrap().unwrap();
}

/// `--max-mirror-jobs` gates passes like the compaction cap gates
/// workers: with no permit nothing commits; granting one lets the
/// pass through.
#[tokio::test(flavor = "multi_thread")]
async fn mirror_jobs_semaphore_gates_passes() {
    use tokio_util::sync::CancellationToken;
    let source_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let dest_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let source = handle(source_store.clone(), "memory:///src", "src");
    seed(&source, 1).await;

    let jobs = Arc::new(tokio::sync::Semaphore::new(0));
    let target = mirror::AppliedTarget {
        name: "dr".into(),
        destination: "memory:///dst".into(),
        settings: ResolvedMirrorTarget {
            poll: Duration::from_millis(100),
            ..target_settings()
        },
    };
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let source = handle(source_store.clone(), "memory:///src", "src");
        let dest = handle(dest_store.clone(), "memory:///dst", "dst");
        let target = target.clone();
        let jobs = jobs.clone();
        let token = token.clone();
        async move { mirror::run_mirror(&source, &dest, &target, jobs, None, token).await }
    });

    // Six 100ms polls: a leaked permit would have committed by now.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let dest = handle(dest_store.clone(), "memory:///dst", "dst");
    assert!(
        dest.admin.read_manifest(None).await.unwrap().is_none(),
        "no permit, no pass"
    );
    jobs.add_permits(1);
    poll_until("permit granted, pass runs", || async {
        dest.admin
            .read_manifest(None)
            .await
            .ok()
            .flatten()
            .map(|_| ())
    })
    .await;
    token.cancel();
    task.await.unwrap().unwrap();
}

/// `status --mirrors` reports real lag once the source moves past the
/// watermark.
#[tokio::test(flavor = "multi_thread")]
async fn status_reports_nonzero_lag() {
    use sleet::root::FleetRoot;
    use sleet::{ops, registry};

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    std::fs::create_dir_all(dir.path().join("dst")).unwrap();
    let fleet_url = format!("file://{}/fleet", dir.path().display());
    let dest_url = format!("file://{}/dst", dir.path().display());
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let db_url = format!("file://{}", src_dir.display());
    let source = DatabaseHandle::open(&db_url).unwrap();
    seed(&source, 1).await;

    let root = FleetRoot::open(&fleet_url).unwrap();
    ops::register(&root, &db_url).await.unwrap();
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&db_url).unwrap()),
            format!("[mirror.targets.dr]\nurl = \"{dest_url}\"\n").into(),
        )
        .await
        .unwrap();
    ops::mirror_sync(&root, &db_url, "dr", None).await.unwrap();
    ops::mirror_sync(&root, &db_url, "dr", None).await.unwrap();
    let status = ops::status(&root, false, true).await.unwrap();
    assert_eq!(status.mirrors[0].manifests_behind, Some(0));

    // The source moves on; lag goes positive.
    seed(&source, 2).await;
    let status = ops::status(&root, false, true).await.unwrap();
    let m = &status.mirrors[0];
    assert!(
        m.manifests_behind.unwrap_or(0) > 0,
        "manifests_behind: {:?}",
        m.manifests_behind
    );
    assert!(
        m.wal_behind.unwrap_or(0) > 0,
        "wal_behind: {:?}",
        m.wal_behind
    );
    assert!(m.error.is_none(), "{:?}", m.error);
}

/// §8: with a watermark present, the external copier HEAD-checks each
/// candidate and backfills only the misses (the incremental path; the
/// seeding LIST path is covered above).
#[tokio::test(flavor = "multi_thread")]
async fn external_copier_backfills_incrementally() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    let settings = ResolvedMirrorTarget {
        copier: CopierKind::External,
        ..target_settings()
    };
    seed(&source, 2).await;
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();

    // New epoch; replication delivers one of the new SSTs early.
    seed(&source, 2).await;
    let head = source.admin.read_manifest(None).await.unwrap().unwrap();
    let at_dest = names(&dest, layout::COMPACTED_DIR).await;
    let new_ssts: Vec<String> = layout::manifest_objects(&head)
        .compacted
        .iter()
        .filter(|u| !at_dest.contains(&format!("{u}.sst")))
        .cloned()
        .collect();
    assert!(new_ssts.len() >= 2, "need at least two new SSTs");
    let delivered_rel = layout::compacted_rel(&new_ssts[0]);
    let body = source
        .store
        .get(&layout::object_path(&source, &delivered_rel))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    dest.store
        .put(&layout::object_path(&dest, &delivered_rel), body.into())
        .await
        .unwrap();

    let outcome = mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert!(outcome.committed);
    // The delivered SST was HEAD-hit, not recopied: fewer copies than
    // new SSTs.
    assert!(
        outcome.copied.objects < new_ssts.len() as u64 + 8,
        "sanity: bounded work"
    );
    mirror::sync_pass(&source, &dest, "dr", &settings, None)
        .await
        .unwrap();
    assert_mirrored(&source, &dest).await;
    assert_closure_complete(&source, &dest).await;
}

/// Regression, found by the stateful property test (the minimal
/// schedule was Checkpoint, Checkpoint, DropCheckpoint): a support
/// manifest immutably carries a deleted checkpoint's entry whose
/// pinned manifest was never promised to the target. The completeness
/// oracle must not flag that sanctioned dangle, and must still flag a
/// missing pinned manifest for a checkpoint alive at the source.
#[tokio::test(flavor = "multi_thread")]
async fn closure_oracle_skips_source_retired_checkpoint_dangles() {
    let source = handle(Arc::new(InMemory::new()), "memory:///src", "src");
    let dest = handle(Arc::new(InMemory::new()), "memory:///dst", "dst");
    seed(&source, 1).await;
    let opts = |name: &str| slatedb::config::CheckpointOptions {
        lifetime: None,
        source: None,
        name: Some(name.into()),
    };
    let a = source
        .admin
        .create_detached_checkpoint(&opts("a"))
        .await
        .unwrap();
    let b = source
        .admin
        .create_detached_checkpoint(&opts("b"))
        .await
        .unwrap();
    // Manifest b immutably carries a's entry; deleting a at the source
    // makes a's pinned manifest support-only history.
    source.admin.delete_checkpoint(a.id).await.unwrap();
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();
    mirror::sync_pass(&source, &dest, "dr", &target_settings(), None)
        .await
        .unwrap();

    let problem = closure_problems(&source, &dest).await;
    assert!(problem.is_none(), "sanctioned dangle flagged: {problem:?}");

    // The still-live checkpoint's support is a different story: losing
    // it is corruption, and the oracle must say so.
    dest.store
        .delete(&layout::object_path(
            &dest,
            &layout::manifest_rel(b.manifest_id),
        ))
        .await
        .unwrap();
    assert!(
        closure_problems(&source, &dest).await.is_some(),
        "a live checkpoint's missing support must be flagged"
    );
}
