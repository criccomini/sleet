//! Chaos tests: multi-node fleets under injected store faults, an
//! asymmetric partition, and reader clock skew. Every run asserts the
//! design's invariants (no panics, ownership converges after faults
//! stop, and duplication is the worst case) rather than specific
//! interleavings.

mod common;

use std::time::Duration;

use common::{Cluster, expected_pairs, poll_until};
use sleet::config::Service;
use sleet::testing::{Op, TestClock};
use sleet::{ops, placement};

/// A fleet running under a 20% fault rate on every store operation
/// keeps going, and once the faults stop it converges to exactly the
/// ranked placement.
#[tokio::test(flavor = "multi_thread")]
async fn faulted_fleet_converges_after_healing() {
    let mut cluster = Cluster::new().await;
    let ids = ["n1", "n2", "n3"];
    let dbs: Vec<String> = (0..6).map(|i| format!("memory:///dbs/chaos{i}")).collect();
    for db in &dbs {
        cluster.register(db).await;
    }
    for id in ids {
        cluster.spawn(id, &Service::ALL);
    }
    // Wait for every node's first heartbeat before injecting faults: a
    // fault on a node's *initial* config read would leave it on
    // built-in defaults (10s heartbeat), which isn't the scenario;
    // the design assumes a node reads its config once before faults.
    for id in ids {
        poll_until("node heartbeats before faults", || async {
            cluster.body(id, &Service::ALL).await.map(|_| ())
        })
        .await;
    }

    // Fault every operation type nodes use, deterministically seeded.
    cluster.store.fail_probability(0.2, 42);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!cluster.any_node_died(), "faults must never kill a node");
    cluster.store.heal();

    for id in ids {
        let want = expected_pairs(id, &ids, &dbs);
        poll_until("post-heal convergence", || async {
            (cluster.task_count(id, &Service::ALL).await == want).then_some(())
        })
        .await;
    }
    assert!(!cluster.any_node_died());
    cluster.shutdown().await;
}

/// An asymmetric partition: one node loses access to the fleet root
/// while its peers stay healthy. The fleet declares it dead and takes
/// over (a double-run at worst, which is safe); when the partition
/// heals the node rejoins and placement converges back.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_node_is_taken_over_and_rejoins() {
    let mut cluster = Cluster::new().await;
    let dbs: Vec<String> = (0..4).map(|i| format!("memory:///dbs/part{i}")).collect();
    for db in &dbs {
        cluster.register(db).await;
    }
    let (a_store, a_root) = cluster.node_root();
    cluster.spawn_on("a", &[Service::Gc], a_root);
    cluster.spawn("b", &[Service::Gc]);

    let ids = ["a", "b"];
    for id in ids {
        let want: u64 = dbs
            .iter()
            .filter(|db| placement::owners(db, Service::Gc, 1, &ids)[0] == id)
            .count() as u64;
        poll_until("initial convergence", || async {
            (cluster.task_count(id, &[Service::Gc]).await == want).then_some(())
        })
        .await;
    }

    // Partition `a` from the fleet root only. Its heartbeats stop, so
    // `b` must take over every database within heartbeat_timeout.
    for op in [Op::Get, Op::Put, Op::List, Op::Delete] {
        a_store.fail_all(op);
    }
    poll_until("survivor owns everything", || async {
        (cluster.task_count("b", &[Service::Gc]).await == dbs.len() as u64).then_some(())
    })
    .await;
    assert!(!cluster.any_node_died(), "a partitioned node keeps running");

    // Heal: `a` re-heartbeats, takes back its share, `b` stands down.
    a_store.heal();
    for id in ids {
        let want: u64 = dbs
            .iter()
            .filter(|db| placement::owners(db, Service::Gc, 1, &ids)[0] == id)
            .count() as u64;
        poll_until("post-heal convergence", || async {
            (cluster.task_count(id, &[Service::Gc]).await == want).then_some(())
        })
        .await;
    }
    cluster.shutdown().await;
}

/// A reader whose clock is skewed far past `heartbeat_timeout` declares
/// every peer dead and takes over the whole fleet. That is the design's
/// stated worst case, a double-run, and it must be stable and safe:
/// the unskewed node keeps its ranked share, the skewed node runs
/// everything, and nobody crashes.
#[tokio::test(flavor = "multi_thread")]
async fn skewed_reader_takes_over_everything_safely() {
    let mut cluster = Cluster::new().await;
    let dbs: Vec<String> = (0..4).map(|i| format!("memory:///dbs/skew{i}")).collect();
    for db in &dbs {
        cluster.register(db).await;
    }

    // `fast` reads heartbeat ages through a clock 30s in the future:
    // every peer looks long dead.
    let clock = TestClock::new(chrono::Utc::now() + chrono::Duration::seconds(30));
    let (_, fast_root) = cluster.node_root();
    let fast_root = fast_root.with_clock(clock);
    cluster.spawn_on("fast", &[Service::Gc], fast_root);
    cluster.spawn("sane", &[Service::Gc]);

    let ids = ["fast", "sane"];
    // The skewed node owns everything (its view: it is alone). The sane
    // node sees both live and keeps exactly its ranked share.
    let sane_share: u64 = dbs
        .iter()
        .filter(|db| placement::owners(db, Service::Gc, 1, &ids)[0] == "sane")
        .count() as u64;
    poll_until("skewed node runs everything", || async {
        (cluster.task_count("fast", &[Service::Gc]).await == dbs.len() as u64).then_some(())
    })
    .await;
    poll_until("sane node keeps its share", || async {
        (cluster.task_count("sane", &[Service::Gc]).await == sane_share).then_some(())
    })
    .await;

    // The overlap is stable, not a crash loop: hold, then require the
    // same steady state again (poll-based: parallel test load can
    // starve heartbeats transiently, which is itself a legal skew).
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!cluster.any_node_died());
    poll_until("overlap steady after hold", || async {
        (cluster.task_count("fast", &[Service::Gc]).await == dbs.len() as u64
            && cluster.task_count("sane", &[Service::Gc]).await == sane_share)
            .then_some(())
    })
    .await;

    // The fleet stays observable throughout.
    let status = ops::status(&cluster.root, false, false).await.unwrap();
    assert_eq!(status.databases.len(), dbs.len());

    cluster.shutdown().await;
}

/// DESIGN-MIRROR §3's completeness invariant, checked on the real code
/// at the model checker's granularity: after every successful mutation
/// of the destination store (each manifest PUT of an ascending commit,
/// each prune DELETE), the destination's latest manifest must hold its
/// full closure. A seeded schedule of writes, checkpoint churn, source
/// GC, passes, prunes, and tail steps runs under injected destination
/// faults (partial copies, partial commits, failed prunes), then heals
/// and must converge to a byte-verified target. Failures reproduce
/// from the printed seed.
mod mirror_completeness {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::Utc;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use object_store::path::Path as StorePath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use sleet::config::{ResolvedGc, ResolvedMirrorTarget};
    use sleet::mirror::{self, layout, pass};
    use sleet::services::{self, DatabaseHandle};
    use sleet::testing::TestStore;

    /// The invariant, exactly the spec's `TargetComplete`: the
    /// destination's latest manifest (if any) holds its own objects,
    /// and for every live checkpoint entry whose checkpoint still
    /// exists at the source, the pinned manifest and its objects.
    /// Entries of checkpoints already retired at the source may dangle
    /// (§3): they resolve nowhere at the source either.
    async fn target_complete(source: &DatabaseHandle, dest: &DatabaseHandle) -> Result<(), String> {
        let head = match dest.admin.read_manifest(None).await {
            Ok(Some(head)) => head,
            Ok(None) => return Ok(()),
            Err(e) => return Err(format!("destination head unreadable: {e}")),
        };
        let src_cps: std::collections::BTreeSet<uuid::Uuid> =
            match source.admin.read_manifest(None).await {
                Ok(Some(src_head)) => src_head.checkpoints().iter().map(|cp| cp.id).collect(),
                Ok(None) => return Ok(()),
                Err(e) => return Err(format!("source head unreadable: {e}")),
            };
        let now = Utc::now();
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
            let manifest = match dest.admin.read_manifest(Some(id)).await {
                Ok(Some(manifest)) => manifest,
                _ => {
                    return Err(format!(
                        "head {}: pinned manifest {id} missing at the destination",
                        head.id()
                    ));
                }
            };
            for rel in layout::manifest_objects(&manifest).rel_names() {
                if dest
                    .store
                    .head(&layout::object_path(dest, &rel))
                    .await
                    .is_err()
                {
                    return Err(format!("head {}: {rel} missing (member {id})", head.id()));
                }
            }
        }
        Ok(())
    }

    /// A destination-store decorator that asserts `target_complete`
    /// after every successful mutation. Reads pass straight through.
    /// Faults are injected by the wrapped store, so a failed operation
    /// mutates nothing and is not checked.
    struct CompleteStore {
        inner: Arc<dyn ObjectStore>,
        /// Handles over the same objects for reading the state each
        /// mutation just produced.
        source: Arc<DatabaseHandle>,
        dest: Arc<DatabaseHandle>,
        seed: u64,
    }

    impl CompleteStore {
        async fn check(&self, op: &str, location: &StorePath) {
            check_at(&self.source, &self.dest, self.seed, op, location).await;
        }
    }

    async fn check_at(
        source: &DatabaseHandle,
        dest: &DatabaseHandle,
        seed: u64,
        op: &str,
        location: &StorePath,
    ) {
        if let Err(problem) = target_complete(source, dest).await {
            panic!("seed {seed}: completeness violated after {op} {location}: {problem}");
        }
    }

    impl std::fmt::Display for CompleteStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CompleteStore({})", self.inner)
        }
    }

    impl std::fmt::Debug for CompleteStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CompleteStore({:?})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for CompleteStore {
        async fn put_opts(
            &self,
            location: &StorePath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let result = self.inner.put_opts(location, payload, opts).await?;
            self.check("put", location).await;
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &StorePath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            // Checked only after single-shot puts: a multipart upload
            // mutates nothing until it finishes, and the schedule's
            // objects stay below the multipart threshold.
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &StorePath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &StorePath,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<bytes::Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<StorePath>>,
        ) -> BoxStream<'static, object_store::Result<StorePath>> {
            let source = self.source.clone();
            let dest = self.dest.clone();
            let seed = self.seed;
            self.inner
                .delete_stream(locations)
                .then(move |result| {
                    let source = source.clone();
                    let dest = dest.clone();
                    async move {
                        if let Ok(path) = &result {
                            check_at(&source, &dest, seed, "delete", path).await;
                        }
                        result
                    }
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&StorePath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&StorePath>,
            offset: &StorePath,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&StorePath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &StorePath,
            to: &StorePath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await?;
            self.check("copy", to).await;
            Ok(())
        }
    }

    fn roll(rng: &mut u64, bound: u64) -> u64 {
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        *rng % bound
    }

    async fn run_seed(seed: u64) {
        let source_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let source = DatabaseHandle::from_parts(
            "memory:///src",
            source_store.clone(),
            StorePath::from("src"),
        );
        // Layers, outermost first: the pass and prune write through
        // CompleteStore (invariant after every mutation) into
        // TestStore (seeded faults) into memory. The checker reads
        // through its own handles below the fault layer.
        let raw: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let faulty = TestStore::new(raw.clone());
        faulty.fail_probability(0.12, seed);
        let source_checker = Arc::new(DatabaseHandle::from_parts(
            "memory:///src",
            source_store.clone(),
            StorePath::from("src"),
        ));
        let checker = Arc::new(DatabaseHandle::from_parts(
            "memory:///dst",
            raw.clone(),
            StorePath::from("dst"),
        ));
        let dest = DatabaseHandle::from_parts(
            "memory:///dst",
            Arc::new(CompleteStore {
                inner: faulty.clone(),
                source: source_checker.clone(),
                dest: checker.clone(),
                seed,
            }),
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
                // No compactor runs here, and a full L0 blocks flushes
                // until one would shrink it; keep the cap above the
                // schedule's worst-case flush count.
                l0_max_ssts: 64,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let mut rng = seed.max(1);
        let mut batch = 0u32;
        let mut checkpoints: Vec<uuid::Uuid> = Vec::new();
        let mut tail: Option<pass::Tail> = None;
        let mut faulted_ops = 0u32;
        for _ in 0..30 {
            match roll(&mut rng, 8) {
                0 | 1 => {
                    for i in 0..=roll(&mut rng, 3) {
                        writer
                            .put(
                                format!("k-{batch}-{i}").as_bytes(),
                                vec![7u8; 64].as_slice(),
                            )
                            .await
                            .unwrap();
                    }
                    batch += 1;
                    writer
                        .flush_with_options(slatedb::config::FlushOptions {
                            flush_type: slatedb::config::FlushType::MemTable,
                        })
                        .await
                        .unwrap();
                }
                2 => {
                    let result = source
                        .admin
                        .create_detached_checkpoint(&slatedb::config::CheckpointOptions {
                            lifetime: None,
                            source: None,
                            name: Some(format!("op-{batch}")),
                        })
                        .await
                        .unwrap();
                    checkpoints.push(result.id);
                }
                3 => {
                    if !checkpoints.is_empty() {
                        let id = checkpoints.remove(0);
                        source.admin.delete_checkpoint(id).await.unwrap();
                    }
                }
                4 => {
                    source
                        .admin
                        .run_gc_once(services::gc_options(&gc))
                        .await
                        .unwrap();
                }
                5 | 6 => {
                    // Passes may fail on injected destination faults;
                    // every mutation they did make was checked.
                    match mirror::sync_pass(&source, &dest, "dr", &settings, None).await {
                        Ok(outcome) => match &mut tail {
                            Some(t) => t.advance_floor(outcome.next_wal_sst_id),
                            None => {
                                tail = Some(
                                    pass::Tail::start(&dest, outcome.next_wal_sst_id)
                                        .await
                                        .unwrap(),
                                )
                            }
                        },
                        Err(_) => faulted_ops += 1,
                    }
                }
                _ => {
                    let pruned = mirror::prune(&source, &dest, "dr", &settings).await;
                    if pruned.is_err() {
                        faulted_ops += 1;
                    }
                    if let Some(t) = &mut tail
                        && t.step(&source, &dest).await.is_err()
                    {
                        faulted_ops += 1;
                    }
                }
            }
        }
        writer.close().await.unwrap();

        // Heal and converge: the survivors of the fault storm must
        // reach the source's head and byte-verify.
        faulty.heal();
        for _ in 0..3 {
            mirror::sync_pass(&source, &dest, "dr", &settings, None)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: healed sync failed: {e}"));
        }
        let src = source.admin.read_manifest(None).await.unwrap().unwrap();
        let dst = checker.admin.read_manifest(None).await.unwrap().unwrap();
        assert_eq!(
            src.id(),
            dst.id(),
            "seed {seed}: not converged ({faulted_ops} faulted ops)"
        );
        if let Err(problem) = target_complete(&source, &checker).await {
            panic!("seed {seed}: healed target incomplete: {problem}");
        }
    }

    /// The schedule under six seeds; each mutation of the destination
    /// is invariant-checked in flight.
    #[tokio::test(flavor = "multi_thread")]
    async fn mirror_completeness_holds_after_every_destination_mutation() {
        for seed in [3, 11, 42, 0x5EE7, 271_828, 3_141_592] {
            run_seed(seed).await;
        }
    }
}
