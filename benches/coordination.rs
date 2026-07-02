//! Scaling benches for the design's millions-of-databases claim: the
//! per-tick placement recompute and the `config_poll` registry read.
//!
//! Run with `cargo bench`. Placement is the pure hot loop every node
//! runs each tick; the poller bench measures a full re-poll over an
//! in-memory registry of empty files (the common case: LIST only, no
//! body GETs).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_store::ObjectStoreExt;
use object_store::path::Path as StorePath;
use sleet::config::Service;
use sleet::root::{ConfigPoller, FleetRoot};
use sleet::testing::TestStore;
use sleet::{placement, registry};

const NODES: usize = 20;

fn node_ids() -> Vec<String> {
    (0..NODES).map(|i| format!("sleet-{i:02}")).collect()
}

fn db_urls(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("s3://fleet/dbs/db-{i:07}"))
        .collect()
}

/// The full ownership recompute a node performs each tick: rank every
/// (database, service) over the candidate set.
fn placement_recompute(c: &mut Criterion) {
    let ids = node_ids();
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut group = c.benchmark_group("placement_recompute");
    group.sample_size(10);
    for n in [1_000usize, 10_000, 100_000] {
        let dbs = db_urls(n);
        group.throughput(Throughput::Elements((n * 3) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &dbs, |b, dbs| {
            b.iter(|| {
                let mut owned = 0u64;
                for db in dbs {
                    for service in Service::ALL {
                        let count = match service {
                            Service::CompactionWorkers => 2,
                            _ => 1,
                        };
                        if placement::owners(db, service, count, &refs).contains(&"sleet-00") {
                            owned += 1;
                        }
                    }
                }
                owned
            })
        });
    }
    group.finish();
}

/// One full config_poll over a registry of empty files: a paginated
/// LIST and zero body GETs.
fn registry_poll(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("registry_poll");
    group.sample_size(10);
    for n in [1_000usize, 10_000, 50_000] {
        let root = FleetRoot::from_parts(
            TestStore::in_memory(),
            StorePath::from("fleet"),
            "memory:///fleet",
        );
        rt.block_on(async {
            for db in db_urls(n) {
                let canonical = registry::canonicalize_url(&db).unwrap();
                root.store()
                    .put(
                        &root.database_path(&canonical),
                        object_store::PutPayload::default(),
                    )
                    .await
                    .unwrap();
            }
        });
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &root, |b, root| {
            b.iter(|| {
                rt.block_on(async {
                    let mut poller = ConfigPoller::default();
                    let state = poller.poll(root).await;
                    assert_eq!(state.databases.len(), n);
                    state.databases.len()
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, placement_recompute, registry_poll);
criterion_main!(benches);
