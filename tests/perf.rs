//! Node footprint at scale, run manually:
//!
//! ```sh
//! cargo test --test perf -- --ignored --nocapture
//! ```
//!
//! Establishes the per-node capacity numbers the design's "caps
//! defaulted from the machine" hand-waves: how long one node takes to
//! own and supervise tens of thousands of pairs, and what it costs in
//! memory.

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use sleet::config::Service;
use sleet::daemon::{self, NodeOptions};
use sleet::heartbeat::{self, Heartbeat};
use sleet::registry;
use sleet::root::FleetRoot;
use tokio_util::sync::CancellationToken;

const DATABASES: usize = 20_000;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual footprint measurement; run with --ignored --nocapture"]
async fn footprint_with_tens_of_thousands_of_tasks() {
    let root = FleetRoot::from_parts(
        Arc::new(InMemory::new()),
        StorePath::from("fleet"),
        "memory:///fleet",
    );
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"1s\"\nheartbeat_timeout = \"5s\"\nconfig_poll = \"2s\"\n"
                .into(),
        )
        .await
        .unwrap();

    let setup = Instant::now();
    for i in 0..DATABASES {
        let url = format!("memory:///dbs/db-{i:06}");
        let canonical = registry::canonicalize_url(&url).unwrap();
        root.store()
            .put(
                &root.database_path(&canonical),
                object_store::PutPayload::default(),
            )
            .await
            .unwrap();
    }
    println!("registered {DATABASES} databases in {:?}", setup.elapsed());

    let shutdown = CancellationToken::new();
    let start = Instant::now();
    let node = tokio::spawn(daemon::run(
        root.clone(),
        NodeOptions {
            node_id: "n1".into(),
            services: vec![Service::Gc],
            ..NodeOptions::default()
        },
        shutdown.clone(),
    ));

    // Wait until the node reports supervising every pair.
    let path = root.node_path(&heartbeat::object_name("n1", &[Service::Gc]));
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let total: u64 = match root.store().get(&path).await {
            Ok(get) => serde_json::from_slice::<Heartbeat>(&get.bytes().await.unwrap())
                .map(|b| b.services.iter().map(|s| s.running + s.backoff).sum())
                .unwrap_or(0),
            Err(_) => 0,
        };
        if total == DATABASES as u64 {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(300),
            "only {total}/{DATABASES} tasks after {:?}",
            start.elapsed()
        );
    }
    println!("supervising {DATABASES} tasks after {:?}", start.elapsed());

    let rss = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok());
    if let Some(kb) = rss {
        println!("process RSS: {} MiB", kb / 1024);
    }

    let stop = Instant::now();
    shutdown.cancel();
    node.await.unwrap().unwrap();
    println!(
        "clean shutdown of {DATABASES} tasks in {:?}",
        stop.elapsed()
    );
}
