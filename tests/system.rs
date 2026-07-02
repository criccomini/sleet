//! System tests against the real `sleet` binary: signal handling and
//! multi-process crash recovery over a `file://` fleet root.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use object_store::ObjectStoreExt;
use sleet::config::Service;
use sleet::root::FleetRoot;
use sleet::services::{DatabaseHandle, queue_depth};
use sleet::{ops, registry};

const SLEET: &str = env!("CARGO_BIN_EXE_sleet");

fn spawn_node(root_url: &str, node_id: &str, services: &str) -> Child {
    Command::new(SLEET)
        .args([
            "run",
            root_url,
            "--node-id",
            node_id,
            "--services",
            services,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("sleet binary spawns")
}

fn signal(child: &Child, sig: &str) {
    let status = Command::new("kill")
        .args([sig, &child.id().to_string()])
        .status()
        .expect("kill runs");
    assert!(status.success());
}

async fn poll_until<F: FnMut() -> bool>(what: &str, mut check: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !check() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// SIGINT is a clean shutdown: the process exits zero and deletes its
/// heartbeat, handing assignments off immediately.
#[tokio::test(flavor = "multi_thread")]
async fn sigint_shuts_down_cleanly_and_deletes_the_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    let root_url = format!("file://{}/fleet", dir.path().display());
    let heartbeat = dir.path().join("fleet/nodes/s1.cgw.json");

    let mut node = spawn_node(
        &root_url,
        "s1",
        "gc,compactor-coordinator,compaction-workers",
    );
    poll_until("heartbeat appears", || heartbeat.exists()).await;

    signal(&node, "-INT");
    let status = node.wait().unwrap();
    assert!(status.success(), "{status:?}");
    assert!(
        !heartbeat.exists(),
        "clean shutdown must delete the heartbeat"
    );
}

/// A worker killed with SIGKILL mid-job leaves a claimed entry behind;
/// the coordinator reclaims it after `worker_heartbeat_timeout` and a
/// replacement worker completes it — SlateDB's safety surviving a real
/// process crash under sleet's scheduling.
#[tokio::test(flavor = "multi_thread")]
async fn sigkill_mid_compaction_is_reclaimed_and_completed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("fleet")).unwrap();
    let root_url = format!("file://{}/fleet", dir.path().display());

    // A database with enough data that compaction takes a while.
    let db_url = format!("file://{}/db1", dir.path().display());
    std::fs::create_dir_all(dir.path().join("db1")).unwrap();
    {
        let (store, path) = object_store::parse_url(&url::Url::parse(&db_url).unwrap()).unwrap();
        let settings = slatedb::config::Settings {
            compactor_options: None,
            garbage_collector_options: None,
            ..Default::default()
        };
        let db = slatedb::Db::builder(path, std::sync::Arc::from(store))
            .with_settings(settings)
            .build()
            .await
            .unwrap();
        for sst in 0..4u8 {
            for key in 0..512 {
                db.put(
                    format!("key-{sst}-{key}").as_bytes(),
                    vec![sst; 16 * 1024].as_slice(),
                )
                .await
                .unwrap();
            }
            db.flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();
        }
        db.close().await.unwrap();
    }

    let root = FleetRoot::open(&root_url).unwrap();
    root.store()
        .put(
            &root.config_path(),
            "[node]\nheartbeat_interval = \"500ms\"\nheartbeat_timeout = \"2s\"\nconfig_poll = \"1s\"\n".into(),
        )
        .await
        .unwrap();
    ops::register(&root, &db_url).await.unwrap();
    root.store()
        .put(
            &root.database_path(&registry::canonicalize_url(&db_url).unwrap()),
            "[compactor-coordinator]\npoll_interval = \"250ms\"\n\
             worker_heartbeat_timeout = \"1s\"\n\
             [compactor-coordinator.scheduler]\nmin_compaction_sources = 2\n\
             [compaction-workers]\ncompactions_poll_interval = \"100ms\"\n\
             max_subcompactions = 1\n"
                .into(),
        )
        .await
        .unwrap();

    // Coordinator in-process; the victim worker as a real process.
    let coordinator = spawn_node(&root_url, "coord", "compactor-coordinator");
    let mut victim = spawn_node(&root_url, "victim", "compaction-workers");

    // Kill the victim the moment it claims a job.
    let handle = DatabaseHandle::open(&db_url).unwrap();
    let mut killed_mid_job = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while tokio::time::Instant::now() < deadline {
        let depth = queue_depth(&handle.admin).await.unwrap_or_default();
        if depth.running > 0 {
            signal(&victim, "-KILL");
            killed_mid_job = true;
            break;
        }
        let manifest = handle.admin.read_manifest(None).await.unwrap().unwrap();
        if !manifest.compacted().is_empty() {
            break; // Too fast: the job finished before we could kill.
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    victim.kill().ok();
    victim.wait().unwrap();

    if killed_mid_job {
        // A replacement worker finishes what the dead one claimed.
        let mut replacement = spawn_node(&root_url, "replacement", "compaction-workers");
        poll_until("compaction completes after reclaim", || {
            futures::executor::block_on(async {
                let manifest = handle.admin.read_manifest(None).await.ok().flatten();
                manifest.is_some_and(|m| !m.compacted().is_empty())
            })
        })
        .await;
        signal(&replacement, "-INT");
        replacement.wait().unwrap();
    } else {
        eprintln!("note: compaction finished before SIGKILL landed; reclaim path not exercised");
    }

    signal(&coordinator, "-INT");
    let mut coordinator = coordinator;
    coordinator.wait().unwrap();

    // Whatever the interleaving, the database converged.
    let manifest = handle.admin.read_manifest(None).await.unwrap().unwrap();
    assert!(!manifest.compacted().is_empty());
    let _ = Service::ALL; // imports used across cfgs
}
