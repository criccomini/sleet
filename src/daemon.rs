//! The `sleet run` daemon: heartbeat, polling, placement, supervision.
//!
//! One tokio process per node. Every `heartbeat_interval` the node PUTs
//! its heartbeat, LISTs `nodes/`, recomputes ownership by rendezvous
//! ranking from the shared inputs, and reconciles its supervised tasks
//! to exactly the assignments it owns. Every `config_poll` it re-reads
//! `sleet.toml` and the registry. On clean shutdown it deletes its
//! heartbeat, handing its assignments off immediately.
//!
//! Assignment is an efficiency mechanism only: every failure mode here
//! at worst double-runs a service, which SlateDB's fencing and CAS
//! claims make safe. A stale live-set read keeps the previous
//! assignments; a fenced coordinator waits one heartbeat interval and
//! reruns only if the next recompute still owns the pair.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use object_store::ObjectStoreExt;
use object_store::PutPayload;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{ResolvedServices, Service};
use crate::heartbeat::{self, Heartbeat, ServiceSummary};
use crate::root::{ConfigPoller, FleetRoot, FleetState, HeartbeatEntry, node_view};
use crate::services::{self, DatabaseHandle};
use crate::{SLATEDB_VERSION, mirror, placement};

/// Node-specific settings, from flags only; everything else lives in
/// the fleet root.
#[derive(Clone, Debug)]
pub struct NodeOptions {
    /// Node identity; must be unique within the fleet.
    pub node_id: String,
    /// Services this node offers.
    pub services: Vec<Service>,
    /// Maximum `(database, target)` mirror jobs copying or pruning on
    /// this node at once.
    pub max_mirror_jobs: usize,
    /// Path to the rclone binary for `copier = "rclone"` targets.
    pub rclone: Option<String>,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            services: Service::ALL.to_vec(),
            max_mirror_jobs: 1,
            rclone: None,
        }
    }
}

/// A daemon that could not start.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// The node id fails [`heartbeat::validate_node_id`].
    #[error("invalid node id: {0}")]
    NodeId(String),
    /// The fleet root could not be opened.
    #[error(transparent)]
    Root(#[from] crate::root::OpenError),
}

/// One assignment: a `(database, service)` pair, or a `(database,
/// mirror, target)` triple when the service is `mirror` (the third
/// element is the target name, `None` for every other service).
pub type Assignment = (String, Service, Option<String>);

/// The assignments `node_id` owns: for every registered database and
/// configured service, the top of the rendezvous ranking over live
/// nodes offering the service; top 1 for gc and coordinator, top
/// `count` for workers, and per applied target for mirror. A node
/// always counts itself live: reading its own heartbeat as stale (a
/// skewed clock, a slow PUT) must not make it drop its share, because
/// peers that consider it dead take over in parallel, which is a safe
/// double-run, whereas excluding itself would leave the share unowned.
/// The daemon calls this every tick; pub so the model-based test
/// drives the same decision function.
pub fn owned_assignments(
    node_id: &str,
    services: &[Service],
    entries: &[HeartbeatEntry],
    state: &FleetState,
) -> HashMap<Assignment, Arc<ResolvedServices>> {
    let mut nodes = node_view(entries, state.config.node.heartbeat_timeout.0);
    if !nodes.iter().any(|n| n.node_id == node_id) {
        nodes.push(crate::root::NodeView {
            node_id: node_id.to_string(),
            services: services.to_vec(),
            age: Duration::ZERO,
        });
    }
    let mut owned = HashMap::new();
    for (url, db) in &state.databases {
        let resolved = Arc::new(state.config.resolve(Some(db)));
        for &service in &resolved.services {
            let candidates: Vec<&str> = nodes
                .iter()
                .filter(|n| n.services.contains(&service))
                .map(|n| n.node_id.as_str())
                .collect();
            if service == Service::Mirror {
                // Placement extends the pair to a triple: each enabled
                // (database, mirror, target) goes to the top-ranked
                // live node offering the service.
                for applied in mirror::applied_targets(url, &resolved.mirror) {
                    if placement::owner_target(url, &applied.name, &candidates) == Some(node_id) {
                        owned.insert((url.clone(), service, Some(applied.name)), resolved.clone());
                    }
                }
                continue;
            }
            let count = match service {
                Service::CompactionWorkers => resolved.workers.count as usize,
                _ => 1,
            };
            let owners = placement::owners(url, service, count, &candidates);
            if owners.contains(&node_id) {
                owned.insert((url.clone(), service, None), resolved.clone());
            }
        }
    }
    owned
}

/// Supervised task state, aggregated into heartbeat bodies.
#[derive(Clone, Copy, PartialEq)]
enum TaskState {
    Running,
    Backoff,
}

/// States are tagged with the supervisor instance that wrote them: on a
/// config-change restart the old and new supervisor briefly share a
/// key, and the old one's shutdown must not erase the new one's entry.
type TaskStates = Arc<Mutex<HashMap<Assignment, (u64, TaskState)>>>;

static NEXT_INSTANCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct RunningTask {
    token: CancellationToken,
    handle: JoinHandle<()>,
    /// Fingerprint of the resolved config the task was started with; a
    /// change restarts the task.
    fingerprint: u64,
}

/// Run a fleet node until `shutdown` fires.
pub async fn run(
    root: FleetRoot,
    options: NodeOptions,
    shutdown: CancellationToken,
) -> Result<(), DaemonError> {
    heartbeat::validate_node_id(&options.node_id).map_err(DaemonError::NodeId)?;
    let node = Node {
        object_name: heartbeat::object_name(&options.node_id, &options.services),
        mirror_jobs: Arc::new(Semaphore::new(options.max_mirror_jobs.max(1))),
        states: TaskStates::default(),
        options,
        root,
    };
    node.run(shutdown).await;
    Ok(())
}

struct Node {
    options: NodeOptions,
    object_name: String,
    root: FleetRoot,
    mirror_jobs: Arc<Semaphore>,
    states: TaskStates,
}

impl Node {
    async fn run(&self, shutdown: CancellationToken) {
        info!(
            node_id = %self.options.node_id,
            root = %self.root.url(),
            heartbeat = %self.object_name,
            "sleet node starting"
        );
        let mut poller = ConfigPoller::default();
        let mut state = poller.poll(&self.root).await;
        log_warnings(&state);
        let mut last_poll = Instant::now();
        let mut tasks: HashMap<Assignment, RunningTask> = HashMap::new();

        loop {
            self.put_heartbeat().await;

            if last_poll.elapsed() >= state.config.node.config_poll.0 {
                state = poller.poll(&self.root).await;
                log_warnings(&state);
                last_poll = Instant::now();
            }

            match self.root.list_heartbeats().await {
                Ok(entries) => {
                    self.housekeeping(&entries, &state).await;
                    let owned = self.owned_assignments(&entries, &state);
                    self.reconcile(&mut tasks, owned, &state);
                }
                // A stale live set at worst double-runs: keep the
                // current assignments and try again next tick.
                Err(e) => warn!("failed to LIST nodes/: {e}; keeping current assignments"),
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(state.config.node.heartbeat_interval.0) => {}
            }
        }

        info!("shutting down: stopping tasks and deleting heartbeat");
        for task in tasks.values() {
            task.token.cancel();
        }
        let stop_all = futures::future::join_all(tasks.into_values().map(|t| t.handle));
        let _ = tokio::time::timeout(Duration::from_secs(30), stop_all).await;
        // Deleting the heartbeat hands every assignment off immediately.
        if let Err(e) = self
            .root
            .store()
            .delete(&self.root.node_path(&self.object_name))
            .await
        {
            warn!("failed to delete heartbeat on shutdown: {e}");
        }
    }

    /// PUT this node's heartbeat: liveness and roles in the name,
    /// versions and aggregate task states in the body.
    async fn put_heartbeat(&self) {
        let mut summaries: HashMap<Service, ServiceSummary> = self
            .options
            .services
            .iter()
            .map(|&service| (service, ServiceSummary::empty(service)))
            .collect();
        for ((_, service, _), (_, state)) in self.states.lock().expect("states lock").iter() {
            let summary = summaries
                .entry(*service)
                .or_insert(ServiceSummary::empty(*service));
            match state {
                TaskState::Running => summary.running += 1,
                TaskState::Backoff => summary.backoff += 1,
            }
        }
        let mut services: Vec<ServiceSummary> = summaries.into_values().collect();
        services.sort_by_key(|s| s.service.letter());
        let body = Heartbeat::new(&self.options.node_id, SLATEDB_VERSION, services);
        let json = serde_json::to_vec(&body).expect("heartbeat serializes");
        let path = self.root.node_path(&self.object_name);
        if let Err(e) = self.root.store().put(&path, PutPayload::from(json)).await {
            warn!("failed to PUT heartbeat: {e}");
        }
    }

    /// Delete this node's old-named heartbeats (a role change) and any
    /// heartbeat dead for 10x `heartbeat_timeout`.
    async fn housekeeping(&self, entries: &[HeartbeatEntry], state: &FleetState) {
        let own = self.root.node_path(&self.object_name);
        let long_dead = state.config.node.heartbeat_timeout.0 * 10;
        for entry in entries {
            let stale_own = entry.node_id == self.options.node_id && entry.location != own;
            if (stale_own || entry.age >= long_dead)
                && let Err(e) = self.root.store().delete(&entry.location).await
            {
                warn!("failed to delete heartbeat {}: {e}", entry.location);
            }
        }
    }

    /// The assignments this node owns; see [`owned_assignments`].
    fn owned_assignments(
        &self,
        entries: &[HeartbeatEntry],
        state: &FleetState,
    ) -> HashMap<Assignment, Arc<ResolvedServices>> {
        owned_assignments(
            &self.options.node_id,
            &self.options.services,
            entries,
            state,
        )
    }

    /// Reconcile supervised tasks to exactly the owned assignments.
    fn reconcile(
        &self,
        tasks: &mut HashMap<Assignment, RunningTask>,
        owned: HashMap<Assignment, Arc<ResolvedServices>>,
        state: &FleetState,
    ) {
        // Stop tasks for assignments no longer owned or whose resolved
        // config changed.
        let stop: Vec<Assignment> = tasks
            .iter()
            .filter(|(key, task)| match owned.get(*key) {
                Some(resolved) => fingerprint(resolved) != task.fingerprint,
                None => true,
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in stop {
            if let Some(task) = tasks.remove(&key) {
                info!(
                    database = %key.0,
                    service = key.1.as_str(),
                    target = key.2.as_deref().unwrap_or(""),
                    "stopping task"
                );
                task.token.cancel();
            }
        }
        // Reap tasks that exited on their own; if the pair is still
        // owned it restarts below with fresh inputs.
        tasks.retain(|_, task| !task.handle.is_finished());

        for (key, resolved) in owned {
            if tasks.contains_key(&key) {
                continue;
            }
            info!(
                database = %key.0,
                service = key.1.as_str(),
                target = key.2.as_deref().unwrap_or(""),
                "starting task"
            );
            let token = CancellationToken::new();
            let handle = tokio::spawn(supervise(
                key.clone(),
                resolved.clone(),
                self.mirror_jobs.clone(),
                self.options.rclone.clone(),
                self.states.clone(),
                token.clone(),
                state.config.node.heartbeat_interval.0,
            ));
            tasks.insert(
                key,
                RunningTask {
                    token,
                    handle,
                    fingerprint: fingerprint(&resolved),
                },
            );
        }
    }
}

/// Supervise one assignment's task: run it, restart with backoff on
/// failure. A fence is treated as view skew rather than a plain
/// failure: wait one heartbeat interval (giving the rival time to
/// refresh and stand down), then rerun; if the pair has actually moved,
/// the daemon's next ownership recompute cancels this task.
async fn supervise(
    key: Assignment,
    resolved: Arc<ResolvedServices>,
    mirror_jobs: Arc<Semaphore>,
    rclone: Option<String>,
    states: TaskStates,
    token: CancellationToken,
    heartbeat_interval: Duration,
) {
    let (url, service, target) = key.clone();
    let run = move |child: CancellationToken| {
        let url = url.clone();
        let target = target.clone();
        let resolved = resolved.clone();
        let mirror_jobs = mirror_jobs.clone();
        let rclone = rclone.clone();
        async move {
            if service == Service::Mirror {
                let name = target.expect("mirror assignments carry a target");
                let Some(applied) = mirror::applied_targets(&url, &resolved.mirror)
                    .into_iter()
                    .find(|t| t.name == name)
                else {
                    // The target no longer applies; the next ownership
                    // recompute drops the assignment.
                    return Ok(());
                };
                let source = DatabaseHandle::open(&url)?;
                let dest = DatabaseHandle::open(&applied.destination)?;
                mirror::run_mirror(&source, &dest, &applied, mirror_jobs, rclone, child)
                    .await
                    .map_err(services::ServiceError::from)
            } else {
                let db = DatabaseHandle::open(&url)?;
                services::run_service(&db, service, &resolved, child).await
            }
        }
    };
    supervise_with(run, key, states, token, heartbeat_interval).await;
}

/// The supervision loop itself, generic over the task body so the
/// backoff policy is unit-testable.
async fn supervise_with<F, Fut>(
    mut run: F,
    key: Assignment,
    states: TaskStates,
    token: CancellationToken,
    heartbeat_interval: Duration,
) where
    F: FnMut(CancellationToken) -> Fut,
    Fut: Future<Output = Result<(), services::ServiceError>>,
{
    let instance = NEXT_INSTANCE.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let (url, service, _) = &key;
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    loop {
        set_state(&states, &key, instance, TaskState::Running);
        let result = run(token.child_token()).await;
        if token.is_cancelled() {
            break;
        }
        let delay = match result {
            // Clean exit without cancellation; the daemon reaps and
            // restarts if still owned.
            Ok(()) => break,
            Err(e) if e.is_fenced() => {
                info!(database = %url, "coordinator fenced; retrying after one heartbeat interval");
                backoff = Duration::from_secs(1);
                heartbeat_interval
            }
            Err(e) => {
                warn!(database = %url, service = service.as_str(), "task failed: {e}");
                backoff = (backoff * 2).min(MAX_BACKOFF);
                backoff
            }
        };
        set_state(&states, &key, instance, TaskState::Backoff);
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
    }
    // Remove only our own entry: a replacement supervisor for the same
    // key may already have written a newer one.
    let mut states = states.lock().expect("states lock");
    if states.get(&key).is_some_and(|(id, _)| *id == instance) {
        states.remove(&key);
    }
}

/// Write a state unless a newer supervisor instance owns the entry.
fn set_state(states: &TaskStates, key: &Assignment, instance: u64, state: TaskState) {
    let mut states = states.lock().expect("states lock");
    match states.get(key) {
        Some((id, _)) if *id > instance => {}
        _ => {
            states.insert(key.clone(), (instance, state));
        }
    }
}

/// Change-detection fingerprint of a resolved config; not a wire format.
fn fingerprint(resolved: &ResolvedServices) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    format!("{resolved:?}").hash(&mut hasher);
    hasher.finish()
}

fn log_warnings(state: &FleetState) {
    for warning in &state.warnings {
        warn!("{warning}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DatabaseConfig, SleetConfig, WorkersOverrides};
    use crate::root::HeartbeatEntry;
    use crate::testing::TestStore;
    use object_store::path::Path as StorePath;
    use slatedb::{CloseReason, Error as SlateError};

    fn node(node_id: &str, services: &[Service]) -> Node {
        let options = NodeOptions {
            node_id: node_id.into(),
            services: services.to_vec(),
            ..NodeOptions::default()
        };
        Node {
            object_name: heartbeat::object_name(&options.node_id, &options.services),
            mirror_jobs: Arc::new(Semaphore::new(1)),
            states: TaskStates::default(),
            root: FleetRoot::from_parts(
                TestStore::in_memory(),
                StorePath::from("fleet"),
                "memory:///fleet",
            ),
            options,
        }
    }

    fn entry(node_id: &str, services: &[Service], age_secs: u64) -> HeartbeatEntry {
        HeartbeatEntry {
            node_id: node_id.into(),
            services: services.to_vec(),
            age: Duration::from_secs(age_secs),
            location: StorePath::from(format!(
                "fleet/nodes/{}",
                heartbeat::object_name(node_id, services)
            )),
        }
    }

    fn state(databases: &[(&str, DatabaseConfig)]) -> FleetState {
        FleetState {
            config: SleetConfig::default(),
            databases: databases
                .iter()
                .map(|(url, db)| (url.to_string(), db.clone()))
                .collect(),
            warnings: vec![],
        }
    }

    /// Ownership: the ranked owner per service, role-filtered, with
    /// `count` worker owners; both nodes agree on the same placement.
    #[test]
    fn owned_assignments_follow_the_ranking() {
        let dbs = state(&[
            ("s3://b/db1", DatabaseConfig::default()),
            ("s3://b/db2", DatabaseConfig::default()),
        ]);
        let entries = vec![entry("n1", &Service::ALL, 1), entry("n2", &Service::ALL, 1)];
        let n1_owned = node("n1", &Service::ALL).owned_assignments(&entries, &dbs);
        let n2_owned = node("n2", &Service::ALL).owned_assignments(&entries, &dbs);
        // Disjoint single-owner services; every pair owned exactly once.
        for url in ["s3://b/db1", "s3://b/db2"] {
            for service in Service::ALL {
                if service == Service::Mirror {
                    // No targets configured: mirror assigns nothing.
                    continue;
                }
                let expected = placement::owners(url, service, 1, &["n1", "n2"])[0];
                let key = (url.to_string(), service, None);
                assert_eq!(n1_owned.contains_key(&key), expected == "n1");
                assert_eq!(n2_owned.contains_key(&key), expected == "n2");
            }
        }
    }

    /// A node never owns a service it doesn't offer, even when it would
    /// win the ranking.
    #[test]
    fn owned_assignments_respect_roles() {
        let dbs = state(&[("s3://b/db1", DatabaseConfig::default())]);
        let entries = vec![
            entry("n1", &[Service::Gc], 1),
            entry("n2", &[Service::CompactionWorkers], 1),
        ];
        let gc_only = node("n1", &[Service::Gc]).owned_assignments(&entries, &dbs);
        assert!(gc_only.contains_key(&("s3://b/db1".into(), Service::Gc, None)));
        assert!(!gc_only.contains_key(&("s3://b/db1".into(), Service::CompactionWorkers, None)));
        // Coordinator has no live offering node: nobody owns it.
        assert!(!gc_only.contains_key(&("s3://b/db1".into(), Service::CompactorCoordinator, None)));
        let workers_only =
            node("n2", &[Service::CompactionWorkers]).owned_assignments(&entries, &dbs);
        assert_eq!(workers_only.len(), 1);
        assert!(workers_only.contains_key(&(
            "s3://b/db1".into(),
            Service::CompactionWorkers,
            None
        )));
    }

    /// `count = 2` puts workers on two nodes; `services = []` yields
    /// nothing; dead nodes are not candidates.
    #[test]
    fn owned_assignments_count_disabled_and_dead() {
        let two_workers = DatabaseConfig {
            compaction_workers: Some(WorkersOverrides {
                count: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        let disabled = DatabaseConfig {
            services: Some(vec![]),
            ..Default::default()
        };
        let dbs = state(&[("s3://b/db1", two_workers), ("s3://b/off", disabled)]);
        let entries = vec![
            entry("n1", &Service::ALL, 1),
            entry("n2", &Service::ALL, 1),
            entry("n3", &Service::ALL, 999), // dead
        ];
        let worker_key = (
            "s3://b/db1".to_string(),
            Service::CompactionWorkers,
            None::<String>,
        );
        let n1 = node("n1", &Service::ALL).owned_assignments(&entries, &dbs);
        let n2 = node("n2", &Service::ALL).owned_assignments(&entries, &dbs);
        let n3 = node("n3", &Service::ALL).owned_assignments(&entries, &dbs);
        assert!(n1.contains_key(&worker_key) && n2.contains_key(&worker_key));
        // n3's own heartbeat reads as stale (its clock may be wrong),
        // but a node always counts itself live: it keeps exactly the
        // share it wins over the full candidate set, same as everyone
        // else.
        for key in n3.keys() {
            let (url, service, _) = key;
            let count = if *service == Service::CompactionWorkers {
                2
            } else {
                1
            };
            assert!(
                placement::owners(url, *service, count, &["n1", "n2", "n3"]).contains(&"n3"),
                "{key:?}"
            );
        }
        for owned in [&n1, &n2] {
            assert!(!owned.keys().any(|(url, ..)| url == "s3://b/off"));
        }
    }

    /// Mirror ownership is per target: each enabled applied target is
    /// its own assignment, placed by the triple hash over mirror
    /// offerers only, and a database with no applicable target yields
    /// no mirror assignments.
    #[test]
    fn owned_assignments_place_mirror_targets() {
        use crate::config::{MirrorOverrides, MirrorTargetOverrides};
        let mirrored = DatabaseConfig {
            mirror: Some(MirrorOverrides {
                targets: [
                    (
                        "dr".to_string(),
                        MirrorTargetOverrides {
                            url: Some("s3://dr/db1".into()),
                            ..Default::default()
                        },
                    ),
                    (
                        "backup".to_string(),
                        MirrorTargetOverrides {
                            url: Some("gs://backups/db1".into()),
                            ..Default::default()
                        },
                    ),
                ]
                .into(),
            }),
            ..Default::default()
        };
        let dbs = state(&[
            ("s3://b/db1", mirrored),
            ("s3://b/plain", DatabaseConfig::default()),
        ]);
        let entries = vec![
            entry("n1", &Service::ALL, 1),
            entry("n2", &Service::ALL, 1),
            entry("n3", &[Service::Gc], 1), // does not offer mirror
        ];
        let mut owners_seen = Vec::new();
        for id in ["n1", "n2", "n3"] {
            let owned = node(id, &Service::ALL).owned_assignments(&entries, &dbs);
            for target in ["dr", "backup"] {
                let key = (
                    "s3://b/db1".to_string(),
                    Service::Mirror,
                    Some(target.to_string()),
                );
                let expected = placement::owner_target("s3://b/db1", target, &["n1", "n2"]);
                assert_eq!(
                    owned.contains_key(&key),
                    expected == Some(id),
                    "{id} {target}"
                );
                if owned.contains_key(&key) {
                    owners_seen.push((id, target));
                }
            }
            // No applicable target: no mirror assignment at all.
            assert!(
                !owned
                    .keys()
                    .any(|(url, service, _)| url == "s3://b/plain" && *service == Service::Mirror)
            );
        }
        assert_eq!(owners_seen.len(), 2, "each target owned exactly once");
    }

    /// Reconciliation: starts owned pairs, stops unowned ones, restarts
    /// on config change, reaps finished tasks.
    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_diffs_tasks_against_ownership() {
        let node = node("n1", &Service::ALL);
        let fleet = state(&[]);
        let mut tasks = HashMap::new();
        let key = ("memory:///db1".to_string(), Service::Gc, None);
        let resolved = Arc::new(SleetConfig::default().resolve(None));

        let mut owned = HashMap::new();
        owned.insert(key.clone(), resolved.clone());
        node.reconcile(&mut tasks, owned.clone(), &fleet);
        assert!(tasks.contains_key(&key));
        let first_token = tasks[&key].token.clone();

        // Same ownership, same config: the task is left alone.
        node.reconcile(&mut tasks, owned.clone(), &fleet);
        assert!(!first_token.is_cancelled());

        // Changed resolved config: the task restarts.
        let mut changed = SleetConfig::default().resolve(None);
        changed.workers.count = 7;
        let mut owned_changed = HashMap::new();
        owned_changed.insert(key.clone(), Arc::new(changed));
        node.reconcile(&mut tasks, owned_changed, &fleet);
        assert!(first_token.is_cancelled());
        assert!(tasks.contains_key(&key));
        let second_token = tasks[&key].token.clone();

        // No longer owned: the task stops and leaves the map.
        node.reconcile(&mut tasks, HashMap::new(), &fleet);
        assert!(second_token.is_cancelled());
        assert!(tasks.is_empty());
    }

    fn fenced() -> services::ServiceError {
        services::ServiceError::SlateDb(SlateError::closed(
            "fenced by another coordinator".into(),
            CloseReason::Fenced,
        ))
    }

    fn plain() -> services::ServiceError {
        services::ServiceError::SlateDb(SlateError::unavailable("boom".into()))
    }

    /// Backoff policy: a fence waits exactly one heartbeat interval and
    /// resets the exponential backoff; plain errors double toward the
    /// cap. Verified under paused time so delays are exact.
    #[tokio::test(start_paused = true)]
    async fn supervisor_backoff_policy() {
        let outcomes = Arc::new(Mutex::new(vec![
            Err(fenced()),
            Err(plain()),
            Err(plain()),
            Ok(()),
        ]));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let run = {
            let outcomes = outcomes.clone();
            let calls = calls.clone();
            move |_child: CancellationToken| {
                let outcomes = outcomes.clone();
                let calls = calls.clone();
                async move {
                    calls.lock().unwrap().push(tokio::time::Instant::now());
                    outcomes.lock().unwrap().remove(0)
                }
            }
        };
        let heartbeat_interval = Duration::from_secs(7);
        supervise_with(
            run,
            ("db".into(), Service::CompactorCoordinator, None),
            TaskStates::default(),
            CancellationToken::new(),
            heartbeat_interval,
        )
        .await;
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 4);
        // Fence: exactly one heartbeat interval.
        assert_eq!(calls[1] - calls[0], heartbeat_interval);
        // Plain errors: exponential from 2s (the fence reset it to 1s).
        assert_eq!(calls[2] - calls[1], Duration::from_secs(2));
        assert_eq!(calls[3] - calls[2], Duration::from_secs(4));
    }

    /// Cancellation during backoff exits promptly and clears the state.
    #[tokio::test(start_paused = true)]
    async fn supervisor_exits_on_cancel() {
        let states = TaskStates::default();
        let token = CancellationToken::new();
        let run = |_child: CancellationToken| async { Err(plain()) };
        let handle = tokio::spawn(supervise_with(
            run,
            ("db".into(), Service::Gc, None),
            states.clone(),
            token.clone(),
            Duration::from_secs(10),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
        handle.await.unwrap();
        assert!(states.lock().unwrap().is_empty());
    }
}
