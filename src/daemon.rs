//! The `sleet run` daemon: heartbeat, polling, placement, supervision.
//!
//! One tokio process per node. Every `heartbeat_interval` the node PUTs
//! its heartbeat, LISTs `nodes/`, recomputes ownership by rendezvous
//! ranking from the shared inputs, and reconciles its supervised tasks
//! to exactly the assignments it owns. Every `config_poll` it re-reads
//! `sleet.toml` and the registry. On clean shutdown it deletes its
//! heartbeat, handing its assignments off immediately.
//!
//! Assignment is purely an efficiency mechanism: every failure mode
//! here at worst double-runs a service, which SlateDB's fencing and CAS
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
use crate::{SLATEDB_VERSION, placement};

/// Node-specific settings, from flags only; everything else lives in
/// the fleet root.
#[derive(Clone, Debug)]
pub struct NodeOptions {
    /// Node identity; must be unique within the fleet.
    pub node_id: String,
    /// Services this node offers.
    pub services: Vec<Service>,
    /// Maximum databases compacting on this node at once.
    pub max_compaction_jobs: usize,
}

/// A daemon that could not start.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    #[error("invalid node id: {0}")]
    NodeId(String),
    #[error(transparent)]
    Root(#[from] crate::root::OpenError),
}

/// One `(database, service)` assignment.
type Assignment = (String, Service);

/// Supervised task state, aggregated into heartbeat bodies.
#[derive(Clone, Copy, PartialEq)]
enum TaskState {
    Running,
    Backoff,
}

type TaskStates = Arc<Mutex<HashMap<Assignment, TaskState>>>;

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
        jobs: Arc::new(Semaphore::new(options.max_compaction_jobs.max(1))),
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
    jobs: Arc<Semaphore>,
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
        for ((_, service), state) in self.states.lock().expect("states lock").iter() {
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

    /// The assignments this node owns: for every registered database
    /// and configured service, the top of the rendezvous ranking over
    /// live nodes offering the service — top 1 for gc and coordinator,
    /// top `count` for workers.
    fn owned_assignments(
        &self,
        entries: &[HeartbeatEntry],
        state: &FleetState,
    ) -> HashMap<Assignment, Arc<ResolvedServices>> {
        let nodes = node_view(entries, state.config.node.heartbeat_timeout.0);
        let mut owned = HashMap::new();
        for (url, db) in &state.databases {
            let resolved = Arc::new(state.config.resolve(Some(db)));
            for &service in &resolved.services {
                let candidates: Vec<&str> = nodes
                    .iter()
                    .filter(|n| n.services.contains(&service))
                    .map(|n| n.node_id.as_str())
                    .collect();
                let count = match service {
                    Service::CompactionWorkers => resolved.workers.count as usize,
                    _ => 1,
                };
                let owners = placement::owners(url, service, count, &candidates);
                if owners.contains(&self.options.node_id.as_str()) {
                    owned.insert((url.clone(), service), resolved.clone());
                }
            }
        }
        owned
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
                info!(database = %key.0, service = key.1.as_str(), "stopping task");
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
            info!(database = %key.0, service = key.1.as_str(), "starting task");
            let token = CancellationToken::new();
            let handle = tokio::spawn(supervise(
                key.clone(),
                resolved.clone(),
                self.jobs.clone(),
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

/// Supervise one `(database, service)` task: run it, restart with
/// backoff on failure. A fence is treated as view skew, not a plain
/// failure: wait one heartbeat interval — giving the rival time to
/// refresh and stand down — then rerun; if the pair has actually moved,
/// the daemon's next ownership recompute cancels this task.
async fn supervise(
    key: Assignment,
    resolved: Arc<ResolvedServices>,
    jobs: Arc<Semaphore>,
    states: TaskStates,
    token: CancellationToken,
    heartbeat_interval: Duration,
) {
    let (url, service) = &key;
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    loop {
        set_state(&states, &key, TaskState::Running);
        let result = match DatabaseHandle::open(url) {
            Ok(db) => {
                services::run_service(&db, *service, &resolved, jobs.clone(), token.child_token())
                    .await
            }
            Err(e) => Err(e),
        };
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
        set_state(&states, &key, TaskState::Backoff);
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
    }
    states.lock().expect("states lock").remove(&key);
}

fn set_state(states: &TaskStates, key: &Assignment, state: TaskState) {
    states
        .lock()
        .expect("states lock")
        .insert(key.clone(), state);
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
