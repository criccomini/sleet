# sleet test plan

How sleet is tested and what remains. The principle: test the design's
claims, not just the code. DESIGN.md promises three things — frozen wire
formats, safety (any failure at worst double-runs a service), and
liveness (ownership converges within one `config_poll` plus one
`heartbeat_interval`; failover within `heartbeat_timeout`) — and every
level below traces back to one of them. `[x]` items exist; `[ ]` items
are planned. Priorities: **P0** = an untested design claim, **P1** =
hardening, **P2** = long-range.

## Invariants under test

- **Formats are frozen**: the placement hash, heartbeat object names and
  bodies, registry file names, and config schemas never change meaning
  across versions.
- **Safety**: duplicate or stale sleet processes are harmless. Provable
  as: `compactor_epoch` only advances, jobs complete exactly once, GC
  deletes are idempotent — never as "no overlap ever happens".
- **Liveness**: after faults stop, every `(database, service)` of every
  registered database runs on exactly the ranked owners within the
  documented bounds, and nowhere else.

## Prerequisite test assets

- [x] **Instrumented store wrapper** — `sleet::testing::TestStore`
  decorates any `ObjectStore` with per-op counters, deterministic fault
  injection (fail-next, fail-all, seeded probability), and a simulated
  `LastModified` driven by `TestClock`.
- [x] **Clock injection** — `root::Clock` seam; `FleetRoot::with_clock`
  controls the reader's side of liveness for skew tests and DST.

## Unit

Pure logic, `cargo test --lib`.

- [x] Placement: frozen score goldens, deterministic ranking, minimal
  disruption on node removal, distinct top-`count` owners
  (`src/placement.rs`).
- [x] Registry: canonicalization idempotence and case-folding, alias
  collapse, scheme rejection, file-name round-trip, oversized-URL
  rejection (`src/registry.rs`).
- [x] Heartbeat: name sort/dedup and round-trip, unknown-letter and
  unknown-field tolerance, `validate_node_id` accept/reject table
  (`src/heartbeat.rs`).
- [x] Config: precedence layering, empty/`services = []` files, layered
  validation, unknown fields (`tests/config.rs`).
- [x] Root: last-good on bad config, alias/invalid/disabled registry
  warnings, `node_view` youngest-name dedup (`src/root.rs`).
- [x] Services: resolved-config → SlateDB options mapping, disabled GC
  directories map to `None`, idle-backoff polling cadence under paused
  time (`src/services.rs`).
- [x] `daemon::owned_assignments` — role filtering, top-`count`
  workers, `services = []`, dead nodes, axiomatic self-liveness
  (`src/daemon.rs`).
- [x] `daemon::reconcile` — stops unowned tasks, restarts on
  fingerprint change, leaves matching tasks alone (`src/daemon.rs`).
- [x] `ConfigPoller` ETag behavior via the counting store: unchanged
  bodies never re-fetched, empty files never fetched, invalid-after-good
  keeps last good, LIST failure keeps the whole map (`src/root.rs`).
- [x] Supervisor backoff policy under paused time: a fence waits exactly
  one heartbeat interval and resets backoff; plain errors double to the
  cap; cancellation exits (`src/daemon.rs`, via `supervise_with`).
- [x] `ops::node_statuses` — dead nodes listed, unreadable bodies yield
  absent versions (`src/ops.rs`).
- [x] Render: populated status text pinned exactly (`src/render.rs`).

## Property-based (proptest)

`tests/props.rs`.

- [x] Registry file names round-trip arbitrary URLs (unicode, spaces),
  never contain `/`; canonicalize is idempotent.
- [x] Placement: ranking is a permutation of candidates; owners are its
  prefix; deterministic; removal is minimally disruptive.
- [x] Config resolution is field-wise last-writer-wins.
- [x] Serialized configs, heartbeats, and the pinned CLI JSON snapshots
  validate against the generated JSON Schemas (`tests/schemas.rs`) —
  this caught `StatusResponse.warnings` being required while
  `skip_serializing_if` omits it.
- [x] URLs whose registry name would exceed the object-store key cap
  are rejected with a clear error (`registry::UrlError::TooLong`).

## Integration (in-process, multi-node)

`tests/cluster.rs` (shared harness in `tests/common/`), `tests/fleet.rs`.

- [x] Register/status round-trip: canonicalization, create-only PUT,
  unoffered-service warnings.
- [x] Single node compacts a real SlateDB database end to end and hands
  off on clean shutdown.
- [x] **Failover on unclean death** — the survivor owns the dead node's
  pairs within `heartbeat_timeout`; the stale heartbeat is housekept
  after 10x.
- [x] **Placement partitioning** — every pair runs on exactly its
  ranked node; per-node task counts match the pure ranking.
- [x] **GC deletes** — after sleet compacts, superseded SSTs are
  removed once SlateDB's 15-minute commit checkpoint is released (the
  test releases it; production GC simply lags by it).
- [x] Config propagation — registry edits add/stop tasks within
  `config_poll`; `services = []` disables; deletion unregisters. Caught
  a real bug: config-change restarts corrupted heartbeat task states
  (fixed with supervisor-instance-tagged state writes).
- [x] Role split — worker-only nodes take workers; `count = 2` spans
  two worker nodes; control services land on the offering node.
- [x] Role change — heartbeat renamed, old name deleted, placement
  converges; clean shutdown hands off immediately.
- [x] Coordinator duel — two coordinators self-resolve: the older is
  fenced, the epoch advances monotonically, the survivor keeps running.
- [x] Worker semaphore — with no permit a worker never claims scheduled
  jobs; after the queue drains the worker stops and returns its permit.

## System (real binary, real stores)

`tests/system.rs`, `tests/s3.rs`, `tests/cmd/`.

- [x] CLI snapshots (trycmd): help pages, register text/JSON, empty
  status text/JSON, invalid node id, invalid database URL, unsupported
  root scheme.
- [x] MinIO in Docker — real S3 semantics: conditional-create register,
  real-ETag poller caching, >1000-entry LIST pagination. Skips with a
  note when Docker is absent.
- [x] Multi-process crash — a worker SIGKILLed mid-compaction leaves a
  claimed job; the coordinator reclaims it after
  `worker_heartbeat_timeout` and a replacement completes it.
- [x] SIGINT exits zero and deletes the heartbeat.
- [x] Linux — `scripts/test-linux.sh` runs the suite in a `rust:1.89`
  container with cached cargo volumes (build jobs capped so the linker
  fits Docker's memory).

## Chaos

`tests/chaos.rs`. All runs assert the safety and liveness invariants,
never specific interleavings.

- [x] Fault-injected fleet — 20% seeded fault rate on every store op;
  no node dies; after healing, ownership converges to the exact
  ranking.
- [x] Partition asymmetry — a node that loses the fleet root only is
  taken over within `heartbeat_timeout` and rejoins after healing.
- [x] Clock skew — a reader skewed past `heartbeat_timeout` takes over
  the whole fleet as a stable double-run. Caught a real bug: a node
  whose own heartbeat read as stale excluded itself and silently
  orphaned its share; self-liveness is now axiomatic (in DESIGN.md).

## Deterministic simulation (DST)

`tests/dst.rs`: paused-time runtime, sim store whose `LastModified`
follows a `TestClock` advanced in lockstep with tokio's virtual clock.

- [x] Sim harness — seeded schedules of crashes, restarts, and registry
  churn; after quiescence every pair is owned by exactly its ranked
  node; outcomes reproduce per seed; three seeds run in ~100ms wall
  time.
- [x] Paused-time cadence tests — heartbeat PUTs and config GETs
  counted against exact virtual time; failover latency bounded at
  `heartbeat_timeout` plus ticks.

## Model checking (FizzBee)

`specs/coordination.fizz`.

- [x] The coordination protocol — heartbeats, per-reader liveness views
  with axiomatic self-liveness, frozen-ranking ownership, the
  fenced-coordinator refresh rule — model-checked exhaustively (21k+
  unique states): fencing order and only-alive-run safety hold
  everywhere, the transient double-run is reachable, and Converged
  (eventually always) proves convergence with no fence livelock under
  budgeted crash/restart/false-suspicion churn. Run with
  `fizz specs/coordination.fizz`.
- [x] Model-based bridge — `tests/dst.rs` replays fizz simulation
  traces (crash/restart schedules) against real daemons and asserts the
  spec's Converged property; skips when `fizz` is absent.

## Performance

- [x] Criterion benches (`benches/coordination.rs`): per-tick placement
  recompute at 1k/10k/100k databases x 20 nodes, and a full
  `config_poll` registry read at 1k/10k/50k entries. Run with
  `cargo bench`.
- [x] Footprint (`tests/perf.rs`, ignored): one node supervising 20k
  pairs — time to converge, RSS, clean-shutdown time. Run with
  `cargo test --test perf -- --ignored --nocapture`.

## Compatibility

- [x] Frozen-format goldens: placement scores, registry name encoding,
  heartbeat name shape.
- [x] Schema drift tests for config, heartbeat, and CLI responses.
- [x] Heartbeat readers ignore unknown fields and unknown service
  letters.
- [x] Version corpus (`tests/corpus.rs`): serialized heartbeats,
  configs, registry names, and placement scores per release, verified
  by current code. Cut a new directory at each release with
  `UPDATE_CORPUS=1 cargo test --test corpus`.
