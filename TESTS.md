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

Two hardcoded seams block the highest-value tests; build these first.

- [ ] **P0: instrumented store wrapper** — an `ObjectStore` decorator
  counting calls per op type and injecting faults (probabilistic errors,
  latency, failed LISTs). Unlocks ETag-cache assertions, idle-backoff
  assertions, and chaos runs.
- [ ] **P1: clock injection** — `root.rs` computes heartbeat ages from
  `Utc::now()`; a `Clock` trait (compatible with slatedb's
  `SystemClock`) unlocks skew tests and DST. Daemon sleeps already work
  under `tokio::time::pause`.

## Unit

Pure logic, `cargo test --lib`.

- [x] Placement: frozen score goldens, deterministic ranking, minimal
  disruption on node removal, distinct top-`count` owners
  (`src/placement.rs`).
- [x] Registry: canonicalization idempotence and case-folding, alias
  collapse, scheme rejection, file-name round-trip (`src/registry.rs`).
- [x] Heartbeat: name sort/dedup and round-trip, unknown-letter and
  unknown-field tolerance (`src/heartbeat.rs`).
- [x] Config: precedence layering, empty/`services = []` files, layered
  validation, unknown fields (`tests/config.rs`).
- [x] Root: last-good on bad config, alias/invalid/disabled registry
  warnings, `node_view` youngest-name dedup (`src/root.rs`).
- [x] Services: resolved-config → SlateDB options mapping, disabled GC
  directories map to `None` (`src/services.rs`).
- [ ] P0: `daemon::owned_assignments` — role filtering, top-`count`
  workers, candidates from heartbeat names, `services = []` yields
  nothing.
- [ ] P0: `daemon::reconcile` — stops unowned tasks, restarts on
  fingerprint change, reaps finished tasks, leaves matching tasks alone.
- [ ] P0: `heartbeat::validate_node_id` — accepts/rejects table,
  including `.`, `/`, empty, over-length.
- [ ] P0: `ConfigPoller` ETag behavior via the counting store: unchanged
  bodies are never re-fetched; empty files are never fetched at all.
- [ ] P1: `ConfigPoller` invalid-after-good keeps the last good per-file
  config; registry LIST failure keeps the whole last-good map.
- [ ] P1: supervisor backoff selection — fence waits exactly one
  heartbeat interval and resets backoff; plain errors back off
  exponentially to the cap.
- [ ] P1: `run_worker_until_drained` stops after two consecutive empty
  checks; the jobs semaphore caps concurrently compacting databases.
- [ ] P1: `ops::node_statuses` — dead nodes listed, unreadable bodies
  yield absent versions.

## Property-based (proptest)

- [ ] P1: registry file names round-trip for arbitrary URLs (unicode,
  spaces, very long paths); canonicalize is idempotent; encoded names
  never contain `/`.
- [ ] P1: placement over arbitrary node sets: ranking is a permutation
  of candidates; owners ⊆ candidates; adding/removing a node changes
  only that node's pairs.
- [ ] P1: config resolution is field-wise last-writer-wins: any field
  set in the registry file wins, any unset field falls through.
- [ ] P1: serialized configs and heartbeats validate against the
  generated JSON Schemas (`jsonschema` crate); today schemas are
  drift-checked but no document is ever validated against them.
- [ ] P2: registry names exceeding the 1024-byte object-key limit are
  rejected with a clear error (currently unhandled).

## Integration (in-process, multi-node)

`tests/fleet.rs`: real daemons over `InMemory`/`file://` stores.

- [x] Register/status round-trip: canonicalization, create-only PUT,
  unoffered-service warnings.
- [x] Single node compacts a real SlateDB database end to end and hands
  off on clean shutdown.
- [ ] P0: **failover on unclean death** — stop a node without cleanup
  (abort its task; no heartbeat delete), assert the survivor owns its
  pairs within `heartbeat_timeout` and the dead heartbeat is
  housekept after 10x.
- [ ] P0: **placement partitioning** — N nodes, M databases: every pair
  owned by exactly the ranked node; ownership sets are disjoint for
  single-slot services and `count`-sized for workers.
- [ ] P0: **GC deletes** — compact with a tiny `min_age`, assert
  superseded SSTs are actually removed from the store; WAL-fence GC
  stays dry-run by default.
- [ ] P1: config propagation — editing `dbs/<db>.toml` restarts the
  task with new options within `config_poll`; `services = []` stops
  tasks; deleting the file unregisters.
- [ ] P1: role split — a `--services compaction-workers` node takes
  workers only; coordinator lands elsewhere; `count = 2` spans two
  worker nodes.
- [ ] P1: role change — restart with different `--services`: old
  heartbeat name deleted, placement shifts, youngest name wins during
  overlap.
- [ ] P1: coordinator duel — two coordinators forced onto one database
  self-resolve with bounded fence exchanges and no livelock.
- [ ] P1: idle backoff — via the counting store, an idle database's
  `.compactions` reads slow toward the ceiling and reset on new work.

## System (real binary, real stores)

- [x] CLI snapshots (trycmd): help pages, register text/JSON, empty
  status text/JSON, invalid node id (`tests/cmd/`).
- [ ] P1: MinIO (or LocalStack) CI job — real S3 semantics for
  conditional PUT (`register`), ETags, LIST pagination; `file://` and
  `memory://` don't exercise these.
- [ ] P1: multi-process crash test — several `sleet run` binaries,
  `kill -9` one mid-compaction, assert job reclaim via
  `worker_heartbeat_timeout` and fleet convergence.
- [ ] P1: SIGINT deletes the heartbeat and exits cleanly (codify the
  manual smoke test).
- [ ] P1: Linux (CI matrix or multipass VM); currently only validated
  on macOS.
- [ ] P2: CLI error snapshots — bad root URL, unsupported scheme,
  invalid database URL, populated-fleet status with `[..]` age
  wildcards.

## Chaos

All chaos runs assert the safety and liveness invariants, not specific
interleavings.

- [ ] P1: fault-injected fleet — multi-node run over the fault store
  (error rates on GET/PUT/LIST/DELETE), then faults stop: no panics,
  ownership converges, epoch monotonic, all scheduled jobs complete.
- [ ] P1: partition asymmetry — one node loses access to the fleet
  root only: it keeps last assignments (double-run, safe) and the
  fleet takes over; verify convergence after heal.
- [ ] P2: clock skew (needs clock injection) — a reader skewed past
  `heartbeat_timeout` declares peers dead and takes over everything;
  assert safety holds and fencing does not livelock.

## Deterministic simulation (DST)

SlateDB is DST-friendly (injectable `SystemClock` and seeds on the
`Admin` builders; see `slatedb-dst`); placement is a pure function;
tokio paused time covers the daemon's sleeps. Missing: the clock seam
and a sim store whose `LastModified` follows the sim clock.

- [ ] P2: sim harness — N virtual nodes in one paused-time runtime over
  one sim store; seeded schedule of crashes, restarts, registry edits,
  faults, and skew; invariant checks each step (every pair owned within
  bound after quiescence, no leaked tasks, epoch monotonic). Failures
  reproduce from the seed.
- [ ] P2: paused-time timing tests as a stepping stone — heartbeat
  cadence, config-poll cadence, failover latency, fence retry delay,
  backoff schedule, all asserted against exact virtual time.

## Model checking (FizzBee)

- [ ] P2: a FizzBee spec of the coordination protocol — heartbeats,
  timeouts, ranking recompute, fencing — model-checked for the
  convergence and no-livelock claims under message delay and crash
  actions. This tests the design itself, independent of the code.
- [ ] P2: model-based tests (fizz-mbt) driving the daemon as the SUT
  from the spec's traces.

## Performance

DESIGN.md claims millions of databases; nothing measures it yet.

- [ ] P2: criterion benches — placement recompute (1M databases x 20
  nodes per tick), `ConfigPoller` over 100k registry entries, reconcile
  diff with 100k assignments.
- [ ] P2: daemon footprint with 100k supervised tasks (tokio task
  count, memory) — establishes the per-node capacity number the design
  hand-waves as "caps defaulted from the machine".

## Compatibility

- [x] Frozen-format goldens: placement scores, registry name encoding,
  heartbeat name shape.
- [x] Schema drift tests for config, heartbeat, and CLI responses.
- [x] Heartbeat readers ignore unknown fields and unknown service
  letters.
- [ ] P2: version corpus — serialized heartbeats, configs, and registry
  names from each release, parsed by current code; grows one directory
  per release. Guards the mixed-version-fleet promise the frozen hash
  makes.
