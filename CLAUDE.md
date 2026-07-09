# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`sleet` is a fleet manager for [SlateDB](https://slatedb.io) databases.
The RFCs under `rfcs/` are the design source of truth
(`rfcs/0001-design.md` for coordination, `rfcs/0002-mirroring.md` for
the mirror service); read them before making changes and keep them
current when the design evolves. The fleet config format
(`sleet.toml` and `dbs/<db>.toml`) is defined by the serde structs in
`src/config.rs`, which generate `schema/config.schema.json`; a test fails
when the two drift.

## Commands

- `cargo test` runs all tests: unit, property, integration, chaos, DST,
  schema drift, CLI snapshots. The MinIO test needs `SLEET_S3_ENDPOINT`
  and skips without it; CI runs MinIO as a service container, and
  `.github/workflows/ci.yml` names the image to run locally. The GCS
  and cross-store tests need `SLEET_GCS_ENDPOINT` (fake-gcs-server;
  the header of `tests/gcs.rs` has the docker command) and skip
  without it. The MBT test needs `SLEET_MBT` and skips without it.
- `UPDATE_SCHEMAS=1 cargo test --test schema_sync` regenerates the
  files under `schema/` after changing `src/config.rs`,
  `src/response.rs`, or `src/heartbeat.rs`.
- `TRYCMD=overwrite cargo test --test cli` updates CLI snapshots in
  `tests/cmd/` after changing command-line behavior.
- `UPDATE_CORPUS=1 cargo test --test corpus` cuts a wire-format corpus
  directory at each release.
- Model checking is local and manual; CI runs only the MBT replay.
  Run the relevant check whenever a spec changes.
  `fizz --experimental_no_state_returns specs/coordination.fizz`
  model-checks the coordination protocol. The flag keeps action
  returns out of state hashes; without it the liveness check does not
  finish. The mirror sync protocol (completeness, prune guards,
  convergence) is `fizz --experimental_processed_queue
  specs/mirror.fizz`, run twice: bare, and with `--preinit-hook-file
  specs/mirror-expiry.cfg` (the full budget product does not fit in
  memory; the spec header explains the split). Racing duplicate
  mirror tasks (create-if-absent commits, two concurrent passes) are
  `specs/mirror-race.fizz`: `fizz --experimental_no_graph
  --exploration_strategy dfs specs/mirror-race.fizz` for safety
  (about 2.5 hours; start it when the machine is free), then one
  witness run per raced behavior, seconds each: `fizz
  --experimental_no_graph --preinit-hook 'WITNESS = "<w>"'
  specs/mirror-race.fizz` for w in race, overlap, restart, dangle.
  Witness runs must FAIL on WitnessReached; that failure is the
  reachability proof (the spec header explains: its state graph does
  not fit in memory, so it declares no liveness, runs without the
  graph via dfs, and cannot use exists assertions, which need one).
  Spot-check the full mirror.fizz product with `fizz -x --max_runs 1
  --seed <n> --preinit-hook-file specs/mirror-sim.cfg
  specs/mirror.fizz`.
- Model-based testing replays the spec's action sequences against the
  real decision code: `fizz specs/coordination-mbt.fizz`, then
  `fizzbee-mbt-server --states_file specs/out/latest &`, then
  `SLEET_MBT=1 cargo test --test mbt`. After editing the spec, rebuild
  the derived MBT spec with `UPDATE_SPECS=1 cargo test --test mbt`.
  The mirror protocol has its own hand-written MBT spec at whole-pass
  granularity (`specs/mirror-mbt.fizz`; its header explains why it is
  not derived from `specs/mirror.fizz`): `fizz specs/mirror-mbt.fizz`,
  restart the server, then `SLEET_MBT_MIRROR=1 cargo test --test mbt
  mirror`. One gate per run: each test needs its own served state
  space.
- `cargo bench` runs the placement and registry-poll scaling benches.
- Run `cargo fmt && cargo clippy --all-targets` before committing.

## Architecture (from the RFCs)

- A single Rust crate, one `sleet` binary: `sleet run <root>` is the
  long-running daemon; other subcommands are one-shot operator tools.
- A fleet lives under one object-store URL, the fleet root: `sleet.toml`
  (policy: defaults, timing), `dbs/` (registry; one file per database,
  empty = defaults, `services = []` = disabled), `nodes/` (heartbeats;
  liveness and offered services come from the object name, versions and
  stats from the body; letters: `c` coordinator, `g` gc, `m` mirror,
  `w` workers). Nodes are stateless; the only node-local config
  is flags.
- Databases are registered manually, with `sleet register <root> <db>`
  or by
  writing `dbs/<db>.toml` directly; auto-discovery is future work. Each
  `(database, service)` is placed by a frozen rendezvous ranking of the
  live nodes offering that service: the top node for gc and
  compactor-coordinator, the top `count` nodes for compaction-workers.
  No assignment state is stored; ownership is recomputed each tick from
  the shared tree.
- Per-database services wrap SlateDB primitives via `slatedb::Admin`:
  garbage collection, standalone compaction coordinators (RFC-0025),
  compaction workers (top-`count` ranked nodes poll `.compactions`),
  and mirroring (`rfcs/0002-mirroring.md`: byte-copy a
  database's manifest closure to per-target destination roots,
  committing manifests with create-if-absent; placement is per
  `(database, mirror, target)` triple; `src/mirror/`).
- Core invariant: safety never depends on sleet's scheduling. Duplicate or
  stale processes must be harmless; mutual exclusion comes only from
  SlateDB's manifest CAS, epoch fencing, and `.compactions` claims;
  assignment is efficiency only. The only dependency is object storage.
  There is no etcd, ZooKeeper, or leader election.

## SlateDB reference

The SlateDB checkout at `~/Code/slatedb` is the reference for APIs and
protocols. Relevant RFCs there: 0001 (manifest), 0004 (checkpoints/clones),
0013 (compaction state), 0025 (distributed compaction), 0026 (GC boundary),
0028 (subcompactions). Key code: `slatedb/src/admin.rs`,
`slatedb/src/compaction_worker.rs`, `slatedb/src/garbage_collector/`,
`slatedb-txn-obj/` (CAS primitives), and `slatedb-cli/`.

## Conventions

- Design docs are terse and mechanism-only: no operational advice, security
  asides, or editorializing; future work goes in the Future work section;
  config options are described neutrally with explicit precedence rules.
- Never use em dashes, in prose or in code comments. Use commas,
  colons, parentheses, or separate sentences instead.
- Wrap prose at ~78 columns.
- Always commit changes using conventional commit syntax (e.g.
  `docs: ...`, `feat: ...`), and commit at every stopping point.
