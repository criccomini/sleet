# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`sleet` is a fleet manager for [SlateDB](https://slatedb.io) databases.
`DESIGN.md` is the design source of truth; read it before making changes
and keep it consistent when the design evolves. The fleet config format
(`sleet.toml` and `dbs/<db>.toml`) is defined by the serde structs in
`src/config.rs`, which generate `schema/config.schema.json`; a test fails
when the two drift.

## Commands

- `cargo test` — all tests: unit, property, integration, chaos, DST,
  schema drift, CLI snapshots. The MinIO test skips without Docker; the
  MBT bridge skips without `fizz`.
- `UPDATE_SCHEMAS=1 cargo test --test schema_sync` — regenerate the
  files under `schema/` after changing `src/config.rs`,
  `src/response.rs`, or `src/heartbeat.rs`.
- `TRYCMD=overwrite cargo test --test cli` — update CLI snapshots in
  `tests/cmd/` after changing command-line behavior.
- `UPDATE_CORPUS=1 cargo test --test corpus` — cut a wire-format corpus
  directory at each release.
- `fizz specs/coordination.fizz` — model-check the coordination
  protocol.
- `scripts/test-linux.sh` — run the suite on Linux in Docker.
- `cargo bench` — placement and registry-poll scaling benches.
- `cargo fmt && cargo clippy --all-targets` before committing.

## Architecture (from DESIGN.md)

- A single Rust crate, one `sleet` binary: `sleet run <root>` is the
  long-running daemon; other subcommands are one-shot operator tools.
- A fleet lives under one object-store URL, the fleet root: `sleet.toml`
  (policy: defaults, timing), `dbs/` (registry; one file per database,
  empty = defaults, `services = []` = disabled), `nodes/` (heartbeats;
  liveness and offered services ride the object name, versions and stats
  ride the body). Nodes are stateless; the only node-local config is
  flags.
- Databases are registered manually — `sleet register <url>` or writing
  `dbs/<db>.toml` directly; auto-discovery is future work. Each
  `(database, service)` is placed by a frozen rendezvous ranking of the
  live nodes offering that service — top node for gc and
  compactor-coordinator, top `count` nodes for compaction-workers; no
  assignment state is stored, ownership is recomputed each tick from the
  shared tree.
- Per-database services wrap SlateDB primitives via `slatedb::Admin`:
  garbage collection, standalone compaction coordinators (RFC-0025), and
  compaction workers (top-`count` ranked nodes poll `.compactions` with
  idle backoff).
  Mirroring is future work.
- Core invariant: safety never depends on sleet's scheduling. Duplicate or
  stale processes must be harmless; mutual exclusion comes only from
  SlateDB's manifest CAS, epoch fencing, and `.compactions` claims —
  assignment is efficiency only. The only dependency is object storage —
  no etcd/ZK/leader election.

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
- Wrap prose at ~78 columns.
- Always commit changes using conventional commit syntax (e.g.
  `docs: ...`, `feat: ...`), and commit at every stopping point.
