# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`sleet` is a fleet manager for [SlateDB](https://slatedb.io) databases.
`DESIGN.md` is the design source of truth; read it before making changes
and keep it consistent when the design evolves. The fleet spec format is
defined by the serde structs in `src/spec.rs`, which generate
`schema/fleet.schema.json`; a test fails when the two drift.

## Commands

- `cargo test` — all tests, including the schema drift check.
- `cargo run -- schema > schema/fleet.schema.json` — regenerate the
  schema after changing `src/spec.rs`.
- `cargo run -- validate --spec examples/fleet.toml` — validate a spec.
- `cargo fmt && cargo clippy --all-targets` before committing.

## Architecture (from DESIGN.md)

- A single Rust crate, one `sleet` binary: `sleet run --spec <path>` is the
  long-running daemon; other subcommands are one-shot operator tools.
- Nodes load a local TOML fleet spec, discover databases under object-store
  prefixes (a prefix with `manifest/*.manifest` is a database), heartbeat to
  a configured object-store location, and rendezvous-hash
  `(database, service)` pairs over the live node set.
- Per-database services wrap SlateDB primitives via `slatedb::Admin`:
  garbage collection, standalone compaction coordinators (RFC-0025), and
  compaction worker pools. Mirroring is future work.
- Core invariant: safety never depends on sleet's scheduling. Duplicate or
  stale processes must be harmless; mutual exclusion comes only from
  SlateDB's manifest CAS, epoch fencing, and `.compactions` claims. The only
  dependency is object storage — no etcd/ZK/leader election.

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
