# sleet — a SlateDB fleet manager

`sleet` operates fleets of [SlateDB](https://slatedb.io) databases. It runs the
background services a SlateDB database needs but that don't belong in the
writer process — garbage collection, compaction coordination, and compaction
execution.

## Goals

- Run GC, compactor coordinators, and compaction workers for many databases
  from a small pool of `sleet` nodes.
- Discover databases automatically under a bucket/prefix — no per-database
  registration.
- No dependencies beyond object storage. All coordination uses SlateDB's
  existing primitives: manifest CAS, epoch fencing, and the `.compactions`
  file (RFC-0001, RFC-0013, RFC-0025).
- Safety never depends on `sleet`'s own scheduling. Duplicate or stale
  `sleet` processes must be harmless.

## Non-goals

- Not a proxy or data-plane component; clients talk to their SlateDB writers
  and readers directly.
- No leader election service. SlateDB's object-store fencing is the only
  mutual-exclusion mechanism.
- No mirroring in v1; see [Future work](#future-work).

## Architecture

A `sleet` deployment is one or more identical nodes running `sleet run`.
Each node loads a **fleet spec** — the set of managed databases and which
services each gets — and runs per-database service tasks.

```
              fleet spec (local file per node)
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   ┌─────────┐         ┌─────────┐         ┌─────────┐
   │  sleet  │         │  sleet  │         │  sleet  │
   └─────────┘         └─────────┘         └─────────┘
    per assigned database, some subset of:
      gc · compactor-coordinator · compaction-worker
                        │
                        ▼
                 database buckets
```

### Fleet spec

A single TOML file on each node's local disk (`sleet run --spec
/etc/sleet/fleet.toml`), reloaded on mtime change. Databases are found by
**discovery roots** or explicit entries; explicit entries take precedence
over discovery for the same path:

```toml
[fleet]
node_id = "sleet-1"                          # default: hostname
heartbeats = "s3://ops/sleet/nodes/"         # omit if single-node
heartbeat_interval = "10s"
node_timeout = "30s"

[defaults]
services = ["gc", "compactor", "workers"]

[defaults.workers]
count = 2

[[discover]]
url = "s3://prod-us/"                        # credentials via env/profile
rescan = "5m"
max_depth = 3
exclude = ["**/tmp/**"]

[[database]]                                 # explicit entry; wins over
url = "gs://analytics/events"                # discovery for the same path
services = ["gc"]
```

The spec format is defined by the serde structs in `src/spec.rs`; the
JSON Schema generated from them is checked in at
`schema/config.schema.json` (`sleet schema`, drift-checked by a test).
`[defaults]`, `[[discover]]` entries, and `[[database]]` entries all
accept the same optional `services` list and `gc`/`compactor`/`workers`
tables, whose fields mirror SlateDB's `GarbageCollectorOptions`,
`CompactorOptions`, and `CompactionWorkerOptions` with SlateDB's
defaults; `workers.count` sets the pool size. `sleet validate --spec`
enforces what the schema cannot: `heartbeat_interval < node_timeout`,
valid object-store URLs and exclude globs, unique database entries and
discovery roots, and scheduler bounds on the resolved settings.

### Discovery

Each node walks its discovery roots every `rescan` using delimited `LIST`s. A
prefix is a database iff `<prefix>/manifest/` contains a `.manifest` object;
database roots aren't recursed into, other prefixes are, up to `max_depth`.
Discovered databases get `[defaults]` and are managed immediately. Nodes
discover independently — listing the same root yields the same set, so
assignments converge within one rescan; skew at worst double-runs a service,
which is safe.

Precedence: built-in defaults → `[defaults]` → longest matching
discovery root → `[[database]]` entry. Unset fields fall through to the
previous layer.

### Assignment and failover

Each node PUTs a heartbeat object at `<heartbeats>/<node_id>` every
`heartbeat_interval`; the body is JSON carrying the node's current
assignments and service states, so fleet state is observable from object
storage alone. The body is defined by the structs in `src/heartbeat.rs`
(`schema/heartbeat.schema.json`); readers ignore unknown fields so
mixed-version fleets coexist, and `version` bumps only on incompatible
change. Liveness never depends on body contents: the live set is nodes
whose heartbeat `LastModified` (object-store clock) is younger than
`node_timeout`. `(database, service)`
pairs are rendezvous-hashed over the live set, recomputed each tick — a dead
node's databases redistribute within ~`node_timeout`, and rebalance when it
returns. Any node deletes heartbeat objects older than 10× `node_timeout`;
deleting a live node's heartbeat is harmless since it re-PUTs on the next
interval.

Assignment is purely an efficiency optimization: false suspicion or partition
at worst double-runs a service, which SlateDB's fencing and CAS claims make
safe. If `heartbeats` is unset, a node assumes it is the
only member. On a failed spec reload, a node keeps its last good spec.

### Process model

`sleet run` is a tokio process. Each `(database, service)` is a supervised
task built on the `slatedb::Admin` API, restarted with backoff on failure.
One-shot subcommands read/edit the fleet spec and object storage; nodes
serve no API.

## Services

### 1. Garbage collection

Wraps `GarbageCollector` (`slatedb/src/garbage_collector.rs`) in long-running
mode, equivalent to `slatedb schedule-gc` but multiplexed across databases.
Per-resource `interval`/`min_age`/`dry_run` come from the fleet spec, with the
SlateDB defaults (`min_age=300s`, `interval=60s`); WAL fence GC dry-runs by
default.

Safety: GC already honors checkpoints, the compaction low-watermark, and
`min_age`; boundary files (RFC-0026) close the stalled-writer race. Two
concurrent GCs perform redundant but idempotent deletes.

### 2. Compactor coordinators

Runs the SlateDB `Compactor` per database with `worker: None` — the standalone
coordinator mode from RFC-0025 (`slatedb run-compactor --no-embedded-worker`).
The coordinator polls the manifest, schedules compactions via the configured
`CompactionScheduler`, writes `Scheduled` entries into `.compactions`, reclaims
jobs whose worker heartbeat exceeds `worker_heartbeat_timeout`, and is the sole
committer of compaction results to the manifest.

Safety: `compactor_epoch` fencing means a newly started coordinator fences any
prior one; duplicate coordinators self-resolve with at most a brief stall.

### 3. Compaction workers

Runs a pool of SlateDB `CompactionWorker`s (RFC-0025 / `slatedb run-worker`).
Workers are stateless: they poll `.compactions` for `Scheduled` jobs, claim
them by CAS, execute (with subcompaction parallelism per RFC-0028), heartbeat,
and write back `Compacted`.

`sleet` treats worker slots as a fleet-wide resource: the spec gives each
database a worker count, and nodes size their pools from their assignments.
Because claims are CAS-based, over-provisioning is safe — losers of a claim
race just move on. Future work: elastic sizing driven by `.compactions` queue
depth across the fleet.

## Observability

- Nodes run no HTTP server and export no metrics API. `sleet status`
  derives fleet state from object storage: node liveness from heartbeat
  ages, assignments and service states from heartbeat bodies, and
  compaction queue depth from `.compactions`.
- Structured logs per `(database, service)`.

## Crate layout

A single `sleet` crate with one binary: `sleet run --spec <path>` is the
long-running daemon; `status`, `db list|add|remove`, `validate`, and
`schema` are one-shots. The fleet spec types live in `src/spec.rs`.
One-shot subcommands take `--format json`; response types in
`src/response.rs` generate `schema/cli.schema.json` (one `$defs`
entry per command), and text rendering lives in `src/render.rs`. The
heartbeat wire format lives in `src/heartbeat.rs`
(`schema/heartbeat.schema.json`).

Depends on `slatedb` (Admin, GarbageCollector, Compactor, CompactionWorker),
`slatedb-txn-obj` (CAS primitives), and `object_store`.

## Future work

- **Mirroring**: continuously replicate a database into another bucket (same
  or different cloud) via manifest-driven copy — copy each manifest's SST
  diff, then conditional-PUT the manifest as the commit point, with a source
  checkpoint and a `GcFilter` protecting not-yet-copied files from GC.

## Open questions

1. Worker pools are currently per-database pollers; a fleet-scale shared pool
   would benefit from a multiplexed `.compactions` poller upstream in SlateDB.
2. Discovery cost on very large or deep buckets: delimited LISTs per rescan
   are cheap, but a root with tens of thousands of prefixes may want an
   inventory-based (e.g. S3 Inventory) discovery backend.
