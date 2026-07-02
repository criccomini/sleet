# sleet — a SlateDB fleet manager

`sleet` operates fleets of [SlateDB](https://slatedb.io) databases. It runs
their background services — garbage collection, compaction coordination, and
compaction execution — outside the writer process, for deployments that
choose to run them separately.

## Goals

- Run GC, compactor coordinators, and compaction workers for millions of
  databases from a small pool of `sleet` nodes.
- Register databases explicitly, with the CLI or by writing
  `dbs/<db>.toml`.
- No dependencies beyond object storage. Mutual exclusion comes from
  SlateDB's primitives — manifest CAS, epoch fencing, and `.compactions`
  claims (RFC-0001, RFC-0013, RFC-0025). `sleet` stores no assignment state:
  ownership is a pure function of shared fleet state in object storage.
- Safety never depends on `sleet`'s own scheduling. Duplicate or stale
  `sleet` processes must be harmless.

## Non-goals

- Not a proxy or data-plane component; clients talk to their SlateDB writers
  and readers directly.
- No leader election. SlateDB's object-store fencing is the only
  mutual-exclusion mechanism.
- No mirroring in v1; see [Future work](#future-work).

## Architecture

`sleet`'s entire state — policy, the database registry, and node liveness —
lives under a single object-store URL, the **fleet root**. Nodes are
stateless, interchangeable processes pointed at the root:

```
sleet run s3://ops/sleet/
```

```
<root>/
  sleet.toml               # policy: defaults, timing
  dbs/<db>.toml            # registry: one file per database, overrides only
  nodes/<node>.<services>.json  # liveness + offered services in the name
```

Each node heartbeats under `nodes/`, computes its assignments by
rendezvous hashing, and runs one supervised task per assignment it owns.

### Fleet config

`sleet.toml` holds fleet-wide policy:

```toml
[node]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
config_poll = "1m"                   # sleet.toml / dbs/ re-read cadence

[database]
services = ["gc", "compactor-coordinator", "compaction-workers"]

[database.compaction-workers]
count = 1                            # worker nodes per database
```

The `[database]` table and `dbs/<db>.toml` files share the same shape: an
optional `services` list and `gc`/`compactor-coordinator`/
`compaction-workers` tables, whose fields mirror SlateDB's
`GarbageCollectorOptions`, `CompactorOptions`, and
`CompactionWorkerOptions` with SlateDB's defaults;
`compaction-workers.count` sets how many nodes run workers for a
database. The config types are defined by the serde structs in
`src/config.rs`; the JSON Schema generated from them is checked in at
`schema/config.schema.json` (drift-checked by a test). Loading enforces
what the schema cannot: `heartbeat_interval < heartbeat_timeout` and
bounds on the resolved settings.

Nodes re-read `sleet.toml` and LIST `dbs/` every `config_poll`; on a
failed read a node keeps the last good config. The listing carries each
entry's size and ETag, so empty registry files — the common case — are
never fetched, and override files are fetched only when their ETag
changes.

### Databases

`dbs/<db>.toml` registers a database. `<db>` is the percent-encoded
database URL, so the filename alone identifies the database and an empty
file is valid. Files are created by operators, directly or with `sleet
register <url>`. `register` canonicalizes URLs before encoding so one
database cannot be registered under two spellings; `status` flags entries
that alias after canonicalization. A file's contents are exactly a
`[database]` table: any field `sleet.toml`'s `[database]` section accepts
may be set per database, and set fields override the fleet-wide value:

- absent file — unregistered.
- empty file — managed with the fleet-wide config.
- `services = []` — registered but disabled; no services run.

Deleting the file unregisters the database.

Effective config is resolved per-field at read time by the assignment
owner: built-in defaults → `[database]` → `dbs/<db>.toml`. Unset fields
fall through to the previous layer.

### Nodes and liveness

Each node PUTs a heartbeat at `nodes/<node_id>.<services>.json` every
`heartbeat_interval`, where `<services>` is the offered services' letters
(`c` = compactor-coordinator, `g` = gc, `w` = compaction-workers) sorted
ascending — e.g. `sleet-1.cgw.json`; node ids must not contain `.`. Everything
assignment needs is in the listing — names carry roles, `LastModified`
carries liveness — so one LIST of `nodes/` per tick is the only read;
heartbeat bodies are never fetched for placement.
The body carries the node's `sleet` and `slatedb` versions and summary
service states for `sleet status`; it is defined by the structs in
`src/heartbeat.rs` (`schema/heartbeat.schema.json`). Readers ignore
unknown fields so mixed-version fleets coexist, and `version` bumps only
on incompatible change.

A node is **live** iff its heartbeat's `LastModified` (object-store clock)
is younger than `heartbeat_timeout` by the reader's clock; skew shifts
failover timing, never safety. A node that changes its offered services
restarts under a new heartbeat name and deletes its old one at startup; if
both are briefly visible, the youngest name per `node_id` wins. To the
hash, a role change is a departure from one service's candidate pool and
an arrival in another's, converging like any membership change. On clean
shutdown a node deletes its heartbeat, handing its assignments off
immediately. Any node deletes heartbeats older than 10×
`heartbeat_timeout`.

### Assignment and failover

Ownership is decided by rendezvous hashing. For a given `(database,
service)`, every live node whose heartbeat offers that service gets a
score — the hash of the pair combined with the node's id — and the
ranking assigns owners: `gc` and `compactor-coordinator` run on the
top-ranked node, and `compaction-workers` runs on the top `count` nodes —
`count` distinct pollers per database, or every offering node if there
are fewer. Removing a node moves only the pairs it
owned; adding one moves only the pairs it now wins. Every node recomputes
ownership each heartbeat tick from the same shared inputs — the `dbs/`
registry and the live set — and runs exactly the pairs it owns. No
assignment state is stored. The hash and its key encoding are frozen,
like a wire format, so mixed-version fleets compute identical placements.

All views derive from the shared tree, so they converge within one
`config_poll` (registry) plus one `heartbeat_interval` (liveness). Until
they do, a pair may briefly run on two nodes — safe — or on none, delaying
it by at most one poll. A dead node's pairs redistribute within
~`heartbeat_timeout`; a joining node takes only the pairs it now wins.

Assignment is purely an efficiency mechanism: every failure mode — stale
reads, clock skew, partitions — at worst double-runs a service, which
SlateDB's fencing and CAS claims make safe. The hash pushes only
lightweight polling loops; the expensive work, compaction execution, is
pulled through `.compactions` job claims and bounded by node capacity
caps.

Nodes must be able to reach every registered database for the services
they offer; placement is capability-blind by construction.

#### Fenced coordinators

A running coordinator can be fenced by `compactor_epoch` at any time: it
means another node started a coordinator for the same database, so the
two disagree about ownership. Fencing is treated as a signal of view
skew, not a plain failure. Instead of blindly restarting, the fenced
task refreshes its inputs — re-reads `nodes/` and the database's
registry entry — and recomputes ownership:

- still the owner — restart the coordinator after one
  `heartbeat_interval`. Restarting bumps `compactor_epoch` and fences
  the rival, which follows this same rule, refreshes, computes that it
  lost, and stands down.
- no longer the owner — stop; the pair has moved.

The wait gives the rival time to refresh and stand down before the
re-fence lands. Mutual fencing lasts only as long as views diverge —
bounded by one `config_poll` plus one `heartbeat_interval` — and costs a
brief compaction stall, never correctness.

### Process model

`sleet run <root>` is a tokio process. Flags cover only what is
node-specific: `--node-id` (required; ids must be unique within a fleet),
`--services` (default: all services), and capacity caps defaulted from
the machine (e.g. maximum concurrent compaction jobs). Heterogeneous
fleets run the same binary with different flags — e.g. large machines
with `--services compaction-workers`. Each owned assignment is a
supervised task built on the `slatedb::Admin` API, restarted with backoff
on failure. One-shot subcommands read the fleet root and object storage;
nodes serve no API.

## Services

### 1. Garbage collection

Wraps `GarbageCollector` (`slatedb/src/garbage_collector.rs`) in
long-running mode, equivalent to `slatedb schedule-gc` but multiplexed
across databases. Per-resource `interval`/`min_age`/`dry_run` come from the
resolved config, with the SlateDB defaults (`min_age=300s`, `interval=60s`);
WAL fence GC dry-runs by default.

Safety: GC already honors checkpoints, the compaction low-watermark, and
`min_age`; boundary files (RFC-0026) close the stalled-writer race. Two
concurrent GCs perform redundant but idempotent deletes.

### 2. Compactor coordinators

Runs the SlateDB `Compactor` per database with `worker: None` — the
standalone coordinator mode from RFC-0025 (`slatedb run-compactor
--no-embedded-worker`). The coordinator polls the manifest, schedules
compactions via the configured `CompactionScheduler`, writes `Scheduled`
entries into `.compactions`, reclaims jobs whose worker heartbeat exceeds
`worker_heartbeat_timeout`, and is the sole committer of compaction results
to the manifest.

Safety: `compactor_epoch` fencing means a newly started coordinator fences
any prior one; duplicate coordinators self-resolve with at most a brief
stall.

### 3. Compaction workers

Runs SlateDB `CompactionWorker`s (RFC-0025 / `slatedb run-worker`) against
each database for which the node ranks in the top `count` workers.
Workers are stateless: they poll `.compactions` for `Scheduled` jobs,
claim them by CAS, execute (with
subcompaction parallelism per RFC-0028), heartbeat, and write back
`Compacted`. Per-database poll intervals back off exponentially while a
database is idle, so mostly-idle fleets cost little.

The ranking bounds who polls; job claims arbitrate execution, so overlap
from reassignment at worst loses a claim race. Per-database parallelism
spans nodes: a database with `count = 8` has eight distinct nodes
competing for its jobs, or every worker node if the pool is smaller.

## Scaling

Coordination cost scales with nodes, not databases. Each node PUTs one
heartbeat and LISTs `nodes/` once per tick; assignment is computed in
memory, never written, and needs recomputing only when the registry or a
candidate set changes. Failover latency — one `heartbeat_timeout` — is
independent of database count.

Fleet state scales with databases: one registry object each, written at
registration. The recurring cost is the registry LIST every `config_poll`,
one request per thousand databases; at millions of databases this is the
first thing to replace with an inventory feed (open question 1).

Steady-state traffic against the databases themselves scales with how many
are managed: GC and coordinators poll each one on their configured
intervals, and worker polling backs off while a database is idle. The
intervals are the lever — a million mostly-idle databases are affordable
to the degree their poll floors are long.

## Observability

- Nodes run no HTTP server and export no metrics API. `sleet status`
  derives fleet state from the tree: node liveness, roles, and versions
  from `nodes/`, intent from `sleet.toml` and `dbs/`, placement by
  computing the same rendezvous ranking, and compaction queue depth from
  `.compactions` (behind `--queues`: one read per database). Services no
  live node offers are reported, not silent.
- Structured logs per `(database, service)`.

## Crate layout

A single `sleet` crate with one binary: `sleet run <root>` is the
long-running daemon; `status` and `register` are one-shots. Config types
(`sleet.toml`, `dbs/*.toml`) live in `src/config.rs`
(`schema/config.schema.json`); the heartbeat format lives in
`src/heartbeat.rs` (`schema/heartbeat.schema.json`). The frozen
rendezvous hash lives in `src/placement.rs`, registry naming in
`src/registry.rs`, fleet-root reads in `src/root.rs`, the daemon in
`src/daemon.rs`, the SlateDB service wrappers in `src/services.rs`, and
the one-shots in `src/ops.rs`. One-shot subcommands take `--format
json`; response types in `src/response.rs` generate
`schema/cli.schema.json` (one `$defs` entry per command), and text
rendering lives in `src/render.rs`.

Depends on `slatedb` (Admin, GarbageCollector, Compactor, CompactionWorker),
`slatedb-txn-obj` (CAS primitives), and `object_store`.

## Future work

- **Mirroring**: continuously replicate a database into another bucket (same
  or different cloud) via manifest-driven copy — copy each manifest's SST
  diff, then conditional-PUT the manifest as the commit point, with a source
  checkpoint and a `GcFilter` protecting not-yet-copied files from GC.
- **Elastic workers**: size worker pools or per-database `count` from
  fleet-wide compaction backlog.
- **Auto-discovery**: scan configured bucket prefixes for databases (a
  prefix with `manifest/*.manifest` is a database) and register what is
  found via create-only PUTs, so concurrent scanners are idempotent and
  never overwrite operator edits.

## Open questions

1. LIST cardinality on very large fleets: `dbs/` polls are delimited
   LISTs; at millions of databases the registry may want an
   inventory-based backend (e.g. S3 Inventory).
2. Every owned database gets its own polling tasks (manifest,
   `.compactions`); idle backoff bounds the cost, but a multiplexed poller
   upstream in SlateDB would let one task serve many databases.
