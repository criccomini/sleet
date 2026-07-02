# sleet — a SlateDB fleet manager

`sleet` operates fleets of [SlateDB](https://slatedb.io) databases. It runs
their background services — garbage collection, compaction coordination, and
compaction execution — outside the writer process, for deployments that
choose to run them separately.

## Goals

- Run GC, compactor coordinators, and compaction workers for millions of
  databases from a small pool of `sleet` nodes. Per-database fleet state is
  one registry file, coordination traffic is independent of database count,
  and idle databases cost only backed-off polling.
- Register databases explicitly — with the CLI or by writing
  `dbs/<db>.toml` — or let optional discovery scan bucket prefixes and
  register what it finds.
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
  sleet.toml        # policy: defaults, discovery roots, timing
  dbs/<db>.toml     # registry: one file per database, overrides only
  nodes/<node>.json # liveness: heartbeat, offered services, versions
```

Each node heartbeats under `nodes/`, scans discovery roots into `dbs/`,
computes its assignments by rendezvous hashing, and runs one supervised
task per assignment it owns.

### Fleet config

`sleet.toml` holds fleet-wide policy:

```toml
[fleet]
heartbeat_interval = "10s"
node_timeout = "30s"
config_poll = "1m"                   # sleet.toml / dbs/ re-read cadence

[defaults]
services = ["gc", "compactor", "workers"]

[defaults.workers]
count = 1                            # worker slots per database

[[discover]]
url = "s3://prod-us/"                # credentials via env/profile
rescan = "5m"
max_depth = 3
exclude = ["**/tmp/**"]
```

`[defaults]`, `[[discover]]` entries, and `dbs/<db>.toml` files all accept
the same optional `services` list and `gc`/`compactor`/`workers` tables,
whose fields mirror SlateDB's `GarbageCollectorOptions`, `CompactorOptions`,
and `CompactionWorkerOptions` with SlateDB's defaults; `workers.count` sets
the number of worker slots. The config types are defined by the serde
structs in `src/spec.rs`; the JSON Schema generated from them is checked in
at `schema/config.schema.json` (drift-checked by a test). Loading enforces
what the schema cannot: `heartbeat_interval < node_timeout`, valid
object-store URLs and exclude globs, unique discovery roots, and bounds on
the resolved settings.

Nodes re-read `sleet.toml` and LIST `dbs/` every `config_poll`, skipping
unchanged objects by ETag; on a failed read a node keeps the last good
config.

### Databases

`dbs/<db>.toml` registers a database. `<db>` is the percent-encoded
database URL, so the filename alone identifies the database and an empty
file is valid. Files are created by operators — directly or with `sleet
register <url>` — or by discovery; contents are overrides only:

- absent file — unmanaged (undiscovered).
- empty file — managed with defaults.
- `services` list or `gc`/`compactor`/`workers` tables — per-database
  overrides.
- `services = []` — explicitly unmanaged. The file tombstones the entry so
  discovery cannot re-create it.

A file is deleted only when its database no longer exists; deleting the
file for a live database under a discovery root only lasts until the next
scan re-creates it.

Effective config is resolved per-field at read time by the assignment
owner: built-in defaults → `[defaults]` → longest matching `[[discover]]`
root → `dbs/<db>.toml`. Unset fields fall through to the previous layer.

### Discovery

Discovery is optional: a fleet with no `[[discover]]` entries manages only
explicitly registered databases.

Each node walks every discovery root every `rescan` using delimited LISTs.
A prefix is a database iff `<prefix>/manifest/` contains a `.manifest`
object; database roots aren't recursed into, other prefixes are, up to
`max_depth`. For each database found, the scanner PUTs an empty
`dbs/<db>.toml` with `If-None-Match: *` — create-only, so concurrent
scanners are idempotent and operator edits and tombstones are never
overwritten.

### Nodes and liveness

Each node PUTs `nodes/<node_id>.json` every `heartbeat_interval`. The body
carries the node's offered services, its `sleet` and `slatedb` versions,
and summary service states; it is defined by the structs in
`src/heartbeat.rs` (`schema/heartbeat.schema.json`). Readers ignore unknown
fields so mixed-version fleets coexist, and `version` bumps only on
incompatible change.

A node is **live** iff its heartbeat's `LastModified` (object-store clock)
is younger than `node_timeout` by the reader's clock; skew shifts failover
timing, never safety. On clean shutdown a node deletes its heartbeat,
handing its assignments off immediately. Any node deletes heartbeats older
than 10× `node_timeout`.

### Assignment and failover

Each `(database, service, slot)` is owned by the live node that maximizes
a rendezvous hash of `(database, service, slot, node_id)`, computed over
the live nodes whose heartbeats offer that service. `gc` and `compactor`
have a single slot; `workers` has `workers.count` slots, so `count` bounds
how many nodes poll a database's compaction queue. Every node recomputes
ownership each heartbeat tick from the same shared inputs — the `dbs/`
registry and the live set — and runs exactly the pairs it owns. No
assignment state is stored. The hash and its key encoding are frozen, like
a wire format, so mixed-version fleets compute identical placements.

All views derive from the shared tree, so they converge within one
`config_poll` (registry) plus one `heartbeat_interval` (liveness). Until
they do, a pair may briefly run on two nodes — safe — or on none, delaying
it by at most one poll. A dead node's pairs redistribute within
~`node_timeout`; a joining node takes only the pairs it now wins.

Assignment is purely an efficiency mechanism: every failure mode — stale
reads, clock skew, partitions — at worst double-runs a service, which
SlateDB's fencing and CAS claims make safe. The hash pushes only
lightweight polling loops; the expensive work, compaction execution, is
pulled through `.compactions` job claims and bounded by node capacity
caps.

Nodes must be able to reach every discovery root for the services they
offer; placement is capability-blind by construction.

### Process model

`sleet run <root>` is a tokio process. Flags cover only what is
node-specific: `--node-id` (default: hostname), `--services` (default: all
services), and capacity caps defaulted from the machine (e.g. maximum
concurrent compaction jobs). Heterogeneous fleets run the same binary with
different flags — e.g. large machines with `--services workers`. Each owned
assignment is a supervised task built on the `slatedb::Admin` API,
restarted with backoff on failure. One-shot subcommands read the fleet
root and object storage; nodes serve no API.

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
each database whose worker slot the node owns. Workers are stateless: they
poll `.compactions` for `Scheduled` jobs, claim them by CAS, execute (with
subcompaction parallelism per RFC-0028), heartbeat, and write back
`Compacted`. Per-database poll intervals back off exponentially while a
database is idle, so mostly-idle fleets cost little.

Slots bound who polls; job claims arbitrate execution, so overlap from
reassignment at worst loses a claim race. Per-database parallelism spans
nodes: a database with `count = 8` has its slots hashed across up to eight
nodes competing for its jobs.

## Observability

- Nodes run no HTTP server and export no metrics API. `sleet status`
  derives fleet state from the tree: node liveness, roles, and versions
  from `nodes/`, intent from `sleet.toml` and `dbs/`, ownership by
  computing the same rendezvous hash, and compaction queue depth from
  `.compactions`. Services no live node offers are reported, not silent.
- Structured logs per `(database, service)`.

## Crate layout

A single `sleet` crate with one binary: `sleet run <root>` is the
long-running daemon; `status` and `register` are one-shots. Config types
(`sleet.toml`, `dbs/*.toml`) live in `src/spec.rs`
(`schema/config.schema.json`). The
heartbeat body lives in `src/heartbeat.rs` (`schema/heartbeat.schema.json`).
One-shot subcommands take `--format json`; response types in
`src/response.rs` generate `schema/cli.schema.json` (one `$defs` entry per
command), and text rendering lives in `src/render.rs`.

Depends on `slatedb` (Admin, GarbageCollector, Compactor, CompactionWorker),
`slatedb-txn-obj` (CAS primitives), and `object_store`.

## Future work

- **Mirroring**: continuously replicate a database into another bucket (same
  or different cloud) via manifest-driven copy — copy each manifest's SST
  diff, then conditional-PUT the manifest as the commit point, with a source
  checkpoint and a `GcFilter` protecting not-yet-copied files from GC.
- **Elastic workers**: size worker pools or per-database slot counts from
  fleet-wide compaction backlog.

## Open questions

1. LIST cardinality on very large fleets: discovery walks and `dbs/` polls
   are delimited LISTs; at millions of databases both may want an
   inventory-based backend (e.g. S3 Inventory).
2. Every owned database gets its own polling tasks (manifest,
   `.compactions`); idle backoff bounds the cost, but a multiplexed poller
   upstream in SlateDB would let one task serve many databases.
