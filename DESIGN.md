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
- No dependencies beyond object storage. Job-level mutual exclusion comes
  from SlateDB's primitives — manifest CAS, epoch fencing, and `.compactions`
  claims (RFC-0001, RFC-0013, RFC-0025); `sleet`'s own coordination state is
  plain objects written with conditional PUTs.
- Safety never depends on `sleet`'s own scheduling. Duplicate or stale
  `sleet` processes must be harmless.

## Non-goals

- Not a proxy or data-plane component; clients talk to their SlateDB writers
  and readers directly.
- No leader election and no membership agreement. Nodes never need a shared
  view of the fleet; every decision is a local read plus a conditional PUT.
- No mirroring in v1; see [Future work](#future-work).

## Architecture

A fleet is a **fleet root**: a `.sleet/` tree under a single object-store
URL. All fleet state — policy, the database registry, node liveness, service
ownership, and work signals — lives in that tree. Nodes are stateless,
interchangeable processes pointed at the root:

```
sleet run s3://ops/sleet/
```

```
.sleet/
  fleet.toml                         # policy: defaults, discovery, timing
  dbs/<db>.toml                      # intent: one file per managed database
  nodes/<node>.json                  # liveness: heartbeat + offered services
  assignments/<db>/<service>-<slot>  # ownership: who runs what
  queue/<db>                         # activity: pending compaction markers
```

Each node heartbeats under `nodes/`, scans discovery roots into `dbs/`,
acquires assignment records for services it offers, and runs one supervised
task per held assignment.

### Fleet config

`fleet.toml` holds fleet-wide policy:

```toml
[fleet]
heartbeat_interval = "10s"
node_timeout = "30s"
config_poll = "1m"                   # fleet.toml / dbs/ re-read cadence

[defaults]
services = ["gc", "compactor", "workers"]

[defaults.workers]
count = 1                            # assignment slots, fleet-wide

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
the number of worker assignment slots. The config types are defined by the
serde structs in `src/spec.rs`; the JSON Schema generated from them is
checked in at `schema/config.schema.json` (drift-checked by a test).
Loading enforces what the schema cannot: `heartbeat_interval <
node_timeout`, valid object-store URLs and exclude globs, unique discovery
roots, and bounds on the resolved settings.

### Databases

`dbs/<db>.toml` registers a database. `<db>` is the percent-encoded
database URL, so the filename alone identifies the database and an empty
file is valid. Files are created by discovery or by operators; contents are
overrides only:

- absent file — unmanaged (undiscovered).
- empty file — managed with defaults.
- `services` list or `gc`/`compactor`/`workers` tables — per-database
  overrides.
- `services = []` — explicitly unmanaged. The file tombstones the entry so
  discovery cannot re-create it.

A file is deleted only when its database no longer exists; deleting the
file for a live database under a discovery root only lasts until the next
scan re-creates it.

Effective config is resolved per-field at read time by the node holding the
assignment: built-in defaults → `[defaults]` → longest matching
`[[discover]]` root → `dbs/<db>.toml`. Unset fields fall through to the
previous layer.

### Discovery

Each node walks every discovery root every `rescan` using delimited LISTs.
A prefix is a database iff `<prefix>/manifest/` contains a `.manifest`
object; database roots aren't recursed into, other prefixes are, up to
`max_depth`. For each database found, the scanner PUTs an empty
`dbs/<db>.toml` with `If-None-Match: *` — create-only, so concurrent
scanners are idempotent and operator edits and tombstones are never
overwritten.

### Nodes and liveness

Each node PUTs `nodes/<node_id>.json` every `heartbeat_interval`, carrying
its offered services and summary service states. The body is defined by the
structs in `src/heartbeat.rs` (`schema/heartbeat.schema.json`); readers
ignore unknown fields so mixed-version fleets coexist, and `version` bumps
only on incompatible change.

A node is **live** iff its heartbeat's `LastModified` (object-store clock)
is younger than `node_timeout` by the reader's clock. Clock skew shifts
takeover timing, never safety. On clean shutdown a node deletes its
heartbeat, voiding all of its assignments at once. Any node deletes
heartbeats older than 10× `node_timeout`.

### Assignments

Each `(database, service, slot)` a database's resolved config calls for is
one object at `assignments/<db>/<service>-<slot>` whose body names the
holder (`src/assignment.rs`, `schema/assignment.schema.json`). `gc` and
`compactor` have a single slot; `workers` has `workers.count` slots, so
`count` bounds how many nodes poll that database's compaction queue. An
assignment is **valid** iff its holder is live.

Nodes acquire greedily and locally:

- A pair is open if its record is absent (acquire by PUT with
  `If-None-Match: *`) or names a dead holder (read, then replace by PUT
  with `If-Match: <etag>`). Exactly one contender wins; losers move on. A
  winner starts services only after its conditional PUT succeeds.
- A node acquires only services it offers, for databases it can reach,
  while under its capacity caps.
- A holder DELETEs its record when the database becomes unmanaged or the
  service fails repeatedly; after releasing on failure it backs off
  re-acquiring that pair, leaving it open for other nodes.
- A node that observes a gap in its own heartbeating longer than
  `node_timeout` re-reads its records before continuing; they may have
  been taken over.
- Anyone may delete records naming a node dead for 10× `node_timeout`.

Assignment is purely an efficiency mechanism. Every failure mode — races,
stale reads, clock skew, partitions — at worst briefly double-runs a
service, which SlateDB's fencing and CAS claims make safe; mutual exclusion
never comes from assignment records. A `(database, service)` with no valid
assignment is directly observable from the tree rather than silently
unowned.

### Process model

`sleet run <root>` is a tokio process. Flags cover only what is
node-specific: `--node-id` (default: hostname), `--services` (default: all
services), and capacity caps defaulted from the machine (maximum held
assignments, maximum concurrent compaction jobs). Heterogeneous fleets run
the same binary with different flags — e.g. large machines with
`--services workers`.

Nodes re-read `fleet.toml` and LIST `dbs/` every `config_poll`, skipping
unchanged objects by ETag; on a failed read a node keeps the last good
config. Heartbeats, liveness checks, and assignment acquisition run every
`heartbeat_interval`. Each held assignment is a supervised task built on
the `slatedb::Admin` API, restarted with backoff on failure. One-shot
subcommands read the fleet root and object storage; nodes serve no API.

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
to the manifest. While `.compactions` holds `Scheduled` entries it touches
`queue/<db>` (see [Compaction queue index](#compaction-queue-index)).

Safety: `compactor_epoch` fencing means a newly started coordinator fences
any prior one; duplicate coordinators self-resolve with at most a brief
stall.

### 3. Compaction workers

Runs SlateDB `CompactionWorker`s (RFC-0025 / `slatedb run-worker`) against
each database whose worker slot the node holds. Workers are stateless: they
poll `.compactions` for `Scheduled` jobs, claim them by CAS, execute (with
subcompaction parallelism per RFC-0028), heartbeat, and write back
`Compacted`.

Slots bound who polls; job claims arbitrate execution, so slot churn or
duplicate holders at worst lose a claim race. Per-database parallelism
spans nodes: a database with `count = 8` may have its slots held by up to
eight nodes competing for its jobs.

### Compaction queue index

Polling every managed database's `.compactions` is wasteful when most are
idle. Coordinators touch `queue/<db>` while their `.compactions` holds
`Scheduled` entries; worker slot holders LIST `queue/` each poll interval,
intersect it with their slots, poll only those databases, and delete
markers they observe drained. The index is advisory: a missing marker
delays compaction by one coordinator poll, a stale marker costs one GET,
and `.compactions` claims remain the sole source of truth.

## Observability

- Nodes run no HTTP server and export no metrics API. `sleet status`
  derives fleet state from the tree: node liveness from heartbeat ages,
  intent from `fleet.toml` and `dbs/`, ownership from `assignments/`, and
  compaction activity from `queue/` and `.compactions`. Open assignments
  and services no live node offers are reported, not silent.
- Structured logs per `(database, service)`.

## Crate layout

A single `sleet` crate with one binary: `sleet run <root>` is the
long-running daemon; `status` is a one-shot. Config types (`fleet.toml`,
`dbs/*.toml`) live in `src/spec.rs` (`schema/config.schema.json`). The
heartbeat body lives in `src/heartbeat.rs` (`schema/heartbeat.schema.json`)
and the assignment record in `src/assignment.rs`
(`schema/assignment.schema.json`). One-shot subcommands take `--format
json`; response types in `src/response.rs` generate `schema/cli.schema.json`
(one `$defs` entry per command), and text rendering lives in
`src/render.rs`.

Depends on `slatedb` (Admin, GarbageCollector, Compactor, CompactionWorker),
`slatedb-txn-obj` (conditional-PUT primitives), and `object_store`.

## Future work

- **Mirroring**: continuously replicate a database into another bucket (same
  or different cloud) via manifest-driven copy — copy each manifest's SST
  diff, then conditional-PUT the manifest as the commit point, with a source
  checkpoint and a `GcFilter` protecting not-yet-copied files from GC.
- **Elastic workers**: size worker-node pools or per-database slot counts
  from fleet-wide `queue/` depth.
- **Rebalancing**: acquisition is first-come, so a new node idles until
  assignments open; voluntary shedding when a node exceeds its share.

## Open questions

1. LIST cardinality on very large fleets: discovery walks, `dbs/`, and
   `assignments/` scans are delimited LISTs; at millions of databases all
   three may want an inventory-based backend (e.g. S3 Inventory).
2. Each active database gets its own polling worker task on a slot holder;
   a multiplexed `.compactions` poller upstream in SlateDB would let one
   task serve many active databases.
