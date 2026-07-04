# RFC 0001: Sleet fleet coordination

Status: accepted

## Summary

Sleet runs SlateDB background services for many databases from a shared pool
of stateless processes. Each process points at one object-store root, writes a
heartbeat there, reads the database registry, and computes the work it owns.

The fleet root is the only coordination dependency. Sleet does not run a
leader election protocol and does not store assignments. When two processes
briefly run the same service for the same database, SlateDB's own fencing and
compare-and-swap operations decide what takes effect.

## Motivation

SlateDB databases need background work: garbage collection, compaction
coordination, compaction execution, and optional mirroring. Running all of
that work in every writer process wastes capacity and makes large fleets hard
to operate.

Sleet moves that work into a small fleet of nodes. Operators register the
database roots they want managed. Nodes then split the work by hashing the
registered database URLs against the live node set.

The design assumes object storage is already the shared service every SlateDB
deployment has. Adding ZooKeeper, etcd, or a custom coordinator would make
safety depend on another system. Sleet avoids that.

## Non-goals

- Sleet is not a proxy. Clients keep opening SlateDB database roots directly.
- Sleet does not coordinate writers or readers.
- Sleet does not discover databases automatically in this RFC.
- Sleet does not add a lock service. SlateDB's manifest CAS, epochs, and
  compaction job claims remain the mutual exclusion mechanisms.

## Fleet root

Every Sleet node receives a fleet root URL:

```sh
sleet run s3://ops/sleet --node-id sleet-1
```

The root contains policy, the database registry, and node heartbeats:

```text
<root>/
  sleet.toml
  dbs/<percent-encoded-database-url>.toml
  nodes/<node-id>.<service-letters>.json
```

Nodes read the same tree and compute the same assignments when their views of
the tree match. No file under the root records "node X owns database Y".

## Registry

A file under `dbs/` registers a database. The file name is the canonical
database URL, percent-encoded, with `.toml` appended. The file contents use
the same shape as the `[database]` table in `sleet.toml`.

Registry states:

- no file: Sleet does not manage the database
- empty file: Sleet manages it with fleet defaults
- `services = []`: Sleet keeps it registered but runs no services

Operators can create registry files directly or use:

```sh
sleet register <fleet-root> <database-url>
```

`register` canonicalizes the database URL before writing the file. That keeps
two spellings of the same root from becoming two registry entries. `status`
reports registry entries that alias after canonicalization.

Deleting the registry file unregisters the database.

## Configuration

`sleet.toml` carries fleet-wide defaults:

```toml
[node]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
config_poll = "1m"

[database]
services = ["gc", "compactor-coordinator", "compaction-workers"]

[database.compaction-workers]
count = 1
```

Sleet resolves each database config in this order:

1. built-in defaults
2. `[database]` in `sleet.toml`
3. the database file under `dbs/`

Fields fall through independently. A database file can override one GC
setting without copying the rest of the fleet policy.

The `[database]` shape includes:

- `services`
- `gc`
- `compactor-coordinator`
- `compaction-workers`
- `mirror`, defined by RFC 0002

GC, coordinator, and worker settings map to SlateDB options. Sleet keeps the
Rust serde types in `src/config.rs` as the source for the generated schema at
`schema/config.schema.json`.

The loader checks constraints the schema cannot express, including:

- `heartbeat_interval` must be shorter than `heartbeat_timeout`
- intervals must be positive
- service names must not repeat
- resolved options must stay within their allowed bounds

Nodes reread `sleet.toml` and list `dbs/` every `config_poll`. A node that
cannot read the new view keeps the last valid one. Registry listings include
size and ETag, so nodes do not fetch empty registry files and refetch override
files only when their ETag changes.

## Heartbeats

Each node writes one heartbeat object every `heartbeat_interval`:

```text
nodes/sleet-1.cgmw.json
```

The service letters are sorted:

| Letter | Service |
| --- | --- |
| `c` | `compactor-coordinator` |
| `g` | `gc` |
| `m` | `mirror` |
| `w` | `compaction-workers` |

`node_id` must be 1 to 128 characters, using letters, numbers, `_`, and `-`.

Placement uses only the heartbeat object name and `LastModified`. The body is
for observability. It carries the Sleet version, SlateDB version, and service
state summarized for `sleet status`. Readers ignore unknown fields in the
body. The heartbeat `version` changes only for incompatible formats.

A reader considers a node live when the heartbeat object is younger than
`heartbeat_timeout`. The timestamp comes from object storage. Local clock skew
can change when failover happens, but it does not create a safety dependency.

A node always includes itself in its own live set. If its heartbeat appears
stale, the node has no reliable proof that it should stop. Counting itself
can cause duplicate work during a skewed view. Counting itself out can leave
its current work idle.

When a node changes its offered services, it writes a heartbeat under the new
service suffix. Housekeeping deletes any older heartbeat with the same
`node_id` and a different suffix. If two names are visible for one `node_id`,
the youngest object wins.

A clean shutdown deletes the heartbeat. Any node may delete heartbeats older
than `10 * heartbeat_timeout`.

## Placement

Sleet uses rendezvous hashing over the database and service key. Each live
node that offers the service receives a score. The highest scores own the
work.

Ownership rules:

- `gc`: the top-ranked node owns `(database, gc)`
- `compactor-coordinator`: the top-ranked node owns
  `(database, compactor-coordinator)`
- `compaction-workers`: the top `count` nodes own
  `(database, compaction-workers)`
- `mirror`: RFC 0002 extends the key with the target name

If fewer worker nodes are live than `count`, every live worker node polls that
database.

Removing a node moves the assignments it owned. Adding a node moves the
assignments whose score it now wins. Nodes recompute ownership each heartbeat
tick from the registry, resolved config, and live node set.

The hash function, key encoding, and tie break are compatibility surface.
Mixed-version fleets must compute the same owners.

## Process model

`sleet run <root>` is a Tokio process. Command-line flags cover node-local
settings:

- `--node-id`
- `--services`
- `--max-compaction-jobs`
- `--max-mirror-jobs`
- `--rclone`

Capacity caps default from the machine's available parallelism. A large fleet
can run specialized nodes, for example worker-heavy machines with:

```sh
sleet run <root> --node-id worker-1 --services compaction-workers
```

Each owned assignment runs as a supervised task. Sleet starts and cancels
tasks as placement changes. One-shot commands read object storage and exit.
Nodes do not serve an HTTP API.

## Services

GC runs SlateDB garbage collection in long-running mode. Per-resource
`interval`, `min_age`, and `dry_run` values come from resolved config. WAL
fence GC dry-runs by default. Duplicate GC is safe because SlateDB GC honors
checkpoints, compaction low-watermarks, and boundary files.

Compactor coordinators run SlateDB's standalone coordinator mode. A coordinator
polls manifests, schedules jobs in `.compactions`, reclaims jobs whose worker
heartbeat expired, and commits completed compactions to the manifest.

Compaction workers poll `.compactions`, claim scheduled jobs with CAS, execute
the compaction, heartbeat while working, and write back the result. Placement
chooses who polls. Job claims choose who performs a specific job. Idle worker
polling backs off per database.

Mirroring copies immutable database objects to another root and commits
target manifests. RFC 0002 defines its config and protocol.

## Failure behavior

Sleet treats placement as an efficiency decision. Safety belongs to SlateDB.

Temporary duplicate ownership can happen after stale reads, partitions, clock
skew, or a node restart. GC deletes are idempotent. Coordinator manifests are
fenced by `compactor_epoch`. Workers compete through `.compactions` claims.
Mirror commits use create-if-absent and identical manifest bodies, covered in
RFC 0002.

Temporary missing ownership can happen while views converge. Work waits for a
later poll.

Expected convergence:

| Change | Bound |
| --- | --- |
| clean node exit | next heartbeat list |
| dead node | about `heartbeat_timeout` |
| registry or config edit | `config_poll` plus one heartbeat tick |

Nodes must reach every database required by the services they offer. Placement
does not test network reachability or credentials.

## Coordinator fencing

A running coordinator can be fenced by a newer `compactor_epoch`. Sleet
treats that as evidence of view skew. The fenced task waits one
`heartbeat_interval`, then reruns. Rerunning bumps the epoch again if the task
still owns the assignment.

The daemon recomputes placement on its normal tick and cancels tasks whose
assignments moved. The wait gives the rival daemon the same opportunity to
stand down. Mutual fencing can last only while the two nodes disagree about
ownership, which is bounded by `config_poll` plus one heartbeat tick. The cost
is a short compaction stall.

## Observability

`sleet status` derives fleet state from object storage:

- node liveness, roles, and versions from `nodes/`
- database intent from `sleet.toml` and `dbs/`
- placement by running the same rendezvous hash
- compaction queue depth when `--compactions` is set
- mirror lag when `--mirrors` is set

The daemon writes structured logs per service assignment. It does not expose
metrics or an API.

## Scaling

Coordination traffic scales with the number of nodes. Each node writes one
heartbeat and lists `nodes/` once per heartbeat tick. Each node lists `dbs/`
once per `config_poll`.

Fleet state scales with the number of registered databases: one registry file
per database. At large fleet sizes, listing `dbs/` is the first pressure
point. An inventory-backed registry is future work.

Database traffic scales with enabled services and their polling intervals.
GC and coordinators poll each managed database. Workers back off while a
database is idle. The current model starts one task per owned database and
service. A multiplexed upstream poller could reduce task count later.

## Compatibility

The following formats are stable once a release depends on them:

- registry file names
- heartbeat object names
- service letters
- rendezvous hash keys and tie break
- JSON schemas generated from config, heartbeat, and CLI response types

New heartbeat body fields are compatible because readers ignore unknown
fields. Config parsing rejects unknown fields, so removing or renaming a
config field requires a migration.

## Future work

Sleet can size worker pools from observed backlog instead of a fixed
per-database `count`.

Sleet can add database discovery by scanning configured bucket prefixes for
SlateDB roots and creating registry files with create-only writes.
