# Sleet

Sleet runs background services for fleets of
[SlateDB](https://slatedb.io) databases.

Point a pool of Sleet nodes at one object-store root. Register the SlateDB
database roots you want managed. The nodes heartbeat, read the registry, and
split the work with rendezvous hashing. The fleet root is the only shared
coordination state.

Sleet handles:

- SlateDB garbage collection
- standalone compaction coordinators
- distributed compaction workers
- optional physical mirroring to another object-store root

Clients keep opening SlateDB databases directly. Sleet moves background work
out of writer processes.

## Why Sleet exists

A deployment with many SlateDB databases should not need one dedicated
background process per database. Sleet lets a small node pool run that work
for the fleet.

Sleet stores intent and liveness in object storage:

```text
<fleet-root>/
  sleet.toml
  dbs/<percent-encoded-database-url>.toml
  nodes/<node-id>.<service-letters>.json
```

There is no assignment table. Each node computes the same placement from the
same inputs: the database registry, resolved config, and live heartbeats. If
two nodes run the same service during a stale view, SlateDB's manifest CAS,
epochs, and `.compactions` claims decide what takes effect.

## Status

Sleet is a pre-1.0 Rust crate. The current design and compatibility rules are
captured in:

- [RFC 0001: Sleet fleet coordination](rfcs/0001-design.md)
- [RFC 0002: Mirroring](rfcs/0002-mirroring.md)

The older design notes remain in [DESIGN.md](DESIGN.md) and
[DESIGN-MIRROR.md](DESIGN-MIRROR.md).

## Quick start

Build the binary:

```sh
cargo build --release
```

Use `s3://ops/sleet` as the fleet root in these examples.

Register a SlateDB database:

```sh
target/release/sleet register s3://ops/sleet s3://app-data/db1
```

Start a node:

```sh
target/release/sleet run s3://ops/sleet --node-id sleet-1
```

Check fleet state:

```sh
target/release/sleet status s3://ops/sleet
```

The node offers all services by default:

```text
gc,compactor-coordinator,compaction-workers,mirror
```

Run specialized pools with `--services`:

```sh
target/release/sleet run s3://ops/sleet \
  --node-id worker-1 \
  --services compaction-workers \
  --max-compaction-jobs 16
```

## Registering databases

The registry lives under `dbs/` in the fleet root. The file name is the
canonical database URL, percent-encoded, with `.toml` appended. An empty file
is valid and uses fleet defaults.

The CLI writes registry files with create-only semantics:

```sh
sleet register s3://ops/sleet s3://app-data/db1
```

A per-database file can override fleet defaults:

```toml
services = ["gc", "compaction-workers"]

[compaction-workers]
count = 4
```

Set `services = []` to keep a database registered while running no services.
Delete the registry file to unregister it.

## Fleet config

`sleet.toml` is optional. Add it when you want explicit timing, service
selection, or database defaults:

```toml
[node]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
config_poll = "1m"

[database]
services = ["gc", "compactor-coordinator", "compaction-workers"]

[database.compaction-workers]
count = 2
```

Config resolves per database in this order:

1. built-in defaults
2. `[database]` in `sleet.toml`
3. the database file under `dbs/`

Fields fall through independently. The generated schema is checked in at
[schema/config.schema.json](schema/config.schema.json).

## Mirroring

Sleet can copy a database to another object-store root and commit manifests
there. A mirror target stays readable at committed manifests, and Sleet is the
only process that writes target manifests while mirroring is enabled.

A fleet-wide target can map a source prefix into a destination prefix:

```toml
[database]
services = ["gc", "compactor-coordinator", "compaction-workers", "mirror"]

[database.mirror.targets.dr]
url = "s3://dr-bucket/mirrors"
source_prefix = "s3://app-data"
mode = "continuous"
copier = "builtin"
poll = "10s"
```

Per-database files can opt out or add targets:

```toml
[mirror.targets.dr]
disabled = true

[mirror.targets.backup]
url = "gs://backups/db1"
mode = "periodic"
interval = "24h"

[mirror.targets.backup.retention]
keep = "30d"
```

Mirror modes:

| Mode | Use |
| --- | --- |
| `continuous` | disaster recovery, migration, read replica targets |
| `periodic` | restore-point backups |
| one-shot sync | operator-triggered copy of one target |

Sleet supports builtin copies, rclone, and external bucket replication for
data objects. Sleet always commits manifests itself.

## CLI

```text
sleet <COMMAND>

Commands:
  run       Run a fleet node
  status    Show fleet state and placement
  register  Register a database
  mirror    Run mirror operations
```

Useful status reads:

```sh
sleet status s3://ops/sleet --compactions
sleet status s3://ops/sleet --mirrors
sleet status s3://ops/sleet --format json
```

Mirror one-shots:

```sh
sleet mirror sync s3://ops/sleet s3://app-data/db1 backup
sleet mirror restore s3://ops/sleet gs://backups/db1 s3://restore/db1
```

JSON responses are described by
[schema/cli.schema.json](schema/cli.schema.json).

## Operating model

Sleet nodes are disposable. Start more nodes to add capacity. Stop nodes
cleanly to delete their heartbeats. If a node dies, peers stop considering it
live after `heartbeat_timeout`.

Node-local flags:

| Flag | Purpose |
| --- | --- |
| `--node-id` | unique identity inside the fleet |
| `--services` | offered service list |
| `--max-compaction-jobs` | databases compacting on one node |
| `--max-mirror-jobs` | mirror jobs on one node |
| `--rclone` | rclone binary used by rclone mirror targets |

Nodes that offer a service must reach every database and destination that
service may touch. Placement does not test credentials or network reachability.

`sleet status` derives state from object storage. Nodes do not serve an API.

## Safety model

Sleet schedules work. SlateDB protects the data.

Duplicate service execution can happen while nodes disagree about heartbeats
or config. The outcomes remain bounded:

- GC deletes are idempotent and checkpoint-aware.
- Coordinators fence each other with `compactor_epoch`.
- Workers claim `.compactions` jobs with CAS.
- Mirrors commit manifests with create-if-absent.

A missing owner delays work until the next poll. It does not change database
state.

## Documentation

Operator docs live under [docs/](docs/):

- [Getting started](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Operations](docs/operations.md)
- [Mirroring](docs/mirroring.md)
- [CLI reference](docs/cli.md)

Design records:

- [RFC 0001: Sleet fleet coordination](rfcs/0001-design.md)
- [RFC 0002: Mirroring](rfcs/0002-mirroring.md)

Examples and schemas:

- [examples/sleet.toml](examples/sleet.toml)
- [examples/db.toml](examples/db.toml)
- [schema/config.schema.json](schema/config.schema.json)
- [schema/heartbeat.schema.json](schema/heartbeat.schema.json)
- [schema/cli.schema.json](schema/cli.schema.json)

## Development

Run the test suite:

```sh
cargo test
```

Run formatting and lints before committing:

```sh
cargo fmt
cargo clippy --all-targets
```

Update generated schemas after changing config, heartbeat, or response types:

```sh
UPDATE_SCHEMAS=1 cargo test --test schema_sync
```

Update CLI snapshots after intentional command output changes:

```sh
TRYCMD=overwrite cargo test --test cli
```

The MinIO-backed S3 test uses `SLEET_S3_ENDPOINT` and skips when the variable
is unset. Model-based tests use `SLEET_MBT`.

## License

MIT
