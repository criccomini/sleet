# Sleet

Sleet runs [SlateDB](https://slatedb.io) background services for a fleet of
databases. A pool of stateless Sleet nodes handles garbage collection,
distributed compaction, and mirroring without adding a coordinator or metadata
database.

Every node reads the same object-store root, discovers the registered
databases and live nodes, and computes its assignments with rendezvous hashing.
Sleet stores no assignment state and runs no leader election. SlateDB's
manifest CAS, epoch fencing, and compaction job claims keep duplicate work
safe while nodes converge on the same view.

```text
                         object store
                    s3://ops/sleet/
                    ├── sleet.toml
                    ├── dbs/*.toml
                    └── nodes/*.json
                           │
             ┌─────────────┼─────────────┐
             │             │             │
        sleet node     sleet node     sleet node
             │             │             │
             └──── SlateDB databases ────┘
```

Sleet manages background work only. Applications continue to open SlateDB
database roots directly.

## Services

| Service | Work |
| --- | --- |
| `gc` | Run SlateDB garbage collection. |
| `compactor-coordinator` | Schedule compactions and commit completed results. |
| `compaction-workers` | Claim and execute jobs from `.compactions`. |
| `mirror` | Copy databases to other object-store roots. |

Nodes offer all four services by default. Each database can enable a subset,
and nodes can form specialized pools with `--services`.

## Quick start

Install the latest prebuilt release:

```sh
curl -fsSL https://raw.githubusercontent.com/criccomini/sleet/main/install.sh | sh
```

The installer verifies the release checksum and writes `sleet` to
`~/.local/bin`. A released version can also be installed from crates.io:

```sh
cargo install sleet --locked
```

Pick an object-store URL for the fleet root, then register a SlateDB database:

```sh
sleet register s3://ops/sleet s3://data/orders
```

Start a node:

```sh
sleet run s3://ops/sleet --node-id sleet-1
```

Start more nodes with unique IDs to add capacity. Nodes discover each other
through heartbeat objects under the fleet root and redistribute work when the
live set changes.

Check the resulting placement:

```sh
sleet status s3://ops/sleet
```

Credentials and provider settings come from the process environment recognized
by the Rust `object_store` crate. A node needs access to the fleet root and to
every database or mirror destination its offered services may own.

## Fleet configuration

The fleet root contains an optional `sleet.toml`, one registry file per
database, and one heartbeat per node:

```text
<root>/
  sleet.toml
  dbs/<percent-encoded-database-url>.toml
  nodes/<node-id>.<service-letters>.json
```

An empty database registry file enables Sleet's defaults. Fleet policy in
`sleet.toml` overrides built-in defaults, and a database's file under `dbs/`
overrides fleet policy field by field.

```toml
[node]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
config_poll = "1m"

[database]
services = ["gc", "compactor-coordinator", "compaction-workers"]

[database.compaction-workers]
count = 2
max_concurrent_compactions = 4
```

Run dedicated worker nodes when services need different resources:

```sh
sleet run s3://ops/sleet \
  --node-id worker-1 \
  --services compaction-workers
```

Deleting a database's registry file unregisters it. Setting `services = []`
keeps it registered while stopping all of its services. See
[Configuration](docs/configuration.md) for the complete format and
[schema/config.schema.json](schema/config.schema.json) for the generated JSON
schema.

## Status

Base status reports live nodes, registered databases, warnings, and service
placement. Optional flags read additional database state:

```sh
sleet status s3://ops/sleet --compactions
sleet status s3://ops/sleet --mirrors
sleet status s3://ops/sleet --format json
```

`--compactions` reads queue depth from each database. `--mirrors` reads source
and destination heads to report lag. JSON responses follow
[schema/cli.schema.json](schema/cli.schema.json).

## Mirroring

The mirror service copies immutable SlateDB objects to another bucket, region,
or cloud, then commits source manifests at the destination. Continuous targets
tail the WAL between sync passes. Periodic targets create restore points that
can be retained and restored later.

```toml
[database]
services = ["gc", "compactor-coordinator", "compaction-workers", "mirror"]

[database.mirror.targets.backup]
url = "gs://backups/orders"
mode = "periodic"
interval = "24h"

[database.mirror.targets.backup.retention]
keep = "30d"
```

Run a configured target once or restore a backup into an empty database root:

```sh
sleet mirror sync s3://ops/sleet s3://data/orders backup
sleet mirror restore gs://backups/orders s3://restores/orders --at 42
```

While a destination is a mirror target, Sleet must be its only manifest writer.
Do not run SlateDB garbage collection there or register it as a source
database. [Mirroring](docs/mirroring.md) covers continuous and periodic modes,
retention, copiers, restore points, and promotion constraints.

## Rust API

The `sleet` crate exposes the same operations for applications that want to
run a node or inspect a fleet in process:

```rust,no_run
use sleet::{Fleet, StatusOptions};

# async fn example() -> Result<(), sleet::Error> {
let fleet = Fleet::open("s3://ops/sleet")?;
fleet.register("s3://data/orders").await?;

let status = fleet
    .status(StatusOptions::default().with_compactions(true))
    .await?;
println!("{} databases", status.databases.len());
# Ok(())
# }
```

Disable the default `cli` feature for library-only builds. See the
[Rust API guide](docs/rust-api.md) for node and mirror examples.

## Documentation

- [Getting started](docs/getting-started.md): installation, credentials, and a
  first node.
- [Architecture](docs/architecture.md): placement, heartbeats, failure
  behavior, and scaling.
- [CLI reference](docs/cli.md): commands, options, and output formats.
- [Operations](docs/operations.md): capacity, logs, registry changes, and cost
  controls.
- [RFC 0001](rfcs/0001-design.md): fleet coordination protocol.
- [RFC 0002](rfcs/0002-mirroring.md): mirror protocol and safety invariants.
- [Examples](examples): complete fleet and per-database configuration.
- [Schemas](schema): configuration, heartbeat, and CLI response schemas.

## Development

Sleet requires Rust 1.89 or newer.

```sh
cargo test
cargo fmt
cargo clippy --all-targets
```

The RFCs under [rfcs](rfcs) are the design source of truth.

## License

[MIT](LICENSE)
