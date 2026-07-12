# Sleet

Sleet runs [SlateDB](https://slatedb.io) background services for a one or more
SlateDB databases.

Supported services:

- `gc`: garbage collection
- `compactor-coordinator`: schedule compactions and commit completed results
- `compaction-workers`: claim and execute jobs from `.compactions`
- `mirror`: copy databases to other object-store roots continuously or periodically

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

The mirror service incrementally copies a SlateDB database to another
object-store root, including roots in another region or cloud. Sleet can copy
objects itself, invoke `rclone`, or work with cloud replication services.

Before committing a manifest, Sleet ensures the destination contains every SST
and checkpoint manifest it references. Each new destination head is loadable
as soon as it appears. Continuous targets tail the WAL between sync passes.
Periodic targets create restore points that can be retained and restored later.

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

```rust
use sleet::{Fleet, StatusOptions};

#[tokio::main]
async fn main() -> Result<(), sleet::Error> {
    let fleet = Fleet::open("s3://ops/sleet")?;
    fleet.register("s3://data/orders").await?;

    let status = fleet
        .status(StatusOptions::default().with_compactions(true))
        .await?;
    println!("{} databases", status.databases.len());

    Ok(())
}
```

Disable the default `cli` feature for library-only builds. See the
[Rust API guide](docs/rust-api.md) for node and mirror examples.

## Documentation

See the following directories for more information:

- [docs](docs): Sleet documentation.
- [examples](examples): Example configuration files.
- [rfcs](rfcs): Sleet design documents.
- [schemas](schema): CLI, configuration, and heartbeat file JSON schemas.

## License

[MIT](LICENSE)
