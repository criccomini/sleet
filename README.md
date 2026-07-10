# Sleet

Sleet is a lightweight fleet manager for [SlateDB](https://slatedb.io)
databases. It coordinates nodes through object storage, assigns per-database
services, and runs SlateDB maintenance work such as garbage collection,
distributed compaction, and mirroring.

## Quick start

Install the CLI from crates.io:

```sh
cargo install sleet --locked
```

Register a SlateDB database:

```sh
sleet register s3://path/to/sleet/state s3://app-data/db1
```

Start a node:

```sh
sleet run s3://path/to/sleet/state --node-id sleet-1
```

Check fleet status:

```sh
sleet status s3://path/to/sleet/state
```

The same operations are available through the Rust API:

```rust,no_run
use sleet::{Fleet, StatusOptions};

let fleet = Fleet::open("s3://path/to/sleet/state")?;
let status = fleet.status(StatusOptions::default()).await?;
println!("{} databases", status.databases.len());
```

By default, nodes offer all services:

```text
gc,compactor-coordinator,compaction-workers,mirror
```

You can run specialized nodes with `--services`:

```sh
sleet run s3://path/to/sleet/state \
  --node-id worker-1 \
  --services compaction-workers
```

## Documentation

See the following directories for more information:

- [docs](docs): Sleet documentation.
- [examples](examples): Example configuration files.
- [rfcs](rfcs): Sleet design documents.
- [schemas](schema): CLI, configuration, and heartbeat file JSON schemas.

## License

MIT
