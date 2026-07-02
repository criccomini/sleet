# sleet

A fleet manager for [SlateDB](https://slatedb.io) databases. `sleet`
runs their background services — garbage collection, compaction
coordination, and compaction execution — outside the writer process,
for millions of databases from a small pool of nodes, with no
dependencies beyond object storage.

**Status: early development.** The daemon, services, and CLI are
implemented against SlateDB 0.14; mirroring and auto-discovery are
future work. See [DESIGN.md](DESIGN.md) for the design.

## Usage

```sh
sleet run s3://ops/sleet/ --node-id sleet-1     # a fleet node
sleet status s3://ops/sleet/ --queues           # nodes, databases, placement
sleet register s3://ops/sleet/ s3://bucket/db   # register a database
```

A fleet lives at a fleet root: `sleet.toml` holds fleet-wide policy and
`dbs/<db>.toml` registers a database. See
[examples/sleet.toml](examples/sleet.toml),
[examples/db.toml](examples/db.toml), and
[schema/config.schema.json](schema/config.schema.json). One-shot
subcommands take `--format json`, with responses documented by
[schema/cli.schema.json](schema/cli.schema.json).

## Developing

```sh
cargo test        # includes schema drift checks and CLI snapshots
cargo fmt && cargo clippy --all-targets
```
