# sleet

A fleet manager for [SlateDB](https://slatedb.io) databases. `sleet`
runs the background services a SlateDB database needs but that don't
belong in the writer process — garbage collection, compaction
coordination, and compaction execution — for many databases from a
small pool of nodes, with no dependencies beyond object storage.

**Status: early development.** The fleet spec format and CLI surface
are implemented; the daemon and service wiring are not. See
[DESIGN.md](DESIGN.md) for the design.

## Usage

```sh
sleet run --spec /etc/sleet/fleet.toml   # the daemon (not yet implemented)
sleet status --spec fleet.toml           # nodes, assignments, health
```

The fleet spec is a TOML file; see [examples/fleet.toml](examples/fleet.toml)
and [schema/config.schema.json](schema/config.schema.json). One-shot
subcommands take `--format json`, with responses documented by
[schema/cli.schema.json](schema/cli.schema.json).

## Developing

```sh
cargo test        # includes schema drift checks and CLI snapshots
cargo fmt && cargo clippy --all-targets
```
