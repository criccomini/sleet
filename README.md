# sleet

A fleet manager for [SlateDB](https://slatedb.io) databases. `sleet`
runs their background services (garbage collection, compaction
coordination, and compaction execution) outside the writer process,
managing many databases from a small pool of nodes.

Its only dependency is object storage. There is no etcd, no leader
election, and no stored assignment state: a fleet is a directory tree
under one object-store URL, nodes are stateless processes pointed at
it, and each node decides what to run by rendezvous-hashing the live
membership. When scheduling goes wrong, the worst case is two nodes
briefly running the same service, which SlateDB's manifest CAS and
epoch fencing make harmless. [DESIGN.md](DESIGN.md) has the details.

**Status: early development.** The daemon, services, and CLI work
against SlateDB 0.14; mirroring and auto-discovery are future work.

## Usage

Pick a fleet root (any object-store URL), register a database, and
start a node:

```sh
sleet register s3://ops/sleet/ s3://bucket/db
sleet run s3://ops/sleet/ --node-id sleet-1
sleet status s3://ops/sleet/
```

Grow the fleet by starting more nodes with different ids; they find
each other through the root. `--services` restricts what a node
offers, so a heterogeneous fleet can point its large machines at
compaction work alone. `status --queues` adds compaction queue depth.

The fleet root holds everything:

```
<root>/
  sleet.toml       # fleet-wide policy (optional)
  dbs/<db>.toml    # one file per registered database
  nodes/           # heartbeats, written by nodes
```

`sleet.toml` sets policy and per-database defaults; `dbs/<db>.toml`
overrides fields for one database, and an empty file accepts the
defaults. See [examples/sleet.toml](examples/sleet.toml) and
[examples/db.toml](examples/db.toml); the field reference is
[schema/config.schema.json](schema/config.schema.json). One-shot
commands take `--format json`, with responses documented by
[schema/cli.schema.json](schema/cli.schema.json).

## Developing

```sh
cargo test                       # unit, property, chaos, DST, snapshots
cargo fmt && cargo clippy --all-targets
fizz specs/coordination.fizz     # model-check the coordination protocol
```

The MinIO test needs Docker and skips without it.
