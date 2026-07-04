# Sleet documentation

Sleet runs background work for many [SlateDB](https://slatedb.io) databases from a small pool of stateless nodes. It handles garbage collection, compaction coordination, compaction workers, and optional mirroring. The only shared dependency is object storage.

This documentation is written for operators, application teams, and maintainers. Start with the page that matches the job in front of you.

| Need | Read |
| --- | --- |
| Bring up a small fleet | [Getting started](getting-started.md) |
| Understand the model before operating it | [Architecture](architecture.md) |
| Write `sleet.toml` or per-database overrides | [Configuration](configuration.md) |
| Run nodes, check status, and change capacity | [Operations](operations.md) |
| Configure DR, backups, or replica targets | [Mirroring](mirroring.md) |
| Check command syntax and JSON outputs | [CLI reference](cli.md) |

## Core idea

A fleet is a directory tree under one object-store URL:

```text
<root>/
  sleet.toml       # optional fleet-wide policy
  dbs/             # one registry file per managed database
  nodes/           # node heartbeats
```

Each `sleet run` process points at the same root. Nodes write heartbeats, read the registry, compute ownership with rendezvous hashing, and run the service loops they own. Sleet stores no assignment records. If two nodes briefly run the same service for one database, SlateDB's CAS, fencing, and job claims keep the duplicate work safe.

## Repository references

The docs link to these source files when they are the better reference:

- [README.md](../README.md) for the shortest project summary.
- [DESIGN.md](../DESIGN.md) for the coordination design.
- [DESIGN-MIRROR.md](../DESIGN-MIRROR.md) for mirror internals.
- [examples/sleet.toml](../examples/sleet.toml) and [examples/db.toml](../examples/db.toml) for complete config examples.
- [schema/config.schema.json](../schema/config.schema.json), [schema/heartbeat.schema.json](../schema/heartbeat.schema.json), and [schema/cli.schema.json](../schema/cli.schema.json) for generated schemas.

The schemas are generated from Rust types and checked by tests, so they are the most precise field reference.
