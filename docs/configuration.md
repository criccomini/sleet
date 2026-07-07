# Configuration

Sleet configuration is layered. Fleet-wide policy lives in `sleet.toml`; each
registered database may override fields in its own `dbs/<db>.toml` file.

## File locations

```text
<root>/
  sleet.toml                # optional fleet-wide config
  dbs/<encoded-url>.toml    # one file per registered database
```

The database file name is the percent-encoded canonical database URL plus
`.toml`. For example:

```text
s3://bucket/db
dbs/s3%3A%2F%2Fbucket%2Fdb.toml
```

`sleet register` creates an empty file and never overwrites an existing one:

```sh
sleet register s3://ops/sleet s3://bucket/db
```

Deleting the file unregisters the database. Setting `services = []` keeps the
database registered but runs no services for it.

## Layering

For each database, Sleet resolves config in this order:

1. built-in defaults
2. `[database]` in `sleet.toml`
3. the database's `dbs/<db>.toml`

Fields fall through independently. A per-database file can override one field
without copying the rest of the fleet policy.

The one exception is a mirror target's `url` and `source_prefix`. If either
field is set at a layer, both fields are taken from that layer. This prevents
an override from inheriting a prefix it did not ask for.

## Minimal fleet config

`sleet.toml` is optional. Add it when you need explicit timing, service
selection, or defaults:

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

See the complete example in [../examples/sleet.toml](../examples/sleet.toml).
A JSON spec is also available in
[../schema/config.schema.json](../schema/config.schema.json).

## Per-database overrides

The contents of `dbs/<db>.toml` have the same shape as the `[database]` table,
without the table name:

```toml
services = ["gc"]

[gc.compacted]
min_age = "30m"
```

See the complete example in [../examples/db.toml](../examples/db.toml). A
JSON spec is also available in
[../schema/config.schema.json](../schema/config.schema.json).

## Services

`services` in `sleet.toml` or `dbs/<db>.toml` controls which services are
enabled for each database. Valid values are:

| Name                    | Effect                                          |
| ----------------------- | ----------------------------------------------- |
| `gc`                    | Run garbage collection.                         |
| `compactor-coordinator` | Schedule compactions and commit completed work. |
| `compaction-workers`    | Poll `.compactions` and execute jobs.           |
| `mirror`                | Run configured mirror targets.                  |

If config `services` is unset, all database services are enabled by default.

## Important defaults

The generated schema is the field reference. These defaults are the values
operators usually need first:

| Area                         | Default                     |
| ---------------------------- | --------------------------- |
| Node heartbeat interval      | `10s`                       |
| Node heartbeat timeout       | `30s`                       |
| Config and registry poll     | `60s`                       |
| GC directory interval        | `60s`                       |
| GC directory minimum age     | `300s`                      |
| GC clone-detach pass         | enabled, `60s` interval     |
| WAL fence GC                 | dry-run by default          |
| Compaction worker count      | `1` node per database       |
| Mirror mode                  | `continuous`                |
| Mirror poll                  | `10s`                       |
| Mirror periodic interval     | `24h`                       |
| Mirror copy parallelism      | `8`                         |
| Mirror source pin checkpoint | `15m` lifetime              |
| Mirror retention             | unset, so nothing is pruned |

Most GC blocks configure a resource directory with `enabled`, `interval`,
`min_age`, and `dry_run`. The clone-detach pass is the exception:
`[gc.detach]` has only `enabled` and `interval`, runs by default, and has no
`min_age` field.

## Validation rules

Sleet rejects invalid config when it reads it. The main rules are:

- unknown fields are errors
- duplicate service names are errors
- `node.heartbeat_interval` must be greater than zero
- `node.heartbeat_interval` must be less than `node.heartbeat_timeout`
- `node.config_poll` must be greater than zero
- service intervals must be greater than zero
- scheduler `min_compaction_sources` must not exceed `max_compaction_sources`
- enabled mirror targets must have a valid `url`
- mirror target names must be 1 to 128 characters of `[A-Za-z0-9_-]`

If a running node fails to read a new config or registry view, it keeps the
last good view and reports a warning through logs and status.

## URL schemes

Sleet accepts object-store URL schemes supported by its registry code:

```text
s3, s3a, gs, az, adl, azure, abfs, abfss, file, memory, http, https
```

Sleet canonicalizes URLs by lowercasing scheme and host and removing trailing
slashes so that `s3://bucket/db` and `S3://bucket/db/` are equivalent.

## Schemas

Sleet offers JSON schemas for config:

- [schema/config.schema.json](../schema/config.schema.json) for `sleet.toml`
  and database config.
