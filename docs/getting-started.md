# Getting started

This guide starts one Sleet node against one registered SlateDB database. It assumes you already have a database root in object storage.

## Build the binary

From the repository root:

```sh
cargo build --release
```

The binary is written to:

```sh
target/release/sleet
```

During development, use `cargo run --` in place of `sleet`:

```sh
cargo run -- status s3://ops/sleet/
```

## Pick a fleet root

Choose an object-store URL that Sleet can use for fleet state:

```sh
export SLEET_ROOT=s3://ops/sleet
```

The root is not a SlateDB database. It is Sleet's own state tree. Sleet creates and reads objects under:

```text
s3://ops/sleet/
  sleet.toml
  dbs/
  nodes/
```

Credentials come from the environment and object-store provider configuration used by the `object_store` crate. Every node must be able to read and write the fleet root. A node must also be able to reach the databases and mirror destinations for the services it offers.

## Register a database

Register one database root:

```sh
sleet register "$SLEET_ROOT" s3://bucket/db
```

Registration creates a file under `dbs/` whose name is the percent-encoded canonical database URL:

```text
dbs/s3%3A%2F%2Fbucket%2Fdb.toml
```

An empty registry file is valid. It means the database uses the fleet-wide defaults from `sleet.toml`, or Sleet's built-in defaults if `sleet.toml` is absent.

## Start a node

Run one node:

```sh
sleet run "$SLEET_ROOT" --node-id sleet-1
```

By default a node offers all services:

```text
gc,compactor-coordinator,compaction-workers,mirror
```

If no mirror targets apply to a database, the mirror service has nothing to run for that database. To run a smaller node, pass a service list:

```sh
sleet run "$SLEET_ROOT" \
  --node-id compact-1 \
  --services compaction-workers \
  --max-compaction-jobs 16
```

Node IDs must be unique within the fleet and must be 1 to 128 characters of letters, numbers, `_`, and `-`.

## Check status

```sh
sleet status "$SLEET_ROOT"
```

Add queue and mirror reads when you need them:

```sh
sleet status "$SLEET_ROOT" --compactions
sleet status "$SLEET_ROOT" --mirrors
sleet status "$SLEET_ROOT" --format json
```

`--compactions` reads each database's `.compactions` state. `--mirrors` reads source and destination heads for each mirror target. Both flags make status more informative and more expensive.

## Add fleet policy

Create `sleet.toml` at the fleet root when defaults are not enough:

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

See [Configuration](configuration.md) for layering, defaults, and per-database overrides.

## Next steps

- Use [Operations](operations.md) to plan node roles, capacity, and failover behavior.
- Use [Mirroring](mirroring.md) to configure disaster recovery, backups, or migration targets.
- Use [Development](development.md) if you are changing Sleet itself.
