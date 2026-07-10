# Getting started

This guide starts one Sleet node against one registered SlateDB database. It
assumes you already have a database root in object storage.

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

We use `s3://ops/sleet` as the example fleet root. In practice, you will need
to pick a location that all nodes can read and write to.

The Sleet root is not a SlateDB database. It is Sleet's own state tree. Sleet
creates and reads objects under:

```text
s3://ops/sleet/
  sleet.toml
  dbs/
  nodes/
```

Credentials and object-store provider options come from the process
environment used by the `object_store` crate. Sleet applies the same process
environment when it opens the fleet root, each database, and each mirror
destination. Every node must therefore run with credentials that can access
the fleet root and every store required by the services it offers.

For specific environment variables, see:

- [AmazonS3Builder::from_env](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env)
- [MicrosoftAzureBuilder::from_env](https://docs.rs/object_store/latest/object_store/azure/struct.MicrosoftAzureBuilder.html#method.from_env)
- [GoogleCloudStorageBuilder::from_env](https://docs.rs/object_store/latest/object_store/gcp/struct.GoogleCloudStorageBuilder.html#method.from_env)

For AWS, configure the process environment before running Sleet:

```sh
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
export AWS_DEFAULT_REGION=us-west-2

sleet register s3://ops/sleet s3://app-data/db
sleet run s3://ops/sleet --node-id sleet-1
```

Omit `AWS_SESSION_TOKEN` when using long-lived access keys. When running on
AWS with an instance, task, or pod role, set `AWS_DEFAULT_REGION` and let the
runtime supply credentials.

## Register a database

Register one database root:

```sh
sleet register s3://ops/sleet s3://bucket/db
```

Registration creates a file under `dbs/` whose name is the percent-encoded
canonical database URL:

```text
dbs/s3%3A%2F%2Fbucket%2Fdb.toml
```

An empty registry file means the database uses the fleet-wide defaults from
`sleet.toml`, or Sleet's built-in defaults if `sleet.toml` is absent.

## Start a node

Run one node:

```sh
sleet run s3://ops/sleet --node-id sleet-1
```

By default a node offers all services:

```text
gc,compactor-coordinator,compaction-workers,mirror
```

If no mirror targets apply to a database, the mirror service has nothing to
run for that database. To run a smaller node, pass a service list:

```sh
sleet run s3://ops/sleet \
  --node-id compact-1 \
  --services compaction-workers
```

Node IDs must be unique within the fleet and must be 1 to 128 characters of
letters, numbers, `_`, and `-`.

## Check status

```sh
sleet status s3://ops/sleet
```

Sleet also offers detailed status flags for compaction and mirroring:

```sh
sleet status s3://ops/sleet --compactions
sleet status s3://ops/sleet --mirrors
```

`--compactions` reads each database's `.compactions` state. `--mirrors` reads
source and destination `.manifest` heads for each mirror target. Both flags
make status more informative, but more expensive.

JSON output format is supported for all commands with `--format json`:

```sh
sleet status s3://ops/sleet --format json
```

## Add fleet policy

You may set fleet-wide defaults in `sleet.toml`:

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

See [Configuration](configuration.md) for layering, defaults, and per-database
overrides.
