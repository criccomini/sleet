# Operations

This page covers the day-to-day work of running Sleet nodes: starting them,
scaling capacity, checking status, and changing the registry.

## Run nodes

Start each node with a unique ID:

```sh
sleet run s3://ops/sleet --node-id sleet-1
```

Run more nodes against the same fleet root to add capacity:

```sh
sleet run s3://ops/sleet --node-id sleet-2
sleet run s3://ops/sleet --node-id sleet-3
```

The default node offers all services:

```text
gc,compactor-coordinator,compaction-workers,mirror
```

Use `--services` to build specialized pools:

```sh
sleet run s3://ops/sleet \
  --node-id compact-1 \
  --services compaction-workers \
  --max-compaction-jobs 32

sleet run s3://ops/sleet \
  --node-id mirror-1 \
  --services mirror \
  --max-mirror-jobs 16
```

`--max-compaction-jobs` caps how many databases compact on one node at the
same time. `--max-mirror-jobs` caps concurrent mirror copy or prune jobs on
one node. Both default to the machine's available parallelism.

## Logs

Sleet logs through Rust's `tracing` filter. If `RUST_LOG` is unset, the
default filter is `sleet=info,warn`.

```sh
RUST_LOG=sleet=debug,object_store=warn sleet run s3://ops/sleet --node-id sleet-1
```

## Register and unregister databases

Register with the CLI:

```sh
sleet register s3://ops/sleet s3://bucket/db
```

Or create the registry file directly under `dbs/`. An empty file uses fleet
defaults.

To stop managing a database, delete its registry file. To keep it visible but
run no services, set:

```toml
services = []
```

Use per-database files for exceptions:

```toml
services = ["gc", "compaction-workers"]

[compaction-workers]
count = 4
```

## Check status

The base command shows nodes, registered databases, and placement:

```sh
sleet status s3://ops/sleet
```

Add compaction queue depth:

```sh
sleet status s3://ops/sleet --compactions
```

Add mirror lag:

```sh
sleet status s3://ops/sleet --mirrors
```

Use JSON for automation:

```sh
sleet status s3://ops/sleet --format json
```

The JSON response schema is
[schema/cli.schema.json](../schema/cli.schema.json).

## Read warnings first

`sleet status` reports conditions that need operator attention. Common
warnings include:

| Warning shape                                     | Meaning                                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------------- |
| `no live node offers ...`                         | A service is enabled for at least one database, but no live node advertises it. |
| `has mirror enabled but no applicable target`     | The database enables `mirror`, but target scope does not match it.              |
| `mirror destinations collide`                     | More than one source maps to the same mirror destination.                       |
| `destination ... is itself a registered database` | A mirror target is also managed as a source, which can fork history.            |
| registry alias or parse warnings                  | A `dbs/` file name is invalid or canonicalizes to another database.             |

Status is derived from object storage. Nodes do not serve an API.

## Change capacity

Sleet nodes are stateless. To add capacity, start more nodes. To remove
capacity, stop nodes cleanly so they delete heartbeats. If a node dies, peers
stop considering it live after `heartbeat_timeout`.

For heterogeneous fleets:

- run CPU-heavy machines with `--services compaction-workers`
- run network-heavy machines with `--services mirror`
- leave small nodes on `gc,compactor-coordinator`
- use per-database `compaction-workers.count` for hot databases

Nodes offering a service must be able to reach every database and destination
that service might touch. Placement does not know about network reachability
or credentials.

## Roll config changes

Nodes re-read `sleet.toml` and `dbs/` every `config_poll`. A config change
usually takes effect within `config_poll` plus one heartbeat tick.

For low-risk changes:

1. Write the new config or registry file.
2. Run `sleet status` and check warnings.
3. Watch node logs for parse or validation errors.

If a node cannot read a new config, it keeps the last good config. That avoids
dropping all assignments because of a transient object-store failure.

## Operate mirrors carefully

While a destination is a mirror target:

- do not register it as a Sleet database
- do not run SlateDB GC against it
- do not write manifests there outside Sleet
- do not propagate delete markers from external replication

Promotion is manual today. Stop source writers, disable the mirror target, run
a final sync if needed, then open the destination as an ordinary database. See
[Mirroring](mirroring.md).

## Cost controls

The main recurring object-store operations are:

- one heartbeat PUT per node per heartbeat interval
- one `nodes/` LIST per node per heartbeat interval
- one `dbs/` LIST per node per `config_poll`
- per-database service polls based on config
- extra status reads when `--compactions` or `--mirrors` is used

Mostly idle compaction workers back off polling up to five minutes. Continuous
mirrors also back off while caught up, capped at five minutes unless the
configured `poll` is larger.
