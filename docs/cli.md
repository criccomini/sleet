# CLI reference

Sleet has one long-running command, `run`, and one-shot operator commands for registration, status, and mirrors.

Related pages: [Getting started](getting-started.md), [Operations](operations.md), [Mirroring](mirroring.md), [Configuration](configuration.md).

## Command overview

```text
sleet <COMMAND>

Commands:
  run       Run a fleet node
  status    Show fleet state and placement
  register  Register a database
  mirror    Run mirror operations
```

Use `-h` for a summary and `--help` for full help:

```sh
sleet run --help
sleet mirror restore --help
```

## `sleet run`

```sh
sleet run [OPTIONS] --node-id <NODE_ID> <ROOT>
```

Runs a node until interrupted.

Important options:

| Option | Meaning |
| --- | --- |
| `--node-id <NODE_ID>` | Required unique node identity. |
| `--services <SERVICES>` | Comma-separated service list. |
| `--max-compaction-jobs <N>` | Databases compacting on this node at once. |
| `--max-mirror-jobs <N>` | Mirror copy or prune jobs on this node at once. |
| `--rclone <PATH>` | Binary for mirror targets using `copier = "rclone"`. |

Service names:

```text
gc
compactor-coordinator
compaction-workers
mirror
```

Example:

```sh
sleet run s3://ops/sleet \
  --node-id worker-1 \
  --services compaction-workers \
  --max-compaction-jobs 16
```

## `sleet register`

```sh
sleet register [OPTIONS] <ROOT> <URL>
```

Creates the registry file for a database. The operation is create-only, so it does not overwrite operator edits.

```sh
sleet register s3://ops/sleet s3://bucket/db
sleet register s3://ops/sleet s3://bucket/db --format json
```

## `sleet status`

```sh
sleet status [OPTIONS] <ROOT>
```

Derives fleet state from the object-store tree.

| Option | Meaning |
| --- | --- |
| `--compactions` | Read each database's compaction queue depth. |
| `--mirrors` | Read mirror source and destination heads and report lag. |
| `--format json` | Emit JSON matching the CLI schema. |

Example:

```sh
sleet status s3://ops/sleet --compactions --mirrors
```

## `sleet mirror sync`

```sh
sleet mirror sync [OPTIONS] <ROOT> <DB> <TARGET>
```

Runs one sync pass for one registered database and target. It prunes afterward when retention is set.

```sh
sleet mirror sync s3://ops/sleet s3://bucket/db backup
```

Use `--rclone <PATH>` when the target uses `copier = "rclone"`.

## `sleet mirror verify`

```sh
sleet mirror verify [OPTIONS] <ROOT> <DB> <TARGET>
```

Checks every restore point's closure at the mirror destination. The command exits nonzero if verification fails.

```sh
sleet mirror verify s3://ops/sleet s3://bucket/db backup
```

## `sleet mirror restore`

```sh
sleet mirror restore [OPTIONS] <ROOT> <BACKUP> <DEST>
```

Copies one restore point from a backup root into an empty destination root.

```sh
sleet mirror restore s3://ops/sleet gs://backups/db1 s3://restore/db1
sleet mirror restore s3://ops/sleet gs://backups/db1 s3://restore/db1 --at 42
```

`--at` accepts a manifest ID or RFC 3339 timestamp. If omitted, Sleet restores the backup's latest manifest.

## `sleet mirror prefixes`

```sh
sleet mirror prefixes --format <FORMAT> <ROOT> <DB> <TARGET>
```

Emits anchored key-prefix filters for an external replication service.

Formats:

```text
s3     S3 bucket replication rules
sts    GCS Storage Transfer Service include prefixes
azure  Azure object replication rules
```

Example:

```sh
sleet mirror prefixes --format s3 s3://ops/sleet s3://bucket/db dr
```

## JSON outputs

One-shot commands that accept `--format json` emit responses defined in [schema/cli.schema.json](../schema/cli.schema.json). Use the schema for automation instead of scraping text output.

