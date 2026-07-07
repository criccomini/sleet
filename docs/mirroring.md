# Mirroring

The mirror service copies a SlateDB database to another object-store root path.
The object-store may be in the same or a different cloud provider. Sleet copies
immutable data objects and commits manifests at the target atomically.

## Use cases

Mirroring supports four operator workflows:

| Workflow                           | Typical mode                             |
| ---------------------------------- | ---------------------------------------- |
| Disaster recovery                  | `continuous`                             |
| Read replica target                | `continuous`                             |
| Point-in-time backups              | `periodic` with retention                |
| Bucket, region, or cloud migration | `continuous` plus a final manual cutover |

## Target configuration

Enable `mirror` and define targets under `[database.mirror.targets]`:

```toml
[database]
services = ["gc", "compactor-coordinator", "compaction-workers", "mirror"]

[database.mirror.targets.dr]
url = "s3://dr-bucket/mirrors"
source_prefix = "s3://user-data"
mode = "continuous"
copier = "builtin"
poll = "10s"
```

With `source_prefix`, a fleet-wide target maps databases under the prefix to
the same relative path at the destination. For example,
a database at `s3://user-data/tenant1/db1` with `source_prefix=s3://user-data`
and a target URL of `s3://dr-bucket/mirrors` will mirror to
`s3://dr-bucket/mirrors/tenant1/db1`.

Without `source_prefix`, `url` is the exact destination for one database.

Mirror sources must be ordinary SlateDB roots. Sleet refuses sources that are
clones (`external_dbs` is set) or that use a separate WAL object store.

Per-database files can opt out or add targets:

```toml
[mirror.targets.dr]
disabled = true

[mirror.targets.backup]
url = "gs://backups/db1"
mode = "periodic"
interval = "24h"

[mirror.targets.backup.retention]
keep = "30d"
```

## Modes

`continuous` syncs `.manifest` and compacted SST data using the `poll` cadence.
In between, it tails WAL SSTs and copies them as they appear.

`periodic` syncs `.manifest` and compacted SST data every `interval`. Each
committed manifest is a restore point.

You may also trigger a single sync pass manually with `sleet mirror sync`:

```sh
sleet mirror sync s3://ops/sleet s3://bucket/db backup
```

When a sync pass has data to copy, Sleet creates a detached source checkpoint
named `sleet-mirror:<target-name>`. While it exists, source GC preserves the
objects needed by the pass. The checkpoint lives for `checkpoint_lifetime`
(`15m` by default), and Sleet refreshes it at half-life while copying. Sleet
deletes the checkpoint after the pass; source GC removes an expired leftover,
which may appear in checkpoint listings until then.

## Copiers

| Copier     | Behavior                                                                             |
| ---------- | ------------------------------------------------------------------------------------ |
| `builtin`  | Sleet copies `wal/` and `compacted/` objects itself.                                 |
| `rclone`   | Sleet builds the file list and runs `rclone copy --files-from`.                      |
| `external` | Bucket replication moves data objects; Sleet backfills misses and commits manifests. |

For `rclone`, you must pass the binary path:

```sh
sleet run s3://ops/sleet --node-id mirror-1 --services mirror --rclone /usr/bin/rclone
sleet mirror sync s3://ops/sleet s3://bucket/db backup --rclone /usr/bin/rclone
```

For `external`, configure replication for the database's `wal/` and
`compacted/` prefixes only. Do not replicate `manifest/` (Sleet is the only
manifest writer at the target).

## Retention and restore

Without retention, Sleet does not prune target objects. Data will grow
unbounded. You may configure retention to keep only recent restore points.

```toml
[mirror.targets.backup.retention]
keep = "30d"
```

Sleet keeps the latest restore point and restore points younger than `keep`,
plus the objects their live checkpoints need. Data deletion also respects
`min_age`.

Restore copies one restore point into an empty destination root:

```sh
sleet mirror restore gs://backups/db1 s3://restore/db1
sleet mirror restore gs://backups/db1 s3://restore/db1 --at 42
sleet mirror restore gs://backups/db1 s3://restore/db1 --at 2026-07-03T12:00:00Z
```

`--at` accepts a manifest ID or an RFC 3339 timestamp. A timestamp resolves to
the newest restore point at or before that time. The timestamp mapping comes
from the backup manifest sequence tracker, which samples at about 60 seconds
with the stock SlateDB settings, so timestamp selection has that granularity.

## Safety rules

While a target is being mirrored:

- only Sleet should write manifests at the target
- external copiers should write only `wal/` and `compacted/`
- SlateDB GC must not run against the target
- the target must not be registered as a Sleet source database
- source writers must be stopped before manual promotion
- nothing (including DB readers) must write manifests into the target

## Reader note

A mirror target is kept as a valid SlateDB database at committed manifests.
Tailing it as a live read replica depends on SlateDB reader support that does
not write checkpoints at the target. The detailed design tracks that in
[RFC 0002](../rfcs/0002-mirroring.md#112-checkpoint-free-reader-slatedb-contribution).
