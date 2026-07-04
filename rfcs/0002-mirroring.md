# RFC 0002: Mirroring

Status: accepted

Depends on: RFC 0001

## Summary

Sleet can mirror a registered SlateDB database into another object-store root.
The mirror copies immutable data objects and commits manifests at the target.
The target is a SlateDB database root, but Sleet is the only process that
writes target manifests while mirroring is enabled.

Mirroring supports disaster recovery, read replicas, backups, and migrations.
All four use the same sync pass. Mode and retention settings change when the
pass runs and what old target objects Sleet later removes.

## Motivation

Operators often need a SlateDB copy in another bucket, region, or cloud. Some
copies are hot standby databases. Others are backup roots with restore points.
The copy must remain readable at every committed manifest, and it must not
depend on Sleet state that lives outside the source and target stores.

Sleet already has a fleet scheduler. This RFC adds a mirror service that uses
that scheduler but keeps the mirror protocol stateless. The target's highest
manifest id is the watermark.

## Non-goals

- Mirroring does not copy the Sleet fleet root.
- Mirroring does not proxy reads or writes.
- Mirroring does not transform records or recompact data at the target.
- Mirroring does not chain mirrors. A target must not also be a registered
  source database.
- Mirroring does not fence writers across roots. Operators must stop source
  writers before promotion.
- Clone sources and databases with a separate WAL object store are excluded.

## SlateDB facts used by the protocol

SlateDB writes new state by committing a new manifest id with create-if-absent.
The immutable object names referenced by manifests live under `wal/` and
`compacted/`. Checkpoints are manifest metadata that pin another manifest and
the objects it references.

Manifest ids can have gaps. Source manifest GC may remove old unpinned
manifests. A mirror therefore copies the source head it can prove complete,
not every intermediate manifest.

Sleet never copies these target-local directories:

- `compactions/`, because compaction claims and epochs are root-local
- `gc/*.boundary`, because boundary files belong to the active root

Zero-byte WAL fence objects are regular `wal/` objects and are copied.

## Target invariants

A valid mirror target satisfies two invariants.

Completeness: the latest target manifest has its closure present. The closure
contains the manifest's data objects, plus the pinned manifest and data
objects for each live checkpoint in that manifest. Support is one level deep,
matching what source GC preserves.

Single writer: while Sleet mirrors a target, only Sleet writes manifests under
that target root. External copy tools may write `wal/` and `compacted/`, but
not `manifest/`.

Completeness makes the latest committed target manifest openable. Single
writer keeps the target history aligned with the source history.

## Configuration

Mirror targets live under the source database config. A fleet-wide target can
be set in `sleet.toml`:

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

With `source_prefix`, Sleet mirrors every registered database under that
prefix to the target `url` plus the source path suffix. Prefix matching uses
path segment boundaries, so `s3://user-data` does not match
`s3://user-database/x`.

Without `source_prefix`, `url` is the exact target for one source database.

`url` and `source_prefix` override together. If either field is set in a
config layer, Sleet takes both from that layer.

A database file can opt out of a fleet target or add targets:

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

A database mirrors when its resolved services include `mirror` and at least
one enabled target applies. A database with no applicable target produces no
mirror assignment. `status` reports that case.

The target name is part of identity. Sleet uses it in placement keys and in
the source checkpoint name. Renaming a target abandons old mirror checkpoints
to expiry and creates a different placement key.

Removing or disabling a target stops the mirror. The destination remains a
valid database at its last committed watermark.

## Placement

RFC 0001 assigns mirror work by rendezvous hashing over:

```text
(database, mirror, target-name)
```

The top-ranked live node offering service letter `m` owns the target. The
node's `--max-mirror-jobs` flag caps concurrent mirror jobs. Mirror nodes
must reach the source root and destination root.

Duplicate mirror tasks are safe. Manifest commits use create-if-absent, and
a race to write the same source manifest body is not a fork.

## Sync pass

Every mode uses the same pass.

1. List target `manifest/` and set `W` to the highest manifest id there.
2. Read the source head `L`.
3. If `L == W`, there is no manifest to commit.
4. Enumerate the closure of `L`.
5. Read the target watermark manifest `W`, if it exists, and subtract its
   closure from `L`'s closure.
6. Copy or confirm the remaining candidate objects.
7. Commit referenced manifests to the target in ascending id order, with
   `L` last.

The pass stores no other state. It syncs directly from `W` to `L`. It does
not replay intermediate manifests, because source GC may already have removed
objects that only those manifests referenced.

The target data directories are not listed during a normal diff. WAL
candidates are checked with HEAD because the continuous WAL tail may already
have copied them. Builtin and rclone copiers copy compacted candidates
directly. The external copier HEADs compacted candidates and backfills misses.

When seeding a target with no watermark, the external copier may list the
target once to avoid a HEAD per object.

If the diff contains no compacted candidates and no WAL misses, Sleet can
commit without creating a source checkpoint. This covers checkpoint-only
manifest changes and unpin commits.

## Source pinning

When a pass must copy data objects, Sleet creates a source checkpoint named:

```text
sleet-mirror:<target-name>
```

The checkpoint lifetime is `checkpoint_lifetime`. Sleet refreshes it at
half-life while the copy runs. If the pin lapses, the pass restarts.

The checkpoint creation writes a new source manifest. Sleet adopts that new
manifest as `L` and computes the diff again. Only then does it copy data.

After committing the target manifests, Sleet deletes the source checkpoint.
That deletion writes one more source manifest. A later pass copies that
pin-removal manifest through the pinless path.

## Commit order

Sleet commits manifests in ascending id order and commits the adopted source
head last. A checkpoint entry cannot point to a manifest id greater than the
manifest that carries it. Ascending order therefore lands a referenced
manifest before a later manifest points at it.

Each manifest that becomes latest during the commit is complete.

## WAL tail

Continuous mode tails WAL SSTs between sync passes. Starting above the
manifest's `next_wal_sst_id`, Sleet copies WALs in ascending id order as they
appear at the source.

SlateDB recovery probes past the WAL id recorded in the manifest. A promoted
target can replay the copied WAL tail the same way a writer recovers after a
crash. Copying WALs in order avoids gaps.

## Modes

`continuous` runs sync passes on `poll` and tails WALs between passes. RPO is
the poll interval plus copy time for any WALs not yet tailed. Without
retention, superseded target objects remain until the target is promoted and
normal SlateDB GC later runs there.

`periodic` runs one pass every `interval`. A pass is due when the target's
latest manifest `LastModified` is older than `interval`. Periodic mode does
not tail WALs. Each committed manifest is a restore point.

`sleet mirror sync <root> <db> <target>` runs one pass regardless of the
configured mode.

Continuous mode copies every compaction rewrite. Periodic mode can skip
short-lived compaction outputs if the interval is longer than the compaction
cycle.

## Copiers

Sleet always commits manifests. The `copier` field controls data movement.

`builtin` streams `wal/` and `compacted/` objects between stores from the
Sleet process. `copy_parallelism` controls concurrency.

`rclone` asks Sleet to build the object list and run `rclone copy
--files-from`. rclone must not touch `manifest/`. Nodes receive the rclone
binary path with `--rclone`.

`external` relies on provider replication for data prefixes, then has Sleet
backfill missing objects before committing manifests. Replication must cover
only `wal/` and `compacted/`. It must not replicate delete markers.

Provider filters are anchored prefixes, not regexes or globs. S3 rules are
include-only and need two rules per database. S3, GCS Storage Transfer, and
Azure object replication all have rule-count limits. Those limits make
external replication a poor fleet-wide default.

`sleet mirror prefixes` emits provider-shaped filter lists for a database and
target.

## Retention and pruning

Without retention, Sleet does not delete target objects.

With retention, target manifests are restore points. The pruner keeps the
latest manifest and every manifest younger than `keep`. For each kept restore
point, it also keeps one level of checkpoint support. Then it deletes old
manifests and unreferenced data objects older than `min_age`.

Two guards protect in-flight sync passes.

The first guard reads the source head and spares every object in that closure.
If Sleet cannot read the source, it does not delete target data objects.

The second guard looks for source checkpoints named
`sleet-mirror:<target-name>`. While any such checkpoint exists, the pruner
does not delete target objects newer than the oldest checkpoint create time,
minus `min_age` as clock slack.

The WAL tail above the latest target manifest is never pruned.

Only the Sleet pruner may delete mirror target objects while mirroring is
enabled. Running SlateDB GC at the target would commit manifests, strip
expired checkpoints, and fork the target history away from the source.

Once a target is promoted and no longer mirrored, it is an ordinary SlateDB
database. Normal GC applies there.

## Restore

`sleet mirror restore` copies one restore point into an empty destination:

```sh
sleet mirror restore <root> <backup-url> <dest-url> --at <point>
```

`--at` accepts a manifest id or an RFC 3339 timestamp. A timestamp maps to the
newest restore point at or before that time. The resolution comes from the
SlateDB sequence tracker, about 60 seconds with stock settings.

Restore refuses a non-empty destination and never deletes. A restore point
must have a complete closure. A support manifest can contain old checkpoint
entries whose pinned manifests are gone; Sleet refuses to restore such a
manifest as the chosen point.

Rolling back in place means restoring into a fresh root and repointing
clients.

## Reading a mirror

A live read replica needs a SlateDB reader mode that follows the latest
manifest without writing checkpoints at the target. A reader-managed
checkpoint would violate single writer.

Until that reader mode exists, clients can mount explicit checkpoints that
the source created and the mirror copied. A long scan should fit inside the
target retention window. Set `keep` longer than the longest replica scan.

Read freshness for a checkpoint-free reader would be the reader poll interval
plus mirror lag. WAL tail replay can reduce failover data loss for promoted
targets.

## Verification

`sleet status --mirrors` reads source and target heads for each mirror target.
It reports manifest lag, WAL lag, and estimated seconds from the sequence
tracker. It also reports destination collisions and databases with no
applicable target.

`sleet mirror verify` checks every restore point's closure at the target. It
checks object existence and size. It does not trust ETags because multipart
ETags do not survive all copy paths.

Verification skips checkpoint entries already retired at the source when they
are carried only by support manifests. Restore enforces the stricter rule for
the selected restore point.

## Promotion

Promotion is manual in this RFC:

1. Stop source writers.
2. Run a final sync while the source is reachable.
3. Disable the mirror target.
4. Open the destination as a normal SlateDB database.

The first writer at the destination bumps `writer_epoch` and replays the WAL
tail. A coordinator started there bumps `compactor_epoch` and builds fresh
compaction state. SlateDB epochs fence within one root, so operators must
sequence source writer shutdown outside Sleet.

A future `sleet mirror promote` command can package those steps and report
the final manifest and WAL id.

## Compatibility

The mirror target name is stable identity. It appears in placement keys and
source checkpoint names.

The object-name rules in `src/mirror/layout.rs` are wire format. The mirror
must keep constructing the same manifest, WAL, and compacted object names for
the SlateDB versions it supports.

The mirror target manifest sequence may have gaps. Readers and restore code
must accept those gaps.

## Future work

Projected manifests could make the target a first-class database while still
byte-copying data objects. That requires a public SlateDB manifest write API.

Logical mirroring could ship WALs and compact independently at the target.
That would reduce cross-store transfer, but the target would no longer be a
byte-for-byte physical mirror.

Clone sources and separate WAL stores can be supported later by copying or
rewriting the additional object references they introduce.
