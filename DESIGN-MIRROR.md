# sleet mirroring

The mirror service replicates a SlateDB database into another
object-store root: another bucket, region, or cloud. This document
extends `DESIGN.md`; placement, config layering, and the process model
apply unchanged unless stated otherwise.

## 1. Goals

1. **Disaster recovery**: a warm standby in another region or cloud,
   with bounded data loss (RPO ~ WAL lag), that fails over by being
   opened as an ordinary database (§10).
2. **Read replicas**: a near-realtime, always-consistent replica for
   serving reads in another region or cloud: snapshot reads by mounting
   mirrored checkpoints today (§5), tailing reads once a checkpoint-free
   reader mode lands upstream (§12.2).
3. **Backups**: point-in-time, incremental, verifiable snapshots with
   retention independent of the source (§7).
4. **Migration**: move a database between buckets, regions, or clouds
   with downtime bounded by the final WAL delta (§10).

## 2. Non-goals

- No data-plane involvement: replica readers open the target directly.
- No cross-root fencing. SlateDB's epochs fence within one root;
  nothing on the target side can stop a writer on the source. Stopping
  source writers at failover is outside sleet.
- No logical transformation: v1 copies bytes. Filtering, format
  changes, and re-compaction at the target are out (§12.3).
- The fleet root (`sleet.toml`, `dbs/`, `nodes/`) is not mirrored;
  mirroring is per database.
- Databases that are clones (`external_dbs` set) or that use a separate
  WAL object store are rejected at registration (§12.4).

## 3. Model

Mirroring builds on three properties of SlateDB's layout:

- Every object under a database root is immutable and uniquely named
  (`manifest/<id:020>.manifest`, `wal/<id:020>.sst`,
  `compacted/<ulid>.sst`) except the `gc/*.boundary` files. New state
  is always a new manifest id committed with create-if-absent.
- A checkpoint is manifest metadata (`id`, `manifest_id`,
  `expire_time`) that pins a manifest and, transitively, every object
  it references against GC.
- Gaps in the manifest id sequence are normal: manifest GC deletes old
  unpinned versions below the latest.

A **mirror target** is a registered database whose config names a
source (§9). The mirror service copies the source's immutable objects
to the same relative names under the target root and commits manifests
as the atomic step. Two invariants define a valid target:

1. **Completeness**: every manifest present at the target has its full
   closure present. The closure of a manifest is every object it
   references: L0 and sorted-run SSTs under `compacted/`, WAL SSTs
   above `replay_after_wal_id`, and, recursively, every manifest one of
   its checkpoints pins.
2. **Single writer**: the mirror task is the only process that writes
   manifests under the target root. External copiers (§8) write only
   `wal/` and `compacted/`.

Completeness makes the target a valid SlateDB database at every
instant: the latest manifest and every checkpoint-pinned manifest open.
Single-writer holds because any other manifest committer would fork the
target's history away from the source's. This is why no other sleet
service may run against a target: SlateDB's GC CASes manifests to strip
expired checkpoints (`garbage_collector.rs`), and a compactor would
commit its own state. `services = ["mirror"]` is exclusive.

Never copied: `compactions/` (job claims and epochs are root-local; a
target opened live starts fresh, §10) and `gc/*.boundary`
(target-local, maintained by reconciliation, §4). Zero-byte WAL fence
objects are ordinary `wal/` objects and copy like any other.

## 4. The sync pass

Every mode runs the same pass:

1. **Watermark.** LIST `manifest/` at the target; the highest id `W` is
   the last committed state. No other state is stored anywhere.
2. **Read.** Read the source's latest manifest `L`. If `L = W`, skip to
   step 6.
3. **Pin.** Create a source checkpoint at `L` named for the target
   (`sleet-mirror:<target-key>`), lifetime `checkpoint_lifetime`,
   refreshed at half-life while the pass runs. The previous pass's pin
   stays until step 7.
4. **Copy.** Enumerate the closure of `L`. LIST `wal/` and `compacted/`
   at the target, diff, and copy the missing objects. An object exists
   at the target iff it is done: names are unique and content is
   immutable, so the check is exact and re-copies are harmless.
5. **Commit.** PUT the closure's manifests in ascending id order, `L`
   last, each with create-if-absent. After each successful create,
   re-read the target's `gc/manifest.boundary` and treat an id at or
   below it as a failed write (RFC-0026, same as any manifest writer).
6. **Tail** (continuous mode). Copy WAL SSTs above `L`'s
   `next_wal_sst_id` in ascending id order as they appear at the
   source. WAL ids are dense, so the tail poll is one GET of the next
   expected id, nearly free when idle. SlateDB's replay discovers WALs
   by probing the store past the manifest's recorded state
   (`TableStore::last_seen_wal_id`), so a target opened live replays
   the copied tail exactly like a writer recovering from a crash.
   Copying in id order means the target never has a WAL gap.
7. **Unpin.** Delete the previous pass's pin checkpoint.

The pass syncs `W` directly to `L`; it cannot replay intermediate
manifests. Only `L` and checkpoint-pinned manifests are protected at
the source, so objects exclusive to unpinned intermediates may already
be deleted. Skipping is safe because manifest id gaps are already
normal.

**Reconciliation** (continuous mode) propagates the source's deletions.
The active set at the target is the closure of the latest manifest plus
every manifest pinned by an unexpired checkpoint: the same rule
SlateDB's GC applies, run read-only against manifests. Data objects
outside the active set and older than `min_age` are deleted. Manifests
that are neither latest nor pinned are deleted after advancing
`gc/manifest.boundary` past them, which closes the race with a stale
mirror task re-committing a deleted id.

Safety follows sleet's core invariant: correctness never depends on
scheduling. Copies are idempotent, the commit is create-if-absent, and
a duplicate or stale mirror task at worst loses a create race or
re-copies bytes; two tasks at different watermarks converge on the same
target. A crashed pass leaves extra data objects and no committed
manifest; the next pass resumes from the watermark. If the mirror is
down long enough for its pin to expire, source GC reclaims and the next
pass re-baselines from the current latest; objects already copied
remain valid because they are immutable.

## 5. Reading a mirror

Replica readers mount the target with `DbReader` and an explicit
checkpoint id. Opening without one is not allowed against a target:
`DbReader` would CAS its own checkpoint into the target manifest,
violating single-writer.

The checkpoints readers mount are created at the source and arrive in
every copied manifest. With `[mirror.serve]` set, the mirror rotates a
**serving checkpoint** at the source: each `refresh`, it creates a new
checkpoint at the source's latest manifest with the configured
`lifetime`; old ones expire. Readers list checkpoints by name at the
target and mount the newest, reopening to advance. Read freshness is
the refresh cadence plus mirror lag. Reconciliation honors unexpired
checkpoints, so a reader holds a consistent snapshot for the serving
checkpoint's lifetime.

## 6. Modes

- **`continuous`**: the pass plus the WAL tail, on a `poll` cadence
  with idle backoff, plus reconciliation. The target tracks the source,
  deletions included. RPO is one poll plus tail copy time.
- **`periodic`**: one pass every `interval`, no WAL tail, no
  reconciliation; retention pruning instead (§7). Each committed
  manifest is a point-in-time cut. Scheduling is stateless: a pass runs
  when the target's latest manifest's `LastModified` is older than
  `interval`.
- One-shot: `sleet mirror sync <root> <target>` runs a single pass
  regardless of mode.

Cost differs by mode. Continuous copies every compaction rewrite, so
cross-store transfer scales with ingest times one plus the compaction
write amplification. A periodic interval longer than the compaction
cycle copies only surviving SSTs.

## 7. Backups and retention

A periodic target's committed manifests are its restore points. With
`[mirror.retention]` set, the pruner keeps the latest manifest plus
every manifest younger than `keep`, and deletes the rest: first advance
the boundary past the pruned manifest ids (RFC-0026), then delete the
manifests, then delete data objects unreferenced by any kept manifest
and older than `min_age`. Unset retention keeps everything.

Restore points map to wall-clock time by the manifest's sequence
tracker, so `--at` accepts a manifest id or a timestamp.

`sleet mirror restore <root> <backup-url> <dest-url> --at <point>` is a
one-shot pass with the chosen manifest as `L`, copying its closure to
the destination and committing it. The destination is then an ordinary
database at that point.

## 8. Copiers

`copier` selects who moves data objects. In every mode but
`external-full`, sleet commits manifests; the copier moves only `wal/`
and `compacted/`.

- **`builtin`**: sleet streams objects between the two stores itself,
  `copy_parallelism` objects at a time.
- **`rclone`**: sleet computes the object list per pass and drives
  `rclone copy --files-from` for the data directories, then commits
  manifests itself. rclone never touches `manifest/`. The binary path
  is a node flag (`--rclone`).
- **`external`**: bucket replication configured outside sleet (S3
  CRR/SRR, GCS Storage Transfer, Azure object replication) ships the
  data directories; the mirror task copies nothing, verifies closure
  completeness, and commits manifests. The replication must cover only
  `wal/` and `compacted/` and must not replicate delete markers;
  reconciliation owns deletions. None of these services support
  regex or glob filters, only anchored key prefixes: S3 rules are
  include-only (two rules per database, 1,000 rules per bucket
  configuration), GCS Storage Transfer has anchored
  `includePrefixes`/`excludePrefixes` (1,000 each), Azure
  `prefixMatch` is include-only. `sleet mirror prefixes <root>
  <target> --format s3|sts|azure` emits the per-database filter lists.
- **`external-full`**: whole-root replication that cannot be filtered.
  sleet writes nothing. The mirror task computes the newest manifest
  whose closure is complete (the **safe watermark**), reports it, and
  alarms on lag. Replicated manifests above the watermark may reference
  objects that have not arrived; readers must mount checkpoints at or
  below the watermark, and taking the target live requires removing
  the manifests above it (§10). No reconciliation, no retention.

## 9. Configuration and placement

A mirror target is registered like any database: a `dbs/<db>.toml` file
keyed by the percent-encoded target URL. `mirror` joins the service
list and the `[database]` table gains a `mirror` table, so fleet-wide
defaults for its fields live in `sleet.toml` under `[database.mirror]`
with the usual per-field precedence: built-in defaults ->
`[database]` -> `dbs/<db>.toml`. `source` cannot be set fleet-wide.

```toml
# dbs/<percent-encoded target url>.toml
services = ["mirror"]

[mirror]
source = "s3://prod/db1"        # required
mode = "continuous"             # continuous | periodic
copier = "builtin"              # builtin | rclone | external | external-full
poll = "10s"                    # continuous: pass and tail cadence
# interval = "24h"              # periodic: cadence between passes
# min_age = "300s"              # reconcile/prune deletion age floor
# checkpoint_lifetime = "15m"   # source pin checkpoint TTL
# copy_parallelism = 8          # builtin: concurrent object copies

# [mirror.serve]                # optional serving checkpoint (§5)
# refresh = "1m"
# lifetime = "1h"

# [mirror.retention]            # periodic mode (§7)
# keep = "30d"
```

`sleet mirror register <root> <source> <target>` writes the file.
Deleting it stops mirroring and leaves the target valid at its
watermark.

Validation at registration and load: `services` containing `mirror`
requires `mirror.source` and excludes every other service (§3);
`interval` requires `mode = "periodic"`; `retention` requires
`periodic`; the source manifest must have empty `external_dbs` and no
separate WAL object store; the target root must be empty or hold a
prior mirror of the same source.

Placement is unchanged: the pair `(target, mirror)` goes to the
top-ranked live node offering the service, heartbeat letter `m`.
`--max-mirror-jobs` caps concurrently syncing targets per node. Nodes
offering `mirror` must reach both the source and target stores;
placement does not consider reachability.

## 10. Taking a mirror live

A target is a valid database at every instant (§3), so going live
needs no conversion step: delete the registration to stop the mirror,
then open the target. The first writer bumps `writer_epoch` and
replays the copied WAL tail; a coordinator bumps `compactor_epoch` and
builds fresh compaction state. For migration, quiescing the source
writers and letting `status --mirrors` show zero lag first bounds the
switch to the final WAL delta; for DR failover the target is simply
opened at its watermark, and data loss is the mirror lag at failure.

Epochs fence within one root only. Nothing on the target side can
stop writers on the source, and a writer still running there keeps
diverging from the stopped mirror; sequencing writer shutdown around
the switch is outside sleet (§2).

An `external-full` target needs one manual step first: `Db::open`
reads the latest manifest, so replicated manifests above the safe
watermark (§8) must be deleted, with the external replication stopped,
before a writer opens it.

A `promote` command automating this is future work (§12.5).

## 11. Observability and verification

`sleet status --mirrors` reads each mirror's source and target heads
and reports lag as manifests behind, WAL ids behind, and estimated
seconds (source and target sequence numbers mapped through the sequence
tracker). For `external-full` it reports the safe watermark. Mirror
task state rides in the heartbeat body like other services.

Every commit already proves closure completeness by existence checks.
`sleet mirror verify <root> <target>` re-checks on demand: existence
and size for every kept manifest's closure, and with `--deep`, a
`DbReader` scan at a checkpoint that re-reads every block through its
checksum. Sizes rather than ETags: multipart ETags do not survive
cross-store copies.

## 12. Future work

### 12.1 Projected manifests

Byte-copy makes the target passive. A projected mode would decode the
source manifest and commit an equivalent one through the sequenced
protocol at the target, preserving target-local state: reader-managed
checkpoints, retention checkpoints, stock GC. The target becomes a
first-class database while data objects stay byte-copied. Needs a
public manifest write API upstream.

### 12.2 Tailing reads

A checkpoint-free reader mode upstream would let replicas tail the
target's manifest sequence instead of reopening at rotated
checkpoints. The mirror already maintains a live manifest sequence at
the target, so this slots in without protocol changes.

### 12.3 Logical mirroring

Ship only the WAL and run an independent compactor at the target. Cuts
cross-store transfer from ingest times write amplification to roughly
ingest, at the cost of a physically divergent target and a second
compaction protocol.

### 12.4 Excluded sources

Clone sources (copy the parent's referenced SSTs to matching relative
paths, or inline them) and databases with a separate WAL object store
(mirror both stores).

### 12.5 Promotion

`sleet mirror promote <root> <target>`: run a final pass while the
source is reachable, rewrite the registry file to drop `[mirror]` and
set normal services, delete `external-full` manifests above the safe
watermark, and report the manifest and WAL id the target ends at.
Until then, taking a mirror live is the manual sequence in §10.

## Open questions

1. Closure enumeration needs manifest decoding. Are 0.14.1's public
   `VersionedManifest` accessors enough, or does sleet decode the
   FlatBuffer and construct paths itself? The layout is frozen like a
   wire format, so a corpus test would pin it either way.
2. Racing mirror tasks can briefly create two pin checkpoints; both
   expire, so this is waste, not a hazard. A create-if-name-absent
   checkpoint API upstream would remove it.
3. Periodic scheduling keys off the target manifest's `LastModified`
   (object-store clock). Coarse but stateless; is an explicit schedule
   anchor worth new state?
4. The WAL tail poll is one GET when caught up, but a badly lagged
   continuous mirror probes many ids; at what lag should it switch to
   a LIST?
