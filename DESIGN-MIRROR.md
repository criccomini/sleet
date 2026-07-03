# sleet mirroring

The mirror service replicates a SlateDB database into another
object-store root: another bucket, region, or cloud. This document
extends `DESIGN.md`; placement, config layering, and the process model
apply unchanged unless stated otherwise.

## 1. Goals

1. **Disaster recovery**: a warm standby in another region or cloud,
   with bounded data loss (RPO ~ WAL lag), that fails over by being
   opened as an ordinary database (§3).
2. **Read replicas**: a near-realtime, always-consistent replica for
   serving reads in another region or cloud: snapshot reads by mounting
   mirrored checkpoints today (§5), tailing reads once a checkpoint-free
   reader mode lands upstream (§11.2).
3. **Backups**: point-in-time, incremental, verifiable snapshots with
   retention independent of the source (§7).
4. **Migration**: move a database between buckets, regions, or clouds
   with downtime bounded by the final WAL delta (§11.5).

## 2. Non-goals

- No data-plane involvement: replica readers open the target directly.
- No cross-root fencing. SlateDB's epochs fence within one root;
  nothing on the target side can stop a writer on the source. Stopping
  source writers at failover is outside sleet.
- No logical transformation: v1 copies bytes. Filtering, format
  changes, and re-compaction at the target are out (§11.3).
- No mirror chains: a destination can never itself be a registered
  database (§9), so mirroring a mirror is not expressible.
- The fleet root (`sleet.toml`, `dbs/`, `nodes/`) is not mirrored;
  mirroring is per database.
- Databases that are clones (`external_dbs` set) or that use a separate
  WAL object store cannot be mirror sources (§11.4).

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

Each registered database may name **mirror targets** (§9): destination
roots the mirror service copies to. A target is a URL, never a
registry entry. The mirror copies the source's immutable objects to
the same relative names under the target root and commits manifests as
the atomic step. Two invariants define a valid target:

1. **Completeness**: the target's latest manifest always has its full
   closure present. The closure is the manifest's own data objects (L0
   and sorted-run SSTs under `compacted/`, WAL SSTs above
   `replay_after_wal_id`) plus, for each live checkpoint in its list,
   the pinned manifest and that manifest's data objects. One level
   only, matching what GC preserves at the source: a pinned manifest's
   own checkpoint entries are history, may point at manifests GC has
   already deleted, and resolve nowhere (readers and GC follow only
   the latest manifest's list), so the source guarantees nothing
   deeper.
2. **Single writer**: the mirror task is the only process that writes
   manifests under the target root. External copiers (§8) write only
   `wal/` and `compacted/`.

Completeness makes the target a valid SlateDB database at every
instant: the latest manifest and every checkpoint-pinned manifest open.
Single-writer holds because any other manifest committer would fork the
target's history away from the source's.

Never copied: `compactions/` (job claims and epochs are root-local; a
coordinator later started against the target builds fresh state) and
`gc/*.boundary` (source-local state; a target has none until it goes
live and its own GC starts fresh).
Zero-byte WAL fence objects are ordinary `wal/` objects and copy like
any other.

## 4. The sync pass

Every mode runs the same pass:

1. **Watermark.** LIST `manifest/` at the target; the highest id `W` is
   the last committed state. No other state is stored anywhere.
2. **Read.** Read the source's latest manifest `L`. If `L = W` there is
   nothing to commit; continuous mode keeps tailing (step 7).
3. **Pin.** Create a source checkpoint at `L` named for the target
   (`sleet-mirror:<target-name>`), lifetime `checkpoint_lifetime`,
   refreshed at half-life while the pass runs.
4. **Copy.** Enumerate the closure of `L` (§3): `L`'s data objects and
   those of every manifest a live checkpoint in `L` pins. LIST `wal/`
   and `compacted/` at the target, diff, and copy the missing objects.
   An object exists at the target iff it is done: names are unique and
   content is immutable, so the check is exact and re-copies are
   harmless. No manifest commits until all of it is present.
5. **Commit.** PUT the closure's manifests in ascending id order, `L`
   last, each with create-if-absent.
6. **Unpin.** Delete the pin. Between passes nothing needs pinning: the
   diff base is the target itself, and checkpoint creation, refresh,
   and deletion are each a manifest CAS at the source, so a caught-up
   mirror holds no standing checkpoints and an idle database sees no
   manifest churn from being mirrored.
7. **Tail** (continuous mode, runs between passes). Copy WAL SSTs above
   `L`'s `next_wal_sst_id` in ascending id order as they appear at the
   source. WAL ids are dense, so the tail poll is one GET of the next
   expected id, nearly free when idle. SlateDB's replay discovers WALs
   by probing the store past the manifest's recorded state
   (`TableStore::last_seen_wal_id`), so a writer opening the target
   later replays the copied tail exactly like one recovering from a
   crash. Copying in id order means the target never has a WAL gap.

The pass syncs `W` directly to `L`; it cannot replay intermediate
manifests. Only `L` and checkpoint-pinned manifests are protected at
the source, so objects exclusive to unpinned intermediates may already
be deleted. Skipping is safe because manifest id gaps are already
normal.

Ascending commit order is critical. A checkpoint pins the manifest
that was latest at its creation, so an entry's `manifest_id` never
exceeds the id of the manifest carrying it, and ascending order lands
every referenced manifest before its referencer. Each transiently
latest manifest during a commit is therefore complete; the only
entries in it that can dangle are checkpoints that died before `L`,
and those resolve nowhere at the source either (readers reach
checkpoints only through the latest manifest's list, and can already
race a deletion between listing and opening).

## 5. Reading a mirror

Replica readers mount the target with `DbReader` and an explicit
checkpoint id. Opening without one is not allowed against a target:
`DbReader` would CAS its own checkpoint into the target manifest,
violating single-writer.

The checkpoints readers mount are created at the source and arrive in
every copied manifest. With a target's `serve` table set (§9), the
mirror rotates a **serving checkpoint** at the source: each `refresh`,
it creates a new checkpoint at the source's latest manifest with the
configured `lifetime`; old ones expire. Readers list checkpoints by
name at the target and mount the newest, reopening to advance. Read
freshness is the refresh cadence plus mirror lag. Nothing at the
target deletes what a mounted snapshot references (§6, §7), so a
reader holds it consistently for the serving checkpoint's lifetime.
Unlike the per-pass pin (§4),
serving checkpoints stand between passes; `refresh` sets the manifest
write cadence that costs the source.

## 6. Modes

- **`continuous`**: the pass plus the WAL tail, on a `poll` cadence
  with idle backoff. RPO is one poll plus tail copy time. Without
  `retention` (§7) nothing is deleted at the target: superseded
  objects accumulate until the target is opened live and its own GC
  reclaims them.
- **`periodic`**: one pass every `interval`, no WAL tail. Each committed
  manifest is a point-in-time cut. Scheduling is stateless: a pass runs
  when the target's latest manifest's `LastModified` is older than
  `interval`.
- One-shot: `sleet mirror sync <root> <db> <target>` runs a single
  pass regardless of mode.

Cost differs by mode. Continuous copies every compaction rewrite, so
cross-store transfer scales with ingest times one plus the compaction
write amplification. A periodic interval longer than the compaction
cycle copies only surviving SSTs. Either way, a caught-up mirror costs
one manifest read per poll and holds no checkpoints (§4), so
mostly-idle databases stay cheap under fleet-wide targets (§9).

## 7. Retention and backups

A target's committed manifests are its restore points. With a
target's `retention` table set (§9), the pruner, in either mode, keeps
the latest manifest, every manifest younger than `keep`, and any
manifest a live checkpoint pins, and deletes the rest: the manifests,
then data objects unreferenced by any kept manifest and older than
`min_age`. The WAL tail above the latest manifest is never pruned.
Pruning skips RFC-0026's boundary protocol: a stale task re-creating
a pruned manifest resurrects harmless litter, below latest and
unreachable, that the next prune deletes; target commits carry no
writer state to lose. Unset retention keeps everything. On a continuous
target, a short `keep` bounds growth at roughly the source's active
set plus one `keep` window of churn.

Pruning is the only deletion that may run against a target. SlateDB's
own GC cannot: it commits manifests (a CAS that strips expired
checkpoints), which violates single-writer (§3), forks the target's
history from the source's, and wedges the next pass, whose
create-if-absent finds the source's next id already taken. The pruner
deletes without writing manifests. Once a target goes live (§11.5) it
is an ordinary database and normal GC applies.

Restore points map to wall-clock time by the manifest's sequence
tracker, so `--at` accepts a manifest id or a timestamp.

`sleet mirror restore <root> <backup-url> <dest-url> --at <point>` is a
one-shot pass with the chosen manifest as `L`, copying its closure to
the destination and committing it. The destination must be empty;
restore refuses anything else and never deletes, so rolling back in
place means restoring to a fresh root and repointing clients. The
destination is then an ordinary database at that point.

## 8. Copiers

`copier` selects who moves data objects. sleet always commits
manifests; the copier moves only `wal/` and `compacted/`.

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
  `wal/` and `compacted/` and must not replicate delete markers: a
  propagated delete could remove an object a committed target manifest
  still references. None of these services support
  regex or glob filters, only anchored key prefixes: S3 rules are
  include-only (two rules per database, 1,000 rules per bucket
  configuration), GCS Storage Transfer has anchored
  `includePrefixes`/`excludePrefixes` (1,000 each), Azure
  `prefixMatch` is include-only. The caps also rule external copiers
  out as fleet-wide defaults (§9). `sleet mirror prefixes <root> <db>
  <target> --format s3|sts|azure` emits the per-database filter lists.

## 9. Configuration and placement

Mirroring is configured on the source: a database's `[mirror.targets]`
table names its destinations, and `mirror` joins `services` like any
other service. Targets are part of the shared `[database]` shape, so
fleet-wide targets live in `sleet.toml` and per-database files
override them per-field, matched by target name, with the usual
precedence: built-in defaults -> `[database]` -> `dbs/<db>.toml`.

`url` is the destination root. On its own it is an exact destination
for one database. Adding `source_prefix` turns the target into a
mapping: it applies to every database under the prefix and sends each
one to `url` plus its path with the prefix stripped, so the fleet
target below mirrors `s3://user-data/tenant1/db1` to
`s3://dr-bucket/mirrors/tenant1/db1`. Prefixes match at path-segment
boundaries (`s3://user-data` does not capture `s3://user-database/x`),
and stripping a fixed prefix cannot send two databases to the same
place. For precedence the two fields travel together: a layer that
sets either overrides both. A database no target applies to does not
mirror; that is how a fleet target is scoped to one bucket or prefix,
and `status` lists databases left uncovered (§10).

```toml
# sleet.toml: databases under s3://user-data mirror to the DR bucket
[database]
services = ["gc", "compactor-coordinator", "compaction-workers", "mirror"]

[database.mirror.targets.dr]
url = "s3://dr-bucket/mirrors"
source_prefix = "s3://user-data"
mode = "continuous"             # continuous | periodic
copier = "builtin"              # builtin | rclone | external
poll = "10s"                    # continuous: pass and tail cadence
# interval = "24h"              # periodic: cadence between passes
# min_age = "300s"              # prune deletion age floor
# checkpoint_lifetime = "15m"   # source pin checkpoint TTL
# copy_parallelism = 8          # builtin: concurrent object copies
```

```toml
# dbs/<percent-encoded source url>.toml: overrides for one database
[mirror.targets.dr]
disabled = true                 # opt out of the fleet-wide target

[mirror.targets.backup]         # add an explicit second target
url = "gs://backups/db1"
mode = "periodic"
interval = "24h"

[mirror.targets.backup.serve]   # optional serving checkpoint (§5)
refresh = "1m"
lifetime = "1h"

[mirror.targets.backup.retention]   # restore-point retention (§7)
keep = "30d"
```

Opting out is explicit, because per-field fall-through cannot unset
an inherited target: `disabled` is an ordinary overridable field. A
database mirrors iff its resolved services include `mirror` and at
least one enabled target applies; zero targets is a no-op, not an
error. Removing or disabling a target stops that mirror and leaves
the destination valid at its watermark. The target name is an
identity: it keys placement and names the source checkpoints (§4,
§5), so renaming one moves its placement and abandons its checkpoints
to expiry.

Placement extends the pair to a triple: each enabled `(database,
mirror, target)` goes to the top-ranked live node offering the
service (heartbeat letter `m`) under the frozen rendezvous hash.
`--max-mirror-jobs` caps concurrent `(database, target)` jobs per
node. Mirror nodes must reach both source and destination stores;
placement does not consider reachability.

## 10. Observability and verification

`sleet status --mirrors` reads source and destination heads per
`(database, target)` and reports lag as manifests behind, WAL ids
behind, and estimated seconds (source and target sequence numbers
mapped through the sequence tracker). It also flags destination
collisions and lists
databases with no applicable target (§9). Mirror task state rides in
the heartbeat body like other services.

Every commit already proves closure completeness by existence checks.
`sleet mirror verify <root> <db> <target>` re-checks on demand: existence
and size for every kept manifest's closure, and with `--deep`, a
`DbReader` scan at a checkpoint that re-reads every block through its
checksum. Sizes rather than ETags: multipart ETags do not survive
cross-store copies.

## 11. Future work

### 11.1 Projected manifests

Byte-copy makes the target passive. A projected mode would decode the
source manifest and commit an equivalent one through the sequenced
protocol at the target, preserving target-local state: reader-managed
checkpoints, retention checkpoints, stock GC. The target becomes a
first-class database while data objects stay byte-copied. Needs a
public manifest write API upstream.

### 11.2 Tailing reads

A checkpoint-free reader mode upstream would let replicas tail the
target's manifest sequence instead of reopening at rotated
checkpoints. The mirror already maintains a live manifest sequence at
the target, so this slots in without protocol changes.

### 11.3 Logical mirroring

Ship only the WAL and run an independent compactor at the target. Cuts
cross-store transfer from ingest times write amplification to roughly
ingest, at the cost of a physically divergent target and a second
compaction protocol.

### 11.4 Excluded sources

Clone sources (copy the parent's referenced SSTs to matching relative
paths, or inline them) and databases with a separate WAL object store
(mirror both stores).

### 11.5 Promotion

`sleet mirror promote <root> <db> <target>`: run a final pass while
the source is reachable, disable the target in the source's registry
file, and report the manifest and WAL id the destination ends at.
Until then, going live is manual:
disable the target to stop the mirror, then open the destination; the
first writer bumps `writer_epoch` and replays the copied WAL tail, and
a coordinator bumps `compactor_epoch` and builds fresh compaction
state. Epochs fence within one root only, so sequencing source writer
shutdown around the switch stays outside sleet (§2).

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
