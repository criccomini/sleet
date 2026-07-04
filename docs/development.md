# Development

This page is for contributors changing Sleet code, schemas, or specs.

Related pages: [Architecture](architecture.md), [Configuration](configuration.md), [Mirroring](mirroring.md), [CLI reference](cli.md).

## Project layout

| Path | Purpose |
| --- | --- |
| [src/main.rs](../src/main.rs) | CLI parsing and command dispatch. |
| [src/ops.rs](../src/ops.rs) | One-shot command implementations and status derivation. |
| [src/render.rs](../src/render.rs) | Text and JSON rendering for CLI responses. |
| [src/daemon.rs](../src/daemon.rs) | Long-running node loop, supervision, and placement reconciliation. |
| [src/config.rs](../src/config.rs) | Config types, defaults, layering, validation, and schema generation. |
| [src/root.rs](../src/root.rs) | Fleet-root reads, registry polling, and heartbeat listing. |
| [src/registry.rs](../src/registry.rs) | URL canonicalization and registry file names. |
| [src/heartbeat.rs](../src/heartbeat.rs) | Heartbeat object naming, body structs, and schema generation. |
| [src/placement.rs](../src/placement.rs) | Frozen rendezvous hash and owner selection. |
| [src/services.rs](../src/services.rs) | SlateDB GC, coordinator, and worker wrappers. |
| [src/mirror/](../src/mirror) | Mirror sync, copy, prune, verify, restore, and layout logic. |
| [src/response.rs](../src/response.rs) | CLI response structs and schema generation. |
| [src/testing.rs](../src/testing.rs) | In-memory test fixtures shared by integration tests and benches. |
| [tests/](../tests) | Unit, integration, CLI snapshot, schema, system, chaos, and property tests. |
| [specs/](../specs) | FizzBee specs and model-based testing inputs. |
| [benches/](../benches) | Criterion benches for placement and registry-poll scaling. |

## Common checks

Run the full Rust suite:

```sh
cargo test
```

Run formatting and lint checks:

```sh
cargo fmt
cargo clippy --all-targets
```

Run the scaling benches:

```sh
cargo bench
```

The MinIO-backed S3 test uses `SLEET_S3_ENDPOINT`. It skips when the variable is unset. CI provides MinIO as a service container.

## Specs and model-based tests

Model-check the coordination spec:

```sh
fizz --experimental_no_state_returns specs/coordination.fizz
```

Model-check the mirror spec in the two exhaustive configurations:

```sh
fizz --experimental_processed_queue specs/mirror.fizz
fizz --experimental_processed_queue --preinit-hook-file specs/mirror-expiry.cfg specs/mirror.fizz
```

The full mirror budget product does not fit in the exhaustive run. Spot-check it with simulation:

```sh
fizz -x --max_runs 1 --seed 1 --preinit-hook-file specs/mirror-sim.cfg specs/mirror.fizz
```

Run model-based tests against the real placement code:

```sh
fizz specs/coordination-mbt.fizz
fizzbee-mbt-server --states_file specs/out/latest &
SLEET_MBT=1 cargo test --test mbt
```

After editing `specs/coordination.fizz`, regenerate the derived MBT spec:

```sh
UPDATE_SPECS=1 cargo test --test mbt
```

## Generated schemas

Sleet checks generated schemas into `schema/`:

- [schema/config.schema.json](../schema/config.schema.json)
- [schema/heartbeat.schema.json](../schema/heartbeat.schema.json)
- [schema/cli.schema.json](../schema/cli.schema.json)

The schema sync tests fail when Rust types and checked-in schemas drift. If you change config, heartbeat, or response structs, update the schema output in the same change.

```sh
UPDATE_SCHEMAS=1 cargo test --test schema_sync
```

## Placement compatibility

The rendezvous hash in [src/placement.rs](../src/placement.rs) is a wire-format decision. Mixed-version fleets must compute the same owners. Do not change the hash, key encoding, or tie break without a migration plan.

The golden tests in that file pin representative scores.

## Config compatibility

Config parsing uses `deny_unknown_fields`. Adding fields is usually safe. Renaming or removing fields breaks existing fleet roots and needs a migration story.

Heartbeat readers ignore unknown fields, so new heartbeat fields are compatible. Increment the heartbeat `version` only for incompatible changes.

## Mirror invariants

Mirror code relies on these rules:

- Sleet is the only writer of target manifests.
- Data objects under `wal/` and `compacted/` are immutable.
- A committed target manifest must have its full closure present.
- Restore writes only to an empty destination.
- Verification checks object existence and size, not ETags.

Read [DESIGN-MIRROR.md](../DESIGN-MIRROR.md) before changing mirror sync, pruning, restore, or external copier behavior.

## CLI snapshots

CLI behavior is covered by trycmd files under [tests/cmd/](../tests/cmd). When command help or text output changes intentionally, update the matching snapshots with the code change.

```sh
TRYCMD=overwrite cargo test --test cli
```

JSON response shape should be validated through [schema/cli.schema.json](../schema/cli.schema.json), not by downstream text parsing.

## Wire-format corpus

The corpus under [tests/corpus/](../tests/corpus) pins config, heartbeat, placement score, and registry-name formats across releases. Cut a new corpus directory at release time:

```sh
UPDATE_CORPUS=1 cargo test --test corpus
```
