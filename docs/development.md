# Development

This page is for contributors changing Sleet code, schemas, or specs.

Related pages: [Architecture](architecture.md), [Configuration](configuration.md), [Mirroring](mirroring.md), [CLI reference](cli.md).

## Project layout

| Path | Purpose |
| --- | --- |
| [src/main.rs](../src/main.rs) | CLI parsing and command dispatch. |
| [src/daemon.rs](../src/daemon.rs) | Long-running node loop, supervision, and placement reconciliation. |
| [src/config.rs](../src/config.rs) | Config types, defaults, layering, validation, and schema generation. |
| [src/root.rs](../src/root.rs) | Fleet-root reads, registry polling, and heartbeat listing. |
| [src/registry.rs](../src/registry.rs) | URL canonicalization and registry file names. |
| [src/placement.rs](../src/placement.rs) | Frozen rendezvous hash and owner selection. |
| [src/services.rs](../src/services.rs) | SlateDB GC, coordinator, and worker wrappers. |
| [src/mirror/](../src/mirror) | Mirror sync, copy, prune, verify, restore, and layout logic. |
| [src/response.rs](../src/response.rs) | CLI response structs and schema generation. |
| [tests/](../tests) | Unit, integration, CLI snapshot, schema, system, chaos, and property tests. |
| [specs/](../specs) | FizzBee specs and model-based testing inputs. |

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

Model-check the coordination spec:

```sh
fizz --experimental_no_state_returns specs/coordination.fizz
```

The MinIO-backed S3 test uses `SLEET_S3_ENDPOINT`. It skips when the variable is unset. CI provides MinIO as a service container.

## Generated schemas

Sleet checks generated schemas into `schema/`:

- [schema/config.schema.json](../schema/config.schema.json)
- [schema/heartbeat.schema.json](../schema/heartbeat.schema.json)
- [schema/cli.schema.json](../schema/cli.schema.json)

The schema sync tests fail when Rust types and checked-in schemas drift. If you change config, heartbeat, or response structs, update the schema output in the same change.

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

JSON response shape should be validated through [schema/cli.schema.json](../schema/cli.schema.json), not by downstream text parsing.

