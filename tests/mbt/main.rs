//! Model-based testing: fizzbee-mbt drives action sequences from the
//! model-checked coordination spec (specs/coordination.fizz) against
//! the real decision code, comparing each action's return value with
//! the model's.
//!
//! The pipeline: `fizz specs/coordination-mbt.fizz` writes the
//! explored state space to `specs/out/latest`; `fizzbee-mbt-server
//! --states_file specs/out/latest` serves it on localhost:50051; this
//! test hosts the adapter as a gRPC plugin and spawns
//! `fizzbee-mbt-runner` to execute sequences against it. `traits.rs`
//! and `test.rs` come from `fizz mbt-scaffold --lang rust`; the
//! adapter in `adapters.rs` maps model actions onto real sleet
//! primitives.
//!
//! MBT states come from `specs/coordination-mbt.fizz`, derived from
//! the verification spec (see `derive_mbt_spec`), because the MBT
//! server reads expected returns from node state, and returns in node
//! hashes blow up the depth-30 liveness check past any CI budget. The
//! derived spec drops the liveness property and shallows exploration;
//! the runner still reaches every fault-budget interleaving.
//!
//! The MBT run is gated on `SLEET_MBT`: unset skips (the tools aren't
//! everywhere); set, a missing server or runner is a failure, not a
//! skip. The drift test below always runs.

mod adapters;
mod test;
mod traits;

/// The mechanical transform from the verification spec to the MBT
/// generation spec: shallower exploration, no liveness (the Converged
/// block and the `liveness: strict` option), and a generated-file
/// banner.
fn derive_mbt_spec(spec: &str) -> String {
    let banner = "# GENERATED from coordination.fizz for MBT state generation; do not\n\
                  # edit. Regenerate with UPDATE_SPECS=1 cargo test --test mbt.\n";
    let marker = "# Convergence and no fence livelock";
    let body = spec
        .split(marker)
        .next()
        .expect("split never yields zero parts")
        .trim_end();
    let body = body.replace("max_actions: 30", "max_actions: 10");
    let body = body.replace("\nliveness: strict", "");
    format!("{banner}{body}\n")
}

/// specs/coordination-mbt.fizz must stay the mechanical derivation of
/// specs/coordination.fizz. `UPDATE_SPECS=1` regenerates it.
#[test]
fn mbt_spec_is_derived_from_the_verification_spec() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let spec = std::fs::read_to_string(root.join("specs/coordination.fizz")).unwrap();
    let derived = derive_mbt_spec(&spec);
    let path = root.join("specs/coordination-mbt.fizz");
    if std::env::var_os("UPDATE_SPECS").is_some() {
        std::fs::write(&path, &derived).unwrap();
        return;
    }
    let current = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        current, derived,
        "specs/coordination-mbt.fizz is stale; run UPDATE_SPECS=1 cargo test --test mbt"
    );
}
