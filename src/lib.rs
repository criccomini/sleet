//! `sleet` is a fleet manager for [SlateDB](https://slatedb.io)
//! databases: it runs their background services (garbage collection,
//! compaction coordination, and compaction execution) outside the
//! writer process. See DESIGN.md for the design.
//!
//! `daemon` is the long-running node loop: it heartbeats, polls the
//! registry (`registry`) under a fleet root (`root`), computes
//! placement (`placement`), and runs the per-database services
//! (`services`). `ops` implements the one-shot operator subcommands.
//! The wire formats are the fleet config (`config`), the heartbeat
//! body (`heartbeat`), and the subcommand responses (`response`,
//! rendered by `render`); each format module generates a JSON Schema
//! checked in under `schema/`.

#![warn(missing_docs)]

pub mod config;
pub mod daemon;
pub mod heartbeat;
pub mod mirror;
pub mod ops;
pub mod placement;
pub mod registry;
pub mod render;
pub mod response;
pub mod root;
pub mod services;
pub mod testing;

/// The slatedb version compiled into this binary (from Cargo.lock via
/// build.rs), carried in heartbeat bodies.
pub const SLATEDB_VERSION: &str = env!("SLATEDB_VERSION");

/// A type's JSON Schema, pretty-printed.
pub(crate) fn schema_pretty<T: schemars::JsonSchema>() -> String {
    let schema = schemars::schema_for!(T);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}
