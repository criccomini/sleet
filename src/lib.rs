//! `sleet` is a fleet manager for [SlateDB](https://slatedb.io)
//! databases: it runs their background services — garbage collection,
//! compaction coordination, and compaction execution — outside the
//! writer process. See DESIGN.md for the design.
//!
//! This crate currently defines the formats and CLI surface: the fleet
//! config (`config`), the heartbeat wire format (`heartbeat`), and the
//! subcommand responses (`response`, rendered by `render`). Each format
//! module generates a JSON Schema checked in under `schema/`.

pub mod config;
pub mod heartbeat;
pub mod placement;
pub mod registry;
pub mod render;
pub mod response;
pub mod root;

/// The slatedb version compiled into this binary (from Cargo.lock via
/// build.rs), carried in heartbeat bodies.
pub const SLATEDB_VERSION: &str = env!("SLATEDB_VERSION");

/// A type's JSON Schema, pretty-printed.
pub(crate) fn schema_pretty<T: schemars::JsonSchema>() -> String {
    let schema = schemars::schema_for!(T);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}
