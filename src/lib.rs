//! `sleet` is a fleet manager for [SlateDB](https://slatedb.io)
//! databases: it runs their background services (garbage collection,
//! compaction coordination, and compaction execution) outside the
//! writer process. See rfcs/ for the design.
//!
//! Use [`Fleet`] for the supported programmatic API. It opens object
//! stores from URLs using credentials and provider settings in the
//! process environment. The caller owns the Tokio runtime, tracing
//! subscriber, and cancellation signal.

#![warn(missing_docs)]

mod api;

#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod daemon;
#[doc(hidden)]
pub mod heartbeat;
#[doc(hidden)]
pub mod mirror;
#[doc(hidden)]
pub mod ops;
#[doc(hidden)]
pub mod placement;
#[doc(hidden)]
pub mod registry;
#[doc(hidden)]
pub mod render;
#[doc(hidden)]
pub mod response;
#[doc(hidden)]
pub mod root;
#[doc(hidden)]
pub mod services;
#[doc(hidden)]
pub mod testing;

mod store;

pub use api::{Error, Fleet, MirrorSyncOptions, StatusOptions, mirror_restore};
pub use config::{HumanDuration, Service};
pub use daemon::NodeOptions;
pub use mirror::RestorePoint;
pub use response::{
    DatabaseStatus, MirrorRestoreResponse, MirrorStatus, MirrorSyncResponse, NodeStatus,
    QueueStatus, RegisterResponse, ServicePlacement, StatusResponse,
};
pub use tokio_util::sync::CancellationToken;

/// The slatedb version compiled into this binary (from Cargo.lock via
/// build.rs), carried in heartbeat bodies.
pub const SLATEDB_VERSION: &str = env!("SLATEDB_VERSION");

/// A type's JSON Schema, pretty-printed.
pub(crate) fn schema_pretty<T: schemars::JsonSchema>() -> String {
    let schema = schemars::schema_for!(T);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}
