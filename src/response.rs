//! Response types for one-shot subcommands run with `--format json`.
//!
//! These structs are the source of truth for `schema/cli.schema.json`;
//! `tests/schema_sync.rs` regenerates it and fails if the two drift.
//! Text rendering lives in `crate::render`.

use schemars::JsonSchema;
use serde::Serialize;

use crate::config::{HumanDuration, Service};

/// The subcommand response JSON Schema, pretty-printed.
pub fn schema_json() -> String {
    crate::schema_pretty::<Response>()
}

/// A response from any subcommand run with `--format json`, one variant
/// per command. Exists to generate the single response schema: each
/// command's response is a named definition under `$defs`, so consumers
/// validate against e.g. `#/$defs/StatusResponse`.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(untagged)]
#[schemars(title = "sleet response")]
pub enum Response {
    Status(StatusResponse),
    Register(RegisterResponse),
}

/// The `sleet status` response, derived from the fleet root: node
/// liveness, roles, and versions from `nodes/`, registered databases
/// from `dbs/`, and placement by computing the same rendezvous ranking
/// the nodes do.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet status response")]
pub struct StatusResponse {
    /// Every fleet member with a heartbeat object.
    pub nodes: Vec<NodeStatus>,

    /// Registered databases and their service placement.
    pub databases: Vec<DatabaseStatus>,

    /// Fleet-level problems: registry entries that alias the same
    /// database, services no live node offers, and the like.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// One fleet member.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct NodeStatus {
    pub node_id: String,

    /// Whether the heartbeat is younger than `heartbeat_timeout`.
    pub live: bool,

    /// Age of the heartbeat object.
    pub heartbeat_age: HumanDuration,

    /// Services the node offers, from its heartbeat object name.
    pub services: Vec<Service>,

    /// The sleet version the node runs, from the heartbeat body; absent
    /// if the body was unreadable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleet_version: Option<String>,

    /// The slatedb version the node runs, from the heartbeat body;
    /// absent if the body was unreadable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slatedb_version: Option<String>,
}

/// One registered database and its service placement.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct DatabaseStatus {
    pub url: String,
    pub services: Vec<ServicePlacement>,

    /// Compaction queue depth from `.compactions`; present only with
    /// `sleet status --queues`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<QueueStatus>,
}

/// Compaction queue depth for one database.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
pub struct QueueStatus {
    /// Jobs waiting for a worker.
    pub claimable: u64,
    /// Jobs a worker is executing.
    pub running: u64,
}

/// Where one database service runs: the top of the service's rendezvous
/// ranking — one node for `gc` and `compactor-coordinator`, the top
/// `count` nodes for `compaction-workers`. Empty means no live node
/// offers the service.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ServicePlacement {
    pub service: Service,
    pub nodes: Vec<String>,
}

/// The `sleet register` response.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet register response")]
pub struct RegisterResponse {
    /// The canonicalized database URL.
    pub url: String,

    /// The registry object written, relative to the fleet root.
    pub file: String,

    /// False if the database was already registered.
    pub created: bool,
}
