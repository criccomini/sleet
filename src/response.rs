//! Response types for one-shot subcommands run with `--format json`.
//!
//! These structs are the source of truth for `schema/cli.schema.json`;
//! `tests/schema_sync.rs` regenerates it and fails if the two drift.
//! Text rendering lives in `crate::render`.

use std::time::Duration;

use schemars::JsonSchema;
use serde::Serialize;

use crate::spec::{HumanDuration, Service};

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
    #[serde(skip_serializing_if = "Vec::is_empty")]
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

    /// The sleet version the node runs, from the heartbeat body.
    pub sleet_version: String,

    /// The slatedb version the node runs, from the heartbeat body.
    pub slatedb_version: String,
}

/// One registered database and its service placement.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct DatabaseStatus {
    pub url: String,
    pub services: Vec<ServicePlacement>,
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

impl StatusResponse {
    /// Placeholder until status is derived from object storage.
    pub fn stub() -> Self {
        let node = |node_id: &str, live, age, services: &[Service]| NodeStatus {
            node_id: node_id.into(),
            live,
            heartbeat_age: Duration::from_secs(age).into(),
            services: services.to_vec(),
            sleet_version: "0.1.0".into(),
            slatedb_version: "0.9.0".into(),
        };
        let all = [
            Service::Gc,
            Service::CompactorCoordinator,
            Service::CompactionWorkers,
        ];
        let place = |service, nodes: &[&str]| ServicePlacement {
            service,
            nodes: nodes.iter().map(|n| n.to_string()).collect(),
        };
        Self {
            nodes: vec![
                node("sleet-1", true, 2, &all),
                node("sleet-2", true, 4, &[Service::CompactionWorkers]),
                node("sleet-3", false, 72, &all),
            ],
            databases: vec![
                DatabaseStatus {
                    url: "s3://prod-us/db1".into(),
                    services: vec![
                        place(Service::Gc, &["sleet-1"]),
                        place(Service::CompactorCoordinator, &["sleet-1"]),
                        place(Service::CompactionWorkers, &["sleet-2", "sleet-1"]),
                    ],
                },
                DatabaseStatus {
                    url: "gs://analytics/events".into(),
                    services: vec![place(Service::Gc, &["sleet-1"])],
                },
            ],
            warnings: vec![],
        }
    }
}
