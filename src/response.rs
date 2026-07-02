//! Response types for one-shot subcommands run with `--format json`.
//!
//! These structs are the source of truth for `schema/cli.schema.json`;
//! `tests/schema_sync.rs` regenerates it and fails if the two drift.
//! Text rendering lives in `crate::render`.

use std::time::Duration;

use schemars::JsonSchema;
use serde::Serialize;

use crate::heartbeat::ServiceState;
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
}

/// The `sleet status` response, derived from object storage: node
/// liveness from heartbeat ages, assignments and service states from
/// heartbeat contents.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet status response")]
pub struct StatusResponse {
    /// Every fleet member with a heartbeat object.
    pub nodes: Vec<NodeStatus>,
    /// Managed databases and their service assignments.
    pub databases: Vec<DatabaseStatus>,
}

/// One fleet member.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct NodeStatus {
    pub node_id: String,
    /// Whether the heartbeat is younger than `node_timeout`.
    pub live: bool,
    /// Age of the heartbeat object.
    pub heartbeat_age: HumanDuration,
}

/// One managed database and its service assignments.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct DatabaseStatus {
    pub url: String,
    pub services: Vec<ServiceStatus>,
}

/// One `(database, service)` assignment.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ServiceStatus {
    pub service: Service,
    /// Node the service is rendezvous-hashed to.
    pub node_id: String,
    pub state: ServiceState,
}

impl StatusResponse {
    /// Placeholder until status is derived from object storage.
    pub fn stub() -> Self {
        let assign = |service, node_id: &str, state| ServiceStatus {
            service,
            node_id: node_id.into(),
            state,
        };
        Self {
            nodes: vec![
                NodeStatus {
                    node_id: "sleet-1".into(),
                    live: true,
                    heartbeat_age: Duration::from_secs(2).into(),
                },
                NodeStatus {
                    node_id: "sleet-2".into(),
                    live: true,
                    heartbeat_age: Duration::from_secs(4).into(),
                },
                NodeStatus {
                    node_id: "sleet-3".into(),
                    live: false,
                    heartbeat_age: Duration::from_secs(72).into(),
                },
            ],
            databases: vec![
                DatabaseStatus {
                    url: "s3://prod-us/db1".into(),
                    services: vec![
                        assign(Service::Gc, "sleet-1", ServiceState::Running),
                        assign(Service::Compactor, "sleet-2", ServiceState::Running),
                        assign(Service::Workers, "sleet-1", ServiceState::Running),
                    ],
                },
                DatabaseStatus {
                    url: "gs://analytics/events".into(),
                    services: vec![assign(Service::Gc, "sleet-2", ServiceState::Backoff)],
                },
            ],
        }
    }
}
