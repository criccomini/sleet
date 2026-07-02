//! Response types for one-shot subcommands run with `--format json`.
//!
//! These structs are the source of truth for the response schemas under
//! `schema/` (`sleet schema <kind>`); `tests/schema_sync.rs` fails if
//! the two drift. Text rendering lives in `crate::render`.

use std::path::Path;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Serialize;

use crate::heartbeat::ServiceState;
use crate::spec::{FleetSpec, HumanDuration, LoadError, Service};

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
    Validate(ValidateResponse),
    Status(StatusResponse),
}

/// The `sleet validate` response.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet validate response")]
pub struct ValidateResponse {
    /// Path of the spec that was checked.
    pub spec: String,
    /// Whether the spec parsed and validated.
    pub valid: bool,
    /// Problems found; empty when valid.
    pub errors: Vec<String>,
}

impl ValidateResponse {
    pub fn new(spec: &Path, result: &Result<FleetSpec, LoadError>) -> Self {
        let errors = match result {
            Ok(_) => Vec::new(),
            Err(LoadError::Invalid(e)) => e.0.clone(),
            Err(e) => vec![e.to_string()],
        };
        Self {
            spec: spec.display().to_string(),
            valid: errors.is_empty(),
            errors,
        }
    }
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
