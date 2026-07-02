//! The heartbeat object: the body a node PUTs at
//! `<heartbeats>/<node_id>` every `heartbeat_interval` — sleet's only
//! cross-node wire format.
//!
//! Liveness comes from the object's `LastModified`, never from the
//! body. The body carries the node's current assignments and service
//! states so `sleet status` observes the fleet from object storage
//! alone.
//!
//! Compatibility: readers ignore unknown fields (mixed-version fleets
//! must coexist), so fields may be added freely; `version` increments
//! only on incompatible change. The JSON Schema at
//! `schema/heartbeat.schema.json` is generated from these structs
//! (`sleet schema heartbeat`); `tests/schema_sync.rs` fails if the two
//! drift.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::spec::Service;

/// Current heartbeat format version.
pub const VERSION: u32 = 1;

/// The body of a heartbeat object.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(title = "sleet heartbeat")]
pub struct Heartbeat {
    /// Heartbeat format version; bumped only on incompatible change.
    pub version: u32,

    /// The node that wrote this heartbeat. Duplicates the object key so
    /// bodies are self-describing.
    pub node_id: String,

    /// The sleet version the node runs.
    pub sleet_version: String,

    /// The node's current service assignments and their states.
    pub assignments: Vec<Assignment>,
}

impl Heartbeat {
    pub fn new(node_id: impl Into<String>, assignments: Vec<Assignment>) -> Self {
        Self {
            version: VERSION,
            node_id: node_id.into(),
            sleet_version: env!("CARGO_PKG_VERSION").into(),
            assignments,
        }
    }
}

/// One `(database, service)` assignment held by a node.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Assignment {
    /// Object-store URL of the database root.
    pub database: String,
    pub service: Service,
    pub state: ServiceState,
}

/// Supervised task state for one service.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ServiceState {
    /// The task is running.
    Running,
    /// The task crashed and is waiting out its restart delay.
    Backoff,
    /// The task was shut down (unassigned or removed from the spec).
    Stopped,
}

impl ServiceState {
    pub fn as_str(self) -> &'static str {
        match self {
            ServiceState::Running => "running",
            ServiceState::Backoff => "backoff",
            ServiceState::Stopped => "stopped",
        }
    }
}

/// The heartbeat JSON Schema, pretty-printed.
pub fn schema_json() -> String {
    crate::schema_pretty::<Heartbeat>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        let hb = Heartbeat::new(
            "sleet-1",
            vec![Assignment {
                database: "s3://bucket/db".into(),
                service: Service::Gc,
                state: ServiceState::Running,
            }],
        );
        let json = serde_json::to_string(&hb).unwrap();
        let back: Heartbeat = serde_json::from_str(&json).unwrap();
        assert_eq!(back.version, VERSION);
        assert_eq!(back.node_id, "sleet-1");
        assert_eq!(back.assignments[0].state, ServiceState::Running);
    }

    /// Readers must tolerate fields added by newer writers.
    #[test]
    fn ignores_unknown_fields() {
        let json = r#"{
            "version": 1,
            "node_id": "sleet-1",
            "sleet_version": "9.9.9",
            "assignments": [],
            "some_future_field": {"x": 1}
        }"#;
        let hb: Heartbeat = serde_json::from_str(json).unwrap();
        assert_eq!(hb.node_id, "sleet-1");
    }
}
