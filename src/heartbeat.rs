//! The heartbeat: one object per node under `nodes/` at the fleet root.
//!
//! Each node PUTs `nodes/<node_id>.<services>.json` every
//! `heartbeat_interval`. Everything placement needs is in the listing:
//! the name carries the node id and its offered services, and
//! `LastModified` carries liveness. `<services>` is the offered
//! services' letters (`c` = compactor-coordinator, `g` = gc, `w` =
//! compaction-workers) sorted ascending — e.g. `sleet-1.cgw.json`. Node
//! ids must not contain `.`.
//!
//! The body is observability-only, read by `sleet status` and never
//! fetched for placement. Compatibility: readers ignore unknown fields
//! (mixed-version fleets must coexist), so fields may be added freely;
//! `version` increments only on incompatible change. The JSON Schema at
//! `schema/heartbeat.schema.json` is generated from these structs by
//! `tests/schema_sync.rs`, which fails if the two drift.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::Service;

/// Current heartbeat format version.
pub const VERSION: u32 = 1;

/// The name of a node's heartbeat object under `nodes/`.
pub fn object_name(node_id: &str, services: &[Service]) -> String {
    let mut letters: Vec<char> = services.iter().map(|s| s.letter()).collect();
    letters.sort_unstable();
    letters.dedup();
    let letters: String = letters.into_iter().collect();
    format!("{node_id}.{letters}.json")
}

/// The node id and offered services a heartbeat object name encodes, if
/// valid. Unknown service letters are ignored so newer nodes offering
/// new services still parse.
pub fn parse_object_name(name: &str) -> Option<(String, Vec<Service>)> {
    let stem = name.strip_suffix(".json")?;
    let (node_id, letters) = stem.rsplit_once('.')?;
    if node_id.is_empty() {
        return None;
    }
    let services = letters.chars().filter_map(Service::from_letter).collect();
    Some((node_id.to_string(), services))
}

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

    /// The slatedb version the node runs.
    pub slatedb_version: String,

    /// Aggregate task states per offered service, across every database
    /// the node owns.
    pub services: Vec<ServiceSummary>,
}

impl Heartbeat {
    pub fn new(
        node_id: impl Into<String>,
        slatedb_version: impl Into<String>,
        services: Vec<ServiceSummary>,
    ) -> Self {
        Self {
            version: VERSION,
            node_id: node_id.into(),
            sleet_version: env!("CARGO_PKG_VERSION").into(),
            slatedb_version: slatedb_version.into(),
            services,
        }
    }
}

/// Aggregate task states for one offered service. Counts, not
/// per-database detail: a node may own tasks for many thousands of
/// databases.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ServiceSummary {
    pub service: Service,

    /// Owned tasks currently running.
    pub running: u64,

    /// Owned tasks that crashed and are waiting out a restart delay.
    pub backoff: u64,
}

/// The heartbeat JSON Schema, pretty-printed.
pub fn schema_json() -> String {
    crate::schema_pretty::<Heartbeat>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_name_sorts_and_dedups_service_letters() {
        let name = object_name(
            "sleet-1",
            &[
                Service::CompactionWorkers,
                Service::Gc,
                Service::CompactorCoordinator,
                Service::Gc,
            ],
        );
        assert_eq!(name, "sleet-1.cgw.json");
        assert_eq!(object_name("n", &[Service::Gc]), "n.g.json");
    }

    #[test]
    fn object_names_roundtrip_and_reject_garbage() {
        let (id, services) = parse_object_name("sleet-1.cgw.json").unwrap();
        assert_eq!(id, "sleet-1");
        assert_eq!(
            services,
            vec![
                Service::CompactorCoordinator,
                Service::Gc,
                Service::CompactionWorkers
            ]
        );
        // Unknown letters are ignored; the node still parses.
        let (id, services) = parse_object_name("sleet-2.gx.json").unwrap();
        assert_eq!(id, "sleet-2");
        assert_eq!(services, vec![Service::Gc]);
        assert_eq!(parse_object_name("no-extension.cgw"), None);
        assert_eq!(parse_object_name("nodot.json"), None);
        assert_eq!(parse_object_name(".g.json"), None);
    }

    #[test]
    fn roundtrips() {
        let hb = Heartbeat::new(
            "sleet-1",
            "0.9.0",
            vec![ServiceSummary {
                service: Service::Gc,
                running: 12,
                backoff: 1,
            }],
        );
        let json = serde_json::to_string(&hb).unwrap();
        let back: Heartbeat = serde_json::from_str(&json).unwrap();
        assert_eq!(back.version, VERSION);
        assert_eq!(back.node_id, "sleet-1");
        assert_eq!(back.slatedb_version, "0.9.0");
        assert_eq!(back.services[0].running, 12);
    }

    /// Readers must tolerate fields added by newer writers.
    #[test]
    fn ignores_unknown_fields() {
        let json = r#"{
            "version": 1,
            "node_id": "sleet-1",
            "sleet_version": "9.9.9",
            "slatedb_version": "9.9.9",
            "services": [],
            "some_future_field": {"x": 1}
        }"#;
        let hb: Heartbeat = serde_json::from_str(json).unwrap();
        assert_eq!(hb.node_id, "sleet-1");
    }
}
