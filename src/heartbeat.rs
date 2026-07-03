//! The heartbeat: one object per node under `nodes/` at the fleet root.
//!
//! Each node PUTs `nodes/<node_id>.<services>.json` every
//! `heartbeat_interval`. Placement never reads a body: it gets the node
//! id and offered services from the name, and liveness from
//! `LastModified`. `<services>` is the offered services' letters
//! (`c` = compactor-coordinator, `g` = gc, `m` = mirror,
//! `w` = compaction-workers) sorted ascending, e.g.
//! `sleet-1.cgmw.json`. Node ids must not contain `.`.
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

/// Check a node id for use in heartbeat object names: nonempty, at most
/// 128 chars of `[A-Za-z0-9_-]`. In particular no `.` (the name
/// separator) and no `/`.
pub fn validate_node_id(node_id: &str) -> Result<String, String> {
    let ok = !node_id.is_empty()
        && node_id.len() <= 128
        && node_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'));
    if ok {
        Ok(node_id.to_string())
    } else {
        Err("node ids are 1-128 chars of [A-Za-z0-9_-]".to_string())
    }
}

/// The name of a node's heartbeat object under `nodes/`.
pub fn object_name(node_id: &str, services: &[Service]) -> String {
    let mut letters: Vec<char> = services.iter().map(|s| s.letter()).collect();
    letters.sort_unstable();
    letters.dedup();
    let letters: String = letters.into_iter().collect();
    format!("{node_id}.{letters}.json")
}

/// The node id and offered services a heartbeat object name encodes, if
/// valid, with services in canonical order. Unknown service letters are
/// ignored so newer nodes offering new services still parse.
pub fn parse_object_name(name: &str) -> Option<(String, Vec<Service>)> {
    let stem = name.strip_suffix(".json")?;
    let (node_id, letters) = stem.rsplit_once('.')?;
    if node_id.is_empty() {
        return None;
    }
    let mut services: Vec<Service> = letters.chars().filter_map(Service::from_letter).collect();
    services.sort_unstable();
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
    /// A current-version heartbeat body for this node, stamping in the
    /// running sleet version.
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

/// Aggregate task states for one offered service. Only counts are
/// reported, because a node may own tasks for many thousands of
/// databases.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ServiceSummary {
    /// The offered service these counts cover.
    pub service: Service,

    /// Owned tasks currently running.
    pub running: u64,

    /// Owned tasks that crashed and are waiting out a restart delay.
    pub backoff: u64,
}

impl ServiceSummary {
    /// A zero-count summary for a service with no owned tasks.
    pub fn empty(service: Service) -> Self {
        Self {
            service,
            running: 0,
            backoff: 0,
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
        let (id, services) = parse_object_name("sleet-1.cgmw.json").unwrap();
        assert_eq!(id, "sleet-1");
        assert_eq!(services, Service::ALL.to_vec());
        let (_, services) = parse_object_name("sleet-1.cgw.json").unwrap();
        assert_eq!(
            services,
            vec![
                Service::Gc,
                Service::CompactorCoordinator,
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

    #[test]
    fn node_ids_are_validated() {
        for ok in ["n", "sleet-1", "a_b-c", "A9", &"x".repeat(128)] {
            assert!(validate_node_id(ok).is_ok(), "{ok:?}");
        }
        for bad in ["", "a.b", "a/b", "a b", "a:b", "ü", &"x".repeat(129)] {
            assert!(validate_node_id(bad).is_err(), "{bad:?}");
        }
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
