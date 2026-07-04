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
    /// `sleet status`.
    Status(StatusResponse),
    /// `sleet register`.
    Register(RegisterResponse),
    /// `sleet mirror sync`.
    MirrorSync(MirrorSyncResponse),
    /// `sleet mirror verify`.
    MirrorVerify(MirrorVerifyResponse),
    /// `sleet mirror restore`.
    MirrorRestore(MirrorRestoreResponse),
    /// `sleet mirror drill`.
    MirrorDrill(MirrorDrillResponse),
    /// `sleet mirror prefixes`.
    MirrorPrefixes(MirrorPrefixesResponse),
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

    /// Per-target mirror lag; present only with `sleet status
    /// --mirrors`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mirrors: Vec<MirrorStatus>,

    /// Fleet-level problems: registry entries that alias the same
    /// database, services no live node offers, and the like.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// One `(database, target)` mirror's lag, from the source and
/// destination heads.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct MirrorStatus {
    /// The source database's canonical URL.
    pub database: String,

    /// The target's name.
    pub target: String,

    /// The destination root the target maps this database to.
    pub destination: String,

    /// The source's latest manifest id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_manifest_id: Option<u64>,

    /// The destination's latest manifest id (the watermark).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_manifest_id: Option<u64>,

    /// Manifests the destination is behind.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifests_behind: Option<u64>,

    /// WAL ids the destination is behind.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_behind: Option<u64>,

    /// Estimated seconds of lag: source and target sequence numbers
    /// mapped through the source's sequence tracker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seconds_behind: Option<u64>,

    /// Age of the newest verify record for this `(database, target)`
    /// under `verify/` at the fleet root; absent when no periodic
    /// verification has recorded an outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_age: Option<HumanDuration>,

    /// Whether the recorded verification passed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify_ok: Option<bool>,

    /// Problems the recorded verification found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify_problems: Option<u64>,

    /// Why lag could not be read, if it could not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One fleet member.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct NodeStatus {
    /// The node's id, from its heartbeat object name.
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
    /// The database's canonical URL.
    pub url: String,

    /// Placement of each configured service.
    pub services: Vec<ServicePlacement>,

    /// Compaction queue depth from `.compactions`; present only with
    /// `sleet status --compactions`.
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
/// ranking. One node for `gc` and `compactor-coordinator`, the top
/// `count` nodes for `compaction-workers`. Empty means no live node
/// offers the service.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ServicePlacement {
    /// The placed service.
    pub service: Service,

    /// The owning nodes, best-ranked first.
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

/// The `sleet mirror sync` response: one pass, plus the prune that
/// follows it when retention is set.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet mirror sync response")]
pub struct MirrorSyncResponse {
    /// The source database's canonical URL.
    pub database: String,

    /// The target's name.
    pub target: String,

    /// The destination root.
    pub destination: String,

    /// The manifest id the destination head ended at.
    pub head: u64,

    /// False when the destination was already at the source's head.
    pub committed: bool,

    /// Manifests written to the destination.
    pub manifests_committed: u64,

    /// Data objects copied.
    pub objects_copied: u64,

    /// Data bytes copied; zero for the rclone copier.
    pub bytes_copied: u64,

    /// Manifests the prune deleted; zero without retention.
    pub pruned_manifests: u64,

    /// Data objects the prune deleted; zero without retention.
    pub pruned_objects: u64,
}

/// The `sleet mirror verify` response: existence and size for every
/// restore point's closure.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet mirror verify response")]
pub struct MirrorVerifyResponse {
    /// The source database's canonical URL.
    pub database: String,

    /// The target's name.
    pub target: String,

    /// The destination root.
    pub destination: String,

    /// Whether bytes were compared (`--deep`), not just sizes.
    pub deep: bool,

    /// Whether every restore point verified.
    pub ok: bool,

    /// Every restore point checked, ascending by manifest id.
    pub points: Vec<RestorePointStatus>,
}

/// One verified restore point.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct RestorePointStatus {
    /// The restore point's manifest id.
    pub manifest_id: u64,

    /// Objects checked in its closure.
    pub objects: u64,

    /// What is missing or mismatched; empty means the point verifies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub problems: Vec<String>,
}

/// The `sleet mirror restore` response.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet mirror restore response")]
pub struct MirrorRestoreResponse {
    /// The backup root restored from.
    pub backup: String,

    /// The destination root restored into.
    pub destination: String,

    /// The restore point committed as the destination's head.
    pub manifest_id: u64,

    /// Manifests committed.
    pub manifests_committed: u64,

    /// Data objects copied.
    pub objects_copied: u64,

    /// Data bytes copied.
    pub bytes_copied: u64,
}

/// The `sleet mirror drill` response: a restore point restored into a
/// scratch root, opened, and fully scanned.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet mirror drill response")]
pub struct MirrorDrillResponse {
    /// The source database's canonical URL.
    pub database: String,

    /// The target's name.
    pub target: String,

    /// The destination root drilled (the backup).
    pub backup: String,

    /// The scratch root restored into.
    pub scratch: String,

    /// Whether the scratch was kept (`--keep`) rather than removed.
    pub kept: bool,

    /// The restore point committed as the scratch's head.
    pub manifest_id: u64,

    /// Manifests committed into the scratch.
    pub manifests_committed: u64,

    /// Data objects copied into the scratch.
    pub objects_copied: u64,

    /// Data bytes copied into the scratch.
    pub bytes_copied: u64,

    /// Keys the full scan read back.
    pub keys: u64,

    /// Key and value bytes the full scan read back.
    pub bytes: u64,
}

/// The `sleet mirror prefixes` response: the anchored key-prefix
/// filter lists an external replication service needs for one
/// database's data directories.
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(title = "sleet mirror prefixes response")]
pub struct MirrorPrefixesResponse {
    /// The source database's canonical URL.
    pub database: String,

    /// The target's name.
    pub target: String,

    /// The destination root.
    pub destination: String,

    /// Which service's configuration shape is emitted.
    pub format: PrefixFormat,

    /// The source bucket (or container).
    pub source_bucket: String,

    /// The destination bucket (or container).
    pub destination_bucket: String,

    /// Source key prefixes to replicate: the database's `wal/` and
    /// `compacted/` directories.
    pub prefixes: Vec<String>,

    /// The same directories under the destination root.
    pub destination_prefixes: Vec<String>,

    /// A configuration snippet in the service's native shape.
    pub configuration: serde_json::Value,
}

/// External replication services `sleet mirror prefixes` can emit
/// filter lists for.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum PrefixFormat {
    /// S3 bucket replication rules.
    S3,
    /// GCS Storage Transfer Service include prefixes.
    Sts,
    /// Azure object replication rules.
    Azure,
}
