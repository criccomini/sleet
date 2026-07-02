//! The fleet spec: the TOML file loaded by `sleet run --spec <path>`.
//!
//! These structs are the source of truth for the spec format. The JSON
//! Schema at `schema/config.schema.json` is generated from them (`sleet
//! schema`); `tests/schema_sync.rs` fails if the two drift.
//!
//! Settings resolve in precedence order: built-in defaults (SlateDB's
//! where a field maps to a SlateDB option) -> `[defaults]` -> longest
//! matching `[[discover]]` root -> exact `[[database]]` entry. Unset
//! fields fall through to the previous layer.

use std::borrow::Cow;
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A duration in humantime form, e.g. `"10s"`, `"5m"`, `"1h 30m"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HumanDuration(pub Duration);

impl From<Duration> for HumanDuration {
    fn from(d: Duration) -> Self {
        Self(d)
    }
}

impl Serialize for HumanDuration {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&humantime::format_duration(self.0))
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        humantime::parse_duration(&s)
            .map(HumanDuration)
            .map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for HumanDuration {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("HumanDuration")
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "description": "A duration in humantime form, e.g. \"10s\", \"5m\", \"1h 30m\".",
            "pattern": "^([0-9]+ *[a-zµ]+ *)+$"
        })
    }
}

/// The sleet fleet spec: the set of managed databases and which services
/// each gets.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "sleet fleet spec")]
pub struct FleetSpec {
    /// Node identity and fleet membership.
    #[serde(default)]
    pub fleet: Fleet,

    /// Service settings applied to every database.
    #[serde(default)]
    pub defaults: ServiceOverrides,

    /// Roots scanned for databases.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub discover: Vec<DiscoverRoot>,

    /// Explicitly managed databases; an entry wins over discovery for the
    /// same URL.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub database: Vec<DatabaseEntry>,
}

/// Node identity and fleet membership.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Fleet {
    /// Node identity within the fleet. Default: hostname.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,

    /// Object-store prefix for node heartbeats. Omit to run single-node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeats: Option<String>,

    /// How often this node writes its heartbeat object.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: HumanDuration,

    /// Nodes whose heartbeat is older than this are treated as dead.
    #[serde(default = "default_node_timeout")]
    pub node_timeout: HumanDuration,
}

impl Default for Fleet {
    fn default() -> Self {
        Self {
            node_id: None,
            heartbeats: None,
            heartbeat_interval: default_heartbeat_interval(),
            node_timeout: default_node_timeout(),
        }
    }
}

fn default_heartbeat_interval() -> HumanDuration {
    Duration::from_secs(10).into()
}

fn default_node_timeout() -> HumanDuration {
    Duration::from_secs(30).into()
}

/// A per-database service.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Service {
    /// Garbage collection.
    Gc,
    /// Standalone compaction coordinator (RFC-0025).
    Compactor,
    /// Compaction worker pool (RFC-0025).
    Workers,
}

impl Service {
    pub fn as_str(self) -> &'static str {
        match self {
            Service::Gc => "gc",
            Service::Compactor => "compactor",
            Service::Workers => "workers",
        }
    }
}

/// Per-database service settings. All fields are optional; unset fields
/// fall through to the next precedence layer.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceOverrides {
    /// Which services run for the database. Default: all of them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub services: Option<Vec<Service>>,

    /// Garbage collection settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcOverrides>,

    /// Compaction coordinator settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactor: Option<CompactorOverrides>,

    /// Compaction worker pool settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workers: Option<WorkersOverrides>,
}

/// Garbage collection settings, per SlateDB resource directory.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GcOverrides {
    /// The manifest directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<GcDirectoryOverrides>,

    /// The WAL directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal: Option<GcDirectoryOverrides>,

    /// Zero-byte WAL fence objects. Deleting fences too young can lose
    /// writes; dry-run by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_fence: Option<GcDirectoryOverrides>,

    /// The compacted SST directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compacted: Option<GcDirectoryOverrides>,

    /// The compactions directory (`.compactions` job state).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactions: Option<GcDirectoryOverrides>,

    /// Detaching clones whose parent references are no longer needed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach: Option<GcDetachOverrides>,
}

/// GC settings for one resource directory.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GcDirectoryOverrides {
    /// Set false to disable GC for this directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    /// How often the GC pass runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<HumanDuration>,

    /// Minimum object age before deletion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_age: Option<HumanDuration>,

    /// Log deletions without performing them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
}

/// Settings for the clone-detach GC pass (no file-age threshold).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GcDetachOverrides {
    /// Set false to disable the detach pass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    /// How often the detach pass runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<HumanDuration>,
}

/// Compaction coordinator settings (SlateDB `CompactorOptions`; sleet
/// always runs the coordinator without an embedded worker).
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompactorOverrides {
    /// How often the coordinator polls the manifest to schedule compactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval: Option<HumanDuration>,

    /// How long manifest updates are retried before giving up.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_update_timeout: Option<HumanDuration>,

    /// Maximum compactions scheduled concurrently.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_compactions: Option<u32>,

    /// How often `Compacted` results are committed to the manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_compacted_interval: Option<HumanDuration>,

    /// Reclaim a `Running` job whose worker heartbeat is older than this.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_heartbeat_timeout: Option<HumanDuration>,

    /// Size-tiered compaction scheduler settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduler: Option<SchedulerOverrides>,
}

/// Size-tiered compaction scheduler settings (SlateDB
/// `SizeTieredCompactionSchedulerOptions`).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerOverrides {
    /// Minimum sources included together in one compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_compaction_sources: Option<u32>,

    /// Maximum sources included together in one compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_compaction_sources: Option<u32>,

    /// A sorted run joins a compaction if its size is below this multiple
    /// of the smallest run already included.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_size_threshold: Option<f32>,
}

/// Compaction worker pool settings (SlateDB `CompactionWorkerOptions`
/// plus the pool size).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkersOverrides {
    /// Worker slots for this database across the fleet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,

    /// Jobs a single worker may hold simultaneously.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_compactions: Option<u32>,

    /// How often workers poll `.compactions` for `Scheduled` jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval: Option<HumanDuration>,

    /// Bytes a worker must process before emitting a heartbeat.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_bytes: Option<u64>,

    /// Minimum wall-clock time between heartbeat writes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_min_interval: Option<HumanDuration>,

    /// Maximum output SST size in bytes before a new one is rolled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_sst_size: Option<u64>,

    /// Concurrent block-fetch tasks per input SST.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fetch_tasks: Option<u32>,

    /// Read-ahead request size in bytes while iterating input SSTs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_to_fetch: Option<u64>,

    /// Maximum subcompactions per job (RFC-0028); values <= 1 disable
    /// subcompactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_subcompactions: Option<u32>,

    /// Write bloom filters for SSTs with at least this many keys. Must
    /// match the writer's `min_filter_keys`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_filter_keys: Option<u32>,

    /// Compression for SSTs the worker writes. Must match the writer's
    /// `compression_codec`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_codec: Option<CompressionCodec>,
}

/// SST compression codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    Snappy,
    Zlib,
    Lz4,
    Zstd,
}

/// A root prefix scanned for databases every `rescan`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoverRoot {
    /// Object-store URL to scan, e.g. `"s3://prod-us/"`.
    pub url: String,

    /// How often the root is rescanned.
    #[serde(default = "default_rescan")]
    pub rescan: HumanDuration,

    /// Maximum prefix depth below the root to descend.
    #[serde(default = "default_max_depth")]
    pub max_depth: u32,

    /// Glob patterns for prefixes to skip.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,

    /// Which services run for databases under this root.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub services: Option<Vec<Service>>,

    /// Garbage collection settings for databases under this root.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcOverrides>,

    /// Compaction coordinator settings for databases under this root.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactor: Option<CompactorOverrides>,

    /// Compaction worker pool settings for databases under this root.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workers: Option<WorkersOverrides>,
}

fn default_rescan() -> HumanDuration {
    Duration::from_secs(300).into()
}

fn default_max_depth() -> u32 {
    3
}

impl DiscoverRoot {
    pub fn overrides(&self) -> ServiceOverrides {
        ServiceOverrides {
            services: self.services.clone(),
            gc: self.gc.clone(),
            compactor: self.compactor.clone(),
            workers: self.workers,
        }
    }
}

/// An explicitly managed database.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatabaseEntry {
    /// Object-store URL of the database root, e.g. `"gs://analytics/events"`.
    pub url: String,

    /// Which services run for this database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub services: Option<Vec<Service>>,

    /// Garbage collection settings for this database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcOverrides>,

    /// Compaction coordinator settings for this database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactor: Option<CompactorOverrides>,

    /// Compaction worker pool settings for this database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workers: Option<WorkersOverrides>,
}

impl DatabaseEntry {
    pub fn overrides(&self) -> ServiceOverrides {
        ServiceOverrides {
            services: self.services.clone(),
            gc: self.gc.clone(),
            compactor: self.compactor.clone(),
            workers: self.workers,
        }
    }
}

// ---------------------------------------------------------------------
// Resolved settings
// ---------------------------------------------------------------------

/// Fully resolved service settings for one database.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedServices {
    pub services: Vec<Service>,
    pub gc: ResolvedGc,
    pub compactor: ResolvedCompactor,
    pub workers: ResolvedWorkers,
}

impl Default for ResolvedServices {
    fn default() -> Self {
        Self {
            services: vec![Service::Gc, Service::Compactor, Service::Workers],
            gc: ResolvedGc::default(),
            compactor: ResolvedCompactor::default(),
            workers: ResolvedWorkers::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedGc {
    pub manifest: ResolvedGcDirectory,
    pub wal: ResolvedGcDirectory,
    pub wal_fence: ResolvedGcDirectory,
    pub compacted: ResolvedGcDirectory,
    pub compactions: ResolvedGcDirectory,
    pub detach: ResolvedGcDetach,
}

impl Default for ResolvedGc {
    /// SlateDB `GarbageCollectorOptions` defaults: every directory enabled
    /// at `interval=60s`, `min_age=300s`; WAL fence GC in dry-run.
    fn default() -> Self {
        let dir = ResolvedGcDirectory::default();
        Self {
            manifest: dir,
            wal: dir,
            wal_fence: ResolvedGcDirectory {
                dry_run: true,
                ..dir
            },
            compacted: dir,
            compactions: dir,
            detach: ResolvedGcDetach::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedGcDirectory {
    pub enabled: bool,
    pub interval: Duration,
    pub min_age: Duration,
    pub dry_run: bool,
}

impl Default for ResolvedGcDirectory {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(60),
            min_age: Duration::from_secs(300),
            dry_run: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedGcDetach {
    pub enabled: bool,
    pub interval: Duration,
}

impl Default for ResolvedGcDetach {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedCompactor {
    pub poll_interval: Duration,
    pub manifest_update_timeout: Duration,
    pub max_concurrent_compactions: u32,
    pub commit_compacted_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub scheduler: ResolvedScheduler,
}

impl Default for ResolvedCompactor {
    /// SlateDB `CompactorOptions` defaults.
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            manifest_update_timeout: Duration::from_secs(300),
            max_concurrent_compactions: 4,
            commit_compacted_interval: Duration::from_secs(1),
            worker_heartbeat_timeout: Duration::from_secs(30),
            scheduler: ResolvedScheduler::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedScheduler {
    pub min_compaction_sources: u32,
    pub max_compaction_sources: u32,
    pub include_size_threshold: f32,
}

impl Default for ResolvedScheduler {
    /// SlateDB `SizeTieredCompactionSchedulerOptions` defaults.
    fn default() -> Self {
        Self {
            min_compaction_sources: 4,
            max_compaction_sources: 8,
            include_size_threshold: 4.0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedWorkers {
    pub count: u32,
    pub max_concurrent_compactions: u32,
    pub poll_interval: Duration,
    pub heartbeat_bytes: u64,
    pub heartbeat_min_interval: Duration,
    pub max_sst_size: u64,
    pub max_fetch_tasks: u32,
    pub bytes_to_fetch: u64,
    pub max_subcompactions: u32,
    pub min_filter_keys: u32,
    pub compression_codec: Option<CompressionCodec>,
}

impl Default for ResolvedWorkers {
    /// SlateDB `CompactionWorkerOptions` defaults, plus `count=1`.
    fn default() -> Self {
        Self {
            count: 1,
            max_concurrent_compactions: 4,
            poll_interval: Duration::from_secs(5),
            heartbeat_bytes: 5 * 1024 * 1024,
            heartbeat_min_interval: Duration::from_secs(5),
            max_sst_size: 256 * 1024 * 1024,
            max_fetch_tasks: 4,
            bytes_to_fetch: 2 * 1024 * 1024,
            max_subcompactions: 4,
            min_filter_keys: 1000,
            compression_codec: None,
        }
    }
}

impl ServiceOverrides {
    fn apply(&self, r: &mut ResolvedServices) {
        if let Some(services) = &self.services {
            r.services = services.clone();
        }
        if let Some(gc) = &self.gc {
            gc.apply(&mut r.gc);
        }
        if let Some(compactor) = &self.compactor {
            compactor.apply(&mut r.compactor);
        }
        if let Some(workers) = &self.workers {
            workers.apply(&mut r.workers);
        }
    }
}

impl GcOverrides {
    fn apply(&self, r: &mut ResolvedGc) {
        for (o, t) in [
            (&self.manifest, &mut r.manifest),
            (&self.wal, &mut r.wal),
            (&self.wal_fence, &mut r.wal_fence),
            (&self.compacted, &mut r.compacted),
            (&self.compactions, &mut r.compactions),
        ] {
            if let Some(o) = o {
                o.apply(t);
            }
        }
        if let Some(detach) = &self.detach {
            detach.apply(&mut r.detach);
        }
    }
}

impl GcDirectoryOverrides {
    fn apply(&self, r: &mut ResolvedGcDirectory) {
        if let Some(v) = self.enabled {
            r.enabled = v;
        }
        if let Some(v) = self.interval {
            r.interval = v.0;
        }
        if let Some(v) = self.min_age {
            r.min_age = v.0;
        }
        if let Some(v) = self.dry_run {
            r.dry_run = v;
        }
    }
}

impl GcDetachOverrides {
    fn apply(&self, r: &mut ResolvedGcDetach) {
        if let Some(v) = self.enabled {
            r.enabled = v;
        }
        if let Some(v) = self.interval {
            r.interval = v.0;
        }
    }
}

impl CompactorOverrides {
    fn apply(&self, r: &mut ResolvedCompactor) {
        if let Some(v) = self.poll_interval {
            r.poll_interval = v.0;
        }
        if let Some(v) = self.manifest_update_timeout {
            r.manifest_update_timeout = v.0;
        }
        if let Some(v) = self.max_concurrent_compactions {
            r.max_concurrent_compactions = v;
        }
        if let Some(v) = self.commit_compacted_interval {
            r.commit_compacted_interval = v.0;
        }
        if let Some(v) = self.worker_heartbeat_timeout {
            r.worker_heartbeat_timeout = v.0;
        }
        if let Some(s) = &self.scheduler {
            s.apply(&mut r.scheduler);
        }
    }
}

impl SchedulerOverrides {
    fn apply(&self, r: &mut ResolvedScheduler) {
        if let Some(v) = self.min_compaction_sources {
            r.min_compaction_sources = v;
        }
        if let Some(v) = self.max_compaction_sources {
            r.max_compaction_sources = v;
        }
        if let Some(v) = self.include_size_threshold {
            r.include_size_threshold = v;
        }
    }
}

impl WorkersOverrides {
    fn apply(&self, r: &mut ResolvedWorkers) {
        if let Some(v) = self.count {
            r.count = v;
        }
        if let Some(v) = self.max_concurrent_compactions {
            r.max_concurrent_compactions = v;
        }
        if let Some(v) = self.poll_interval {
            r.poll_interval = v.0;
        }
        if let Some(v) = self.heartbeat_bytes {
            r.heartbeat_bytes = v;
        }
        if let Some(v) = self.heartbeat_min_interval {
            r.heartbeat_min_interval = v.0;
        }
        if let Some(v) = self.max_sst_size {
            r.max_sst_size = v;
        }
        if let Some(v) = self.max_fetch_tasks {
            r.max_fetch_tasks = v;
        }
        if let Some(v) = self.bytes_to_fetch {
            r.bytes_to_fetch = v;
        }
        if let Some(v) = self.max_subcompactions {
            r.max_subcompactions = v;
        }
        if let Some(v) = self.min_filter_keys {
            r.min_filter_keys = v;
        }
        if let Some(v) = self.compression_codec {
            r.compression_codec = Some(v);
        }
    }
}

impl FleetSpec {
    /// Resolve service settings for a database URL: built-in defaults ->
    /// `[defaults]` -> longest matching discovery root -> exact
    /// `[[database]]` entry.
    pub fn resolve(&self, url: &str) -> ResolvedServices {
        let mut r = ResolvedServices::default();
        self.defaults.apply(&mut r);
        if let Some(root) = self
            .discover
            .iter()
            .filter(|d| url_under_root(url, &d.url))
            .max_by_key(|d| d.url.trim_end_matches('/').len())
        {
            root.overrides().apply(&mut r);
        }
        if let Some(db) = self
            .database
            .iter()
            .find(|d| normalize_url(&d.url) == normalize_url(url))
        {
            db.overrides().apply(&mut r);
        }
        r
    }
}

fn normalize_url(url: &str) -> &str {
    url.trim_end_matches('/')
}

fn url_under_root(url: &str, root: &str) -> bool {
    let url = normalize_url(url);
    let root = normalize_url(root);
    url == root || url.starts_with(&format!("{root}/"))
}

// ---------------------------------------------------------------------
// Validation and loading
// ---------------------------------------------------------------------

/// URL schemes accepted for object-store locations, matching
/// `object_store::parse_url`.
const URL_SCHEMES: &[&str] = &[
    "s3", "s3a", "gs", "az", "adl", "azure", "abfs", "abfss", "file", "memory", "http", "https",
];

/// One or more fleet spec validation errors.
#[derive(Debug, thiserror::Error)]
#[error("invalid fleet spec:\n  {}", .0.join("\n  "))]
pub struct SpecError(pub Vec<String>);

/// A fleet spec that failed to load.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read fleet spec: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse fleet spec: {0}")]
    Parse(#[from] toml::de::Error),
    #[error(transparent)]
    Invalid(#[from] SpecError),
}

/// Read, parse, and validate a fleet spec from a TOML file.
pub fn load(path: &Path) -> Result<FleetSpec, LoadError> {
    let spec: FleetSpec = toml::from_str(&std::fs::read_to_string(path)?)?;
    spec.validate()?;
    Ok(spec)
}

/// The fleet spec JSON Schema, pretty-printed.
pub fn schema_json() -> String {
    crate::schema_pretty::<FleetSpec>()
}

impl FleetSpec {
    /// Check cross-field invariants the schema cannot express.
    pub fn validate(&self) -> Result<(), SpecError> {
        let mut errs = Vec::new();

        if let Some(id) = &self.fleet.node_id {
            let ok = !id.is_empty()
                && id.len() <= 128
                && id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
            if !ok {
                errs.push(format!(
                    "fleet.node_id {id:?} must be 1-128 chars of [A-Za-z0-9._-]"
                ));
            }
        }
        if let Some(hb) = &self.fleet.heartbeats {
            check_url(hb, "fleet.heartbeats", &mut errs);
        }
        if self.fleet.heartbeat_interval.0.is_zero() {
            errs.push("fleet.heartbeat_interval must be > 0".into());
        }
        if self.fleet.heartbeat_interval >= self.fleet.node_timeout {
            errs.push(format!(
                "fleet.heartbeat_interval ({}) must be < fleet.node_timeout ({})",
                humantime::format_duration(self.fleet.heartbeat_interval.0),
                humantime::format_duration(self.fleet.node_timeout.0),
            ));
        }

        check_overrides(&self.defaults, "defaults", &mut errs);

        let mut roots = HashSet::new();
        for (i, d) in self.discover.iter().enumerate() {
            let at = format!("discover[{i}]");
            check_url(&d.url, &format!("{at}.url"), &mut errs);
            if !roots.insert(normalize_url(&d.url)) {
                errs.push(format!("{at}.url {:?} duplicates an earlier root", d.url));
            }
            if d.max_depth == 0 {
                errs.push(format!("{at}.max_depth must be >= 1"));
            }
            if d.rescan.0.is_zero() {
                errs.push(format!("{at}.rescan must be > 0"));
            }
            for pat in &d.exclude {
                if let Err(e) = globset::Glob::new(pat) {
                    errs.push(format!("{at}.exclude glob {pat:?} is invalid: {e}"));
                }
            }
            check_overrides(&d.overrides(), &at, &mut errs);
        }

        let mut dbs = HashSet::new();
        for (i, db) in self.database.iter().enumerate() {
            let at = format!("database[{i}]");
            check_url(&db.url, &format!("{at}.url"), &mut errs);
            if !dbs.insert(normalize_url(&db.url)) {
                errs.push(format!("{at}.url {:?} duplicates an earlier entry", db.url));
            }
            check_overrides(&db.overrides(), &at, &mut errs);
        }

        // Scheduler bounds must hold after layering, not per block.
        for (url, at) in self
            .database
            .iter()
            .map(|d| (d.url.as_str(), "database"))
            .chain(self.discover.iter().map(|d| (d.url.as_str(), "discover")))
        {
            let s = self.resolve(url).compactor.scheduler;
            if s.min_compaction_sources > s.max_compaction_sources {
                errs.push(format!(
                    "resolved scheduler for {at} {url:?}: min_compaction_sources \
                     ({}) exceeds max_compaction_sources ({})",
                    s.min_compaction_sources, s.max_compaction_sources
                ));
            }
        }
        {
            let s = self.resolve("").compactor.scheduler;
            if s.min_compaction_sources > s.max_compaction_sources {
                errs.push(format!(
                    "defaults: min_compaction_sources ({}) exceeds \
                     max_compaction_sources ({})",
                    s.min_compaction_sources, s.max_compaction_sources
                ));
            }
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(SpecError(errs))
        }
    }
}

fn check_url(s: &str, at: &str, errs: &mut Vec<String>) {
    match url::Url::parse(s) {
        Ok(u) if URL_SCHEMES.contains(&u.scheme()) => {}
        Ok(u) => errs.push(format!(
            "{at}: unsupported URL scheme {:?} (expected one of {})",
            u.scheme(),
            URL_SCHEMES.join(", ")
        )),
        Err(e) => errs.push(format!("{at}: invalid URL {s:?}: {e}")),
    }
}

fn check_overrides(o: &ServiceOverrides, at: &str, errs: &mut Vec<String>) {
    if let Some(services) = &o.services {
        let mut seen = HashSet::new();
        for s in services {
            if !seen.insert(*s) {
                errs.push(format!("{at}.services lists {s:?} more than once"));
            }
        }
    }
    if let Some(gc) = &o.gc {
        for (name, dir) in [
            ("manifest", &gc.manifest),
            ("wal", &gc.wal),
            ("wal_fence", &gc.wal_fence),
            ("compacted", &gc.compacted),
            ("compactions", &gc.compactions),
        ] {
            if let Some(dir) = dir
                && dir.interval.is_some_and(|d| d.0.is_zero())
            {
                errs.push(format!("{at}.gc.{name}.interval must be > 0"));
            }
        }
        if let Some(detach) = &gc.detach
            && detach.interval.is_some_and(|d| d.0.is_zero())
        {
            errs.push(format!("{at}.gc.detach.interval must be > 0"));
        }
    }
    if let Some(c) = &o.compactor {
        for (name, d) in [
            ("poll_interval", c.poll_interval),
            ("manifest_update_timeout", c.manifest_update_timeout),
            ("commit_compacted_interval", c.commit_compacted_interval),
            ("worker_heartbeat_timeout", c.worker_heartbeat_timeout),
        ] {
            if d.is_some_and(|d| d.0.is_zero()) {
                errs.push(format!("{at}.compactor.{name} must be > 0"));
            }
        }
        if c.max_concurrent_compactions == Some(0) {
            errs.push(format!(
                "{at}.compactor.max_concurrent_compactions must be >= 1"
            ));
        }
        if let Some(s) = &c.scheduler {
            if s.min_compaction_sources == Some(0) {
                errs.push(format!(
                    "{at}.compactor.scheduler.min_compaction_sources must be >= 1"
                ));
            }
            if s.max_compaction_sources == Some(0) {
                errs.push(format!(
                    "{at}.compactor.scheduler.max_compaction_sources must be >= 1"
                ));
            }
            if s.include_size_threshold
                .is_some_and(|t| !(t.is_finite() && t > 0.0))
            {
                errs.push(format!(
                    "{at}.compactor.scheduler.include_size_threshold must be a \
                     positive number"
                ));
            }
        }
    }
    if let Some(w) = &o.workers {
        if w.count == Some(0) {
            errs.push(format!(
                "{at}.workers.count must be >= 1 (drop the \"workers\" service \
                 to run none)"
            ));
        }
        if w.max_concurrent_compactions == Some(0) {
            errs.push(format!(
                "{at}.workers.max_concurrent_compactions must be >= 1"
            ));
        }
        if w.poll_interval.is_some_and(|d| d.0.is_zero()) {
            errs.push(format!("{at}.workers.poll_interval must be > 0"));
        }
        if w.max_fetch_tasks == Some(0) {
            errs.push(format!("{at}.workers.max_fetch_tasks must be >= 1"));
        }
        for (name, v) in [
            ("max_sst_size", w.max_sst_size),
            ("bytes_to_fetch", w.bytes_to_fetch),
        ] {
            if v == Some(0) {
                errs.push(format!("{at}.workers.{name} must be > 0"));
            }
        }
    }
}
