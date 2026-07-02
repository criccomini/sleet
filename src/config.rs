//! The fleet config: `sleet.toml` at the fleet root, plus the
//! per-database registry files under `dbs/`.
//!
//! These structs are the source of truth for the config format. The JSON
//! Schema at `schema/config.schema.json` is generated from them by
//! `tests/schema_sync.rs`, which fails if the two drift. `sleet.toml`
//! validates against the root schema; a `dbs/<db>.toml` file is exactly a
//! `[database]` table and validates against `#/$defs/DatabaseConfig`.
//!
//! Settings resolve per-field in precedence order: built-in defaults
//! (SlateDB's where a field maps to a SlateDB option) -> `[database]` ->
//! `dbs/<db>.toml`. Unset fields fall through to the previous layer.

use std::borrow::Cow;
use std::collections::HashSet;
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

/// The fleet config: `sleet.toml` at the fleet root.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "sleet fleet config")]
pub struct SleetConfig {
    /// Settings every node follows.
    #[serde(default)]
    pub node: NodeConfig,

    /// Service settings applied to every database; a `dbs/<db>.toml`
    /// file overrides them per field.
    #[serde(default)]
    pub database: DatabaseConfig,
}

/// Settings every node follows. These are fleet-wide agreements: nodes
/// must judge liveness with the same timeout for placement to converge.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// How often each node writes its heartbeat object.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: HumanDuration,

    /// Nodes whose heartbeat is older than this are treated as dead.
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout: HumanDuration,

    /// How often nodes re-read `sleet.toml` and LIST `dbs/`.
    #[serde(default = "default_config_poll")]
    pub config_poll: HumanDuration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat_interval(),
            heartbeat_timeout: default_heartbeat_timeout(),
            config_poll: default_config_poll(),
        }
    }
}

fn default_heartbeat_interval() -> HumanDuration {
    Duration::from_secs(10).into()
}

fn default_heartbeat_timeout() -> HumanDuration {
    Duration::from_secs(30).into()
}

fn default_config_poll() -> HumanDuration {
    Duration::from_secs(60).into()
}

/// A per-database service.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, clap::ValueEnum,
)]
#[serde(rename_all = "kebab-case")]
pub enum Service {
    /// Garbage collection.
    Gc,
    /// Standalone compaction coordinator (RFC-0025).
    CompactorCoordinator,
    /// Compaction workers (RFC-0025).
    CompactionWorkers,
}

impl Service {
    pub fn as_str(self) -> &'static str {
        match self {
            Service::Gc => "gc",
            Service::CompactorCoordinator => "compactor-coordinator",
            Service::CompactionWorkers => "compaction-workers",
        }
    }

    /// The service's letter in a heartbeat object name (see
    /// `crate::heartbeat`).
    pub fn letter(self) -> char {
        match self {
            Service::Gc => 'g',
            Service::CompactorCoordinator => 'c',
            Service::CompactionWorkers => 'w',
        }
    }

    /// The service a heartbeat name letter encodes, if known.
    pub fn from_letter(letter: char) -> Option<Self> {
        match letter {
            'g' => Some(Service::Gc),
            'c' => Some(Service::CompactorCoordinator),
            'w' => Some(Service::CompactionWorkers),
            _ => None,
        }
    }

    /// Every service, in heartbeat-letter order.
    pub const ALL: [Service; 3] = [
        Service::CompactorCoordinator,
        Service::Gc,
        Service::CompactionWorkers,
    ];
}

/// Per-database service settings: the `[database]` table of `sleet.toml`
/// and, verbatim, the contents of a `dbs/<db>.toml` registry file. All
/// fields are optional; unset fields fall through to the next precedence
/// layer.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "sleet database config")]
pub struct DatabaseConfig {
    /// Which services run for the database. Default: all of them. An
    /// explicit empty list registers the database but runs nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub services: Option<Vec<Service>>,

    /// Garbage collection settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcOverrides>,

    /// Compaction coordinator settings.
    #[serde(
        rename = "compactor-coordinator",
        skip_serializing_if = "Option::is_none"
    )]
    pub compactor_coordinator: Option<CoordinatorOverrides>,

    /// Compaction worker settings.
    #[serde(rename = "compaction-workers", skip_serializing_if = "Option::is_none")]
    pub compaction_workers: Option<WorkersOverrides>,
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
pub struct CoordinatorOverrides {
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

/// Compaction worker settings (SlateDB `CompactionWorkerOptions` plus
/// `count`).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkersOverrides {
    /// Worker nodes for this database: the top `count` nodes of the
    /// database's rendezvous ranking poll its compaction queue.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,

    /// Jobs a single worker may hold simultaneously.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_compactions: Option<u32>,

    /// How often workers poll `.compactions` for `Scheduled` jobs; sleet
    /// backs the interval off exponentially while the database is idle.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactions_poll_interval: Option<HumanDuration>,

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

// ---------------------------------------------------------------------
// Resolved settings
// ---------------------------------------------------------------------

/// Fully resolved service settings for one database.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedServices {
    pub services: Vec<Service>,
    pub gc: ResolvedGc,
    pub coordinator: ResolvedCoordinator,
    pub workers: ResolvedWorkers,
}

impl Default for ResolvedServices {
    fn default() -> Self {
        Self {
            services: vec![
                Service::Gc,
                Service::CompactorCoordinator,
                Service::CompactionWorkers,
            ],
            gc: ResolvedGc::default(),
            coordinator: ResolvedCoordinator::default(),
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
pub struct ResolvedCoordinator {
    pub poll_interval: Duration,
    pub manifest_update_timeout: Duration,
    pub max_concurrent_compactions: u32,
    pub commit_compacted_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub scheduler: ResolvedScheduler,
}

impl Default for ResolvedCoordinator {
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
    pub compactions_poll_interval: Duration,
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
            compactions_poll_interval: Duration::from_secs(5),
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

impl DatabaseConfig {
    fn apply(&self, r: &mut ResolvedServices) {
        if let Some(services) = &self.services {
            r.services = services.clone();
        }
        if let Some(gc) = &self.gc {
            gc.apply(&mut r.gc);
        }
        if let Some(coordinator) = &self.compactor_coordinator {
            coordinator.apply(&mut r.coordinator);
        }
        if let Some(workers) = &self.compaction_workers {
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

impl CoordinatorOverrides {
    fn apply(&self, r: &mut ResolvedCoordinator) {
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
        if let Some(v) = self.compactions_poll_interval {
            r.compactions_poll_interval = v.0;
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

impl SleetConfig {
    /// Resolve service settings for a database: built-in defaults ->
    /// `[database]` -> the database's `dbs/<db>.toml` contents, if any.
    pub fn resolve(&self, db: Option<&DatabaseConfig>) -> ResolvedServices {
        let mut r = ResolvedServices::default();
        self.database.apply(&mut r);
        if let Some(db) = db {
            db.apply(&mut r);
        }
        r
    }
}

// ---------------------------------------------------------------------
// Validation and parsing
// ---------------------------------------------------------------------

/// One or more config validation errors.
#[derive(Debug, thiserror::Error)]
#[error("invalid config:\n  {}", .0.join("\n  "))]
pub struct ConfigError(pub Vec<String>);

/// A config that failed to parse or validate.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("failed to parse config: {0}")]
    Toml(#[from] toml::de::Error),
    #[error(transparent)]
    Invalid(#[from] ConfigError),
}

/// Parse and validate a `sleet.toml`.
pub fn parse_config(toml: &str) -> Result<SleetConfig, ParseError> {
    let config: SleetConfig = toml::from_str(toml)?;
    config.validate()?;
    Ok(config)
}

/// Parse a `dbs/<db>.toml` registry file and validate it layered on the
/// fleet config. An empty file is valid and means "fleet-wide config".
pub fn parse_database(fleet: &SleetConfig, toml: &str) -> Result<DatabaseConfig, ParseError> {
    let db: DatabaseConfig = toml::from_str(toml)?;
    fleet.validate_database(&db)?;
    Ok(db)
}

/// The fleet config JSON Schema, pretty-printed.
pub fn schema_json() -> String {
    crate::schema_pretty::<SleetConfig>()
}

impl SleetConfig {
    /// Check cross-field invariants the schema cannot express.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut errs = Vec::new();

        if self.node.heartbeat_interval.0.is_zero() {
            errs.push("node.heartbeat_interval must be > 0".into());
        }
        if self.node.heartbeat_interval >= self.node.heartbeat_timeout {
            errs.push(format!(
                "node.heartbeat_interval ({}) must be < node.heartbeat_timeout ({})",
                humantime::format_duration(self.node.heartbeat_interval.0),
                humantime::format_duration(self.node.heartbeat_timeout.0),
            ));
        }
        if self.node.config_poll.0.is_zero() {
            errs.push("node.config_poll must be > 0".into());
        }

        check_database(&self.database, "database", &mut errs);
        check_resolved(&self.resolve(None), "database", &mut errs);

        if errs.is_empty() {
            Ok(())
        } else {
            Err(ConfigError(errs))
        }
    }

    /// Check a `dbs/<db>.toml`'s fields, plus the bounds that must hold
    /// on the layered result rather than per block.
    pub fn validate_database(&self, db: &DatabaseConfig) -> Result<(), ConfigError> {
        let mut errs = Vec::new();
        check_database(db, "", &mut errs);
        check_resolved(&self.resolve(Some(db)), "resolved", &mut errs);
        if errs.is_empty() {
            Ok(())
        } else {
            Err(ConfigError(errs))
        }
    }
}

/// `field` prefixed with its containing table, if any.
fn loc(at: &str, field: &str) -> String {
    if at.is_empty() {
        field.to_string()
    } else {
        format!("{at}.{field}")
    }
}

fn check_resolved(r: &ResolvedServices, at: &str, errs: &mut Vec<String>) {
    let s = r.coordinator.scheduler;
    if s.min_compaction_sources > s.max_compaction_sources {
        errs.push(format!(
            "{}: min_compaction_sources ({}) exceeds max_compaction_sources ({})",
            loc(at, "compactor-coordinator.scheduler"),
            s.min_compaction_sources,
            s.max_compaction_sources
        ));
    }
}

fn check_database(o: &DatabaseConfig, at: &str, errs: &mut Vec<String>) {
    if let Some(services) = &o.services {
        let mut seen = HashSet::new();
        for s in services {
            if !seen.insert(*s) {
                errs.push(format!(
                    "{} lists {:?} more than once",
                    loc(at, "services"),
                    s.as_str()
                ));
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
                errs.push(format!(
                    "{} must be > 0",
                    loc(at, &format!("gc.{name}.interval"))
                ));
            }
        }
        if let Some(detach) = &gc.detach
            && detach.interval.is_some_and(|d| d.0.is_zero())
        {
            errs.push(format!("{} must be > 0", loc(at, "gc.detach.interval")));
        }
    }
    if let Some(c) = &o.compactor_coordinator {
        let cc = "compactor-coordinator";
        for (name, d) in [
            ("poll_interval", c.poll_interval),
            ("manifest_update_timeout", c.manifest_update_timeout),
            ("commit_compacted_interval", c.commit_compacted_interval),
            ("worker_heartbeat_timeout", c.worker_heartbeat_timeout),
        ] {
            if d.is_some_and(|d| d.0.is_zero()) {
                errs.push(format!("{} must be > 0", loc(at, &format!("{cc}.{name}"))));
            }
        }
        if c.max_concurrent_compactions == Some(0) {
            errs.push(format!(
                "{} must be >= 1",
                loc(at, &format!("{cc}.max_concurrent_compactions"))
            ));
        }
        if let Some(s) = &c.scheduler {
            if s.min_compaction_sources == Some(0) {
                errs.push(format!(
                    "{} must be >= 1",
                    loc(at, &format!("{cc}.scheduler.min_compaction_sources"))
                ));
            }
            if s.max_compaction_sources == Some(0) {
                errs.push(format!(
                    "{} must be >= 1",
                    loc(at, &format!("{cc}.scheduler.max_compaction_sources"))
                ));
            }
            if s.include_size_threshold
                .is_some_and(|t| !(t.is_finite() && t > 0.0))
            {
                errs.push(format!(
                    "{} must be a positive number",
                    loc(at, &format!("{cc}.scheduler.include_size_threshold"))
                ));
            }
        }
    }
    if let Some(w) = &o.compaction_workers {
        let cw = "compaction-workers";
        if w.count == Some(0) {
            errs.push(format!(
                "{} must be >= 1 (drop \"compaction-workers\" from services to run none)",
                loc(at, &format!("{cw}.count"))
            ));
        }
        if w.max_concurrent_compactions == Some(0) {
            errs.push(format!(
                "{} must be >= 1",
                loc(at, &format!("{cw}.max_concurrent_compactions"))
            ));
        }
        if w.compactions_poll_interval.is_some_and(|d| d.0.is_zero()) {
            errs.push(format!(
                "{} must be > 0",
                loc(at, &format!("{cw}.compactions_poll_interval"))
            ));
        }
        if w.max_fetch_tasks == Some(0) {
            errs.push(format!(
                "{} must be >= 1",
                loc(at, &format!("{cw}.max_fetch_tasks"))
            ));
        }
        for (name, v) in [
            ("max_sst_size", w.max_sst_size),
            ("bytes_to_fetch", w.bytes_to_fetch),
        ] {
            if v == Some(0) {
                errs.push(format!("{} must be > 0", loc(at, &format!("{cw}.{name}"))));
            }
        }
    }
}
