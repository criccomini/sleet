//! The fleet root: the object-store tree holding all fleet state, and
//! the reads every node performs against it.
//!
//! Layout under the root URL:
//!
//! ```text
//! <root>/
//!   sleet.toml               policy
//!   dbs/<db>.toml            registry
//!   nodes/<node>.<svc>.json  heartbeats
//! ```
//!
//! `ConfigPoller` implements the `config_poll` read: re-read
//! `sleet.toml` and LIST `dbs/`, keeping the last good config on failed
//! reads, skipping unchanged bodies by ETag, and never fetching empty
//! registry files. `list_heartbeats` + `node_view` implement the
//! per-tick liveness read.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::TryStreamExt;
use object_store::path::Path as StorePath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::config::{self, DatabaseConfig, Service, SleetConfig};
use crate::{heartbeat, registry};

/// The time source for heartbeat ages. Liveness compares a heartbeat's
/// `LastModified` (object-store clock) against the reader's clock; this
/// seam lets tests and simulations control the reader's side.
pub trait Clock: Send + Sync {
    /// The reader's current time.
    fn now(&self) -> chrono::DateTime<Utc>;
}

/// The wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        Utc::now()
    }
}

/// A fleet root that could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The root URL was rejected.
    #[error("invalid fleet root: {0}")]
    Url(#[from] registry::UrlError),
    /// The URL parsed but no object store could be built for it.
    #[error("failed to open fleet root store: {0}")]
    Store(#[from] object_store::Error),
}

/// The fleet root: an object store plus the prefix the tree lives under.
#[derive(Clone)]
pub struct FleetRoot {
    store: Arc<dyn ObjectStore>,
    prefix: StorePath,
    url: String,
    clock: Arc<dyn Clock>,
}

impl FleetRoot {
    /// Open a fleet root from its URL. Credentials come from the
    /// environment, per `object_store`.
    pub fn open(url: &str) -> Result<Self, OpenError> {
        let canonical = registry::canonicalize_url(url)?;
        let parsed = url::Url::parse(&canonical).expect("canonical URL reparses");
        let (store, prefix) = object_store::parse_url(&parsed)?;
        Ok(Self::from_parts(store.into(), prefix, &canonical))
    }

    /// A root over an existing store, for tests and embedding.
    pub fn from_parts(store: Arc<dyn ObjectStore>, prefix: StorePath, url: &str) -> Self {
        Self {
            store,
            prefix,
            url: url.to_string(),
            clock: Arc::new(SystemClock),
        }
    }

    /// Replace the reader's clock (tests and simulations).
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The object store the fleet tree lives in.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The canonical fleet root URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// `<root>/sleet.toml`.
    pub fn config_path(&self) -> StorePath {
        self.prefix.clone().join("sleet.toml")
    }

    /// `<root>/dbs/`.
    pub fn dbs_prefix(&self) -> StorePath {
        self.prefix.clone().join("dbs")
    }

    /// `<root>/nodes/`.
    pub fn nodes_prefix(&self) -> StorePath {
        self.prefix.clone().join("nodes")
    }

    /// The registry file for a canonical database URL. Built by parsing
    /// rather than joining: `join` would percent-encode the name's `%`
    /// signs a second time.
    pub fn database_path(&self, canonical_url: &str) -> StorePath {
        let name = registry::file_name(canonical_url);
        StorePath::parse(format!("{}/{}", self.dbs_prefix(), name))
            .expect("registry file names are valid paths")
    }

    /// A heartbeat object under `nodes/`.
    pub fn node_path(&self, object_name: &str) -> StorePath {
        self.nodes_prefix().join(object_name)
    }

    /// Every heartbeat object under `nodes/`, with parsed names and
    /// ages. Objects that aren't heartbeats are ignored.
    pub async fn list_heartbeats(&self) -> Result<Vec<HeartbeatEntry>, object_store::Error> {
        let now = self.clock.now();
        let metas = self.list(&self.nodes_prefix()).await?;
        let mut entries = Vec::new();
        for meta in metas {
            let Some(name) = meta.location.filename() else {
                continue;
            };
            let Some((node_id, services)) = heartbeat::parse_object_name(name) else {
                continue;
            };
            let age = (now - meta.last_modified).to_std().unwrap_or_default();
            entries.push(HeartbeatEntry {
                node_id,
                services,
                age,
                location: meta.location,
            });
        }
        Ok(entries)
    }

    /// LIST a prefix, treating a missing prefix as empty.
    async fn list(&self, prefix: &StorePath) -> Result<Vec<ObjectMeta>, object_store::Error> {
        match self.store.list(Some(prefix)).try_collect().await {
            Ok(metas) => Ok(metas),
            Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }
}

/// One heartbeat object under `nodes/`.
#[derive(Clone, Debug)]
pub struct HeartbeatEntry {
    /// The node id, from the object name.
    pub node_id: String,
    /// The services the node offers, from the object name.
    pub services: Vec<Service>,
    /// The heartbeat's age: the reader's clock minus `LastModified`.
    pub age: Duration,
    /// The heartbeat object's path.
    pub location: StorePath,
}

/// One live node, deduplicated to the youngest heartbeat per node id.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeView {
    /// The node id.
    pub node_id: String,
    /// The services the node offers.
    pub services: Vec<Service>,
    /// The age of the node's youngest heartbeat.
    pub age: Duration,
}

/// The youngest heartbeat per node id: when a node briefly has two
/// names (a role change), the youngest wins.
pub fn youngest_per_node(entries: &[HeartbeatEntry]) -> BTreeMap<&str, &HeartbeatEntry> {
    let mut by_id: BTreeMap<&str, &HeartbeatEntry> = BTreeMap::new();
    for entry in entries {
        match by_id.get(entry.node_id.as_str()) {
            Some(existing) if existing.age <= entry.age => {}
            _ => {
                by_id.insert(&entry.node_id, entry);
            }
        }
    }
    by_id
}

/// The live node set: heartbeats younger than `heartbeat_timeout`, the
/// youngest name winning when a node briefly has two (a role change).
pub fn node_view(entries: &[HeartbeatEntry], heartbeat_timeout: Duration) -> Vec<NodeView> {
    youngest_per_node(entries)
        .into_values()
        .filter(|e| e.age < heartbeat_timeout)
        .map(|e| NodeView {
            node_id: e.node_id.clone(),
            services: e.services.clone(),
            age: e.age,
        })
        .collect()
}

/// Everything a node knows about the fleet's intent after one
/// `config_poll`: the policy and the registry.
#[derive(Clone, Debug, Default)]
pub struct FleetState {
    /// The fleet policy from `sleet.toml`.
    pub config: SleetConfig,
    /// Registered databases by canonical URL.
    pub databases: BTreeMap<String, DatabaseConfig>,
    /// Registry entries that alias another, aren't canonical, or fail
    /// to parse.
    pub warnings: Vec<String>,
}

/// The `config_poll` loop's read state: last-good config plus per-file
/// ETag caches so unchanged bodies are never re-fetched.
#[derive(Default)]
pub struct ConfigPoller {
    config_etag: Option<String>,
    config: SleetConfig,
    files: HashMap<String, CachedFile>,
    /// The last successful registry view, kept whole when a LIST fails.
    /// The `files` cache cannot rebuild it: empty registry files (the
    /// common case) are never fetched, so they are never cached there.
    databases: BTreeMap<String, DatabaseConfig>,
}

struct CachedFile {
    etag: Option<String>,
    config: Option<DatabaseConfig>,
}

impl ConfigPoller {
    /// Re-read `sleet.toml` and the `dbs/` registry. Never fails: on a
    /// failed read the last good config is kept and a warning recorded.
    pub async fn poll(&mut self, root: &FleetRoot) -> FleetState {
        let mut warnings = Vec::new();
        self.poll_config(root, &mut warnings).await;
        let databases = self.poll_registry(root, &mut warnings).await;
        FleetState {
            config: self.config.clone(),
            databases,
            warnings,
        }
    }

    async fn poll_config(&mut self, root: &FleetRoot, warnings: &mut Vec<String>) {
        let path = root.config_path();
        let result = root.store().get(&path).await;
        let get = match result {
            Ok(get) => get,
            Err(object_store::Error::NotFound { .. }) => {
                // No sleet.toml: built-in defaults.
                self.config_etag = None;
                self.config = SleetConfig::default();
                return;
            }
            Err(e) => {
                warnings.push(format!(
                    "failed to read {path}: {e}; keeping last good config"
                ));
                return;
            }
        };
        let etag = get.meta.e_tag.clone();
        if etag.is_some() && etag == self.config_etag {
            return;
        }
        let body = match get.bytes().await {
            Ok(body) => body,
            Err(e) => {
                warnings.push(format!(
                    "failed to read {path}: {e}; keeping last good config"
                ));
                return;
            }
        };
        match std::str::from_utf8(&body)
            .map_err(|e| e.to_string())
            .and_then(|s| config::parse_config(s).map_err(|e| e.to_string()))
        {
            Ok(config) => {
                self.config = config;
                self.config_etag = etag;
            }
            Err(e) => {
                warnings.push(format!("invalid {path}: {e}; keeping last good config"));
            }
        }
    }

    async fn poll_registry(
        &mut self,
        root: &FleetRoot,
        warnings: &mut Vec<String>,
    ) -> BTreeMap<String, DatabaseConfig> {
        let metas = match root.list(&root.dbs_prefix()).await {
            Ok(metas) => metas,
            Err(e) => {
                warnings.push(format!("failed to list registry: {e}; keeping last good"));
                return self.databases.clone();
            }
        };

        // Decode and canonicalize every entry, then order canonical
        // spellings first so an alias never shadows the real entry.
        let mut entries = Vec::new();
        for meta in &metas {
            let Some(name) = meta.location.filename() else {
                continue;
            };
            let Some(decoded) = registry::parse_file_name(name) else {
                warnings.push(format!("ignoring non-registry object {}", meta.location));
                continue;
            };
            match registry::canonicalize_database_url(&decoded) {
                Ok(canonical) => {
                    if canonical != decoded {
                        warnings.push(format!(
                            "registry entry {name} is not canonical (decodes to \
                             {decoded:?}, canonical {canonical:?})"
                        ));
                    }
                    entries.push((canonical != decoded, name.to_string(), canonical, meta));
                }
                Err(e) => {
                    warnings.push(format!("ignoring registry entry {name}: {e}"));
                }
            }
        }
        entries.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));

        let mut databases = BTreeMap::new();
        let mut seen: HashMap<String, String> = HashMap::new();
        for (_, name, url, meta) in entries {
            if let Some(winner) = seen.get(&url) {
                warnings.push(format!(
                    "registry entries {winner} and {name} alias {url}; using {winner}"
                ));
                continue;
            }
            let db = self.file_config(root, meta, &name, warnings).await;
            seen.insert(url.clone(), name);
            databases.insert(url, db);
        }
        self.files
            .retain(|name, _| seen.values().any(|n| n == name));
        self.databases = databases.clone();
        databases
    }

    /// The effective `DatabaseConfig` for one registry file: empty files
    /// never need a GET, unchanged ETags reuse the cache, unparseable
    /// bodies keep their last good version or disable the database.
    async fn file_config(
        &mut self,
        root: &FleetRoot,
        meta: &ObjectMeta,
        name: &str,
        warnings: &mut Vec<String>,
    ) -> DatabaseConfig {
        if meta.size == 0 {
            return DatabaseConfig::default();
        }
        if let Some(cached) = self.files.get(name)
            && cached.etag.is_some()
            && cached.etag == meta.e_tag
            && let Some(config) = &cached.config
        {
            return config.clone();
        }
        let body = match root.store().get(&meta.location).await {
            Ok(get) => get.bytes().await,
            Err(e) => Err(e),
        };
        let parsed = body
            .map_err(|e| e.to_string())
            .and_then(|b| String::from_utf8(b.to_vec()).map_err(|e| e.to_string()))
            .and_then(|s| config::parse_database(&self.config, &s).map_err(|e| e.to_string()));
        match parsed {
            Ok(db) => {
                self.files.insert(
                    name.to_string(),
                    CachedFile {
                        etag: meta.e_tag.clone(),
                        config: Some(db.clone()),
                    },
                );
                db
            }
            Err(e) => {
                if let Some(cached) = self.files.get(name)
                    && let Some(config) = &cached.config
                {
                    warnings.push(format!(
                        "invalid registry entry {name}: {e}; keeping last good"
                    ));
                    return config.clone();
                }
                warnings.push(format!(
                    "invalid registry entry {name}: {e}; database disabled"
                ));
                DatabaseConfig {
                    services: Some(Vec::new()),
                    ..DatabaseConfig::default()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    fn root() -> FleetRoot {
        FleetRoot::from_parts(
            Arc::new(InMemory::new()),
            StorePath::from("fleet"),
            "memory:///fleet",
        )
    }

    async fn put(root: &FleetRoot, path: &StorePath, body: &str) {
        root.store()
            .put(path, PutPayload::from(body.to_string()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn empty_root_yields_defaults() {
        let root = root();
        let mut poller = ConfigPoller::default();
        let state = poller.poll(&root).await;
        assert!(state.databases.is_empty());
        assert!(state.warnings.is_empty());
        assert_eq!(
            state.config.node.heartbeat_interval.0,
            Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn registry_files_resolve_and_alias_warnings_fire() {
        let root = root();
        put(
            &root,
            &root.config_path(),
            "[database]\nservices = [\"gc\"]",
        )
        .await;
        // Empty file: registered with fleet-wide config, no GET needed.
        put(&root, &root.database_path("s3://b/empty"), "").await;
        // Override file.
        put(
            &root,
            &root.database_path("s3://b/tuned"),
            "services = [\"gc\", \"compaction-workers\"]",
        )
        .await;
        // Invalid file: registered but disabled.
        put(&root, &root.database_path("s3://b/broken"), "nope = 1").await;
        // Alias of s3://b/empty under a non-canonical spelling.
        let alias = StorePath::parse(format!(
            "{}/{}",
            root.dbs_prefix(),
            registry::file_name("s3://b/empty/")
        ))
        .unwrap();
        put(&root, &alias, "").await;

        let mut poller = ConfigPoller::default();
        let state = poller.poll(&root).await;

        assert_eq!(state.databases.len(), 3);
        let resolved = state
            .config
            .resolve(state.databases.get("s3://b/empty").map(|v| v as _));
        assert_eq!(resolved.services, vec![Service::Gc]);
        let tuned = state.config.resolve(state.databases.get("s3://b/tuned"));
        assert_eq!(
            tuned.services,
            vec![Service::Gc, Service::CompactionWorkers]
        );
        let broken = state.config.resolve(state.databases.get("s3://b/broken"));
        assert!(broken.services.is_empty());
        assert_eq!(state.warnings.len(), 3, "{:?}", state.warnings);
        assert!(state.warnings.iter().any(|w| w.contains("not canonical")));
        assert!(state.warnings.iter().any(|w| w.contains("disabled")));
        // The canonical spelling wins the alias collision.
        let alias_warning = state
            .warnings
            .iter()
            .find(|w| w.contains("alias"))
            .expect("alias warning");
        assert!(
            alias_warning.contains("using s3%3A%2F%2Fb%2Fempty.toml"),
            "{alias_warning}"
        );
    }

    /// The design's read-cost promise: empty registry files are never
    /// fetched, and override files are fetched only when their ETag
    /// changes.
    #[tokio::test]
    async fn poller_never_refetches_unchanged_bodies() {
        use crate::testing::{Op, TestStore};
        let store = TestStore::in_memory();
        let root = FleetRoot::from_parts(store.clone(), StorePath::from("fleet"), "memory:///f");
        put(
            &root,
            &root.config_path(),
            "[database]\nservices = [\"gc\"]",
        )
        .await;
        put(&root, &root.database_path("s3://b/empty"), "").await;
        put(&root, &root.database_path("s3://b/tuned"), "services = []").await;

        let mut poller = ConfigPoller::default();
        poller.poll(&root).await;
        let first_pass_gets = store.counters().count(Op::Get);
        // Config + the one non-empty override; the empty file costs no GET.
        assert_eq!(first_pass_gets, 2);

        poller.poll(&root).await;
        // Second pass re-reads only sleet.toml (to see its ETag); the
        // unchanged override body is served from the cache.
        assert_eq!(store.counters().count(Op::Get), first_pass_gets + 1);

        // A changed override is re-fetched once.
        put(
            &root,
            &root.database_path("s3://b/tuned"),
            "services = [\"gc\"]",
        )
        .await;
        let state = poller.poll(&root).await;
        assert_eq!(store.counters().count(Op::Get), first_pass_gets + 3);
        assert_eq!(
            state
                .config
                .resolve(state.databases.get("s3://b/tuned"))
                .services,
            vec![Service::Gc]
        );
    }

    /// An override that turns invalid keeps its last good version; a
    /// failed registry LIST keeps the whole last-good view, including
    /// empty-file databases (never body-fetched, so never in the file
    /// cache) and non-canonical entries under their canonical keys.
    #[tokio::test]
    async fn poller_keeps_last_good_per_file_and_on_list_failure() {
        use crate::testing::{Op, TestStore};
        let store = TestStore::in_memory();
        let root = FleetRoot::from_parts(store.clone(), StorePath::from("fleet"), "memory:///f");
        put(
            &root,
            &root.database_path("s3://b/db"),
            "services = [\"gc\"]",
        )
        .await;
        // Registered with an empty file: the common case.
        put(&root, &root.database_path("s3://b/empty"), "").await;
        // Registered under a non-canonical spelling.
        let noncanonical = StorePath::parse(format!(
            "{}/{}",
            root.dbs_prefix(),
            registry::file_name("s3://b/extra/")
        ))
        .unwrap();
        put(&root, &noncanonical, "").await;

        let mut poller = ConfigPoller::default();
        let state = poller.poll(&root).await;
        assert_eq!(state.databases.len(), 3);
        assert_eq!(
            state
                .config
                .resolve(state.databases.get("s3://b/db"))
                .services,
            vec![Service::Gc]
        );

        // Now corrupt the override: the last good version stays.
        put(&root, &root.database_path("s3://b/db"), "nope = 1").await;
        let state = poller.poll(&root).await;
        assert_eq!(
            state
                .config
                .resolve(state.databases.get("s3://b/db"))
                .services,
            vec![Service::Gc]
        );
        assert!(
            state
                .warnings
                .iter()
                .any(|w| w.contains("keeping last good"))
        );

        // And a failed LIST keeps the whole registry view: the override,
        // the empty-file database, and the non-canonical entry under its
        // canonical key.
        store.fail_next(Op::List, 1);
        let state = poller.poll(&root).await;
        assert_eq!(state.databases.len(), 3, "{:?}", state.databases.keys());
        for db in ["s3://b/db", "s3://b/empty", "s3://b/extra"] {
            assert!(state.databases.contains_key(db), "{db} missing");
        }
        assert_eq!(
            state
                .config
                .resolve(state.databases.get("s3://b/db"))
                .services,
            vec![Service::Gc]
        );
        assert!(state.warnings.iter().any(|w| w.contains("failed to list")));
    }

    #[tokio::test]
    async fn bad_fleet_config_keeps_last_good() {
        let root = root();
        put(
            &root,
            &root.config_path(),
            "[database]\nservices = [\"gc\"]",
        )
        .await;
        let mut poller = ConfigPoller::default();
        let state = poller.poll(&root).await;
        assert_eq!(state.config.resolve(None).services, vec![Service::Gc]);

        put(&root, &root.config_path(), "not toml [").await;
        let state = poller.poll(&root).await;
        assert_eq!(state.config.resolve(None).services, vec![Service::Gc]);
        assert!(state.warnings.iter().any(|w| w.contains("last good")));
    }

    #[tokio::test]
    async fn node_view_dedups_youngest_and_drops_dead() {
        let entries = vec![
            HeartbeatEntry {
                node_id: "a".into(),
                services: vec![Service::Gc],
                age: Duration::from_secs(5),
                location: StorePath::from("nodes/a.g.json"),
            },
            HeartbeatEntry {
                node_id: "a".into(),
                services: Service::ALL.to_vec(),
                age: Duration::from_secs(2),
                location: StorePath::from("nodes/a.cgw.json"),
            },
            HeartbeatEntry {
                node_id: "dead".into(),
                services: Service::ALL.to_vec(),
                age: Duration::from_secs(120),
                location: StorePath::from("nodes/dead.cgw.json"),
            },
        ];
        let view = node_view(&entries, Duration::from_secs(30));
        assert_eq!(view.len(), 1);
        assert_eq!(view[0].node_id, "a");
        assert_eq!(view[0].services, Service::ALL.to_vec());
    }
}
