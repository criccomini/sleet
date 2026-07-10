//! Supported programmatic API for controlling a Sleet fleet.

use std::collections::HashSet;

use tokio_util::sync::CancellationToken;

use crate::config::Service;
use crate::daemon::NodeOptions;
use crate::mirror::RestorePoint;
use crate::response::{
    MirrorRestoreResponse, MirrorSyncResponse, RegisterResponse, StatusResponse,
};
use crate::root::FleetRoot;
use crate::{daemon, ops};

/// A fleet root opened with object-store settings from the process
/// environment.
#[derive(Clone)]
pub struct Fleet {
    root: FleetRoot,
}

impl Fleet {
    /// Open a fleet root URL. Object-store credentials and provider
    /// options come from the process environment.
    pub fn open(url: &str) -> Result<Self, Error> {
        Ok(Self {
            root: FleetRoot::open(url)?,
        })
    }

    /// Construct a facade over an existing root. This is an unstable
    /// seam for tests and specialized embedding.
    #[doc(hidden)]
    pub fn from_root(root: FleetRoot) -> Self {
        Self { root }
    }

    /// The canonical fleet root URL.
    pub fn url(&self) -> &str {
        self.root.url()
    }

    /// Register a database with a create-only registry write.
    pub async fn register(&self, database_url: &str) -> Result<RegisterResponse, Error> {
        Ok(ops::register(&self.root, database_url).await?)
    }

    /// Derive fleet status from object storage.
    pub async fn status(&self, options: StatusOptions) -> Result<StatusResponse, Error> {
        Ok(ops::status(&self.root, options.compactions, options.mirrors).await?)
    }

    /// Run one mirror sync pass for a registered database and target.
    pub async fn sync_mirror(
        &self,
        database_url: &str,
        target: &str,
        options: MirrorSyncOptions,
    ) -> Result<MirrorSyncResponse, Error> {
        Ok(ops::mirror_sync(&self.root, database_url, target, options.rclone.as_deref()).await?)
    }

    /// Run a fleet node until `shutdown` is cancelled. The caller owns
    /// the Tokio runtime, signal handling, and tracing subscriber.
    pub async fn run_node(
        &self,
        options: NodeOptions,
        shutdown: CancellationToken,
    ) -> Result<(), Error> {
        let mut seen = HashSet::new();
        if let Some(duplicate) = options
            .services
            .iter()
            .find(|service| !seen.insert(**service))
        {
            return Err(Error::DuplicateService {
                service: *duplicate,
            });
        }
        daemon::run(self.root.clone(), options, shutdown).await?;
        Ok(())
    }
}

/// Restore a backup URL into an empty destination URL.
pub async fn mirror_restore(
    backup_url: &str,
    destination_url: &str,
    point: RestorePoint,
) -> Result<MirrorRestoreResponse, Error> {
    Ok(ops::mirror_restore(backup_url, destination_url, point).await?)
}

/// Optional, more expensive status probes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatusOptions {
    compactions: bool,
    mirrors: bool,
}

impl StatusOptions {
    /// Include each database's compaction queue depth.
    pub fn with_compactions(mut self, enabled: bool) -> Self {
        self.compactions = enabled;
        self
    }

    /// Include mirror source and destination lag.
    pub fn with_mirrors(mut self, enabled: bool) -> Self {
        self.mirrors = enabled;
        self
    }
}

/// Node-local options for a one-shot mirror sync.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MirrorSyncOptions {
    rclone: Option<String>,
}

impl MirrorSyncOptions {
    /// Set the rclone binary used by a target configured with the
    /// rclone copier.
    pub fn with_rclone(mut self, rclone: impl Into<String>) -> Self {
        self.rclone = Some(rclone.into());
        self
    }
}

/// A supported API operation failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The fleet root URL was rejected.
    #[error("invalid fleet root: {0}")]
    FleetRootUrl(#[source] crate::registry::UrlError),
    /// The fleet root's object store could not be built.
    #[error("failed to open fleet root store: {0}")]
    FleetRootStore(#[source] object_store::Error),
    /// A database or destination URL was rejected.
    #[error(transparent)]
    Url(#[from] crate::registry::UrlError),
    /// An object-store operation failed.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    /// The database is not registered.
    #[error("{url} is not registered; `sleet register` it first")]
    NotRegistered {
        /// The canonical database URL.
        url: String,
    },
    /// No enabled mirror target of that name applies.
    #[error("no enabled mirror target {target:?} applies to {url}")]
    NoSuchMirrorTarget {
        /// The requested target.
        target: String,
        /// The canonical database URL.
        url: String,
    },
    /// A database service could not be opened or run.
    #[error(transparent)]
    Service(#[from] crate::services::ServiceError),
    /// A mirror protocol operation failed.
    #[error(transparent)]
    Mirror(#[from] crate::mirror::MirrorError),
    /// The node id is not valid for a heartbeat object name.
    #[error("invalid node id: {0}")]
    InvalidNodeId(String),
    /// A node's offered service list contains a duplicate.
    #[error("services lists {name:?} more than once", name = service.as_str())]
    DuplicateService {
        /// The repeated service.
        service: Service,
    },
}

impl From<crate::root::OpenError> for Error {
    fn from(error: crate::root::OpenError) -> Self {
        match error {
            crate::root::OpenError::Url(source) => Self::FleetRootUrl(source),
            crate::root::OpenError::Store(source) => Self::FleetRootStore(source),
        }
    }
}

impl From<ops::OpsError> for Error {
    fn from(error: ops::OpsError) -> Self {
        match error {
            ops::OpsError::Url(source) => Self::Url(source),
            ops::OpsError::Store(source) => Self::Store(source),
            ops::OpsError::NotRegistered { url } => Self::NotRegistered { url },
            ops::OpsError::NoSuchTarget { target, url } => Self::NoSuchMirrorTarget { target, url },
            ops::OpsError::Service(source) => Self::Service(source),
            ops::OpsError::Mirror(source) => Self::Mirror(source),
        }
    }
}

impl From<daemon::DaemonError> for Error {
    fn from(error: daemon::DaemonError) -> Self {
        match error {
            daemon::DaemonError::NodeId(reason) => Self::InvalidNodeId(reason),
            daemon::DaemonError::Root(source) => source.into(),
        }
    }
}
