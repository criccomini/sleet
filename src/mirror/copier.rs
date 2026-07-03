//! Data-object copiers (DESIGN-MIRROR §8). `copier` selects who moves
//! `wal/` and `compacted/` objects; sleet always commits manifests
//! itself, so no copier ever touches `manifest/`.

use std::sync::atomic::{AtomicU64, Ordering};

use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStoreExt, WriteMultipart};
use tracing::debug;

use super::MirrorError;
use super::layout::{self, object_path};
use crate::config::{CopierKind, ResolvedMirrorTarget};
use crate::services::DatabaseHandle;

/// Objects at or below this size are copied with a single PUT; larger
/// ones stream through a multipart upload.
const MULTIPART_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Concurrent HEADs while the external copier probes the target.
const HEAD_PARALLELISM: usize = 16;

/// What one copy call moved.
#[derive(Clone, Copy, Debug, Default)]
pub struct Copied {
    /// Objects copied.
    pub objects: u64,
    /// Bytes copied; zero for rclone, which does not report per-object
    /// sizes back.
    pub bytes: u64,
}

/// A copier bound to one `(source, destination)` pair.
pub struct Copier<'a> {
    kind: CopierKind,
    parallelism: usize,
    rclone: Option<String>,
    source: &'a DatabaseHandle,
    dest: &'a DatabaseHandle,
}

impl<'a> Copier<'a> {
    /// A copier for one pass.
    pub fn new(
        settings: &ResolvedMirrorTarget,
        rclone: Option<&str>,
        source: &'a DatabaseHandle,
        dest: &'a DatabaseHandle,
    ) -> Self {
        Self {
            kind: settings.copier,
            parallelism: settings.copy_parallelism.max(1) as usize,
            rclone: rclone.map(String::from),
            source,
            dest,
        }
    }

    /// Narrow the compacted candidate list to what this copier must
    /// move. The builtin and rclone copiers copy candidates outright
    /// (names are unique and content immutable, so a re-copy is
    /// harmless); the external copier HEADs each one and backfills only
    /// the misses, or LISTs the target once when seeding an empty
    /// watermark.
    pub async fn plan_compacted(
        &self,
        candidates: Vec<String>,
        seeding: bool,
    ) -> Result<Vec<String>, MirrorError> {
        match self.kind {
            CopierKind::Builtin | CopierKind::Rclone => Ok(candidates),
            CopierKind::External => {
                if seeding {
                    let present: std::collections::BTreeSet<String> =
                        layout::list_compacted(self.dest)
                            .await?
                            .into_iter()
                            .map(|(ulid, _)| ulid)
                            .collect();
                    Ok(candidates
                        .into_iter()
                        .filter(|ulid| !present.contains(ulid))
                        .collect())
                } else {
                    let dest = self.dest;
                    let misses: Vec<Option<String>> = futures::stream::iter(candidates)
                        .map(|ulid| async move {
                            let path = object_path(dest, &layout::compacted_rel(&ulid));
                            match dest.store.head(&path).await {
                                Ok(_) => Ok(None),
                                Err(object_store::Error::NotFound { .. }) => Ok(Some(ulid)),
                                Err(e) => Err(MirrorError::from(e)),
                            }
                        })
                        .buffer_unordered(HEAD_PARALLELISM)
                        .try_collect()
                        .await?;
                    Ok(misses.into_iter().flatten().collect())
                }
            }
        }
    }

    /// Copy the given relative object names from source to destination.
    pub async fn copy(&self, names: &[String]) -> Result<Copied, MirrorError> {
        if names.is_empty() {
            return Ok(Copied::default());
        }
        match self.kind {
            // The external copier backfills its misses through the
            // builtin path.
            CopierKind::Builtin | CopierKind::External => self.copy_builtin(names).await,
            CopierKind::Rclone => self.copy_rclone(names).await,
        }
    }

    async fn copy_builtin(&self, names: &[String]) -> Result<Copied, MirrorError> {
        let bytes = AtomicU64::new(0);
        futures::stream::iter(names)
            .map(Ok)
            .try_for_each_concurrent(self.parallelism, |name| {
                let bytes = &bytes;
                async move {
                    let copied = copy_object(self.source, self.dest, name).await?;
                    bytes.fetch_add(copied, Ordering::Relaxed);
                    debug!(object = %name, bytes = copied, "copied");
                    Ok::<(), MirrorError>(())
                }
            })
            .await?;
        Ok(Copied {
            objects: names.len() as u64,
            bytes: bytes.into_inner(),
        })
    }

    /// Drive `rclone copy --files-from` over the data directories.
    /// rclone never touches `manifest/`: the list is data objects only.
    async fn copy_rclone(&self, names: &[String]) -> Result<Copied, MirrorError> {
        let rclone = self.rclone.as_deref().unwrap_or("rclone");
        let source = rclone_remote(&self.source.url)?;
        let dest = rclone_remote(&self.dest.url)?;
        let list = std::env::temp_dir().join(format!(
            "sleet-rclone-{}-{:x}.txt",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        tokio::fs::write(&list, names.join("\n") + "\n")
            .await
            .map_err(|e| MirrorError::Rclone(format!("failed to write files-from list: {e}")))?;
        let output = tokio::process::Command::new(rclone)
            .arg("copy")
            .arg("--files-from")
            .arg(&list)
            .arg(&source)
            .arg(&dest)
            .kill_on_drop(true)
            .output()
            .await;
        let _ = tokio::fs::remove_file(&list).await;
        let output =
            output.map_err(|e| MirrorError::Rclone(format!("failed to run {rclone:?}: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let tail: String = stderr
                .lines()
                .rev()
                .take(5)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("; ");
            return Err(MirrorError::Rclone(format!(
                "{rclone} exited with {}: {tail}",
                output.status
            )));
        }
        Ok(Copied {
            objects: names.len() as u64,
            bytes: 0,
        })
    }
}

/// Copy one object by relative name, streaming large ones through a
/// multipart upload.
async fn copy_object(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    rel: &str,
) -> Result<u64, MirrorError> {
    let get = source.store.get(&object_path(source, rel)).await?;
    let size = get.meta.size;
    let to = object_path(dest, rel);
    if size <= MULTIPART_THRESHOLD {
        let bytes = get.bytes().await?;
        dest.store.put(&to, bytes.into()).await?;
    } else {
        let upload = dest.store.put_multipart(&to).await?;
        let mut write = WriteMultipart::new(upload);
        let mut stream = get.into_stream();
        while let Some(chunk) = stream.try_next().await? {
            write.wait_for_capacity(8).await?;
            write.write(&chunk);
        }
        write.finish().await?;
    }
    Ok(size)
}

/// The rclone remote spec for an object-store URL, using rclone's
/// connection-string backends so no named remote configuration is
/// needed; credentials come from the environment, matching how sleet
/// itself opens stores.
pub fn rclone_remote(url: &str) -> Result<String, MirrorError> {
    let parsed = url::Url::parse(url)
        .map_err(|_| MirrorError::Rclone(format!("cannot map {url:?} to an rclone remote")))?;
    let host = parsed.host_str().unwrap_or_default();
    let path = parsed.path().trim_start_matches('/');
    let backend = match parsed.scheme() {
        "s3" | "s3a" => ":s3,env_auth",
        "gs" => ":gcs,env_auth",
        "az" | "azure" | "abfs" | "abfss" | "adl" => ":azureblob,env_auth",
        "file" => {
            return Ok(parsed
                .to_file_path()
                .map_err(|_| MirrorError::Rclone(format!("bad file URL {url:?}")))?
                .display()
                .to_string());
        }
        other => {
            return Err(MirrorError::Rclone(format!(
                "no rclone backend for scheme {other:?}"
            )));
        }
    };
    Ok(format!("{backend}:{host}/{path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rclone_remotes_map_by_scheme() {
        assert_eq!(
            rclone_remote("s3://bucket/a/b").unwrap(),
            ":s3,env_auth:bucket/a/b"
        );
        assert_eq!(
            rclone_remote("gs://bucket/db").unwrap(),
            ":gcs,env_auth:bucket/db"
        );
        assert_eq!(
            rclone_remote("az://container/db").unwrap(),
            ":azureblob,env_auth:container/db"
        );
        assert_eq!(rclone_remote("file:///tmp/db").unwrap(), "/tmp/db");
        assert!(rclone_remote("memory:///db").is_err());
    }
}
