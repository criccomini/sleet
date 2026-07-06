//! Model-based testing for the mirror sync protocol: fizzbee-mbt
//! drives action sequences from specs/mirror-mbt.fizz against the
//! real pass and prune over in-memory stores, comparing every
//! action's return with the model's.
//!
//! The adapter maps the spec's whole-pass granularity onto the public
//! API: `Pass` is one `mirror::sync_pass` (its return encodes
//! caught-up versus commit, the manifests committed, and whether data
//! copied), `Prune` is one `mirror::prune::prune_at` with a
//! far-future now so every age has lapsed (its return encodes
//! manifests deleted and kept), and the churn actions run a real
//! writer, real checkpoints, and real source GC. Guards and budgets
//! mirror the spec's; a disabled action returns `None` without
//! touching the stores.
//!
//! Gated on `SLEET_MBT_MIRROR` (not `SLEET_MBT`: the two MBT tests
//! need different served state spaces and share the plugin socket, so
//! they must run against different server instances).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use fizzbee_mbt::config::TestOptions;
use fizzbee_mbt::error::MbtError;
use fizzbee_mbt::traits::*;
use fizzbee_mbt::types::{Arg, RoleId};
use fizzbee_mbt::value::Value;
use object_store::path::Path as StorePath;
use sleet::config::{ResolvedGc, ResolvedMirrorTarget};
use sleet::mirror;
use sleet::services::{self, DatabaseHandle};
use tokio::sync::Mutex;

/// The spec's actions, one method per action (all top-level).
#[async_trait]
pub trait MirrorModel: Model {
    async fn action_writercommit(&self, args: &[Arg]) -> Result<Value, MbtError>;
    async fn action_operatorcheckpoint(&self, args: &[Arg]) -> Result<Value, MbtError>;
    async fn action_deleteoperatorcheckpoint(&self, args: &[Arg]) -> Result<Value, MbtError>;
    async fn action_gcsweep(&self, args: &[Arg]) -> Result<Value, MbtError>;
    async fn action_pass(&self, args: &[Arg]) -> Result<Value, MbtError>;
    async fn action_prune(&self, args: &[Arg]) -> Result<Value, MbtError>;
}

/// Routes actions from the MBT framework to the model adapter. The
/// spec has no roles: every action is top-level.
pub struct MirrorActionDispatcher<T>
where
    T: MirrorModel + Send + Sync + 'static,
{
    model: Option<T>,
}

impl<T> MirrorActionDispatcher<T>
where
    T: MirrorModel + Send + Sync + 'static,
{
    pub fn new(model: T) -> Self {
        MirrorActionDispatcher { model: Some(model) }
    }
}

#[async_trait]
impl<T> Model for MirrorActionDispatcher<T>
where
    T: Model + MirrorModel + Send + Sync + 'static,
{
    async fn init(&mut self) -> Result<(), MbtError> {
        self.model
            .as_mut()
            .ok_or_else(|| MbtError::other("Model not found"))?
            .init()
            .await
    }

    async fn cleanup(&mut self) -> Result<(), MbtError> {
        self.model
            .as_mut()
            .ok_or_else(|| MbtError::other("Model not found"))?
            .cleanup()
            .await
    }
}

#[async_trait]
impl<T> DispatchModel for MirrorActionDispatcher<T>
where
    T: MirrorModel,
    T: Send + Sync + 'static,
{
    async fn execute(
        &self,
        role_id: &RoleId,
        function_name: &str,
        args: &[Arg],
    ) -> Result<Value, MbtError> {
        let model = self
            .model
            .as_ref()
            .ok_or_else(|| MbtError::other("Model is not initialized"))?;
        match role_id.role_name.as_str() {
            "" => match function_name {
                "WriterCommit" => model.action_writercommit(args).await,
                "OperatorCheckpoint" => model.action_operatorcheckpoint(args).await,
                "DeleteOperatorCheckpoint" => model.action_deleteoperatorcheckpoint(args).await,
                "GcSweep" => model.action_gcsweep(args).await,
                "Pass" => model.action_pass(args).await,
                "Prune" => model.action_prune(args).await,
                _ => Err(MbtError::other(format!(
                    "Unknown top-level action: '{function_name}'"
                ))),
            },
            other => Err(MbtError::other(format!("Unknown role: {other}"))),
        }
    }

    fn get_roles(&self) -> Result<Vec<RoleId>, MbtError> {
        Ok(Vec::new())
    }
}

/// One source/destination pair under test, with the spec's budgets
/// mirrored so guards agree.
struct Harness {
    source: DatabaseHandle,
    dest: DatabaseHandle,
    settings: ResolvedMirrorTarget,
    gc: ResolvedGc,
    op_cps: Vec<uuid::Uuid>,
    batch: u32,
    next_cp_name: u32,
    writes_left: u32,
    op_creates_left: u32,
    op_deletes_left: u32,
    gcs_left: u32,
    passes_left: u32,
    prunes_left: u32,
}

impl Harness {
    async fn new() -> Result<Self, MbtError> {
        let source = DatabaseHandle::from_parts(
            "memory:///src",
            Arc::new(object_store::memory::InMemory::new()),
            StorePath::from("src"),
        );
        let dest = DatabaseHandle::from_parts(
            "memory:///dst",
            Arc::new(object_store::memory::InMemory::new()),
            StorePath::from("dst"),
        );
        let settings = ResolvedMirrorTarget {
            keep: Some(Duration::from_millis(1)),
            ..ResolvedMirrorTarget::default()
        };
        let mut gc = ResolvedGc::default();
        for dir in [
            &mut gc.manifest,
            &mut gc.wal,
            &mut gc.compacted,
            &mut gc.compactions,
        ] {
            dir.min_age = Duration::ZERO;
        }
        gc.wal_fence.enabled = false;
        gc.detach.enabled = false;
        let mut harness = Self {
            source,
            dest,
            settings,
            gc,
            op_cps: Vec::new(),
            batch: 0,
            next_cp_name: 0,
            writes_left: 2,
            op_creates_left: 1,
            op_deletes_left: 1,
            gcs_left: 2,
            passes_left: 4,
            prunes_left: 1,
        };
        // Genesis mirrors the spec's Init: one seeded write.
        harness.write_batch().await?;
        Ok(harness)
    }

    /// One writer flush: open a real writer, put a batch, flush the
    /// memtable, close. Opening per action keeps slatedb's background
    /// tasks from mutating the source between actions.
    async fn write_batch(&mut self) -> Result<(), MbtError> {
        let writer = slatedb::Db::builder(self.source.path.clone(), self.source.store.clone())
            .with_settings(slatedb::config::Settings {
                compactor_options: None,
                garbage_collector_options: None,
                ..Default::default()
            })
            .build()
            .await
            .map_err(MbtError::from_err)?;
        writer
            .put(format!("k-{}", self.batch).as_bytes(), b"v")
            .await
            .map_err(MbtError::from_err)?;
        self.batch += 1;
        writer
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .map_err(MbtError::from_err)?;
        writer.close().await.map_err(MbtError::from_err)?;
        Ok(())
    }
}

pub struct MirrorModelAdapter {
    harness: Option<Arc<Mutex<Harness>>>,
}

#[async_trait]
impl MirrorModel for MirrorModelAdapter {
    async fn action_writercommit(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness().lock().await;
        if h.writes_left == 0 {
            return Ok(Value::None);
        }
        h.writes_left -= 1;
        h.write_batch().await?;
        Ok(Value::Str("committed".to_string()))
    }

    async fn action_operatorcheckpoint(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness().lock().await;
        if h.op_creates_left == 0 {
            return Ok(Value::None);
        }
        h.op_creates_left -= 1;
        let name = format!("op-{}", h.next_cp_name);
        h.next_cp_name += 1;
        let result = h
            .source
            .admin
            .create_detached_checkpoint(&slatedb::config::CheckpointOptions {
                lifetime: None,
                source: None,
                name: Some(name),
            })
            .await
            .map_err(MbtError::from_err)?;
        h.op_cps.push(result.id);
        Ok(Value::Str("created".to_string()))
    }

    async fn action_deleteoperatorcheckpoint(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness().lock().await;
        if h.op_deletes_left == 0 || h.op_cps.is_empty() {
            return Ok(Value::None);
        }
        h.op_deletes_left -= 1;
        let victim = h.op_cps.remove(0);
        h.source
            .admin
            .delete_checkpoint(victim)
            .await
            .map_err(MbtError::from_err)?;
        Ok(Value::Str("deleted".to_string()))
    }

    async fn action_gcsweep(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness().lock().await;
        if h.gcs_left == 0 {
            return Ok(Value::None);
        }
        h.gcs_left -= 1;
        let options = services::gc_options(&h.gc);
        h.source
            .admin
            .run_gc_once(options)
            .await
            .map_err(MbtError::from_err)?;
        Ok(Value::Str("swept".to_string()))
    }

    /// One whole sync pass; the return must reproduce the model's
    /// decision: caught-up, or commit with the same manifest count
    /// and the same copied-data verdict.
    async fn action_pass(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness().lock().await;
        if h.passes_left == 0 {
            return Ok(Value::None);
        }
        h.passes_left -= 1;
        let outcome = mirror::sync_pass(&h.source, &h.dest, "dr", &h.settings, None)
            .await
            .map_err(MbtError::from_err)?;
        if !outcome.committed {
            return Ok(Value::Str("caught-up".to_string()));
        }
        Ok(Value::Str(format!(
            "commit:{}:{}",
            outcome.manifests_committed,
            if outcome.copied.objects > 0 {
                "data"
            } else {
                "nodata"
            }
        )))
    }

    /// One whole prune with every age lapsed; the return must
    /// reproduce the model's kept and deleted manifest counts.
    async fn action_prune(&self, _args: &[Arg]) -> Result<Value, MbtError> {
        let mut h = self.harness().lock().await;
        if h.prunes_left == 0 {
            return Ok(Value::None);
        }
        h.prunes_left -= 1;
        let far_future = Utc::now() + chrono::Duration::days(3650);
        let report = mirror::prune::prune_at(&h.source, &h.dest, "dr", &h.settings, far_future)
            .await
            .map_err(MbtError::from_err)?;
        if report.kept_manifests == 0 {
            return Ok(Value::Str("empty".to_string()));
        }
        Ok(Value::Str(format!(
            "deleted:{}:kept:{}",
            report.deleted_manifests, report.kept_manifests
        )))
    }
}

impl MirrorModelAdapter {
    fn harness(&self) -> &Arc<Mutex<Harness>> {
        self.harness.as_ref().expect("initialized")
    }
}

#[async_trait]
impl Model for MirrorModelAdapter {
    async fn init(&mut self) -> Result<(), MbtError> {
        self.harness = Some(Arc::new(Mutex::new(Harness::new().await?)));
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), MbtError> {
        self.harness = None;
        Ok(())
    }
}

pub fn new_mirror_model() -> MirrorModelAdapter {
    MirrorModelAdapter { harness: None }
}

/// Test volume: sequential only (one shared harness per run); depth
/// matches the spec's max_actions.
fn get_mirror_test_options() -> TestOptions {
    TestOptions {
        max_seq_runs: Some(300),
        max_parallel_runs: Some(0),
        max_actions: Some(8),
    }
}

#[test]
fn mirror_model_replays_on_the_real_code() -> Result<(), MbtError> {
    if std::env::var_os("SLEET_MBT_MIRROR").is_none() {
        eprintln!("note: SLEET_MBT_MIRROR unset; skipping mirror model-based test");
        return Ok(());
    }
    let dispatcher = MirrorActionDispatcher::new(new_mirror_model());
    fizzbee_mbt::run_mbt_test(dispatcher, get_mirror_test_options())?;
    Ok(())
}
