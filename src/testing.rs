//! Test support: the seams the chaos and DST test suites need.
//!
//! `TestStore` decorates any `ObjectStore` with per-operation counters,
//! deterministic fault injection, and, when given a `TestClock`, a
//! simulated `LastModified`, so heartbeat ages follow virtual time
//! instead of the wall clock. `TestClock` also implements
//! `crate::root::Clock` for injection into `FleetRoot`.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::root::Clock;

/// A controllable clock: `now()` returns virtual time, advanced
/// explicitly by tests.
pub struct TestClock(Mutex<DateTime<Utc>>);

impl TestClock {
    /// A clock reading `start`.
    pub fn new(start: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(start)))
    }

    /// Advance virtual time by `by`.
    pub fn advance(&self, by: std::time::Duration) {
        let mut now = self.0.lock().expect("clock lock");
        *now += chrono::Duration::from_std(by).expect("duration fits");
    }

    /// Set virtual time to `to`.
    pub fn set(&self, to: DateTime<Utc>) {
        *self.0.lock().expect("clock lock") = to;
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("clock lock")
    }
}

/// Coarse operation classes for counting and fault injection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// GET, including ranged reads.
    Get,
    /// PUT, including multipart uploads.
    Put,
    /// LIST, in all its variants.
    List,
    /// DELETE.
    Delete,
    /// Server-side copy.
    Copy,
}

/// Per-operation call counters.
#[derive(Default)]
pub struct Counters {
    get: AtomicU64,
    put: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
}

impl Counters {
    /// Calls of `op` so far, including failed ones.
    pub fn count(&self, op: Op) -> u64 {
        self.cell(op).load(Ordering::SeqCst)
    }

    fn bump(&self, op: Op) {
        self.cell(op).fetch_add(1, Ordering::SeqCst);
    }

    fn cell(&self, op: Op) -> &AtomicU64 {
        match op {
            Op::Get => &self.get,
            Op::Put => &self.put,
            Op::List => &self.list,
            Op::Delete => &self.delete,
            Op::Copy => &self.copy,
        }
    }
}

#[derive(Default)]
struct Faults {
    /// Fail the next N calls of an op.
    next: HashMap<Op, u64>,
    /// Fail every call of these ops until healed.
    always: HashMap<Op, bool>,
    /// Fail any op with this probability, from a seeded xorshift.
    probability: Option<(f64, u64)>,
    /// Sleep this long before each call of an op (async methods only;
    /// list streams are not delayed).
    latency: HashMap<Op, std::time::Duration>,
}

/// An `ObjectStore` decorator with counters, deterministic faults, and
/// optional simulated `LastModified`.
pub struct TestStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<Counters>,
    faults: Arc<Mutex<Faults>>,
    clock: Option<Arc<TestClock>>,
    times: Arc<Mutex<HashMap<Path, DateTime<Utc>>>>,
}

impl TestStore {
    /// Decorate an existing store.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            counters: Arc::default(),
            faults: Arc::default(),
            clock: None,
            times: Arc::default(),
        })
    }

    /// A fresh in-memory store.
    pub fn in_memory() -> Arc<Self> {
        Self::new(Arc::new(object_store::memory::InMemory::new()))
    }

    /// A fresh in-memory store whose `LastModified` follows `clock`.
    pub fn in_memory_at(clock: Arc<TestClock>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            counters: Arc::default(),
            faults: Arc::default(),
            clock: Some(clock),
            times: Arc::default(),
        })
    }

    /// The per-operation call counters.
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Fail the next `n` calls of `op`.
    pub fn fail_next(&self, op: Op, n: u64) {
        *self
            .faults
            .lock()
            .expect("faults")
            .next
            .entry(op)
            .or_default() += n;
    }

    /// Fail every call of `op` until `heal`.
    pub fn fail_all(&self, op: Op) {
        self.faults.lock().expect("faults").always.insert(op, true);
    }

    /// Fail any operation with probability `p`, deterministically from
    /// `seed`.
    pub fn fail_probability(&self, p: f64, seed: u64) {
        self.faults.lock().expect("faults").probability = Some((p, seed.max(1)));
    }

    /// Sleep `by` before every call of `op`, so tests can hold an
    /// operation in flight (e.g. a copy that outlives a pin lifetime).
    pub fn set_latency(&self, op: Op, by: std::time::Duration) {
        self.faults.lock().expect("faults").latency.insert(op, by);
    }

    /// Clear all fault injection.
    pub fn heal(&self) {
        *self.faults.lock().expect("faults") = Faults::default();
    }

    fn latency(&self, op: Op) -> Option<std::time::Duration> {
        self.faults
            .lock()
            .expect("faults")
            .latency
            .get(&op)
            .copied()
    }

    fn check(&self, op: Op) -> Result<(), object_store::Error> {
        self.counters.bump(op);
        let mut faults = self.faults.lock().expect("faults");
        if faults.always.get(&op).copied().unwrap_or(false) {
            return Err(fault(op));
        }
        if let Some(n) = faults.next.get_mut(&op)
            && *n > 0
        {
            *n -= 1;
            return Err(fault(op));
        }
        if let Some((p, seed)) = &mut faults.probability {
            // xorshift64: deterministic per seed, independent of order
            // of ops only in the sense of a fixed sequence.
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            let roll = (*seed >> 11) as f64 / (1u64 << 53) as f64;
            if roll < *p {
                return Err(fault(op));
            }
        }
        Ok(())
    }

    fn stamp(&self, location: &Path) {
        if let Some(clock) = &self.clock {
            self.times
                .lock()
                .expect("times")
                .insert(location.clone(), clock.now());
        }
    }

    /// Override one object's simulated `LastModified`, so a single
    /// heartbeat can read as stale while the rest stay fresh. Only
    /// meaningful on stores built with a `TestClock`.
    pub fn set_modified(&self, location: &Path, time: DateTime<Utc>) {
        self.times
            .lock()
            .expect("times")
            .insert(location.clone(), time);
    }

    fn restamp(&self, mut meta: ObjectMeta) -> ObjectMeta {
        if self.clock.is_some()
            && let Some(time) = self.times.lock().expect("times").get(&meta.location)
        {
            meta.last_modified = *time;
        }
        meta
    }
}

fn fault(op: Op) -> object_store::Error {
    object_store::Error::Generic {
        store: "TestStore",
        source: format!("injected {op:?} fault").into(),
    }
}

impl fmt::Display for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore({})", self.inner)
    }
}

impl fmt::Debug for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore({:?})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for TestStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check(Op::Put)?;
        if let Some(by) = self.latency(Op::Put) {
            tokio::time::sleep(by).await;
        }
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.stamp(location);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check(Op::Put)?;
        let result = self.inner.put_multipart_opts(location, opts).await?;
        self.stamp(location);
        Ok(result)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.check(Op::Get)?;
        if let Some(by) = self.latency(Op::Get) {
            tokio::time::sleep(by).await;
        }
        let mut result = self.inner.get_opts(location, options).await?;
        result.meta = self.restamp(result.meta);
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.check(Op::Get)?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        if let Err(e) = self.check(Op::Delete) {
            return futures::stream::once(async move { Err(e) }).boxed();
        }
        let times = self.times.clone();
        self.inner
            .delete_stream(locations)
            .map_ok(move |path| {
                times.lock().expect("times").remove(&path);
                path
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(e) = self.check(Op::List) {
            return futures::stream::once(async move { Err(e) }).boxed();
        }
        let times: HashMap<Path, DateTime<Utc>> = if self.clock.is_some() {
            self.times.lock().expect("times").clone()
        } else {
            HashMap::new()
        };
        self.inner
            .list(prefix)
            .map_ok(move |mut meta| {
                if let Some(time) = times.get(&meta.location) {
                    meta.last_modified = *time;
                }
                meta
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Err(e) = self.check(Op::List) {
            return futures::stream::once(async move { Err(e) }).boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.check(Op::List)?;
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects = result
            .objects
            .into_iter()
            .map(|meta| self.restamp(meta))
            .collect();
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check(Op::Copy)?;
        self.inner.copy_opts(from, to, options).await?;
        self.stamp(to);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn counts_and_faults() {
        let store = TestStore::in_memory();
        let path = Path::from("x");
        store.put(&path, PutPayload::from("v")).await.unwrap();
        assert_eq!(store.counters().count(Op::Put), 1);

        store.fail_next(Op::Get, 1);
        assert!(store.get(&path).await.is_err());
        assert!(store.get(&path).await.is_ok());
        assert_eq!(store.counters().count(Op::Get), 2);

        store.fail_all(Op::List);
        assert!(store.list(None).try_collect::<Vec<_>>().await.is_err());
        store.heal();
        assert_eq!(
            store
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn sim_clock_stamps_last_modified() {
        let clock = TestClock::new(Utc::now());
        let store = TestStore::in_memory_at(clock.clone());
        let path = Path::from("hb");
        store.put(&path, PutPayload::from("v")).await.unwrap();
        let t0 = clock.now();

        clock.advance(std::time::Duration::from_secs(120));
        let metas: Vec<ObjectMeta> = store.list(None).try_collect().await.unwrap();
        assert_eq!(metas[0].last_modified, t0, "age follows virtual time");

        store.put(&path, PutPayload::from("v2")).await.unwrap();
        let metas: Vec<ObjectMeta> = store.list(None).try_collect().await.unwrap();
        assert_eq!(metas[0].last_modified, clock.now());
    }
}
