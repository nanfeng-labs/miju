#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
use futures_core::{Stream, stream::BoxStream};
use futures_util::{
    StreamExt,
    future::{Ready, ready},
    task::AtomicWaker,
};
use miju_codec::json::JsonCodec;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{self, BufRead, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    num::ParseIntError,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex, RwLock, Weak},
    task::{Context, Poll},
    time::Duration,
};
use tempfile::{NamedTempFile, TempPath};

pub use crate::error::FileStorageError;

pub use self::util::RawTask;
use miju_core::{
    backend::{
        Backend, BackendConfig, TaskResult, TryNewBackend, WaitForCompletion, WireFormatBackend,
        finalize::Durable,
    },
    error::BoxDynError,
    features_table,
    task::{
        ExecutionContext, Task,
        builder::TaskBuilder,
        metadata::MetadataStore,
        status::Status,
        task_id::{RandomId, TaskId},
    },
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use std::io::BufReader;

use fd_lock::RwLock as FileLock;

mod adapter;
mod error;
mod shared;
mod sink;
mod util;

pub use self::shared::SharedJsonStore;
pub use adapter::Adapter;

/// Handles the sync policy for when to flush pending changes to disk.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SyncPolicy {
    /// Flush to disk after every change.
    Instant,
    /// Accumulate changes; caller drives `tick()` on a timer.
    Periodic(Duration),
    /// Never flush automatically; caller drives `flush()`.
    Manual,
}

#[derive(Debug)]
enum PendingChange {
    /// Append a new entry (already serialized, includes `\n`).
    Append {
        bytes: Vec<u8>,
        /// Retry a partial write at the same offset instead of appending twice.
        offset: Option<u64>,
    },
    /// Rewrite the line_id` with new content.
    RewriteLine {
        line_id: usize,
        /// The fully serialized replacement line, including `\n`.
        new_bytes: Vec<u8>,
    },
}

/// A backend that persists to a file using json encoding
///
/// *Warning*: This backend is not optimized for high-throughput scenarios and is best suited for development, testing, or low-volume workloads.
///
/// # Example
///
/// Creates a temporary JSON storage backend
/// ```rust
/// # use miju_file_storage::JsonStorage;;
/// # pub fn setup_json_storage() -> JsonStorage<u32> {
/// let mut backend: JsonStorage<u32> = JsonStorage::new_temp().unwrap();
/// # backend
/// # }
/// ```
#[doc = features_table! {
    setup = r#"
        # {
        #   use miju_file_storage::JsonStorage;;
        #   let mut backend: JsonStorage<u32> = JsonStorage::new_temp().unwrap();
        #   backend
        # };
    "#,
    Backend => supported("Basic Backend functionality", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => limited("Serialization support for arguments. Only accepts `json`", false),
    WebUI => not_implemented("Expose a web interface for monitoring tasks"),
    FetchById => not_implemented("Allow fetching a task by its ID"),
    RegisterWorker => not_supported("Allow registering a worker with the backend"),
    "[`PipeExt`](miju_core::backend::ext::pipe::PipeExt)" => supported("Allow other backends to pipe to this backend", false),
    BackendFactory => supported("Share the same storage across multiple workers via [`SharedJsonStore`]", false),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => not_implemented("Resume a task by its ID"),
    ResumeAbandoned => not_implemented("Resume abandoned tasks"),
    ListWorkers => not_supported("List all workers registered with the backend"),
    ListTasks => not_implemented("List all tasks in the backend"),
}]
#[derive(Debug)]
pub struct FileStorage<Args, A: Adapter> {
    adapter: A,
    lock: Arc<RwLock<FileLock<File>>>,
    path: PathBuf,
    temp_path: Option<Arc<TempPath>>,
    /// In-memory view of all entries; index == job_id.
    entries: Arc<RwLock<Vec<RawTask>>>,
    /// Staged mutations not yet flushed to disk.
    pending: Arc<RwLock<Vec<PendingChange>>>,
    waiters: Arc<Mutex<Vec<Weak<AtomicWaker>>>>,
    reader_waker: Arc<AtomicWaker>,
    /// Next job_id to yield from the `Stream` impl.
    read_cursor: usize,
    sync_policy: SyncPolicy,
    last_flush: std::time::Instant,
    codec: JsonCodec<Value>,
    _args: PhantomData<Args>,
}

impl<Args, A> Clone for FileStorage<Args, A>
where
    A: Adapter + Clone,
{
    fn clone(&self) -> Self {
        Self {
            adapter: self.adapter.clone(),
            lock: Arc::clone(&self.lock),
            path: self.path.clone(),
            temp_path: self.temp_path.clone(),
            entries: Arc::clone(&self.entries),
            pending: Arc::clone(&self.pending),
            waiters: Arc::clone(&self.waiters),
            reader_waker: Arc::new(AtomicWaker::new()),
            read_cursor: self.read_cursor,
            sync_policy: self.sync_policy.clone(),
            last_flush: self.last_flush,
            codec: self.codec.clone(),
            _args: PhantomData,
        }
    }
}

/// A JSON adapter for serializing and deserializing tasks.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonAdapter;

/// A `FileStorage` using `JsonAdapter` for line encoding.
pub type JsonStorage<Args> = FileStorage<Args, JsonAdapter>;

impl Adapter for JsonAdapter {
    type Line = serde_json::Value;

    type Error = serde_json::Error;

    fn serialize(&self, line: &Self::Line) -> Result<Vec<u8>, Self::Error> {
        let mut bytes = serde_json::to_vec(line)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    fn deserialize(&self, raw: &[u8]) -> Result<Self::Line, Self::Error> {
        serde_json::from_slice(raw.trim_ascii())
    }

    fn to_entry(&self, line: Self::Line) -> Result<RawTask, Self::Error> {
        serde_json::from_value(line)
    }

    fn from_entry(entry: &RawTask) -> Result<Self::Line, Self::Error> {
        serde_json::to_value(entry)
    }
}

impl<A: Adapter, Args> FileStorage<Args, A> {
    /// Creates a new `FileStorage` instance using the specified file path.
    ///
    /// Existing records and results are loaded without truncating the file.
    /// Invalid records return an error and leave the file unchanged.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, FileStorageError<A>>
    where
        A: Default,
    {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(FileStorageError::Io)?;

        Self::from_file(file, path)
    }

    fn from_file(file: File, path: PathBuf) -> Result<Self, FileStorageError<A>>
    where
        A: Default,
    {
        let adapter = A::default();
        let lock = Arc::new(RwLock::new(FileLock::new(file)));

        let entries = {
            let guard = lock.read().map_err(|e| {
                FileStorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    e.to_string(),
                ))
            })?;
            let guard = guard
                .try_read()
                .map_err(|_| FileStorageError::WouldBlockLock)?;
            Self::load_entries(&adapter, &guard)?
        };

        Ok(Self {
            adapter,
            lock,
            path,
            temp_path: None,
            entries: Arc::new(RwLock::new(entries)),
            pending: Default::default(),
            waiters: Default::default(),
            reader_waker: Arc::new(AtomicWaker::new()),
            read_cursor: 0,
            sync_policy: SyncPolicy::Instant,
            last_flush: std::time::Instant::now(),
            codec: JsonCodec::default(),
            _args: PhantomData,
        })
    }

    /// Creates a new temporary `FileStorage` instance.
    ///
    /// The original temporary file's permissions are retained. The file is
    /// removed when the storage and all of its clones are dropped.
    pub fn new_temp() -> Result<Self, FileStorageError<A>>
    where
        A: Default,
    {
        let (file, path) = NamedTempFile::new()?.into_parts();
        let mut storage = Self::from_file(file, path.to_path_buf())?;
        storage.temp_path = Some(Arc::new(path));
        Ok(storage)
    }

    /// Attach a result to the entry identified by `line_id`.
    pub fn set_result(
        &mut self,
        line_id: usize,
        result: serde_json::Value,
    ) -> Result<(), FileStorageError<A>> {
        {
            let mut entries = self
                .entries
                .try_write()
                .map_err(|_| FileStorageError::WouldBlockLock)?;
            let entry = entries
                .get_mut(line_id)
                .ok_or(FileStorageError::JobNotFound { line_id })?;

            let mut pending = self
                .pending
                .try_write()
                .map_err(|_| FileStorageError::WouldBlockLock)?;
            let mut updated = entry.clone();
            updated.result = Some(result);

            // Serialize the updated entry through the adapter's native Line type.
            let line = A::from_entry(&updated).map_err(FileStorageError::AdapterError)?;
            let new_bytes = self
                .adapter
                .serialize(&line)
                .map_err(FileStorageError::AdapterError)?;

            *entry = updated;
            pending.push(PendingChange::RewriteLine { line_id, new_bytes });
        }

        if matches!(self.sync_policy, SyncPolicy::Instant) {
            self.flush()?;
        }
        Ok(())
    }

    /// Persist all pending changes under an exclusive file lock.
    ///
    /// On failure, pending changes remain available for another call. A partial
    /// append is overwritten at its original offset when retried. This does not
    /// sync the file or containing directory to durable media.
    pub fn flush(&mut self) -> Result<(), FileStorageError<A>> {
        let raw_tasks = self
            .entries
            .try_read()
            .map_err(|_| FileStorageError::WouldBlockLock)?;
        let mut pending = self
            .pending
            .try_write()
            .map_err(|_| FileStorageError::WouldBlockLock)?;
        if pending.is_empty() {
            return Ok(());
        }

        let mut outer_guard = self.lock.write().map_err(|e| {
            FileStorageError::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                e.to_string(),
            ))
        })?;
        let mut guard = outer_guard
            .try_write()
            .map_err(|_| FileStorageError::WouldBlockLock)?;

        // Partition: rewrites require a full-file atomic swap; appends do not.
        let has_rewrites = pending
            .iter()
            .any(|c| matches!(c, PendingChange::RewriteLine { .. }));

        if has_rewrites {
            // Keep the batch until replacement succeeds; new mutations cannot
            // race the entries snapshot while these guards are held.
            let mut rewrites = HashMap::new();
            for change in pending.iter() {
                if let PendingChange::RewriteLine { line_id, new_bytes } = change {
                    rewrites.insert(*line_id, new_bytes);
                }
            }

            // Secure creation in the same directory keeps replacement atomic.
            let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty());
            let mut tmp = NamedTempFile::new_in(parent.unwrap_or_else(|| Path::new(".")))?;

            if let Some(headers) = self.adapter.header(&raw_tasks) {
                tmp.write_all(&headers)?;
            }
            for (job_id, entry) in raw_tasks.iter().enumerate() {
                if let Some(bytes) = rewrites.get(&job_id) {
                    tmp.write_all(bytes)?;
                } else {
                    let line = A::from_entry(entry).map_err(FileStorageError::AdapterError)?;
                    let bytes = self
                        .adapter
                        .serialize(&line)
                        .map_err(FileStorageError::AdapterError)?;
                    tmp.write_all(&bytes)?;
                }
            }
            tmp.flush()?;
            tmp.as_file()
                .set_permissions(guard.metadata()?.permissions())?;

            let new_file = tmp
                .persist(&self.path)
                .map_err(|e| FileStorageError::Io(e.error))?;
            drop(guard);
            *outer_guard = FileLock::new(new_file);
        } else {
            // Appends only — fast path: seek to end and write.
            for change in pending.iter_mut() {
                if let PendingChange::Append { bytes, offset } = change {
                    let position = if let Some(position) = *offset {
                        position
                    } else {
                        let position = guard.seek(SeekFrom::End(0))?;
                        if position > 0 {
                            let mut last = [0];
                            guard.seek(SeekFrom::End(-1))?;
                            guard.read_exact(&mut last)?;
                            // Loading also accepts a final line without a terminator.
                            if last[0] != b'\n' {
                                bytes.insert(0, b'\n');
                            }
                        }
                        *offset = Some(position);
                        position
                    };
                    guard.seek(SeekFrom::Start(position))?;
                    guard.write_all(bytes)?;
                }
            }
            guard.flush().map_err(FileStorageError::Io)?;
        }

        pending.clear();
        self.last_flush = std::time::Instant::now();
        Ok(())
    }

    /// Drive periodic syncing. Call this from your timer loop.
    pub fn tick(&mut self) -> Result<(), FileStorageError<A>> {
        if let SyncPolicy::Periodic(interval) = self.sync_policy.clone() {
            if self.last_flush.elapsed() >= interval {
                self.flush()?;
            }
        }
        Ok(())
    }

    /// Read every non-blank line from `file` and decode via the adapter.
    fn load_entries(adapter: &A, file: &File) -> Result<Vec<RawTask>, FileStorageError<A>> {
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(0))
            .map_err(FileStorageError::Io)?;

        let mut entries = Vec::new();
        let mut raw: Vec<u8> = Vec::new();

        loop {
            raw.clear();
            let n = reader
                .read_until(b'\n', &mut raw)
                .map_err(FileStorageError::Io)?;
            if n == 0 {
                break;
            }
            let trimmed = raw.trim_ascii();
            if trimmed.is_empty() {
                continue;
            }

            if adapter.is_header(trimmed) {
                continue;
            }
            let line = adapter
                .deserialize(trimmed)
                .map_err(FileStorageError::AdapterError)?;
            entries.push(
                adapter
                    .to_entry(line)
                    .map_err(FileStorageError::AdapterError)?,
            );
        }
        Ok(entries)
    }
}

impl<A: Adapter + Unpin, Args: Unpin> Stream for FileStorage<Args, A> {
    type Item = Result<(usize, RawTask), FileStorageError<A>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let raw_tasks = this
            .entries
            .try_read()
            .map_err(|_| FileStorageError::WouldBlockLock)?;
        if this.read_cursor < raw_tasks.len() {
            let line_id = this.read_cursor;
            let job = raw_tasks[line_id].clone();
            this.read_cursor += 1;
            Poll::Ready(Some(Ok((line_id, job))))
        } else {
            // Register while holding entries so an append cannot miss this reader.
            let mut waiters = this
                .waiters
                .lock()
                .map_err(|_| FileStorageError::WouldBlockLock)?;
            this.reader_waker.register(cx.waker());
            waiters.retain(|waiter| waiter.strong_count() > 0);
            let waiter = Arc::downgrade(&this.reader_waker);
            if !waiters.iter().any(|existing| existing.ptr_eq(&waiter)) {
                waiters.push(waiter);
            }
            Poll::Pending
        }
    }
}

impl<Args, A> Backend for FileStorage<Args, A>
where
    Args: 'static + Send + Serialize + for<'de> Deserialize<'de> + Unpin,
    A: Adapter + Unpin,
    A::Error: std::error::Error + Send + Sync + 'static,
{
    type Task = Task<Value>;
    type Error = FileStorageError<A>;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if self.lock.try_read().is_ok() {
            Poll::Ready(Ok(()))
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.poll_next_unpin(cx).map_ok(|(line_id, mut job)| {
            job.ctx.insert("line_id", line_id.to_string()).unwrap();
            let task = TaskBuilder::new(job.args)
                .with_metadata(job.ctx)
                .task_id(job.task_id);
            task.build()
        })
    }

    fn poll_close(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.flush()?;
        Poll::Ready(Ok(()))
    }
}

impl<Args, A> BackendConfig for FileStorage<Args, A>
where
    Args: 'static + Send + Serialize + for<'de> Deserialize<'de> + Unpin,
    A: Adapter + Unpin + Clone,
    A::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type Id = RandomId;
    type Kind = Durable;
    type Config = PathBuf;
    type Layer = AcknowledgeLayer<Self>;
    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }

    fn config(&self) -> &Self::Config {
        &self.path
    }
}

impl<Args, A> WireFormatBackend for FileStorage<Args, A>
where
    Args: 'static + Send + Serialize + for<'de> Deserialize<'de> + Unpin,
    A: Adapter + Unpin,
    A::Error: std::error::Error + Send + Sync + 'static,
{
    type Codec = JsonCodec<Value>;
    type Compact = Value;
    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

/// A CSV adapter for serializing and deserializing tasks.
#[derive(Debug, Clone, Default)]
pub struct CsvAdapter {
    /// Column names, in BTreeMap (alphabetical) order.
    /// Populated on first serialize (write path) or first deserialize (read path).
    header: Arc<Mutex<Option<Vec<String>>>>,
}

/// A `FileStorage` using `CsvAdapter` for line encoding.
pub type CsvStorage<Args> = FileStorage<Args, CsvAdapter>;

impl Adapter for CsvAdapter {
    type Line = BTreeMap<String, String>;

    type Error = io::Error;

    fn serialize(&self, line: &Self::Line) -> Result<Vec<u8>, Self::Error> {
        let mut out = Vec::new();

        let mut header = self
            .header
            .lock()
            .map_err(|_| io::Error::other("header mutex poisoned"))?;

        if header.is_none() {
            let cols: Vec<String> = line.keys().cloned().collect();
            out.extend_from_slice(format!("{}\n", cols.join(",")).as_bytes());
            *header = Some(cols);
        }

        let row = line
            .values()
            .map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(",");

        out.extend_from_slice(format!("{row}\n").as_bytes());

        Ok(out)
    }

    fn deserialize(&self, raw: &[u8]) -> Result<Self::Line, Self::Error> {
        let row = std::str::from_utf8(raw)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .trim_end_matches('\n');

        let header = self
            .header
            .lock()
            .map_err(|_| io::Error::other("header mutex poisoned"))?;

        let cols = header.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "CsvAdapter: deserialize called before header was parsed",
            )
        })?;

        let map = row
            .split(',')
            .zip(cols.iter())
            .map(|(value, key)| (key.clone(), value.to_owned()))
            .collect::<BTreeMap<_, _>>();

        Ok(map)
    }

    fn to_entry(&self, line: Self::Line) -> Result<RawTask, Self::Error> {
        let args = util::to_value(Some("args"), &line);

        let mut result = None;

        if line.contains_key("result") {
            result = Some(util::to_value(Some("result"), &line));
        }

        let idempotency_key = line.get("idempotency_key").cloned();

        let task_id = line
            .get("task_id")
            .and_then(|s| FromStr::from_str(s).ok())
            .unwrap_or(TaskId::from_string(RandomId::default()));

        let ctx = line
            .iter()
            .filter(|(k, _)| k.starts_with("ctx."))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<String, String>>();

        let ctx = MetadataStore::from_map(ctx);

        Ok(RawTask {
            task_id,
            args,
            ctx,
            result,
            idempotency_key,
        })
    }

    fn from_entry(entry: &RawTask) -> Result<Self::Line, Self::Error> {
        let mut line = BTreeMap::new();

        line.insert("task_id".to_owned(), entry.task_id.to_string());

        let args = util::from_value(Some("args"), &entry.args);

        line.extend(args);

        let result = entry
            .result
            .as_ref()
            .map(|r| util::from_value(Some("result"), r))
            .unwrap_or_else(|| util::from_value(Some("result"), &serde_json::Value::Null));

        line.extend(result);

        if let Some(idempotency_key) = &entry.idempotency_key {
            line.insert("idempotency_key".to_owned(), idempotency_key.clone());
        }

        for (k, value) in entry.ctx.iter() {
            line.insert(format!("ctx.{k}"), value.clone());
        }

        Ok(line)
    }

    fn is_header(&self, raw: &[u8]) -> bool {
        let Ok(mut header) = self.header.lock() else {
            return false;
        };

        if header.is_none() {
            if let Ok(row) = std::str::from_utf8(raw) {
                let cols: Vec<String> = row
                    .trim_end_matches('\n')
                    .split(',')
                    .map(|s| s.to_owned())
                    .collect();

                *header = Some(cols);
            }

            return true; // this line is the header, skip it
        }

        false
    }

    fn header(&self, entries: &Vec<RawTask>) -> Option<Vec<u8>> {
        let first = entries.first()?;

        let line = Self::from_entry(first).ok()?;

        let cols: Vec<String> = line.keys().cloned().collect();
        Some(format!("{}\n", cols.join(",")).as_bytes().to_vec())
    }
}

impl<Args, Res, A> Acknowledge<Res> for FileStorage<Args, A>
where
    Args: Send + 'static + Debug,
    Res: Serialize,
    A: Adapter + Clone,
{
    type Error = FileStorageError<A>;

    type Future = Ready<Result<(), Self::Error>>;

    fn ack(&mut self, res: &Result<Res, BoxDynError>, ctx: &ExecutionContext) -> Self::Future {
        let res = |this: &mut Self| {
            let val = serde_json::to_value(res.as_ref().map_err(|e| e.to_string()))?;
            let line_id = ctx
                .metadata()
                .get("line_id")
                .unwrap()
                .parse()
                .map_err(|e: ParseIntError| FileStorageError::Parse(e.to_string()))?;
            this.set_result(line_id, val)?;
            Ok(())
        };

        ready(res(self))
    }
}

impl<Args, A, O> WaitForCompletion<O> for FileStorage<Args, A>
where
    O: Send + DeserializeOwned + 'static + Unpin + Serialize,
    A: Adapter + Unpin + Sync + Clone,
    A::Error: std::error::Error + Send + Sync + 'static,
    Args: Serialize + DeserializeOwned + 'static + Send + Sync + Unpin,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O>, FileStorageError<A>>>;
    fn wait_for(&mut self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        use futures_util::StreamExt;
        use std::{collections::HashSet, time::Duration};

        let task_ids: HashSet<_> = task_ids.into_iter().collect();
        struct PollState<Res, T, A: Adapter> {
            vault: FileStorage<T, A>,
            pending_tasks: HashSet<TaskId>,
            poll_interval: Duration,
            _phantom: std::marker::PhantomData<Res>,
        }
        let state = PollState {
            vault: self.clone(),
            pending_tasks: task_ids,
            poll_interval: Duration::from_millis(100),
            _phantom: std::marker::PhantomData,
        };
        futures_util::stream::unfold(state, |mut state: PollState<Value, Args, A>| {
            async move {
                if state.pending_tasks.is_empty() {
                    return None;
                }

                loop {
                    // Check for completed tasks
                    let vault = &state.vault;

                    let completed_task = {
                        let vault = vault.entries.try_read().ok()?;
                        vault.iter().find_map(|value| {
                            let attempt = value
                                .ctx
                                .get("attempt")
                                .map(FromStr::from_str)
                                .transpose()
                                .unwrap_or_default()
                                .unwrap_or_default();
                            let task_id = value.task_id.clone();
                            if state.pending_tasks.contains(&task_id) {
                                Some((task_id, value.result.clone()?, attempt))
                            } else {
                                None
                            }
                        })
                    };

                    if let Some((task_id, result, attempt)) = completed_task {
                        state.pending_tasks.remove(&task_id);
                        let result: Result<O, String> = match serde_json::from_value(result) {
                            Ok(result) => result,
                            Err(error) => {
                                return Some((Err(FileStorageError::Json(error)), state));
                            }
                        };
                        return Some((
                            Ok(TaskResult {
                                task_id,
                                status: Status::Done,
                                result,
                                attempt,
                            }),
                            state,
                        ));
                    }

                    // No completed tasks, wait and try again
                    miju_core::timer::sleep(state.poll_interval).await;
                }
            }
        })
        .boxed()
    }

    async fn check_status(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<O>>, Self::Error> {
        use miju_core::task::status::Status;
        use std::collections::HashSet;
        let task_ids: HashSet<_> = task_ids.into_iter().collect();
        let mut results = Vec::new();
        for task_id in task_ids {
            if let Some(value) = self
                .entries
                .try_read()
                .unwrap()
                .iter()
                .find(|s| s.task_id == task_id)
            {
                let attempt = value
                    .ctx
                    .get("attempt")
                    .map(FromStr::from_str)
                    .transpose()
                    .unwrap_or_default()
                    .unwrap_or_default();
                if value.result.is_none() {
                    results.push(TaskResult {
                        task_id: task_id.clone(),
                        status: Status::Pending,
                        result: Err("Task not completed yet".to_owned()),
                        attempt,
                    });
                    continue;
                }
                let result = match serde_json::from_value::<Result<O, String>>(
                    value.result.clone().unwrap(),
                ) {
                    Ok(result) => TaskResult {
                        task_id: task_id.clone(),
                        status: Status::Done,
                        result,
                        attempt,
                    },
                    Err(e) => TaskResult {
                        task_id: task_id.clone(),
                        status: Status::Failed,
                        result: Err(format!("Deserialization error: {e}")),
                        attempt,
                    },
                };
                results.push(result);
            }
        }
        Ok(results)
    }
}

impl<Args: Send + 'static, A> TryNewBackend for FileStorage<Args, A>
where
    A: Adapter + Default + Unpin + Sync + Clone,
    A::Error: std::error::Error + Send + Sync + 'static,
    Args: Serialize + DeserializeOwned + 'static + Send + Sync + Unpin,
{
    type Backend = Self;
    fn try_new(config: Self::Config) -> Result<Self::Backend, Self::Error> {
        Self::new(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use miju_core::{
        backend::TaskSink,
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    #[tokio::test]
    async fn json_push_preserves_id_metadata_and_disk_record() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        let id = TaskId::from_string(RandomId::default());
        let task = TaskBuilder::new(42_u32)
            .task_id(id.clone())
            .idempotency_key("report-42")
            .build();
        store.push_task(task).await.unwrap();

        let record: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(record["args"], 42);
        assert_eq!(record["task_id"], serde_json::to_value(&id).unwrap());
        assert_eq!(record["idempotency_key"], "report-42");
        let (line_id, entry) = store.next().await.unwrap().unwrap();
        assert_eq!(line_id, 0);
        assert_eq!(entry.task_id, id);
        assert_eq!(entry.args, 42);
    }

    #[tokio::test]
    async fn reopening_preserves_tasks_and_results() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        let id = TaskId::from_string(RandomId::default());
        store
            .push_task(
                TaskBuilder::new(7_u32)
                    .task_id(id.clone())
                    .idempotency_key("restored")
                    .build(),
            )
            .await
            .unwrap();
        store.set_result(0, serde_json::json!({"Ok": 14})).unwrap();
        drop(store);

        let mut reopened = JsonStorage::<u32>::try_new(path).unwrap();
        let results =
            <JsonStorage<u32> as WaitForCompletion<u32>>::check_status(&mut reopened, [id.clone()])
                .await
                .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].result, Ok(14));
        let (_, task) = reopened.next().await.unwrap().unwrap();
        assert_eq!(task.task_id, id);
        assert_eq!(task.args, 7);
        reopened
            .push_task(TaskBuilder::new(99_u32).idempotency_key("restored").build())
            .await
            .unwrap();
        reopened.push(8).await.unwrap();
        assert_eq!(reopened.next().await.unwrap().unwrap().1.args, 8);
    }

    #[test]
    fn invalid_existing_records_are_not_modified() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let invalid = b"not a JSON record\n";
        std::fs::write(&path, invalid).unwrap();
        assert!(matches!(
            JsonStorage::<u32>::new(&path),
            Err(FileStorageError::AdapterError(_))
        ));
        assert_eq!(std::fs::read(path).unwrap(), invalid);
    }

    #[tokio::test]
    async fn csv_reopens_and_rewrites_a_stable_schema() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.csv");
        let mut store = CsvStorage::<u32>::new(&path).unwrap();
        store.push(7).await.unwrap();
        store.set_result(0, serde_json::json!(14)).unwrap();
        drop(store);

        let mut reopened = CsvStorage::<u32>::new(&path).unwrap();
        let (_, entry) = reopened.next().await.unwrap().unwrap();
        assert_eq!(entry.args, 7);
        assert_eq!(entry.result, Some(serde_json::json!(14)));
        reopened.push(8).await.unwrap();
        reopened.set_result(1, serde_json::json!(16)).unwrap();
        drop(reopened);

        let mut reopened = CsvStorage::<u32>::new(&path).unwrap();
        assert_eq!(reopened.next().await.unwrap().unwrap().1.args, 7);
        let (_, entry) = reopened.next().await.unwrap().unwrap();
        assert_eq!(entry.args, 8);
        assert_eq!(entry.result, Some(serde_json::json!(16)));
        assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 3);
    }

    #[tokio::test]
    async fn reopening_an_unterminated_record_allows_appending() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        store.push(7).await.unwrap();
        drop(store);
        let mut bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.pop(), Some(b'\n'));
        std::fs::write(&path, &bytes).unwrap();

        let mut reopened = JsonStorage::<u32>::new(&path).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        reopened.push(8).await.unwrap();
        drop(reopened);
        let mut reopened = JsonStorage::<u32>::new(&path).unwrap();
        assert_eq!(reopened.next().await.unwrap().unwrap().1.args, 7);
        assert_eq!(reopened.next().await.unwrap().unwrap().1.args, 8);
    }

    #[tokio::test]
    async fn temporary_storage_is_private_and_lives_until_last_clone() {
        let store = JsonStorage::<u32>::new_temp().unwrap();
        let path = store.config().clone();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o077,
                0
            );
        }
        let mut clone = store.clone();
        drop(store);
        assert!(path.exists());
        clone.push(42).await.unwrap();
        assert_eq!(clone.next().await.unwrap().unwrap().1.args, 42);
        clone.set_result(0, serde_json::json!({"Ok": 84})).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o077,
                0
            );
        }
        drop(clone);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn clones_append_to_the_rewritten_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        store.push(1).await.unwrap();
        let mut clone = store.clone();
        store.set_result(0, serde_json::json!({"Ok": 2})).unwrap();
        clone.push(3).await.unwrap();
        let records: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["result"], serde_json::json!({"Ok": 2}));
        assert_eq!(records[1]["args"], 3);
        clone.set_result(1, serde_json::json!({"Ok": 4})).unwrap();
        store.push(5).await.unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 3);
    }

    #[tokio::test]
    async fn failed_rewrite_can_be_retried_without_losing_changes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let backup = directory.path().join("saved.json");
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        store.push(1).await.unwrap();
        let original = std::fs::read(&path).unwrap();
        std::fs::rename(&path, &backup).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(store.set_result(0, serde_json::json!({"Ok": 2})).is_err());
        assert!(store.flush().is_err());
        assert_eq!(std::fs::read(&backup).unwrap(), original);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);

        std::fs::remove_dir(&path).unwrap();
        std::fs::rename(&backup, &path).unwrap();
        store.push(3).await.unwrap();
        store.flush().unwrap();
        store.flush().unwrap();
        let records: Vec<Value> = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["result"], serde_json::json!({"Ok": 2}));
        assert_eq!(records[1]["args"], 3);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn rewrite_preserves_permissions_and_ignores_preexisting_temporary_paths() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let unrelated = directory.path().join("unrelated.txt");
        std::fs::write(&unrelated, b"preserve me").unwrap();
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        store.push(1).await.unwrap();
        symlink(&unrelated, path.with_extension("tmp")).unwrap();
        store.set_result(0, serde_json::json!({"Ok": 2})).unwrap();
        assert_eq!(std::fs::read(unrelated).unwrap(), b"preserve me");
        assert!(
            !std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[tokio::test]
    async fn concurrent_duplicate_keys_are_not_staged_twice() {
        use futures_util::SinkExt;
        use std::sync::Barrier;

        static SERIALIZING: Barrier = Barrier::new(2);
        static RELEASE: Barrier = Barrier::new(2);

        #[derive(Debug, Clone, Default)]
        struct PausingAdapter;

        impl Adapter for PausingAdapter {
            type Line = Value;
            type Error = serde_json::Error;

            fn serialize(&self, line: &Value) -> Result<Vec<u8>, Self::Error> {
                if line["args"] == 1 {
                    SERIALIZING.wait();
                    RELEASE.wait();
                }
                JsonAdapter.serialize(line)
            }

            fn deserialize(&self, raw: &[u8]) -> Result<Value, Self::Error> {
                JsonAdapter.deserialize(raw)
            }

            fn to_entry(&self, line: Value) -> Result<RawTask, Self::Error> {
                JsonAdapter.to_entry(line)
            }

            fn from_entry(entry: &RawTask) -> Result<Value, Self::Error> {
                JsonAdapter::from_entry(entry)
            }
        }

        let mut store = FileStorage::<u32, PausingAdapter>::new_temp().unwrap();
        let mut first = store.clone();
        let sender = std::thread::spawn(move || {
            first.start_send_unpin(
                TaskBuilder::new(serde_json::json!(1))
                    .idempotency_key("same")
                    .build(),
            )
        });
        SERIALIZING.wait();
        let second = TaskBuilder::new(serde_json::json!(2))
            .idempotency_key("same")
            .build();
        let competing = store.start_send_unpin(second.clone());
        RELEASE.wait();
        sender.join().unwrap().unwrap();
        if let Err(error) = competing {
            assert!(matches!(error, FileStorageError::WouldBlockLock));
            store.start_send_unpin(second).unwrap();
        }
        store.flush().unwrap();
        assert_eq!(
            std::fs::read_to_string(store.config())
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn failed_result_encoding_does_not_change_the_stored_entry() {
        #[derive(Debug, Clone, Default)]
        struct RejectResults;

        impl Adapter for RejectResults {
            type Line = Value;
            type Error = serde_json::Error;

            fn serialize(&self, line: &Value) -> Result<Vec<u8>, Self::Error> {
                if !line["result"].is_null() {
                    return Err(serde_json::Error::io(io::Error::other("result rejected")));
                }
                JsonAdapter.serialize(line)
            }

            fn deserialize(&self, raw: &[u8]) -> Result<Value, Self::Error> {
                JsonAdapter.deserialize(raw)
            }

            fn to_entry(&self, line: Value) -> Result<RawTask, Self::Error> {
                JsonAdapter.to_entry(line)
            }

            fn from_entry(entry: &RawTask) -> Result<Value, Self::Error> {
                JsonAdapter::from_entry(entry)
            }
        }

        let mut store = FileStorage::<u32, RejectResults>::new_temp().unwrap();
        store.push(7).await.unwrap();
        assert!(store.set_result(0, serde_json::json!({"Ok": 14})).is_err());
        store.flush().unwrap();
        assert_eq!(store.next().await.unwrap().unwrap().1.result, None);
    }

    #[tokio::test]
    async fn append_wakes_all_waiting_clones() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        #[derive(Default)]
        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut writer = JsonStorage::<u32>::new_temp().unwrap();
        let mut first = writer.clone();
        let mut second = writer.clone();
        let mut dropped = writer.clone();
        let old_count = Arc::new(WakeCount::default());
        let old_waker = Waker::from(old_count.clone());
        assert!(
            first
                .poll_next_unpin(&mut Context::from_waker(&old_waker))
                .is_pending()
        );
        let first_count = Arc::new(WakeCount::default());
        let first_waker = Waker::from(first_count.clone());
        let second_count = Arc::new(WakeCount::default());
        let second_waker = Waker::from(second_count.clone());
        for _ in 0..3 {
            assert!(
                first
                    .poll_next_unpin(&mut Context::from_waker(&first_waker))
                    .is_pending()
            );
            assert!(
                second
                    .poll_next_unpin(&mut Context::from_waker(&second_waker))
                    .is_pending()
            );
        }
        let dropped_count = Arc::new(WakeCount::default());
        let dropped_waker = Waker::from(dropped_count.clone());
        assert!(
            dropped
                .poll_next_unpin(&mut Context::from_waker(&dropped_waker))
                .is_pending()
        );
        drop(dropped);
        drop(dropped_waker);
        assert_eq!(Arc::strong_count(&dropped_count), 1);

        writer.push(7).await.unwrap();
        assert_eq!(old_count.0.load(Ordering::SeqCst), 0);
        assert_eq!(first_count.0.load(Ordering::SeqCst), 1);
        assert_eq!(second_count.0.load(Ordering::SeqCst), 1);
        assert_eq!(first.next().await.unwrap().unwrap().1.args, 7);
        assert_eq!(second.next().await.unwrap().unwrap().1.args, 7);

        assert!(
            first
                .poll_next_unpin(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        writer.push(8).await.unwrap();
        assert_eq!(first_count.0.load(Ordering::SeqCst), 2);
        assert_eq!(second_count.0.load(Ordering::SeqCst), 1);
        assert_eq!(first.next().await.unwrap().unwrap().1.args, 8);
    }

    #[tokio::test]
    async fn duplicate_idempotency_key_does_not_append() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        for value in [1_u32, 2] {
            store
                .push_task(TaskBuilder::new(value).idempotency_key("same").build())
                .await
                .unwrap();
        }
        assert_eq!(store.entries.read().unwrap().len(), 1);
        assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 1);
    }

    #[tokio::test]
    async fn completion_result_is_rewritten_and_reported() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.json");
        let mut store = JsonStorage::<u32>::new(&path).unwrap();
        let id = TaskId::from_string(RandomId::default());
        store
            .push_task(TaskBuilder::new(7_u32).task_id(id.clone()).build())
            .await
            .unwrap();
        let pending =
            <JsonStorage<u32> as WaitForCompletion<u32>>::check_status(&mut store, [id.clone()])
                .await
                .unwrap();
        assert_eq!(pending[0].status, Status::Pending);
        store.set_result(0, serde_json::json!({"Ok": 14})).unwrap();
        let result = <JsonStorage<u32> as WaitForCompletion<u32>>::wait_for(&mut store, [id])
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, Status::Done);
        assert_eq!(result.result, Ok(14));
        let disk: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(disk["result"], serde_json::json!({"Ok": 14}));
    }

    #[test]
    fn missing_result_target_returns_typed_error() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = JsonStorage::<u32>::new(directory.path().join("tasks.json")).unwrap();
        assert!(matches!(
            store.set_result(10, Value::Null),
            Err(FileStorageError::JobNotFound { line_id: 10 })
        ));
    }

    #[test]
    fn json_adapter_loads_records_and_reports_invalid_input() {
        let entry = RawTask {
            task_id: TaskId::from_string(RandomId::default()),
            args: serde_json::json!({"text": "comma, newline\n"}),
            ctx: MetadataStore::default(),
            result: None,
            idempotency_key: None,
        };
        let mut file = NamedTempFile::new().unwrap();
        let adapter = JsonAdapter;
        file.write_all(b"\n").unwrap();
        file.write_all(
            &adapter
                .serialize(&JsonAdapter::from_entry(&entry).unwrap())
                .unwrap(),
        )
        .unwrap();
        let loaded = JsonStorage::<Value>::load_entries(&adapter, file.as_file()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].args, entry.args);
        assert_eq!(loaded[0].task_id, entry.task_id);
        assert!(adapter.deserialize(b"invalid JSON").is_err());
    }

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn json_worker() {
        let mut json_store = JsonStorage::new_temp().unwrap();
        for i in 0..ITEMS {
            json_store.push(i).await.unwrap();
        }

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                worker.stop().unwrap();
                Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-json")
            .backend(json_store)
            .on_event(|worker, ev| {
                println!("On Event = {ev:?} from = {}", worker.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn csv_worker() {
        #[derive(Debug, Serialize, Deserialize, Default)]
        struct Email {
            to: String,
            subject: String,
            index: u32,
        }

        let mut csv_store = CsvStorage::new_temp().unwrap();
        for i in 0..ITEMS {
            csv_store
                .push(Email {
                    subject: "Test".to_owned(),
                    to: "test".to_owned(),
                    index: i,
                })
                .await
                .unwrap();
        }

        async fn task(task: Email, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task.index == ITEMS - 1 {
                worker.stop().unwrap();
                Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-csv")
            .backend(csv_store)
            .on_event(|worker, ev| {
                println!("On Event = {ev:?} from = {}", worker.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
