//! # In-memory backend based on channels
//!
//! An in-memory backend suitable for testing, prototyping, or lightweight task processing scenarios where persistence is not required.
//!
//! ## Features
//! - Generic in-memory queue for any task type.
//! - Implements [`Backend`] for integration with workers.
//! - Sink support: Ability to push new tasks.
//!
//! A detailed feature list can be found in the [capabilities](crate::backend::memory::MemoryStorage#capabilities) section.
//!
//! ## Example
//!
//! ```rust
//! # use miju_core::backend::memory::MemoryStorage;
//! # use miju_core::worker::context::WorkerContext;
//! # use miju_core::worker::builder::WorkerBuilder;
//! # use miju_core::backend::TaskSink;
//!
//! async fn handler(_: u32, worker: WorkerContext) {
//!     worker.stop().unwrap();
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut store = MemoryStorage::new();
//!     store.push(42).await.unwrap();
//!
//!     let worker = WorkerBuilder::new("int-worker")
//!         .backend(store)
//!         .build(handler);
//!
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Note
//! This backend is not persistent and is intended for use cases where durability is not required.
//! For production workloads, consider using a persistent backend such as PostgreSQL or Redis.
//!
//! ## See Also
//! - [`Backend`]
//! - [`WorkerContext`]
use crate::backend::finalize::Ephemeral;
use crate::backend::{Backend, BackendConfig, TryNewBackend};
use crate::error::BoxDynError;
use crate::{
    task::{
        Task,
        task_id::{RandomId, TaskId},
    },
    worker::context::WorkerContext,
};
use futures_channel::mpsc::{SendError, UnboundedSender, unbounded};
use futures_core::ready;
use futures_sink::Sink;
use futures_timer::Delay;
use futures_util::lock::{Mutex, MutexGuard, OwnedMutexGuard};
use futures_util::{FutureExt, SinkExt, Stream, StreamExt};
use std::collections::HashSet;
use std::{
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex},
    task::{Context, Poll},
    time::Duration,
};
use tower_layer::Identity;

/// A boxed in-memory task receiver stream
pub type BoxedReceiver<Args> = Pin<Box<dyn Stream<Item = Task<Args>> + Send>>;

/// In-memory queue that is based on channels
///
///
/// ## Example
/// ```rust
/// # use miju_core::backend::memory::MemoryStorage;
/// # fn setup() -> MemoryStorage<u32> {
/// let mut backend = MemoryStorage::new();
/// # backend
/// # }
/// ```
///
#[doc = features_table! {
    setup = r#"
        # {
        #   use miju_core::backend::memory::MemoryStorage;
        #   MemoryStorage::new()
        # };
    "#,
    Backend => supported("Basic Backend functionality", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => not_supported("Serialization support for arguments"),

    PipeExt => not_implemented("Allow other backends to pipe to this backend"),
    BackendFactory => not_supported("Share the same storage across multiple workers"),

    Update => not_supported("Allow updating a task"),
    FetchById => not_supported("Allow fetching a task by its ID"),
    Reschedule => not_supported("Reschedule a task"),

    ResumeById => not_supported("Resume a task by its ID"),
    ResumeAbandoned => not_supported("Resume abandoned tasks"),
    Vacuum => not_supported("Vacuum the task storage"),

    Workflow => not_implemented("Flexible enough to support workflows"),
    WaitForCompletion => not_implemented("Wait for tasks to complete without blocking"), // Requires Clone

    RegisterWorker => not_supported("Allow registering a worker with the backend"),
    ListWorkers => not_supported("List all workers registered with the backend"),
    ListTasks => not_supported("List all tasks in the backend"),
}]
pub struct MemoryStorage<Args> {
    pub(super) sender: MemorySink<Args>,
    pub(super) receiver: std::sync::Mutex<BoxedReceiver<Args>>,
}

impl<Args: Send + 'static> Default for MemoryStorage<Args> {
    fn default() -> Self {
        Self::new()
    }
}

/// Error type for MemoryStorage operations
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MemoryStorageError {
    /// Error occurred while sending a task to the in-memory channel
    #[error("Failed to send task: {0}")]
    SendError(#[from] SendError),
    /// Error occurred while flushing the in-memory channel
    #[error("Failed to add task to storage: {0}")]
    Other(BoxDynError),
}

impl<Args: Send + 'static> MemoryStorage<Args> {
    /// Create a new in-memory storage
    #[must_use]
    pub fn new() -> Self {
        let (sender, receiver) = unbounded();
        let sender = Box::new(ChannelSink(sender))
            as Box<dyn Sink<Task<Args>, Error = MemoryStorageError> + Send + Sync + Unpin>;
        Self {
            sender: MemorySink {
                inner: Arc::new(futures_util::lock::Mutex::new(sender)),
                idempotency_keys: Default::default(),
                unflushed_keys: Vec::new(),
                lock_retry: None,
                readiness: Default::default(),
                ready: None,
            },
            receiver: receiver.boxed().into(),
        }
    }
    /// Create a storage given a sender and receiver
    #[must_use]
    pub fn new_with(sender: MemorySink<Args>, receiver: BoxedReceiver<Args>) -> Self {
        Self {
            sender,
            receiver: receiver.into(),
        }
    }
}

impl<Args> Sink<Task<Args>> for MemoryStorage<Args> {
    type Error = MemoryStorageError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Task<Args>) -> Result<(), Self::Error> {
        self.as_mut().sender.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_close_unpin(cx)
    }
}

/// Converts every channel error. `SinkMapErr` consumes its mapper on the first
/// error and panics on the next, e.g. on a second send after the channel closes.
struct ChannelSink<Args>(UnboundedSender<Task<Args>>);

impl<Args> Sink<Task<Args>> for ChannelSink<Args> {
    type Error = MemoryStorageError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_ready_unpin(cx).map_err(Into::into)
    }

    fn start_send(self: Pin<&mut Self>, item: Task<Args>) -> Result<(), Self::Error> {
        self.get_mut().0.start_send_unpin(item).map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_flush_unpin(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_close_unpin(cx).map_err(Into::into)
    }
}

type ArcMemorySink<Args> = Arc<
    Mutex<Box<dyn Sink<Task<Args>, Error = MemoryStorageError> + Send + Sync + Unpin + 'static>>,
>;

type ArcIdempotencySet = Arc<SyncMutex<HashSet<String>>>;

/// Memory sink for sending tasks to the in-memory backend
///
/// Successful readiness reserves the next send for this handle. Other clones
/// wait for send readiness until it sends, flushes, closes, or drops. Flushing
/// or closing the sink does not wait for another handle's unused permit.
/// The underlying sink mutex is not held between calls. Direct access to the
/// sink passed to [`Self::new`] must coordinate readiness with these handles.
///
/// Lock contention during `start_send` returns [`MemoryStorageError::Other`]
/// without submitting the task. Idempotency keys recorded by a handle stay
/// provisional until its next successful flush: contention or an error from the
/// underlying sink before then releases them, so the tasks can be retried with
/// the same keys. If the underlying sink stored a task before reporting the
/// error, such a retry relies on that sink to deduplicate it.
pub struct MemorySink<Args> {
    pub(super) inner: ArcMemorySink<Args>,
    pub(super) idempotency_keys: ArcIdempotencySet,
    unflushed_keys: Vec<String>,
    lock_retry: Option<Delay>,
    readiness: Arc<Mutex<()>>,
    ready: Option<OwnedMutexGuard<()>>,
}

impl<Args> MemorySink<Args> {
    /// Build a new memory sink given a sink
    pub fn new(sink: ArcMemorySink<Args>) -> Self {
        Self {
            inner: sink,
            idempotency_keys: Arc::new(SyncMutex::new(HashSet::new())),
            unflushed_keys: Vec::new(),
            lock_retry: None,
            readiness: Default::default(),
            ready: None,
        }
    }

    // A deferring sink may only write accepted tasks during readiness or flush,
    // so those can fail after the keys were recorded. Releasing the keys keeps a
    // retry from being dropped as a duplicate of a task that was never stored.
    fn release_unflushed_keys(&mut self) {
        if self.unflushed_keys.is_empty() {
            return;
        }
        let mut keys = self.idempotency_keys.lock().unwrap();
        for key in self.unflushed_keys.drain(..) {
            keys.remove(&key);
        }
    }

    fn settle_unflushed_keys(
        &mut self,
        result: &Poll<Result<(), MemoryStorageError>>,
        flush: bool,
    ) {
        match result {
            Poll::Ready(Err(_)) => self.release_unflushed_keys(),
            Poll::Ready(Ok(())) if flush => self.unflushed_keys.clear(),
            Poll::Ready(Ok(())) | Poll::Pending => {}
        }
    }

    fn poll_lock(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        MutexGuard<'_, Box<dyn Sink<Task<Args>, Error = MemoryStorageError> + Send + Sync + Unpin>>,
    > {
        if let Some(lock) = self.inner.try_lock() {
            self.lock_retry = None;
            return Poll::Ready(lock);
        }

        Self::poll_retry(&mut self.lock_retry, cx);
        Poll::Pending
    }

    fn poll_retry(lock_retry: &mut Option<Delay>, cx: &mut Context<'_>) {
        // A retained mutex waiter can strand other owners when a sink operation is cancelled.
        let retry = lock_retry.get_or_insert_with(|| Delay::new(Duration::from_millis(1)));
        if retry.poll_unpin(cx).is_ready() {
            retry.reset(Duration::from_millis(1));
            if retry.poll_unpin(cx).is_ready() {
                cx.waker().wake_by_ref();
            }
        }
    }

    fn poll_inner_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), MemoryStorageError>> {
        let result = {
            let mut lock = ready!(self.poll_lock(cx));
            Pin::new(&mut *lock).poll_ready_unpin(cx)
        };
        self.settle_unflushed_keys(&result, false);
        result
    }
}

impl<Args> std::fmt::Debug for MemorySink<Args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySink")
            .field("inner", &"<Sink>")
            .field("idempotency_keys", &"<IdempotencySet>")
            .finish()
    }
}

impl<Args> Clone for MemorySink<Args> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            idempotency_keys: Arc::clone(&self.idempotency_keys),
            unflushed_keys: Vec::new(),
            lock_retry: None,
            readiness: Arc::clone(&self.readiness),
            ready: None,
        }
    }
}

impl<Args> Sink<Task<Args>> for MemorySink<Args> {
    type Error = MemoryStorageError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.ready.is_some() {
            return Poll::Ready(Ok(()));
        }
        let Some(permit) = this.readiness.try_lock_owned() else {
            Self::poll_retry(&mut this.lock_retry, cx);
            return Poll::Pending;
        };
        let result = this.poll_inner_ready(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            this.ready = Some(permit);
        }
        result
    }

    fn start_send(self: Pin<&mut Self>, mut item: Task<Args>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let permit = this
            .ready
            .take()
            .or_else(|| this.readiness.try_lock_owned());
        let recorded = if let Some(key) = item.idempotency_key() {
            let mut keys = this.idempotency_keys.lock().unwrap();

            if keys.contains(key) {
                return Ok(());
            }

            keys.insert(key.to_owned());
            this.unflushed_keys.push(key.to_owned());
            true
        } else {
            false
        };

        if item.task_id().is_none() {
            let task = item
                .into_builder()
                .task_id(TaskId::from_string(RandomId::default()));
            item = task.build();
        }

        let result = match permit.as_ref().and_then(|_| this.inner.try_lock()) {
            Some(mut sink) => Pin::new(&mut *sink).start_send_unpin(item),
            None => Err(MemoryStorageError::Other(
                "Memory sink is busy; task was not sent".into(),
            )),
        };
        // Only this task missed the sink; earlier accepted tasks keep their keys.
        if result.is_err() && recorded {
            if let Some(key) = this.unflushed_keys.pop() {
                this.idempotency_keys.lock().unwrap().remove(&key);
            }
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        drop(this.ready.take());
        let result = {
            let mut lock = ready!(this.poll_lock(cx));
            Pin::new(&mut *lock).poll_flush_unpin(cx)
        };
        this.settle_unflushed_keys(&result, true);
        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        drop(this.ready.take());
        let result = {
            let mut lock = ready!(this.poll_lock(cx));
            Pin::new(&mut *lock).poll_close_unpin(cx)
        };
        this.settle_unflushed_keys(&result, true);
        result
    }
}

impl<Args> std::fmt::Debug for MemoryStorage<Args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("sender", &self.sender)
            .field("receiver", &"<Stream>")
            .finish()
    }
}

impl<Args> Stream for MemoryStorage<Args> {
    type Item = Task<Args>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.lock().unwrap().poll_next_unpin(cx)
    }
}

// MemoryStorage as a Backend
impl<Args> Backend for MemoryStorage<Args> {
    type Task = Task<Args>;

    type Error = MemoryStorageError;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_inner_ready(cx)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.receiver
            .lock()
            .unwrap()
            .poll_next_unpin(cx)
            .map(|item| item.map(Ok))
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        // Every `MemorySink` clone shares this sender, so closing it would end the
        // queue for producers and for workers that share this storage. Flush only.
        self.sender.poll_flush_unpin(cx)
    }
}

impl<Args> BackendConfig for MemoryStorage<Args> {
    type Id = RandomId;

    type Args = Args;

    type Kind = Ephemeral;

    type Config = ();

    type Layer = Identity;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}

impl<T: Send + 'static> TryNewBackend for MemoryStorage<T> {
    type Backend = Self;
    fn try_new(_: Self::Config) -> Result<Self::Backend, Self::Error> {
        Ok(Self::new())
    }
}
