use futures_core::stream::BoxStream;
use futures_sink::Sink;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt::Debug, sync::Arc};

use miju_core::{
    backend::{
        factory::BackendFactory,
        memory::{MemorySink, MemoryStorage, MemoryStorageError},
        queue::Queue,
    },
    task::{Task, builder::TaskBuilder},
};

use crate::JsonStorage;
/// Sharable JSON based backend.
///
/// The [`SharedJsonStore`] allows multiple task types to be stored
/// and processed concurrently using a single temporary JSON file.
/// It is useful for testing, prototyping,
/// or sharing state between workers in a single process.
///
/// Polling a receiver waits synchronously if a writer holds the shared entry
/// lock. This can block the polling thread and has no fixed time limit.
///
/// # Example
///
/// ```rust,no_run
/// # use miju_core::backend::factory::BackendFactory;
/// # use miju_core::task::Task;
/// # use miju_core::worker::context::WorkerContext;
/// # use miju_core::worker::builder::WorkerBuilder;
/// # use miju_file_storage::SharedJsonStore;
/// # use miju_core::error::BoxDynError;
/// # use std::time::Duration;
/// # use miju_core::backend::TaskSink;
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() {
///     let mut store = SharedJsonStore::new();
///     let mut int_store = store.create().unwrap();
///     int_store.push(42).await.unwrap();
///
///     async fn task(
///         task: u32,
///         worker: WorkerContext,
///     ) -> Result<(), BoxDynError> {
///         tokio::time::sleep(Duration::from_millis(2)).await;
///         worker.stop()?;
///         Ok(())
///     }
///
///     let int_worker = WorkerBuilder::new("int-worker")
///         .backend(int_store)
///         .build(task)
///         .run();
///
///     int_worker.await.unwrap();
/// }
/// ```
///
/// Streams are filtered by task type; handles use the memory backend contract.
/// See the crate-level limitations before using this store with concurrent writers.
#[derive(Debug, Clone)]
pub struct SharedJsonStore {
    inner: JsonStorage<Value>,
}

impl Default for SharedJsonStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedJsonStore {
    /// Create a new instance of the shared JSON store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: JsonStorage::new_temp().unwrap(),
        }
    }
}

impl<Args: Send + Serialize + for<'de> Deserialize<'de> + Unpin + 'static + Clone>
    BackendFactory<Args> for SharedJsonStore
{
    type Backend = MemoryStorage<Args>;

    type Error = MemoryStorageError;

    fn create(&mut self) -> Result<Self::Backend, Self::Error> {
        self.create_with_config(())
    }

    fn create_with_config(&mut self, _: ()) -> Result<Self::Backend, Self::Error> {
        let (sender, receiver) = self.create_channel::<Args>();
        let sender = MemorySink::new(Arc::new(futures_util::lock::Mutex::new(sender)));
        Ok(MemoryStorage::new_with(sender, receiver))
    }
}

type BoxSink<Args> =
    Box<dyn Sink<Task<Args>, Error = MemoryStorageError> + Send + Sync + Unpin + 'static>;

impl SharedJsonStore {
    fn create_channel<Args: 'static + for<'de> Deserialize<'de> + Serialize + Send + Unpin>(
        &self,
    ) -> (BoxSink<Args>, BoxStream<'static, Task<Args>>) {
        // Create a channel for communication
        let sender = self.inner.clone();

        let queue_config = std::any::type_name::<Args>();

        // Create a wrapped sender that will insert into the in-memory store
        let wrapped_sender = {
            let sender = sender.clone();

            sender
                .sink_map_err(|e| MemoryStorageError::Other(e.into()))
                .with_flat_map(move |mut task: Task<Args>| {
                    task.inject_metadata(&Queue::from(queue_config)).unwrap();
                    let res = task.try_map_args(|s| {
                        serde_json::to_value(s).map_err(|e| MemoryStorageError::Other(e.into()))
                    });

                    futures_util::stream::iter(vec![res])
                })
        };

        // Create a stream that filters by type T
        let filtered_stream = {
            let queue_config = std::any::type_name::<Args>().to_owned();
            let mut sender = sender;
            futures_util::stream::poll_fn(move |cx| sender.poll_next_inner(cx, true))
                .map(|s| s.unwrap())
                .filter_map(move |(_, job)| {
                    let queue_config = queue_config.clone();
                    async move {
                        let queue = job.ctx.get("queue").unwrap_or_default();
                        if queue == queue_config {
                            let args = Args::deserialize(&job.args).ok()?;
                            let task = TaskBuilder::new(args)
                                .with_metadata(job.ctx)
                                .task_id(job.task_id)
                                .build();
                            Some(task)
                        } else {
                            None
                        }
                    }
                })
        };

        // Combine the sender and receiver
        let sender = Box::new(wrapped_sender)
            as Box<dyn Sink<Task<Args>, Error = MemoryStorageError> + Send + Sync + Unpin>;
        let receiver = filtered_stream.boxed();

        (sender, receiver)
    }
}
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use miju_core::error::BoxDynError;

    use miju_core::worker::context::WorkerContext;
    use miju_core::{
        backend::{TaskSink, factory::BackendFactory},
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use super::*;

    #[test]
    fn submission_callbacks_run_outside_the_entry_lock() {
        use futures_util::FutureExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct Args(Option<Arc<dyn Fn() + Send + Sync>>);
        impl Serialize for Args {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                if let Some(check) = &self.0 {
                    check();
                }
                serializer.serialize_u32(42)
            }
        }
        impl<'de> Deserialize<'de> for Args {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let _ = u32::deserialize(deserializer)?;
                Ok(Self(None))
            }
        }
        struct Extension {
            on_clone: Arc<dyn Fn() + Send + Sync>,
            on_drop: Arc<dyn Fn() + Send + Sync>,
        }
        impl Clone for Extension {
            fn clone(&self) -> Self {
                (self.on_clone)();
                Self {
                    on_clone: Arc::clone(&self.on_clone),
                    on_drop: Arc::clone(&self.on_drop),
                }
            }
        }
        impl Drop for Extension {
            fn drop(&mut self) {
                (self.on_drop)();
            }
        }
        let mut factory = SharedJsonStore::new();
        let mut producer: MemoryStorage<Args> = factory.create().unwrap();
        let serialized = Arc::new(AtomicUsize::new(0));
        let cloned = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let locked = Arc::new(AtomicUsize::new(0));
        let observe = |count: &Arc<AtomicUsize>| -> Arc<dyn Fn() + Send + Sync> {
            let count = Arc::clone(count);
            let locked = Arc::clone(&locked);
            let entries = Arc::clone(&factory.inner.entries);
            Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                if entries.try_write().is_err() {
                    locked.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        // Missing task ID exercises raw start_send's into_builder, including
        // extension cloning and destruction through the real public send path.
        let task = TaskBuilder::new(Args(Some(observe(&serialized))))
            .data(Extension {
                on_clone: observe(&cloned),
                on_drop: observe(&dropped),
            })
            .build();
        assert!(task.task_id().is_none());
        // Keep a shared context so into_builder cannot simply unwrap its Arc.
        let retained = task.clone();
        producer.push_task(task).now_or_never().unwrap().unwrap();
        assert!(dropped.load(Ordering::SeqCst) > 0);
        drop(retained);
        assert!(serialized.load(Ordering::SeqCst) > 0);
        assert!(cloned.load(Ordering::SeqCst) > 0);
        assert!(dropped.load(Ordering::SeqCst) > 0);
        assert_eq!(locked.load(Ordering::SeqCst), 0);
    }

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_shared() {
        let mut store = SharedJsonStore::new();
        let mut string_store = store.create().unwrap();
        let mut int_store = store.create_with_config(()).unwrap();
        for i in 0..ITEMS {
            string_store.push(format!("ITEM: {i}")).await.unwrap();
            int_store.push(i).await.unwrap();
        }

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if task == ITEMS - 1 {
                worker.stop()?;
                Err("Worker stopped!")?;
            }
            Ok(())
        }

        let string_worker = WorkerBuilder::new("rango-tango-string")
            .backend(string_store)
            .on_event(|worker, ev| {
                println!("CTX {:?}, On Event = {ev:?}", worker.name());
            })
            .build(|req: String, worker: WorkerContext| async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                println!("{req}");
                if req.ends_with(&(ITEMS - 1).to_string()) {
                    worker.stop().unwrap();
                }
            })
            .run();

        let int_worker = WorkerBuilder::new("rango-tango-int")
            .backend(int_store)
            .on_event(|worker, ev| {
                println!("CTX {:?}, On Event = {ev:?}", worker.name());
            })
            .build(task)
            .run();

        let _ = futures_util::future::join(int_worker, string_worker).await;
    }
}
