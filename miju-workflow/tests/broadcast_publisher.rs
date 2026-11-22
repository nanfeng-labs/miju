//! Exercise acknowledgement ownership when several publishers use the same sink.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_channel::{mpsc, oneshot};
use futures_util::{FutureExt, SinkExt, future, stream};
use miju_core::{
    backend::{
        WaitForCompletion,
        ext::shared::Shared,
        memory::{MemorySink, MemoryStorageError},
        persistence::{Persisted, Persistence, TaskEvent},
    },
    task::{Task, builder::TaskBuilder, task_id::TaskId},
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_workflow::{
    BroadcastFlow,
    broadcast::{Broadcast, Decision, FailureKind, when},
    in_memory::InMemoryWorkflow,
};

type Queue = Shared<InMemoryWorkflow<u32>>;
type WriteResult = Result<(), MemoryStorageError>;

/// The first batch waits for the test to release the write. Later writes succeed.
#[derive(Clone)]
struct Store {
    first_write: Arc<Mutex<Option<oneshot::Receiver<WriteResult>>>>,
    stored: Arc<Mutex<Vec<u32>>>,
}

impl Persistence for Store {
    type Compact = Vec<u8>;
    type Response = ();
    type Error = MemoryStorageError;

    async fn push_tasks(&mut self, tasks: Vec<Task<Vec<u8>>>) -> WriteResult {
        let first = self.first_write.lock().unwrap().take();
        if let Some(result) = first {
            result.await.unwrap()?;
        }
        let values = tasks
            .iter()
            .map(|task| serde_json::from_slice::<u32>(&task.args).unwrap());
        self.stored.lock().unwrap().extend(values);
        Ok(())
    }

    async fn register(&mut self, _: &WorkerContext) -> WriteResult {
        Ok(())
    }

    async fn heartbeat(&mut self, _: &WorkerContext) -> WriteResult {
        Ok(())
    }

    async fn fetch_next(&mut self, _: &WorkerContext) -> Result<Vec<Task<Vec<u8>>>, Self::Error> {
        Ok(Vec::new())
    }

    async fn reenqueue_abandoned(
        &mut self,
        _: Vec<Task<Vec<u8>>>,
        _: &WorkerContext,
    ) -> Result<u64, Self::Error> {
        Ok(0)
    }

    async fn handle_events(&mut self, _: Vec<TaskEvent<()>>, _: &WorkerContext) -> WriteResult {
        Ok(())
    }
}

fn broadcast() -> (
    Broadcast<u32, Queue>,
    oneshot::Sender<WriteResult>,
    Arc<Mutex<Vec<u32>>>,
) {
    let (release, first_write) = oneshot::channel();
    let stored = Arc::default();
    let store = Store {
        first_write: Arc::new(Mutex::new(Some(first_write))),
        stored: Arc::clone(&stored),
    };
    let flow = BroadcastFlow::new("orders")
        .subscriber("disk")
        .subscriber("healthy")
        .build_with(|name| {
            if name == "disk" {
                let sink = MemorySink::new(Arc::new(futures_util::lock::Mutex::new(Box::new(
                    Persisted::new(store.clone()),
                ))));
                InMemoryWorkflow::new_with(sink, Box::pin(stream::pending()))
            } else {
                InMemoryWorkflow::create()
            }
        })
        .unwrap();
    (flow, release, stored)
}

#[tokio::test]
async fn concurrent_publishers_attribute_a_flush_error_to_its_own_delivery() {
    let (flow, release, stored) = broadcast();
    let mut a = flow.publisher();
    let mut b = flow.publisher();
    let mut first = Box::pin(a.publish(1));
    let mut second = Box::pin(b.publish(2));
    assert!(first.as_mut().now_or_never().is_none());
    assert!(second.as_mut().now_or_never().is_none());
    release
        .send(Err(MemoryStorageError::Other("write failed".into())))
        .unwrap();

    // Poll the competing publication first, while the original flush is ready to fail.
    let (second, first) = tokio::time::timeout(Duration::from_secs(5), future::join(second, first))
        .await
        .unwrap();
    let first = first.unwrap();
    let second = second.unwrap();
    for (receipt, kind) in [
        (first, FailureKind::Unconfirmed),
        (second, FailureKind::Disabled),
    ] {
        assert_eq!(receipt.deliveries.len(), 1);
        assert_eq!(receipt.deliveries[0].subscriber, "healthy");
        assert_eq!(receipt.failures.len(), 1);
        assert_eq!(receipt.failures[0].subscriber, "disk");
        assert_eq!(receipt.failures[0].kind, kind);
    }
    assert!(stored.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_publishers_preserve_an_accepted_write_and_release_waiters() {
    let (flow, release, stored) = broadcast();
    let mut a = flow.publisher();
    let mut b = flow.publisher();
    let mut first = Box::pin(a.publish(1));
    assert!(first.as_mut().now_or_never().is_none());
    assert!(b.publish(2).now_or_never().is_none()); // Drop the waiting publication.
    drop(first); // Cancel the owner while storage is still flushing its task.
    let mut third = Box::pin(b.publish(3));
    assert!(third.as_mut().now_or_never().is_none());
    release.send(Ok(())).unwrap();

    let receipt = tokio::time::timeout(Duration::from_secs(5), third)
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.failures.is_empty());
    assert_eq!(receipt.deliveries.len(), 2);
    assert_eq!(*stored.lock().unwrap(), [1, 3]);
}

#[tokio::test]
async fn a_cancelled_publishers_flush_error_disables_the_member_before_the_next_send() {
    let (flow, release, stored) = broadcast();
    assert!(flow.publisher().publish(1).now_or_never().is_none());
    release
        .send(Err(MemoryStorageError::Other("write failed".into())))
        .unwrap();
    let mut publisher = flow.publisher();
    let second = tokio::time::timeout(Duration::from_secs(5), publisher.publish(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.failures.len(), 1);
    assert_eq!(second.failures[0].subscriber, "disk");
    assert_eq!(second.failures[0].kind, FailureKind::Disabled);
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(second.deliveries[0].subscriber, "healthy");
    assert!(stored.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_stopping_worker_leaves_the_write_result_to_the_publisher() {
    let (flow, release, stored) = broadcast();
    let context = WorkerContext::new("disk");
    let worker = WorkerBuilder::new(&context)
        .backend(flow.backend("disk").unwrap())
        .build(when(|_: &u32| true, |_: u32| async {}));
    let mut running = Box::pin(worker.run());
    assert!(running.as_mut().now_or_never().is_none());

    let mut publisher = flow.publisher();
    let mut write = Box::pin(publisher.publish(1));
    assert!(write.as_mut().now_or_never().is_none());
    context.stop().unwrap();
    release
        .send(Err(MemoryStorageError::Other("write failed".into())))
        .unwrap();
    // Poll shutdown first while the publisher's failed flush is ready.
    let stopped = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap();
    let receipt = write.await.unwrap();
    assert_eq!(receipt.deliveries.len(), 1);
    assert_eq!(receipt.deliveries[0].subscriber, "healthy");
    assert_eq!(receipt.failures[0].kind, FailureKind::Unconfirmed);
    assert!(stored.lock().unwrap().is_empty());
    stopped.unwrap();
}

#[tokio::test]
async fn a_worker_can_drain_its_receiver_after_the_sending_handle_fails() {
    let (tx, rx) = mpsc::unbounded();
    drop(rx);
    let sink = MemorySink::new(Arc::new(futures_util::lock::Mutex::new(Box::new(
        tx.sink_map_err(MemoryStorageError::from),
    ))));
    let id = TaskId::from(11);
    let queued = TaskBuilder::new(b"17".to_vec()).task_id(id.clone()).build();
    let mut queue = InMemoryWorkflow::<u32>::new_with(sink, Box::pin(stream::iter([queued])));
    let orders = BroadcastFlow::new("orders")
        .subscriber("disk")
        .build_with(|_| queue.clone())
        .unwrap();

    let receipt = orders.publisher().publish(18).await.unwrap();
    assert_eq!(receipt.failures[0].kind, FailureKind::NotSent);
    WorkerBuilder::new("disk")
        .backend(orders.backend("disk").unwrap())
        .build(when(|_: &u32| true, |n: u32| async move { n }))
        .run()
        .await
        .unwrap();
    let results = WaitForCompletion::<Decision<u32>>::check_status(&mut queue, [id])
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].result, Ok(Decision::Handled(17)));
}
