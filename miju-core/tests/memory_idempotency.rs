//! Regression tests for in-memory task deduplication.

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt, lock::Mutex, task::noop_waker_ref};
use miju_core::{
    backend::memory::{MemorySink, MemoryStorageError},
    task::{Task, builder::TaskBuilder},
};
use tokio::sync::Barrier;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_preserve_the_original_task() {
    let (sender, mut receiver) = mpsc::unbounded::<Task<u32>>();
    let mut sink = MemorySink::new(Arc::new(Mutex::new(Box::new(
        sender.sink_map_err(MemoryStorageError::SendError),
    ))));
    let mut cx = Context::from_waker(noop_waker_ref());
    // Exercise long keys while concurrent submissions wait for readiness.
    let key = "k".repeat(2_000_000);
    assert!(matches!(
        sink.poll_ready_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    sink.start_send_unpin(TaskBuilder::new(7).idempotency_key(key.clone()).build())
        .unwrap();

    for _ in 0..4 {
        let senders: Vec<_> = (0..4)
            .map(|_| {
                let sender = sink.clone();
                (
                    sender,
                    TaskBuilder::new(8).idempotency_key(key.clone()).build(),
                )
            })
            .collect();
        let barrier = Arc::new(Barrier::new(senders.len()));
        let handles = senders.into_iter().map(|(mut sender, task)| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                sender.send(task).await
            })
        });
        let results = futures_util::future::join_all(handles).await;
        assert!(
            results
                .into_iter()
                .all(|result| matches!(result, Ok(Ok(())))),
            "concurrent duplicate submissions must succeed without panicking"
        );
    }

    let Poll::Ready(Some(task)) = receiver.poll_next_unpin(&mut cx) else {
        panic!("the original task was not delivered");
    };
    assert_eq!(task.args, 7);
    assert!(task.task_id().is_some());
    assert!(receiver.poll_next_unpin(&mut cx).is_pending());

    assert!(matches!(
        sink.poll_ready_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    sink.start_send_unpin(Task::new(9)).unwrap();
    assert!(matches!(receiver.poll_next_unpin(&mut cx), Poll::Ready(Some(task)) if task.args == 9));
}

#[test]
fn debug_does_not_expose_idempotency_keys() {
    let (sender, _receiver) = mpsc::unbounded::<Task<u32>>();
    let mut sink = MemorySink::new(Arc::new(Mutex::new(Box::new(
        sender.sink_map_err(MemoryStorageError::SendError),
    ))));
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(matches!(
        sink.poll_ready_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    sink.start_send_unpin(TaskBuilder::new(1).idempotency_key("private-key").build())
        .unwrap();
    assert!(!format!("{sink:?}").contains("private-key"));
}
