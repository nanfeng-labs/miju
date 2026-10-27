//! Regression tests for in-memory task deduplication.

use std::{
    sync::{Arc, Barrier},
    task::{Context, Poll},
    thread,
};

use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt, lock::Mutex, task::noop_waker_ref};
use miju_core::{
    backend::memory::{MemorySink, MemoryStorageError},
    task::{Task, builder::TaskBuilder},
};

#[test]
fn concurrent_duplicates_preserve_the_original_task() {
    let (sender, mut receiver) = mpsc::unbounded::<Task<u32>>();
    let mut sink = MemorySink::new(Arc::new(Mutex::new(Box::new(
        sender.sink_map_err(MemoryStorageError::SendError),
    ))));
    let mut cx = Context::from_waker(noop_waker_ref());
    // A long legal key makes the concurrent lookups overlap without a private test hook.
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
                let mut sender = sink.clone();
                assert!(matches!(
                    sender.poll_ready_unpin(&mut cx),
                    Poll::Ready(Ok(()))
                ));
                (
                    sender,
                    TaskBuilder::new(8).idempotency_key(key.clone()).build(),
                )
            })
            .collect();
        let barrier = Barrier::new(senders.len());
        let results = thread::scope(|scope| {
            let handles: Vec<_> = senders
                .into_iter()
                .map(|(mut sender, task)| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        sender.start_send_unpin(task)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join())
                .collect::<Vec<_>>()
        });
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
