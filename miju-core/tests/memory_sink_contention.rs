//! Wakeup and lock lifetime contracts for the in-memory sink.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
    time::{Duration, Instant},
};

use futures_channel::mpsc::unbounded;
use futures_util::{Sink, SinkExt, StreamExt, lock::Mutex, task::noop_waker_ref};
use miju_core::{
    backend::{
        Backend,
        memory::{MemorySink, MemoryStorage, MemoryStorageError},
    },
    task::Task,
    worker::context::WorkerContext,
};

type SharedSink =
    Arc<Mutex<Box<dyn Sink<Task<u32>, Error = MemoryStorageError> + Send + Sync + Unpin>>>;

struct Notifications {
    count: AtomicUsize,
    thread: Thread,
}

impl Default for Notifications {
    fn default() -> Self {
        Self {
            count: AtomicUsize::new(0),
            thread: thread::current(),
        }
    }
}

impl Notifications {
    fn wait_for_wake(&self, previous: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while self.count.load(Ordering::SeqCst) == previous {
            let now = Instant::now();
            assert!(
                now < deadline,
                "a pending sink operation must be notified so it can retry"
            );
            thread::park_timeout(deadline - now);
        }
    }
}

impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.thread.unpark();
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Ready,
    Flush,
    Close,
}

impl Operation {
    fn poll(
        self,
        sink: &mut MemorySink<u32>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MemoryStorageError>> {
        match self {
            Self::Ready => sink.poll_ready_unpin(cx),
            Self::Flush => sink.poll_flush_unpin(cx),
            Self::Close => sink.poll_close_unpin(cx),
        }
    }
}

fn check_unlock_notification(operation: Operation) {
    let (sender, _receiver) = unbounded::<Task<u32>>();
    let shared: SharedSink = Arc::new(Mutex::new(Box::new(
        sender.sink_map_err(MemoryStorageError::SendError),
    )));
    let mut sink = MemorySink::new(Arc::clone(&shared));
    let held = shared.try_lock().unwrap();
    let notifications = Arc::new(Notifications::default());
    let waker = Waker::from(Arc::clone(&notifications));
    let mut cx = Context::from_waker(&waker);

    assert!(operation.poll(&mut sink, &mut cx).is_pending());
    drop(held);
    notifications.wait_for_wake(0);
    assert!(matches!(
        operation.poll(&mut sink, &mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(
        shared.try_lock().is_some(),
        "completed polls must release the shared sink"
    );
}

#[test]
fn pending_readiness_is_notified_after_unlock() {
    check_unlock_notification(Operation::Ready);
}

#[test]
fn pending_flush_is_notified_after_unlock() {
    check_unlock_notification(Operation::Flush);
}

#[test]
fn pending_close_is_notified_after_unlock() {
    check_unlock_notification(Operation::Close);
}

#[test]
fn consumer_readiness_leaves_the_producer_able_to_send() {
    let (sender, receiver) = unbounded::<Task<u32>>();
    let shared: SharedSink = Arc::new(Mutex::new(Box::new(
        sender.sink_map_err(MemoryStorageError::SendError),
    )));
    let mut producer = MemorySink::new(shared);
    let mut backend = MemoryStorage::new_with(producer.clone(), receiver.boxed());
    let mut cx = Context::from_waker(noop_waker_ref());
    let worker = WorkerContext::new("memory-consumer");

    assert!(matches!(
        Backend::poll_ready(&mut backend, &mut cx, &worker),
        Poll::Ready(Ok(()))
    ));
    assert!(Backend::poll_next(&mut backend, &mut cx, &worker).is_pending());
    assert!(matches!(
        producer.poll_ready_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    Pin::new(&mut producer).start_send(Task::new(13)).unwrap();
    assert!(
        matches!(Backend::poll_next(&mut backend, &mut cx, &worker), Poll::Ready(Some(Ok(task))) if task.args == 13)
    );
}
