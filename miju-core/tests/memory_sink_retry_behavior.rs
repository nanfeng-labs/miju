//! Lock contention must make progress without stranding peers or polling an idle sink.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
    time::{Duration, Instant},
};

use futures_util::{Sink, SinkExt, lock::Mutex, task::AtomicWaker};
use miju_core::{
    backend::memory::{MemorySink, MemoryStorageError},
    task::Task,
};

type SharedSink =
    Arc<Mutex<Box<dyn Sink<Task<u32>, Error = MemoryStorageError> + Send + Sync + Unpin>>>;

struct Notifications {
    count: AtomicUsize,
    thread: Thread,
}

impl Notifications {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            thread: thread::current(),
        })
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn wait_after(&self, previous: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while self.count() == previous {
            let now = Instant::now();
            assert!(
                now < deadline,
                "a live operation must receive a retry notification"
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

#[derive(Default)]
struct Gate {
    ready: AtomicBool,
    waker: AtomicWaker,
    polls: AtomicUsize,
}

struct GatedSink(Arc<Gate>);

impl Sink<Task<u32>> for GatedSink {
    type Error = MemoryStorageError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.polls.fetch_add(1, Ordering::SeqCst);
        self.0.waker.register(cx.waker());
        if self.0.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, _: Task<u32>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_ready(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_ready(cx)
    }
}

fn setup(ready: bool) -> (MemorySink<u32>, SharedSink, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    gate.ready.store(ready, Ordering::SeqCst);
    let shared: SharedSink = Arc::new(Mutex::new(Box::new(GatedSink(Arc::clone(&gate)))));
    (MemorySink::new(Arc::clone(&shared)), shared, gate)
}

#[test]
fn long_contention_retries_without_busy_polling() {
    let (mut sink, shared, gate) = setup(true);
    let held = shared.try_lock().unwrap();
    let notifications = Notifications::new();
    let waker = Waker::from(Arc::clone(&notifications));
    let mut cx = Context::from_waker(&waker);
    let start = Instant::now();
    let mut polls = 0;
    while start.elapsed() < Duration::from_millis(30) {
        let previous = notifications.count();
        assert!(sink.poll_flush_unpin(&mut cx).is_pending());
        polls += 1;
        assert!(
            polls < 256,
            "contended operations must not continuously self-wake"
        );
        notifications.wait_after(previous);
    }
    assert_eq!(gate.polls.load(Ordering::SeqCst), 0);
    drop(held);
    assert!(matches!(
        sink.poll_flush_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    println!("contention duration={:?}, polls={polls}", start.elapsed());
}

#[test]
fn cancelled_retry_has_no_recurring_notifications() {
    let (mut sink, shared, _) = setup(true);
    let held = shared.try_lock().unwrap();
    let notifications = Notifications::new();
    let waker = Waker::from(Arc::clone(&notifications));
    assert!(
        sink.poll_flush_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );
    // The borrowing operation is abandoned, but both the sink and lock owner stay alive.
    notifications.wait_after(0);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(
        notifications.count(),
        1,
        "an abandoned handle must not re-arm itself"
    );
    drop(held);
    assert!(matches!(
        sink.poll_flush_unpin(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn inner_pending_uses_only_the_inner_notification_after_contention() {
    let (mut sink, shared, gate) = setup(false);
    let held = shared.try_lock().unwrap();
    let notifications = Notifications::new();
    let waker = Waker::from(Arc::clone(&notifications));
    let mut cx = Context::from_waker(&waker);
    assert!(sink.poll_flush_unpin(&mut cx).is_pending());
    notifications.wait_after(0);
    drop(held);
    assert!(sink.poll_flush_unpin(&mut cx).is_pending());
    let previous = notifications.count();
    thread::sleep(Duration::from_millis(20));
    assert_eq!(
        notifications.count(),
        previous,
        "inner Pending must not start retry polling"
    );
    assert_eq!(gate.polls.load(Ordering::SeqCst), 1);
    gate.ready.store(true, Ordering::SeqCst);
    gate.waker.wake();
    notifications.wait_after(previous);
    assert!(matches!(
        sink.poll_flush_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn clones_wait_independently_and_resuming_updates_the_waker() {
    let (mut first, shared, _) = setup(true);
    let mut second = first.clone();
    let held = shared.try_lock().unwrap();
    let first_notifications = Notifications::new();
    let second_notifications = Notifications::new();
    let stale = Waker::from(Notifications::new());
    assert!(
        first
            .poll_ready_unpin(&mut Context::from_waker(&stale))
            .is_pending()
    );
    let first_waker = Waker::from(Arc::clone(&first_notifications));
    let second_waker = Waker::from(Arc::clone(&second_notifications));
    assert!(
        first
            .poll_close_unpin(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second
            .poll_flush_unpin(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    drop(held);
    first_notifications.wait_after(0);
    second_notifications.wait_after(0);
    assert!(matches!(
        first.poll_close_unpin(&mut Context::from_waker(&first_waker)),
        Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        second.poll_flush_unpin(&mut Context::from_waker(&second_waker)),
        Poll::Ready(Ok(()))
    ));
}
