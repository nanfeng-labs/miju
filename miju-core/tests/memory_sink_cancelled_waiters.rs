//! Cancelling a sink operation must preserve external mutex notifications.

#![forbid(unsafe_code)]
use futures_util::{Sink, future::poll_fn, lock::Mutex};
use miju_core::{
    backend::memory::{MemorySink, MemoryStorageError},
    task::Task,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

type Shared =
    Arc<Mutex<Box<dyn Sink<Task<u32>, Error = MemoryStorageError> + Send + Sync + Unpin>>>;
struct Inner;
impl Sink<Task<u32>> for Inner {
    type Error = MemoryStorageError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, _: Task<u32>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
#[derive(Default)]
struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn setup() -> (MemorySink<u32>, Shared) {
    let shared: Shared = Arc::new(Mutex::new(Box::new(Inner)));
    (MemorySink::new(shared.clone()), shared)
}
fn counted() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count::default());
    let waker = Waker::from(count.clone());
    (count, waker)
}
fn operation(
    sink: &mut MemorySink<u32>,
    which: u8,
    cx: &mut Context<'_>,
) -> Poll<Result<(), MemoryStorageError>> {
    match which {
        0 => Pin::new(sink).poll_ready(cx),
        1 => Pin::new(sink).poll_flush(cx),
        _ => Pin::new(sink).poll_close(cx),
    }
}
fn cancellation_must_not_strand_external_owner(
    which: u8,
    cancel_after_unlock: bool,
    extra_unlocks: usize,
) {
    let (mut sink, shared) = setup();
    let held = shared.try_lock().unwrap();
    let (stale, stale_waker) = counted();
    let (active, active_waker) = counted();
    let mut pending_operation = Box::pin(poll_fn(|cx| operation(&mut sink, which, cx)));
    assert!(
        pending_operation
            .as_mut()
            .poll(&mut Context::from_waker(&stale_waker))
            .is_pending()
    );
    let mut external = Box::pin(shared.clone().lock_owned());
    assert!(
        external
            .as_mut()
            .poll(&mut Context::from_waker(&active_waker))
            .is_pending()
    );
    if cancel_after_unlock {
        drop(held);
        drop(pending_operation);
    } else {
        drop(pending_operation);
        drop(held);
    }
    for _ in 0..extra_unlocks {
        drop(shared.try_lock().expect("the underlying lock is available"));
    }
    let active_count = active.0.load(Ordering::SeqCst);
    println!(
        "operation={which} cancel_after_unlock={cancel_after_unlock} extra_unlocks={extra_unlocks} stale_notifications={} active_notifications={active_count}",
        stale.0.load(Ordering::SeqCst)
    );
    assert!(
        active_count > 0,
        "cancelling a pending Sink operation while retaining its handle must not strand an independent public mutex owner"
    );
    assert!(
        external
            .as_mut()
            .poll(&mut Context::from_waker(&active_waker))
            .is_ready()
    );
    // Keep the sink alive until after both notification and resumed acquisition.
    std::hint::black_box(&sink);
}
#[test]
fn cancelled_readiness_before_unlock_preserves_external_waiter() {
    cancellation_must_not_strand_external_owner(0, false, 0);
}
#[test]
fn cancelled_flush_before_unlock_preserves_external_waiter() {
    cancellation_must_not_strand_external_owner(1, false, 0);
}
#[test]
fn cancelled_close_before_unlock_preserves_external_waiter() {
    cancellation_must_not_strand_external_owner(2, false, 0);
}
#[test]
fn cancelled_flush_after_unlock_preserves_external_waiter() {
    cancellation_must_not_strand_external_owner(1, true, 0);
}
#[test]
fn repeated_unlocks_preserve_external_waiter_after_cancel() {
    cancellation_must_not_strand_external_owner(1, false, 3);
}
#[test]
fn dropping_entire_sink_allows_external_waiter_to_proceed() {
    let (mut sink, shared) = setup();
    let held = shared.try_lock().unwrap();
    let (_, stale_waker) = counted();
    let (active, active_waker) = counted();
    let mut pending_operation = Box::pin(poll_fn(|cx| Pin::new(&mut sink).poll_flush(cx)));
    assert!(
        pending_operation
            .as_mut()
            .poll(&mut Context::from_waker(&stale_waker))
            .is_pending()
    );
    let mut external = Box::pin(shared.clone().lock_owned());
    assert!(
        external
            .as_mut()
            .poll(&mut Context::from_waker(&active_waker))
            .is_pending()
    );
    drop(pending_operation);
    drop(held);
    drop(sink);
    assert!(active.0.load(Ordering::SeqCst) > 0);
    assert!(
        external
            .as_mut()
            .poll(&mut Context::from_waker(&active_waker))
            .is_ready()
    );
}
