//! Distinct polling operations on one shared handle retain their own waiters.
#![cfg(feature = "shared")]

use futures_channel::oneshot;
use futures_util::{Sink, future::poll_fn, task::noop_waker_ref};
use miju_core::{
    backend::{Backend, FetchById, WireFormatBackend, codec::Codec, ext::shared::Shared},
    task::{Task, task_id::TaskId},
    worker::context::WorkerContext,
};
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Clone)]
struct Identity;
impl Codec<u32> for Identity {
    type Compact = u32;
    type Error = Infallible;
    fn encode(&self, value: &u32) -> Result<u32, Infallible> {
        Ok(*value)
    }
    fn decode(&self, value: &u32) -> Result<u32, Infallible> {
        Ok(*value)
    }
}

#[derive(Default)]
struct Gate {
    ready: AtomicBool,
    saved: Mutex<Option<Waker>>,
}
impl Gate {
    fn poll(&self, cx: &Context<'_>) -> Poll<Result<(), Infallible>> {
        if self.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            *self.saved.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    fn signal(&self) {
        self.ready.store(true, Ordering::SeqCst);
        let saved = self.saved.lock().unwrap().take();
        if let Some(waker) = saved {
            waker.wake();
        }
    }
}

struct Probe {
    gate: Arc<Gate>,
    lookup: Option<oneshot::Receiver<()>>,
}
impl WireFormatBackend for Probe {
    type Codec = Identity;
    type Compact = u32;
    fn codec(&self) -> &Identity {
        &Identity
    }
}
impl Backend for Probe {
    type Task = Task<u32>;
    type Error = Infallible;
    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Infallible>> {
        self.gate.poll(cx)
    }
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Task<u32>, Infallible>>> {
        self.gate.poll(cx).map(|r| Some(r.map(|()| Task::new(7))))
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Infallible>> {
        self.gate.poll(cx)
    }
}
impl Sink<Task<u32>> for Probe {
    type Error = Infallible;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.gate.poll(cx)
    }
    fn start_send(self: Pin<&mut Self>, _: Task<u32>) -> Result<(), Infallible> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.gate.poll(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.gate.poll(cx)
    }
}
impl FetchById for Probe {
    async fn fetch_by_id(&mut self, _: &TaskId) -> Result<Option<Task<u32>>, Infallible> {
        self.lookup.take().unwrap().await.unwrap();
        Ok(None)
    }
}

fn fixture() -> (Shared<Probe>, Arc<Gate>, oneshot::Sender<()>) {
    let gate = Arc::new(Gate::default());
    let (tx, rx) = oneshot::channel();
    (
        Shared::new(Probe {
            gate: gate.clone(),
            lookup: Some(rx),
        }),
        gate,
        tx,
    )
}
fn poll(shared: &mut Shared<Probe>, operation: usize, cx: &mut Context<'_>) -> Poll<()> {
    let worker = WorkerContext::new("operation-waiters");
    match operation {
        0 => Backend::poll_ready(shared, cx, &worker).map(Result::unwrap),
        1 => {
            Backend::poll_next(shared, cx, &worker).map(|r| assert_eq!(r.unwrap().unwrap().args, 7))
        }
        2 => Backend::poll_close(shared, cx, &worker).map(Result::unwrap),
        3 => Sink::poll_ready(Pin::new(shared), cx).map(Result::unwrap),
        4 => Sink::poll_flush(Pin::new(shared), cx).map(Result::unwrap),
        5 => Sink::poll_close(Pin::new(shared), cx).map(Result::unwrap),
        _ => unreachable!(),
    }
}
#[derive(Default)]
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn counter() -> (Arc<CountWake>, Waker) {
    let count = Arc::new(CountWake::default());
    (count.clone(), count.into())
}

#[test]
fn independent_futures_on_one_handle_both_receive_backend_notifications() {
    for first in 0..6 {
        for second in 0..6 {
            if first == second {
                continue;
            }
            let (shared, gate, _tx) = fixture();
            let handle = Mutex::new(shared);
            let (a, aw) = counter();
            let (b, bw) = counter();
            let mut fa = Box::pin(poll_fn(|cx| poll(&mut handle.lock().unwrap(), first, cx)));
            let mut fb = Box::pin(poll_fn(|cx| poll(&mut handle.lock().unwrap(), second, cx)));
            assert!(fa.as_mut().poll(&mut Context::from_waker(&aw)).is_pending());
            assert!(fb.as_mut().poll(&mut Context::from_waker(&bw)).is_pending());
            assert_eq!(a.0.load(Ordering::SeqCst), 0);
            assert_eq!(b.0.load(Ordering::SeqCst), 0);
            gate.signal();
            assert_eq!(
                a.0.load(Ordering::SeqCst),
                1,
                "first operation {first}, second {second}"
            );
            assert_eq!(b.0.load(Ordering::SeqCst), 1);
            assert!(fa.as_mut().poll(&mut Context::from_waker(&aw)).is_ready());
            assert!(fb.as_mut().poll(&mut Context::from_waker(&bw)).is_ready());
        }
    }
}

#[test]
fn distinct_operations_both_wake_after_management_releases_the_lock() {
    let (mut shared, _gate, tx) = fixture();
    let mut manager = shared.clone();
    let id = 1u64.into();
    let mut lookup = Box::pin(manager.fetch_by_id(&id));
    let mut quiet = Context::from_waker(noop_waker_ref());
    assert!(lookup.as_mut().poll(&mut quiet).is_pending());
    let (a, aw) = counter();
    let (b, bw) = counter();
    assert!(poll(&mut shared, 0, &mut Context::from_waker(&aw)).is_pending());
    assert!(poll(&mut shared, 4, &mut Context::from_waker(&bw)).is_pending());
    tx.send(()).unwrap();
    assert!(lookup.as_mut().poll(&mut quiet).is_ready());
    drop(lookup);
    assert_eq!(a.0.load(Ordering::SeqCst), 1);
    assert_eq!(b.0.load(Ordering::SeqCst), 1);
}

#[test]
fn repoll_replaces_only_that_operation_and_drop_releases_every_waiter() {
    for locked in [false, true] {
        let (mut shared, gate, tx) = fixture();
        let mut manager = shared.clone();
        let id = 1u64.into();
        let mut lookup = Box::pin(manager.fetch_by_id(&id));
        if locked {
            assert!(
                lookup
                    .as_mut()
                    .poll(&mut Context::from_waker(noop_waker_ref()))
                    .is_pending()
            );
        }
        let (old, old_waker) = counter();
        let (new, new_waker) = counter();
        assert!(poll(&mut shared, 0, &mut Context::from_waker(&old_waker)).is_pending());
        assert!(poll(&mut shared, 0, &mut Context::from_waker(&new_waker)).is_pending());
        let waiters: Vec<_> = (1..6)
            .map(|op| {
                let (count, waker) = counter();
                assert!(poll(&mut shared, op, &mut Context::from_waker(&waker)).is_pending());
                (count, waker)
            })
            .collect();
        if locked {
            tx.send(()).unwrap();
            assert!(
                lookup
                    .as_mut()
                    .poll(&mut Context::from_waker(noop_waker_ref()))
                    .is_ready()
            );
        } else {
            gate.signal();
        }
        assert_eq!(old.0.load(Ordering::SeqCst), 0);
        assert_eq!(new.0.load(Ordering::SeqCst), 1);
        for (count, _) in &waiters {
            assert_eq!(count.0.load(Ordering::SeqCst), 1);
        }
        drop(lookup);
        gate.ready.store(false, Ordering::SeqCst);
        for (op, (_, waker)) in waiters.iter().enumerate() {
            assert!(poll(&mut shared, op + 1, &mut Context::from_waker(waker)).is_pending());
        }
        drop(shared);
        for (count, _) in &waiters {
            assert_eq!(Arc::strong_count(count), 2);
        }
    }
}
