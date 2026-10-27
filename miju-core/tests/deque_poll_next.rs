//! A live deque waits for future tasks after its queued tasks are consumed.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use futures_sink::Sink;
use miju_core::{
    backend::{Backend, dequeue::VecDequeBackend},
    task::Task,
    worker::context::WorkerContext,
};

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct SendOnDrop(VecDequeBackend<u32>);

impl Wake for SendOnDrop {
    fn wake(self: Arc<Self>) {
        drop(self);
    }
}

impl Drop for SendOnDrop {
    fn drop(&mut self) {
        send(&mut self.0, &mut Context::from_waker(Waker::noop()), 47);
    }
}

fn send(queue: &mut VecDequeBackend<u32>, cx: &mut Context<'_>, value: u32) {
    assert!(matches!(
        Sink::poll_ready(Pin::new(queue), cx),
        Poll::Ready(Ok(()))
    ));
    Pin::new(queue).start_send(Task::new(value)).unwrap();
}

fn take(queue: &mut VecDequeBackend<u32>, cx: &mut Context<'_>, worker: &WorkerContext) -> u32 {
    match queue.poll_next(cx, worker) {
        Poll::Ready(Some(Ok(task))) => task.args,
        other => panic!("expected a queued task, got {other:?}"),
    }
}

#[test]
fn peer_draining_a_ready_queue_waits_for_later_tasks() {
    let mut queue = VecDequeBackend::new();
    let mut peer = queue.clone();
    let wakes = Arc::new(WakeCounter::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let worker = WorkerContext::new("waiting-consumer");

    send(&mut peer, &mut cx, 7);
    assert!(matches!(
        Backend::poll_ready(&mut queue, &mut cx, &worker),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(take(&mut peer, &mut cx, &worker), 7);
    assert!(queue.poll_next(&mut cx, &worker).is_pending());

    send(&mut peer, &mut cx, 9);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert_eq!(take(&mut queue, &mut cx, &worker), 9);
}

#[test]
fn empty_next_replaces_cancelled_wait_and_preserves_fifo() {
    let mut queue = VecDequeBackend::with_capacity(2);
    let mut peer = queue.clone();
    let old_wakes = Arc::new(WakeCounter::default());
    let old_waker = Waker::from(old_wakes.clone());
    let worker = WorkerContext::new("replacement-consumer");
    assert!(
        peer.poll_next(&mut Context::from_waker(&old_waker), &worker)
            .is_pending()
    );
    drop(peer);
    drop(old_waker);

    let wakes = Arc::new(WakeCounter::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(queue.poll_next(&mut cx, &worker).is_pending());
    send(&mut queue.clone(), &mut cx, 3);
    send(&mut queue.clone(), &mut cx, 4);
    assert_eq!(old_wakes.0.load(Ordering::SeqCst), 0);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert_eq!(take(&mut queue, &mut cx, &worker), 3);
    assert_eq!(take(&mut queue, &mut cx, &worker), 4);
    assert!(queue.poll_next(&mut cx, &worker).is_pending());
}

#[test]
fn replacing_an_old_waker_allows_its_destructor_to_send() {
    let (completed, result) = mpsc::channel();
    let poller = thread::spawn(move || {
        let mut queue = VecDequeBackend::new();
        let old = Waker::from(Arc::new(SendOnDrop(queue.clone())));
        let worker = WorkerContext::new("waker-destructor");
        assert!(
            Backend::poll_ready(&mut queue, &mut Context::from_waker(&old), &worker).is_pending()
        );
        drop(old);

        let wakes = Arc::new(WakeCounter::default());
        let waker = Waker::from(wakes.clone());
        let next = queue.poll_next(&mut Context::from_waker(&waker), &worker);
        completed
            .send((
                matches!(next, Poll::Ready(Some(Ok(task))) if task.args == 47),
                wakes.0.load(Ordering::SeqCst),
            ))
            .unwrap();
    });
    let (received, wakes) = result
        .recv_timeout(Duration::from_secs(3))
        .expect("replacing the old waker must not deadlock its destructor's send");
    poller.join().unwrap();
    assert!(received);
    assert!(wakes > 0);
}
