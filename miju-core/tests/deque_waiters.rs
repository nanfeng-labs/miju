//! Each live deque consumer keeps its own pending notification.

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
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
}

fn pending(queue: &mut VecDequeBackend<u32>, waker: &Waker) {
    assert!(
        Backend::poll_ready(
            queue,
            &mut Context::from_waker(waker),
            &WorkerContext::new("consumer"),
        )
        .is_pending()
    );
}

fn send(queue: &mut VecDequeBackend<u32>, value: u32) {
    assert!(matches!(
        Sink::poll_ready(Pin::new(queue), &mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    Pin::new(queue).start_send(Task::new(value)).unwrap();
}

#[test]
fn live_consumer_is_notified_after_another_clone_is_dropped() {
    let mut queue = VecDequeBackend::new();
    let mut peer = queue.clone();
    let wakes = Arc::new(WakeCounter::default());
    let waker = Waker::from(wakes.clone());
    let peer_waker = Waker::from(Arc::new(WakeCounter::default()));
    pending(&mut queue, &waker);
    pending(&mut peer, &peer_waker);
    drop(peer);
    drop(peer_waker);

    send(&mut queue.clone(), 7);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        queue.poll_next(
            &mut Context::from_waker(&waker),
            &WorkerContext::new("consumer"),
        ),
        Poll::Ready(Some(Ok(task))) if task.args == 7
    ));
}

#[test]
fn repoll_replaces_only_that_consumers_notification() {
    let mut queue = VecDequeBackend::<u32>::new();
    let mut peer = queue.clone();
    let old = Arc::new(WakeCounter::default());
    let replacement = Arc::new(WakeCounter::default());
    let other = Arc::new(WakeCounter::default());
    pending(&mut queue, &Waker::from(old.clone()));
    pending(&mut peer, &Waker::from(other.clone()));
    pending(&mut queue, &Waker::from(replacement.clone()));

    send(&mut queue.clone(), 7);
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert!(replacement.0.load(Ordering::SeqCst) > 0);
    assert!(other.0.load(Ordering::SeqCst) > 0);
}

#[test]
fn repoll_and_cancel_release_only_that_consumers_waker() {
    let mut queue = VecDequeBackend::new();
    let survivor = Arc::new(WakeCounter::default());
    let survivor_waker = Waker::from(survivor.clone());
    pending(&mut queue, &survivor_waker);

    for _ in 0..128 {
        let mut peer = queue.clone();
        let old = Arc::new(WakeCounter::default());
        let old_weak = Arc::downgrade(&old);
        let old_waker = Waker::from(old);
        pending(&mut peer, &old_waker);
        drop(old_waker);

        let replacement = Arc::new(WakeCounter::default());
        let replacement_weak = Arc::downgrade(&replacement);
        let replacement_waker = Waker::from(replacement);
        pending(&mut peer, &replacement_waker);
        assert!(old_weak.upgrade().is_none());
        drop(replacement_waker);
        drop(peer);
        assert!(replacement_weak.upgrade().is_none());
    }

    send(&mut queue.clone(), 9);
    assert!(survivor.0.load(Ordering::SeqCst) > 0);
}

fn empty_for(_: &str) -> VecDequeBackend<&str> {
    VecDequeBackend::new()
}

#[test]
fn notification_cleanup_does_not_restrict_borrowed_payload_drop_order() {
    let mut queue;
    let input = String::from("borrowed payload type");
    queue = empty_for(&input);
    assert!(
        Backend::poll_ready(
            &mut queue,
            &mut Context::from_waker(Waker::noop()),
            &WorkerContext::new("borrowed-consumer"),
        )
        .is_pending()
    );
}

struct SendOnWake(Mutex<Option<VecDequeBackend<u32>>>);

impl Wake for SendOnWake {
    fn wake(self: Arc<Self>) {
        let sender = self.0.lock().unwrap().take();
        if let Some(mut sender) = sender {
            send(&mut sender, 11);
        }
    }
}

#[test]
fn notifying_a_consumer_allows_its_waker_to_send() {
    let (completed, result) = mpsc::channel();
    let poller = thread::spawn(move || {
        let mut queue = VecDequeBackend::new();
        let waker = Waker::from(Arc::new(SendOnWake(Mutex::new(Some(queue.clone())))));
        pending(&mut queue, &waker);
        send(&mut queue.clone(), 7);
        let mut cx = Context::from_waker(Waker::noop());
        let worker = WorkerContext::new("callback-consumer");
        for expected in [7, 11] {
            assert!(matches!(
                queue.poll_next(&mut cx, &worker),
                Poll::Ready(Some(Ok(task))) if task.args == expected
            ));
        }
        completed.send(()).unwrap();
    });
    result
        .recv_timeout(Duration::from_secs(3))
        .expect("a notification must allow its callback's public send to complete");
    poller.join().unwrap();
}
