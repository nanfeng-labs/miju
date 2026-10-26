//! Queue readiness notifications under concurrent submission.

use std::{
    pin::Pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
};

use futures_sink::Sink;
use futures_util::task::noop_waker_ref;
use miju_core::{
    backend::{Backend, dequeue::VecDequeBackend},
    task::Task,
    worker::context::WorkerContext,
};

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn deque_submission_racing_readiness_is_ready_or_notified() {
    const ROUNDS: u32 = 10_000;
    let mut consumer = VecDequeBackend::new();
    let mut producer = consumer.clone();
    let rendezvous = Barrier::new(2);
    let worker = WorkerContext::new("readiness-race");
    let mut missed_notifications = 0;
    let mut received = Vec::new();

    thread::scope(|scope| {
        let rendezvous = &rendezvous;
        scope.spawn(move || {
            let mut cx = Context::from_waker(noop_waker_ref());
            for value in 0..ROUNDS {
                rendezvous.wait();
                assert!(matches!(
                    Sink::poll_ready(Pin::new(&mut producer), &mut cx),
                    Poll::Ready(Ok(()))
                ));
                Sink::start_send(Pin::new(&mut producer), Task::new(value)).unwrap();
                rendezvous.wait();
            }
        });
        for _ in 0..ROUNDS {
            let count = Arc::new(WakeCount(AtomicUsize::new(0)));
            let waker = Waker::from(count.clone());
            let mut cx = Context::from_waker(&waker);
            rendezvous.wait();
            let ready = Backend::poll_ready(&mut consumer, &mut cx, &worker);
            rendezvous.wait();
            if ready.is_pending() && count.0.load(Ordering::SeqCst) == 0 {
                missed_notifications += 1;
            }
            // Keep driving after recording a failure so the producer can finish.
            if matches!(
                Backend::poll_ready(&mut consumer, &mut cx, &worker),
                Poll::Ready(Ok(()))
            ) {
                if let Poll::Ready(Some(Ok(task))) =
                    Backend::poll_next(&mut consumer, &mut cx, &worker)
                {
                    received.push(task.args);
                }
            }
        }
    });

    assert_eq!(missed_notifications, 0, "a pending consumer missed a send");
    assert_eq!(received, (0..ROUNDS).collect::<Vec<_>>());
}
