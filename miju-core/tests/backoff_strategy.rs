//! Backoff wake continuity and compatibility through public polling APIs.
#![cfg(feature = "sleep")]

use std::{
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use futures_util::SinkExt;

use miju_core::{
    backend::{
        Backend,
        ext::poll_strategy::{
            BackoffConfig, BackoffStrategy, PollSnapshot, PollStrategy, PollWith, Strategy,
        },
        memory::MemoryStorage,
    },
    task::Task,
    worker::context::WorkerContext,
};

struct Notify(mpsc::Sender<()>);

impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn assert_idle_backend_keeps_waking(strategy: impl PollStrategy) {
    let mut backend = PollWith::new(MemoryStorage::<()>::new(), strategy);
    let worker = WorkerContext::new("idle-backoff");
    let (tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Notify(tx)));
    let mut cx = Context::from_waker(&waker);
    assert!(backend.poll_ready(&mut cx, &worker).is_ready());

    for tick in 1..=3 {
        assert!(backend.poll_next(&mut cx, &worker).is_pending());
        assert!(
            rx.recv_timeout(Duration::from_secs(2)).is_ok(),
            "idle backend did not receive wake {tick}"
        );
    }
}

#[test]
fn backoff_keeps_waking_an_idle_backend() {
    let config = BackoffConfig::new(Duration::from_millis(40)).with_jitter(0.0);
    assert_idle_backend_keeps_waking(BackoffStrategy::new(Duration::from_millis(10), config));
}

#[test]
fn combined_backoff_keeps_waking_without_poll_notifications() {
    let config = BackoffConfig::new(Duration::from_millis(40)).with_jitter(0.0);
    let strategy = BackoffStrategy::new(Duration::from_millis(10), config);
    assert_idle_backend_keeps_waking(Strategy::new().apply(strategy));
}

#[test]
fn zero_backoff_keeps_waking_an_idle_backend() {
    let config = BackoffConfig::new(Duration::ZERO).with_jitter(0.0);
    assert_idle_backend_keeps_waking(BackoffStrategy::new(Duration::ZERO, config));
}

#[test]
fn backoff_successor_uses_the_latest_poll_result() {
    let config = BackoffConfig::new(Duration::from_secs(1)).with_jitter(0.0);
    let strategy = BackoffStrategy::new(Duration::from_millis(40), config);
    let mut backend = PollWith::new(MemoryStorage::<()>::new(), strategy);
    let worker = WorkerContext::new("updated-backoff");
    let (tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Notify(tx)));
    let mut cx = Context::from_waker(&waker);
    assert!(backend.poll_ready(&mut cx, &worker).is_ready());
    assert!(backend.poll_next(&mut cx, &worker).is_pending());
    rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let started = Instant::now();
    assert!(backend.poll_next(&mut cx, &worker).is_pending());
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    // Both pending polls count: 40 ms -> 80 ms -> 160 ms.
    assert!(started.elapsed() >= Duration::from_millis(120));
}

#[test]
fn previously_notified_backoff_keeps_waking_after_becoming_a_child() {
    let config = BackoffConfig::new(Duration::from_millis(40)).with_jitter(0.0);
    let mut strategy = BackoffStrategy::new(Duration::from_millis(10), config);
    strategy.on_poll(&PollSnapshot::new());
    assert_idle_backend_keeps_waking(Strategy::new().apply(strategy));
}

fn notification() -> (Waker, mpsc::Receiver<()>) {
    let (tx, rx) = mpsc::channel();
    (Waker::from(Arc::new(Notify(tx))), rx)
}

fn config() -> BackoffConfig {
    BackoffConfig::new(Duration::from_secs(2)).with_jitter(0.0)
}

#[test]
fn pending_poll_does_not_restart_the_running_timer() {
    let mut strategy = BackoffStrategy::new(Duration::from_millis(200), config());
    let (waker, rx) = notification();
    let mut cx = Context::from_waker(&waker);
    let idle = PollSnapshot::new();
    let started = Instant::now();
    assert!(strategy.poll_drive(&mut cx, &idle).is_pending());
    strategy.on_poll(&idle);
    for _ in 0..6 {
        assert!(strategy.poll_drive(&mut cx, &idle).is_pending());
        strategy.on_poll(&idle);
    }
    rx.recv_timeout(Duration::from_millis(700))
        .expect("initial timer must keep its 200 ms deadline");
    assert!(started.elapsed() >= Duration::from_millis(170));
}

#[test]
fn ready_backend_resets_future_backoff_to_the_default() {
    let mut strategy = BackoffStrategy::new(Duration::from_millis(20), config());
    for _ in 0..5 {
        strategy.on_poll(&PollSnapshot::new());
    }
    let mut backend = PollWith::new(MemoryStorage::<u32>::new(), strategy);
    let worker = WorkerContext::new("ready-reset");
    let (waker, rx) = notification();
    let mut cx = Context::from_waker(&waker);
    assert!(backend.poll_ready(&mut cx, &worker).is_ready());
    assert!(backend.poll_next(&mut cx, &worker).is_pending());
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(backend.poll_ready_unpin(&mut cx).is_ready());
    backend.start_send_unpin(Task::new(42)).unwrap();
    while rx.try_recv().is_ok() {}
    assert!(matches!(
        backend.poll_next(&mut cx, &worker),
        Poll::Ready(Some(Ok(_)))
    ));
    // This next explicit poll also creates the timer on the unmodified baseline.
    assert!(backend.poll_next(&mut cx, &worker).is_pending());
    rx.recv_timeout(Duration::from_millis(300))
        .expect("ready result resets the next timer to 20 ms");
}

#[test]
fn clone_and_waker_replacement_keep_notifications_separate() {
    let mut original = BackoffStrategy::new(Duration::from_millis(200), config());
    let idle = PollSnapshot::new();
    let (first, first_rx) = notification();
    let (latest, latest_rx) = notification();
    let (cloned_waker, cloned_rx) = notification();
    assert!(
        original
            .poll_drive(&mut Context::from_waker(&first), &idle)
            .is_pending()
    );
    first_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(
        original
            .poll_drive(&mut Context::from_waker(&first), &idle)
            .is_ready()
    );
    let mut cloned = original.clone();
    assert!(
        cloned
            .poll_drive(&mut Context::from_waker(&cloned_waker), &idle)
            .is_pending()
    );
    let _ = original.poll_drive(&mut Context::from_waker(&latest), &idle);
    original.on_poll(&idle);
    latest_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    cloned_rx.recv_timeout(Duration::from_secs(2)).unwrap();
}

#[test]
fn direct_strategy_survives_zero_duration_waker_changes() {
    let mut strategy = BackoffStrategy::new(Duration::ZERO, config());
    let snapshot = PollSnapshot::new();
    for _ in 0..300 {
        let (waker, rx) = notification();
        let mut cx = Context::from_waker(&waker);
        let _ = strategy.poll_drive(&mut cx, &snapshot);
        strategy.on_poll(&snapshot);
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
}

#[test]
fn drop_releases_the_timer_waker() {
    let mut strategy = BackoffStrategy::new(Duration::from_millis(20), config());
    let snapshot = PollSnapshot::new();
    let (tx, rx) = mpsc::channel();
    let notify = Arc::new(Notify(tx));
    let weak = Arc::downgrade(&notify);
    let waker = Waker::from(notify);
    assert!(
        strategy
            .poll_drive(&mut Context::from_waker(&waker), &snapshot)
            .is_pending()
    );
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(
        strategy
            .poll_drive(&mut Context::from_waker(&waker), &snapshot)
            .is_ready()
    );
    drop(strategy);
    drop(waker);
    let deadline = Instant::now() + Duration::from_secs(2);
    while weak.upgrade().is_some() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        weak.upgrade().is_none(),
        "dropping the strategy retained its task waker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_executor_repolls_after_wake_during_delayed_feedback() {
    let task = tokio::spawn(async {
        let mut strategy = BackoffStrategy::new(
            Duration::from_millis(20),
            BackoffConfig::new(Duration::from_millis(80)).with_jitter(0.0),
        );
        let snapshot = PollSnapshot::new();
        let mut ticks = 0;
        futures_util::future::poll_fn(|cx| {
            if strategy.poll_drive(cx, &snapshot).is_ready() {
                ticks += 1;
                if ticks == 1 {
                    // Controlled scheduler-pause analogue, outside any production Backend:
                    // the timer expires while this executor task is still being polled.
                    std::thread::sleep(Duration::from_millis(150));
                }
            }
            strategy.on_poll(&snapshot);
            if ticks >= 3 {
                Poll::Ready(ticks)
            } else {
                Poll::Pending
            }
        })
        .await
    });
    let result = tokio::time::timeout(Duration::from_secs(3), task).await;
    assert!(
        matches!(result, Ok(Ok(3))),
        "executor did not consume queued wake and resume polling: {result:?}"
    );
}
