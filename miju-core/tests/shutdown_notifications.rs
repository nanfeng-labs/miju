//! Shutdown notifies independently parked tokens and workers.

use miju_core::{
    backend::memory::MemoryStorage,
    monitor::{Monitor, shutdown::Shutdown},
    worker::builder::WorkerBuilder,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Notifications(AtomicUsize);

impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_shutdown(token: &mut Shutdown, count: &Arc<Notifications>) -> Poll<()> {
    let waker = Waker::from(count.clone());
    Pin::new(token).poll(&mut Context::from_waker(&waker))
}

#[test]
fn every_shutdown_clone_keeps_its_own_notification() {
    let shutdown = Shutdown::new();
    let mut first = shutdown.clone();
    let mut second = shutdown.clone();
    let a = Arc::new(Notifications::default());
    let replaced = Arc::new(Notifications::default());
    let b = Arc::new(Notifications::default());
    assert!(poll_shutdown(&mut first, &a).is_pending());
    assert!(poll_shutdown(&mut second, &replaced).is_pending());
    assert!(poll_shutdown(&mut second, &b).is_pending());
    shutdown.start_shutdown();
    assert_eq!(a.0.load(Ordering::SeqCst), 1);
    assert_eq!(b.0.load(Ordering::SeqCst), 1);
    assert_eq!(replaced.0.load(Ordering::SeqCst), 0);
    assert!(poll_shutdown(&mut first, &a).is_ready());
    assert!(poll_shutdown(&mut second, &b).is_ready());
    shutdown.start_shutdown();
    assert_eq!(a.0.load(Ordering::SeqCst), 1);
    assert_eq!(b.0.load(Ordering::SeqCst), 1);
}

#[test]
fn dropped_shutdown_clone_releases_only_its_notification() {
    let shutdown = Shutdown::new();
    let mut dropped = shutdown.clone();
    let mut live = shutdown.clone();
    let a = Arc::new(Notifications::default());
    let weak = Arc::downgrade(&a);
    assert!(poll_shutdown(&mut live, &Arc::new(Notifications::default())).is_pending());
    assert!(poll_shutdown(&mut dropped, &a).is_pending());
    drop(a);
    drop(dropped);
    assert!(weak.upgrade().is_none());
    shutdown.start_shutdown();
    assert!(poll_shutdown(&mut live, &Arc::new(Notifications::default())).is_ready());
}

#[test]
fn shutdown_before_poll_and_default_token_are_ready() {
    let shutdown = Shutdown::default();
    shutdown.start_shutdown();
    let mut later = shutdown.clone();
    let notifications = Arc::new(Notifications::default());
    assert!(poll_shutdown(&mut later, &notifications).is_ready());
    assert_eq!(notifications.0.load(Ordering::SeqCst), 0);
    assert!(shutdown.is_shutting_down());
}

#[test]
fn registration_racing_shutdown_never_leaves_an_unnotified_pending_token() {
    for _ in 0..64 {
        let shutdown = Shutdown::new();
        let mut token = shutdown.clone();
        let notifications = Arc::new(Notifications::default());
        let barrier = Arc::new(Barrier::new(2));
        let result = std::thread::scope(|scope| {
            let signal = scope.spawn(|| {
                barrier.wait();
                shutdown.start_shutdown();
            });
            barrier.wait();
            let result = poll_shutdown(&mut token, &notifications);
            signal.join().unwrap();
            result
        });
        assert!(result.is_ready() || notifications.0.load(Ordering::SeqCst) > 0);
        assert!(poll_shutdown(&mut token, &notifications).is_ready());
    }
}

fn settle<F: Future>(mut future: Pin<&mut F>, count: &Arc<Notifications>) -> Option<F::Output> {
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    for _ in 0..16 {
        count.0.store(0, Ordering::SeqCst);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return Some(output),
            Poll::Pending if count.0.load(Ordering::SeqCst) == 0 => return None,
            Poll::Pending => {}
        }
    }
    panic!("monitor did not settle within its notification bound");
}

#[test]
fn earlier_monitor_snapshot_shuts_down_workers_registered_later() {
    for earlier_count in [0, 1] {
        let monitor = Monitor::new().register_n(earlier_count, |index, _| {
            WorkerBuilder::new(format!("early-{index}"))
                .backend(MemoryStorage::new())
                .build(|_: u32| async {})
        });
        let earlier = monitor.context();
        let monitor = monitor.register_n(2 - earlier_count, |index, _| {
            WorkerBuilder::new(format!("later-{index}"))
                .backend(MemoryStorage::new())
                .build(|_: u32| async {})
        });
        let current = monitor.context();
        assert_eq!(earlier.workers().len(), earlier_count);
        assert_eq!(current.workers().len(), 2);
        let count = Arc::new(Notifications::default());
        let mut run = Box::pin(monitor.run());
        assert!(settle(run.as_mut(), &count).is_none());
        assert!(current.workers().iter().all(|worker| worker.is_running()));
        earlier.shutdown().unwrap();
        settle(run.as_mut(), &count)
            .expect("shutdown through an earlier snapshot left a parked worker")
            .unwrap();
        assert!(
            current
                .workers()
                .iter()
                .all(|worker| worker.is_terminated())
        );
        assert_eq!(earlier.workers().len(), earlier_count);
    }
}
