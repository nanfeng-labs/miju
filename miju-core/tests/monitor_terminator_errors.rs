//! Monitor termination preserves observed signal errors while completing shutdown.

use std::{
    future::{Future, pending, ready},
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_channel::oneshot;
use futures_util::{FutureExt, StreamExt, stream};
use miju_core::{
    backend::{ext::BackendExt, memory::MemoryStorage},
    monitor::{Monitor, MonitorError},
    worker::builder::WorkerBuilder,
};

fn failing_worker() -> Monitor {
    Monitor::new().register(|_| {
        WorkerBuilder::new("backend-failure")
            .backend(
                stream::iter([Err::<u32, _>(io::Error::other("provider unavailable"))]).boxed(),
            )
            .build(|_: u32| async {})
    })
}

fn idle_worker() -> Monitor {
    Monitor::new().register(|_| {
        WorkerBuilder::new("idle")
            .backend(MemoryStorage::<u32>::new())
            .build(|_: u32| async {})
    })
}

fn signal_error() -> impl Future<Output = io::Result<()>> {
    ready(Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "signal unavailable",
    )))
}

#[test]
fn worker_error_is_preserved_before_signal_or_terminator_finishes() {
    let result = failing_worker()
        .with_terminator(pending())
        .run_with_signal(pending())
        .now_or_never()
        .expect("the failed worker must complete without waiting for shutdown");

    let Err(MonitorError::ExitError(error)) = result else {
        panic!("the selected worker failure must remain observable: {result:?}");
    };
    assert_eq!(error.0.len(), 1);
    assert!(error.to_string().contains("provider unavailable"));
}

#[test]
fn terminator_preserves_worker_error_while_another_worker_is_still_shutting_down() {
    let failure_observed = Arc::new(AtomicBool::new(false));
    let failed = failure_observed.clone();
    let cleanup_started = Arc::new(AtomicBool::new(false));
    let cleanup = cleanup_started.clone();
    let monitor = failing_worker()
        .register(move |_| {
            let cleanup = cleanup.clone();
            WorkerBuilder::new("blocked-cleanup")
                .backend(MemoryStorage::<u32>::new().before_stop(move |_| {
                    cleanup.store(true, Ordering::SeqCst);
                    pending()
                }))
                .build(|_: u32| async {})
        })
        .should_restart(move |_, _| {
            failed.store(true, Ordering::SeqCst);
            false
        });
    let context = monitor.context();
    let (terminate, terminator) = oneshot::channel::<()>();
    let mut run = monitor
        .with_terminator(terminator.map(Result::unwrap))
        .run_with_signal(ready(Ok(())))
        .boxed();

    assert!(run.as_mut().now_or_never().is_none());
    assert!(failure_observed.load(Ordering::SeqCst));
    assert!(run.as_mut().now_or_never().is_none());
    assert!(cleanup_started.load(Ordering::SeqCst));
    assert!(
        !context
            .get_worker("blocked-cleanup")
            .unwrap()
            .is_terminated()
    );
    terminate.send(()).unwrap();
    let result = run
        .now_or_never()
        .expect("the terminator must finish without waiting for blocked cleanup");

    let Err(MonitorError::ExitError(error)) = result else {
        panic!("the observed worker error must survive forced termination: {result:?}");
    };
    assert_eq!(error.0.len(), 1);
    assert!(error.to_string().contains("provider unavailable"));
}

#[test]
fn signal_error_is_preserved_when_the_terminator_finishes_first() {
    let result = idle_worker()
        .with_terminator(ready(()))
        .run_with_signal(signal_error())
        .now_or_never()
        .expect("the ready terminator must complete shutdown");

    let Err(MonitorError::ShutdownSignal(error)) = result else {
        panic!("the selected signal failure must remain observable: {result:?}");
    };
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "signal unavailable");
}

#[test]
fn signal_error_is_preserved_when_workers_finish_before_the_terminator() {
    let mut run = idle_worker()
        .with_terminator(pending())
        .run_with_signal(signal_error())
        .boxed();

    assert!(run.as_mut().now_or_never().is_none());
    let result = run
        .now_or_never()
        .expect("the idle worker must finish after observing shutdown");

    let Err(MonitorError::ShutdownSignal(error)) = result else {
        panic!("the observed signal failure must survive worker completion: {result:?}");
    };
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "signal unavailable");
}

#[test]
fn worker_shutdown_error_is_reported_unless_the_signal_failed() {
    for signal_failed in [false, true] {
        let monitor = Monitor::new().register(|_| {
            WorkerBuilder::new("cleanup-failure")
                .backend(
                    stream::pending::<io::Result<u32>>()
                        .boxed()
                        .before_stop(|_| ready(Err(io::Error::other("cleanup failed")))),
                )
                .build(|_: u32| async {})
        });
        let signal = if signal_failed {
            signal_error().boxed()
        } else {
            ready(Ok(())).boxed()
        };
        let mut run = monitor
            .with_terminator(pending())
            .run_with_signal(signal)
            .boxed();

        assert!(run.as_mut().now_or_never().is_none());
        let result = run
            .now_or_never()
            .expect("the worker must finish after cleanup fails");

        match result {
            Err(MonitorError::ShutdownSignal(error)) if signal_failed => {
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                assert_eq!(error.to_string(), "signal unavailable");
            }
            Err(MonitorError::ExitError(error)) if !signal_failed => {
                assert_eq!(error.0.len(), 1);
                assert!(error.to_string().contains("cleanup failed"));
            }
            result => {
                panic!("unexpected shutdown result for signal_failed={signal_failed}: {result:?}")
            }
        }
    }
}

#[test]
fn signal_error_waits_for_worker_cleanup() {
    let (finish, cleanup) = oneshot::channel::<()>();
    let cleanup = cleanup.shared();
    let monitor = Monitor::new().register(move |_| {
        let cleanup = cleanup.clone();
        WorkerBuilder::new("cleanup")
            .backend(MemoryStorage::<u32>::new().before_stop(move |_| {
                let cleanup = cleanup.clone();
                async move {
                    cleanup.await.unwrap();
                    Ok(())
                }
            }))
            .build(|_: u32| async {})
    });
    let context = monitor.context();
    let mut run = monitor
        .with_terminator(pending())
        .run_with_signal(signal_error())
        .boxed();

    assert!(run.as_mut().now_or_never().is_none());
    assert!(context.workers()[0].is_shutting_down());
    assert!(run.as_mut().now_or_never().is_none());
    assert!(!context.workers()[0].is_terminated());

    finish.send(()).unwrap();
    let result = run
        .now_or_never()
        .expect("shutdown must finish once cleanup is released");
    assert!(matches!(result, Err(MonitorError::ShutdownSignal(_))));
    assert!(context.workers()[0].is_terminated());
}

#[test]
fn terminator_can_force_exit_while_worker_cleanup_is_pending() {
    for signal_failed in [false, true] {
        let monitor = Monitor::new().register(|_| {
            WorkerBuilder::new("blocked-cleanup")
                .backend(MemoryStorage::<u32>::new().before_stop(|_| pending()))
                .build(|_: u32| async {})
        });
        let context = monitor.context();
        let (terminate, terminator) = oneshot::channel::<()>();
        let signal = if signal_failed {
            signal_error().boxed()
        } else {
            ready(Ok(())).boxed()
        };
        let mut run = monitor
            .with_terminator(terminator.map(Result::unwrap))
            .run_with_signal(signal)
            .boxed();

        assert!(run.as_mut().now_or_never().is_none());
        assert!(run.as_mut().now_or_never().is_none());
        terminate.send(()).unwrap();
        let result = run
            .now_or_never()
            .expect("the terminator must end shutdown without waiting for cleanup");

        if signal_failed {
            assert!(matches!(result, Err(MonitorError::ShutdownSignal(_))));
        } else {
            assert!(result.is_ok());
        }
        assert!(!context.workers()[0].is_terminated());
    }
}

#[cfg(feature = "sleep")]
#[test]
fn shutdown_timeout_preserves_signal_error_after_workers_finish() {
    let mut run = idle_worker()
        .shutdown_timeout(std::time::Duration::from_secs(60))
        .run_with_signal(signal_error())
        .boxed();

    assert!(run.as_mut().now_or_never().is_none());
    let result = run
        .now_or_never()
        .expect("worker completion must not wait for the shutdown timeout");
    assert!(matches!(result, Err(MonitorError::ShutdownSignal(_))));
}

#[test]
fn worker_error_is_returned_before_the_signal_is_polled() {
    let result = failing_worker()
        .with_terminator(ready(()))
        .run_with_signal(signal_error())
        .now_or_never()
        .expect("the failed worker must complete on the first poll");

    assert!(matches!(result, Err(MonitorError::ExitError(_))));
}

#[test]
fn empty_monitor_still_finishes_before_a_ready_failed_signal() {
    let result = Monitor::new()
        .with_terminator(ready(()))
        .run_with_signal(signal_error())
        .now_or_never()
        .expect("an empty monitor must complete on the first poll");

    assert!(result.is_ok());
}

#[test]
fn successful_termination_stays_successful() {
    let result = idle_worker()
        .with_terminator(ready(()))
        .run_with_signal(ready(Ok(())))
        .now_or_never()
        .expect("the ready terminator must complete shutdown");

    assert!(result.is_ok());
}

#[test]
fn no_terminator_preserves_worker_error() {
    let result = failing_worker()
        .run()
        .now_or_never()
        .expect("the failed worker must complete on the first poll");

    assert!(matches!(result, Err(MonitorError::ExitError(_))));
}

#[test]
fn no_terminator_preserves_signal_error() {
    let result = idle_worker()
        .run_with_signal(signal_error())
        .now_or_never()
        .expect("the idle worker must shut down on the first poll");

    assert!(matches!(result, Err(MonitorError::ShutdownSignal(_))));
}
