//! Monitor termination preserves the result of the branch that finishes first.

use std::{
    future::{Future, pending, ready},
    io,
};

use futures_util::{FutureExt, StreamExt, stream};
use miju_core::{
    backend::memory::MemoryStorage,
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
fn worker_error_keeps_priority_when_both_branches_can_finish() {
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
