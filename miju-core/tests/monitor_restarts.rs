//! Worker restarts yield without losing progress or shutdown notifications.

use std::{
    future::{pending, ready},
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::{FutureExt, StreamExt, stream};
use miju_core::{
    backend::memory::MemoryStorage,
    monitor::{Monitor, MonitorError},
    worker::{builder::WorkerBuilder, event::Event},
};

fn failing_worker() -> Monitor {
    Monitor::new().register(|_| {
        WorkerBuilder::new("failing")
            .backend(stream::iter([Err::<u32, _>(io::Error::other("unavailable"))]).boxed())
            .build(|_: u32| async {})
    })
}

#[tokio::test(flavor = "current_thread")]
async fn immediate_failures_let_another_task_run_before_exhausting_restarts() {
    let observed = Arc::new(AtomicBool::new(false));
    let flag = observed.clone();
    let other_task = tokio::spawn(async move {
        flag.store(true, Ordering::SeqCst);
    });
    let flag = observed.clone();
    let monitor = failing_worker()
        .should_restart(move |worker, _| worker.restarts() < 64 && !flag.load(Ordering::SeqCst));
    let context = monitor.context();

    let result = monitor.run().await;
    let ran_before_exit = observed.load(Ordering::SeqCst);
    other_task.await.unwrap();

    assert!(matches!(result, Err(MonitorError::ExitError(_))));
    assert!(
        ran_before_exit,
        "restarting workers must yield to other tasks"
    );
    assert!(context.workers()[0].restarts() < 64);
}

#[tokio::test]
async fn immediate_failures_let_another_worker_process_its_task() {
    let processed = Arc::new(AtomicBool::new(false));
    let flag = processed.clone();
    let monitor = failing_worker()
        .register(move |_| {
            let flag = flag.clone();
            WorkerBuilder::new("healthy")
                .backend(stream::iter([Ok::<_, io::Error>(42_u32)]).boxed())
                .build(move |value: u32| {
                    let flag = flag.clone();
                    async move {
                        assert_eq!(value, 42);
                        flag.store(true, Ordering::SeqCst);
                    }
                })
        })
        .should_restart(move |worker, _| {
            worker.restarts() < 64 && !processed.load(Ordering::SeqCst)
        });
    let context = monitor.context();

    let result = monitor.run().await;

    assert!(matches!(result, Err(MonitorError::ExitError(_))));
    assert!(context.get_worker("healthy").unwrap().is_terminated());
    assert!(context.get_worker("failing").unwrap().restarts() < 64);
}

#[tokio::test]
async fn shutdown_signal_is_observed_before_the_restart_budget_is_exhausted() {
    for signal_failed in [false, true] {
        let monitor = failing_worker().should_restart(|worker, _| worker.restarts() < 64);
        let context = monitor.context();
        let signal = if signal_failed {
            Err(io::Error::other("signal failed"))
        } else {
            Ok(())
        };

        let result = monitor
            .with_terminator(pending())
            .run_with_signal(ready(signal))
            .await;

        if signal_failed {
            assert!(matches!(result, Err(MonitorError::ShutdownSignal(_))));
        } else {
            assert!(
                result.is_ok(),
                "shutdown must stop the restarting worker: {result:?}"
            );
        }
        assert!((1..64).contains(&context.workers()[0].restarts()));
        assert!(context.workers()[0].is_terminated());
    }
}

#[test]
fn panic_before_worker_waker_registration_still_schedules_the_restart() {
    let monitor = Monitor::new()
        .register(|_| {
            WorkerBuilder::new("start-panic")
                .backend(MemoryStorage::<u32>::new())
                .build(|_: u32| async {})
        })
        .on_event(|_, event| {
            if matches!(event, Event::Start) {
                panic!("start failed");
            }
        })
        .should_restart(|worker, _| worker.restarts() < 1);
    let context = monitor.context();
    let mut run = monitor.run().boxed();

    assert!(run.as_mut().now_or_never().is_none(), "restart must yield");
    let result = run
        .now_or_never()
        .expect("the failed worker must be scheduled again without an external wake");

    let Err(MonitorError::ExitError(error)) = result else {
        panic!("the final panic must remain observable: {result:?}");
    };
    assert!(error.to_string().contains("start failed"));
    assert_eq!(context.workers()[0].restarts(), 1);
}
