//! Current worker names remain visible through retained monitor contexts.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::StreamExt;
use miju_core::{
    monitor::Monitor,
    worker::{builder::WorkerBuilder, event::Event},
};

#[tokio::test]
async fn monitor_context_finds_the_restarted_worker_by_its_current_name() {
    let names = Arc::new(Mutex::new(Vec::new()));
    let observed = names.clone();
    let monitor = Monitor::new()
        .register(|restart| {
            WorkerBuilder::new(format!("worker-{restart}"))
                .backend(
                    futures_util::stream::iter([Err::<u32, _>(std::io::Error::other("restart"))])
                        .boxed(),
                )
                .build(|_: u32| async {})
        })
        .should_restart(|worker, _| worker.restarts() < 1)
        .on_event(move |worker, event| {
            if matches!(event, Event::Start) {
                observed.lock().unwrap().push(worker.name());
            }
        });
    let context = monitor.context();
    assert!(monitor.run().await.is_err());
    assert_eq!(*names.lock().unwrap(), ["worker-0", "worker-1"]);
    assert_eq!(context.workers()[0].restarts(), 1);
    let current = context.get_worker("worker-1");
    assert!(
        current.is_ok(),
        "the worker reported by the restart must be found by its current name"
    );
    assert_eq!(current.unwrap().name(), "worker-1");
}

#[tokio::test]
async fn retained_contexts_control_repeatedly_restarted_workers_by_current_name() {
    for changing_name in [false, true] {
        let monitor = Monitor::new()
            .register(move |restart| {
                let name = if changing_name {
                    format!("worker-{restart}")
                } else {
                    "stable".to_owned()
                };
                WorkerBuilder::new(name)
                    .backend(
                        futures_util::stream::iter([Err::<u32, _>(std::io::Error::other(
                            "restart",
                        ))])
                        .boxed(),
                    )
                    .build(|_: u32| async {})
            })
            .should_restart(|worker, _| worker.restarts() < 2);
        let context = monitor.context();
        let retained = context.workers()[0].clone();
        let initial_name = retained.name();
        let observer = context.clone();
        let starts = Arc::new(Mutex::new(Vec::new()));
        let observed = starts.clone();
        let monitor = monitor.on_event(move |worker, event| {
            if !matches!(event, Event::Start) {
                return;
            }
            let name = worker.name();
            assert_eq!(observer.get_worker(&name).unwrap().name(), name);
            assert_eq!(observer.workers()[0].name(), name);
            observed.lock().unwrap().push(name.clone());
            observer.pause_worker(&name).unwrap();
            assert!(worker.is_paused());
            observer.resume_worker(&name).unwrap();
            assert!(worker.is_running());
            observer.wake_worker(&name).unwrap();
            if worker.restarts() == 2 {
                observer.stop_worker(&name).unwrap();
            }
        });
        tokio::time::timeout(Duration::from_secs(5), monitor.run())
            .await
            .unwrap()
            .unwrap();
        let expected = if changing_name { "worker-2" } else { "stable" };
        assert_eq!(retained.name(), expected);
        assert_eq!(context.get_worker(expected).unwrap().name(), expected);
        assert_eq!(retained.restarts(), 2);
        assert!(retained.is_shutting_down());
        assert_eq!(
            initial_name,
            if changing_name { "worker-0" } else { "stable" }
        );
        assert_eq!(starts.lock().unwrap().len(), 3);
        if changing_name {
            assert!(context.get_worker("worker-0").is_err());
        }
    }
}
