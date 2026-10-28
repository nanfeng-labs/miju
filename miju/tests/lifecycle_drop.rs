//! Observe interrupted task executions through workers and task contexts.

#![cfg(all(feature = "timeout", feature = "catch-panic"))]

use std::{future::pending, time::Duration};

#[cfg(feature = "task-context")]
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake},
};

use miju::prelude::*;

#[cfg(feature = "task-context")]
use miju_core::task::context::WaitForExecutionFuture;

#[cfg(feature = "task-context")]
#[derive(Default)]
struct CountWake(AtomicUsize);

#[cfg(feature = "task-context")]
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Observation {
    task: Task<usize>,
    #[cfg(feature = "task-context")]
    execution: (
        TaskContext,
        Pin<Box<WaitForExecutionFuture>>,
        Arc<CountWake>,
    ),
}

async fn interrupted_worker(panic_on_poll: bool) -> Observation {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(7usize)
        .task_id("interrupted".to_owned().into())
        .build();
    storage.push_task(task.clone()).await.unwrap();
    #[cfg(feature = "task-context")]
    let captured = Arc::new(Mutex::new(None));
    #[cfg(feature = "task-context")]
    let capture = captured.clone();
    let worker = WorkerBuilder::new("interrupted")
        .backend(storage)
        .timeout(Duration::from_secs(1))
        .catch_panic()
        .on_event(move |worker, event| {
            if let Event::Error(error) = event {
                if panic_on_poll {
                    assert!(error.is::<AbortError>());
                    assert!(error.to_string().contains("interrupted by panic"));
                } else {
                    assert!(error.is::<tower::timeout::error::Elapsed>());
                }
                assert_eq!(worker.task_count(), 0);
                worker.stop().unwrap();
            }
        })
        .build(move |_: usize, _worker: WorkerContext| {
            #[cfg(feature = "task-context")]
            {
                let context = _worker.get_task("interrupted").unwrap();
                let mut wait = Box::pin(context.executed());
                let wake = Arc::new(CountWake::default());
                let waker = wake.clone().into();
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                *capture.lock().unwrap() = Some((context, wait, wake));
            }
            async move {
                if panic_on_poll {
                    tokio::task::yield_now().await;
                    panic!("interrupted by panic");
                }
                pending::<()>().await;
            }
        });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .expect("worker must finish after the outer layer interrupts its task")
        .unwrap();
    Observation {
        task,
        #[cfg(feature = "task-context")]
        execution: captured.lock().unwrap().take().unwrap(),
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_marks_the_interrupted_task_killed() {
    let observed = interrupted_worker(false).await;
    assert_eq!(observed.task.status(), Status::Killed);
    assert_eq!(observed.task.attempt(), 1);
}

#[tokio::test(start_paused = true)]
async fn caught_poll_panic_marks_the_interrupted_task_killed() {
    let observed = interrupted_worker(true).await;
    assert_eq!(observed.task.status(), Status::Killed);
    assert_eq!(observed.task.attempt(), 1);
}

#[cfg(feature = "task-context")]
#[tokio::test(start_paused = true)]
async fn interruption_releases_execution_waiters() {
    for panic_on_poll in [false, true] {
        let observed = interrupted_worker(panic_on_poll).await;
        let (context, wait, wake) = observed.execution;
        assert!(
            wake.0.load(Ordering::SeqCst) > 0,
            "interruption must wake execution waiters"
        );
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("the interrupted execution must finish waiting");
        assert!(context.is_cancelled());
        assert!(!context.is_completed());
    }
}

#[cfg(feature = "retry")]
#[tokio::test(start_paused = true)]
async fn a_timed_out_attempt_can_retry_with_a_fresh_execution() {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(())
        .task_id("retry-interrupted".to_owned().into())
        .build();
    storage.push_task(task.clone()).await.unwrap();
    #[cfg(feature = "task-context")]
    let contexts = Arc::new(Mutex::new(Vec::new()));
    #[cfg(feature = "task-context")]
    let captured = contexts.clone();
    let worker = WorkerBuilder::new("retry-interrupted")
        .backend(storage)
        .retry(RetryPolicy::retries(1))
        .timeout(Duration::from_secs(1))
        .on_event(|worker, event| match event {
            Event::Success(_) => worker.stop().unwrap(),
            Event::Error(error) => panic!("the next attempt must succeed: {error}"),
            _ => {}
        })
        .build(move |(): (), attempt: Attempt, _worker: WorkerContext| {
            #[cfg(feature = "task-context")]
            captured
                .lock()
                .unwrap()
                .push(_worker.get_task("retry-interrupted").unwrap());
            async move {
                if attempt.current() == 1 {
                    pending::<()>().await;
                }
            }
        });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.attempt(), 2);
    assert_eq!(task.status(), Status::Done);
    #[cfg(feature = "task-context")]
    {
        let contexts = contexts.lock().unwrap();
        assert_eq!(contexts.len(), 2);
        assert!(contexts[0].is_cancelled());
        assert!(contexts[1].is_completed());
    }
}
