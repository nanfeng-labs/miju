//! Exercise graceful shutdown while retry middleware is waiting.
#![cfg(feature = "retry")]
use miju::{layers::retry::RetryPolicy, prelude::*};
use miju_core::error::{AbortError, WorkerStateError};
use std::{
    error::Error,
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tower::retry::backoff::Backoff;

#[derive(Default, Debug)]
struct TimerState {
    scheduled: AtomicUsize,
    finished: AtomicBool,
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
#[derive(Clone, Debug)]
struct ObservedBackoff {
    state: Arc<TimerState>,
    delay: Duration,
}
struct ObservedWait {
    timer: Pin<Box<tokio::time::Sleep>>,
    state: Arc<TimerState>,
}
impl Future for ObservedWait {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        *self.state.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.state.cancelled.load(Ordering::SeqCst) || self.timer.as_mut().poll(cx).is_ready() {
            self.state.finished.store(true, Ordering::SeqCst);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
impl Backoff for ObservedBackoff {
    type Future = ObservedWait;
    fn next_backoff(&mut self) -> Self::Future {
        self.state.scheduled.fetch_add(1, Ordering::SeqCst);
        ObservedWait {
            timer: Box::pin(tokio::time::sleep(self.delay)),
            state: self.state.clone(),
        }
    }
}

#[derive(Debug)]
struct Observation {
    finished: bool,
    attempts: usize,
    handler_calls: usize,
    timer_finished: bool,
    errors: Vec<String>,
    shutdown_errors: usize,
}
async fn stopping_case(cancel_wait: bool, delay: Duration) -> Observation {
    let state = Arc::new(TimerState::default());
    let errors = Arc::new(Mutex::new(Vec::new()));
    let events = errors.clone();
    let shutdown_errors = Arc::new(AtomicUsize::new(0));
    let stopped_errors = shutdown_errors.clone();
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let calls = handler_calls.clone();
    let backoff = ObservedBackoff {
        state: state.clone(),
        delay,
    };
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(())
        .task_id("shutdown-probe".to_owned().into())
        .build();
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let worker = WorkerBuilder::new("shutdown-probe")
        .backend(storage)
        .retry(RetryPolicy::retries(1).with_backoff(backoff))
        .on_event(move |_, event| {
            if let Event::Error(e) = event {
                if e.downcast_ref::<AbortError>()
                    .and_then(Error::source)
                    .and_then(|e| e.downcast_ref::<WorkerStateError>())
                    .is_some_and(|e| matches!(e, WorkerStateError::ShuttingDown))
                {
                    stopped_errors.fetch_add(1, Ordering::SeqCst);
                }
                events.lock().unwrap().push(e.to_string());
            }
        })
        .build(move |(): ()| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("original task failure")) }
        });
    let controller = state.clone();
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        worker.run_with_ctx(move |worker| {
            let controller = controller.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                assert_eq!(controller.scheduled.load(Ordering::SeqCst), 1);
                assert_eq!(worker.task_count(), 0);
                worker.stop()?;
                if cancel_wait {
                    controller.cancelled.store(true, Ordering::SeqCst);
                    if let Some(w) = controller.waker.lock().unwrap().take() {
                        w.wake();
                    }
                }
                Ok(())
            }
        }),
    )
    .await;
    let errors = errors.lock().unwrap().clone();
    Observation {
        finished: matches!(result, Ok(Ok(()))),
        attempts: observed.attempt(),
        handler_calls: handler_calls.load(Ordering::SeqCst),
        shutdown_errors: shutdown_errors.load(Ordering::SeqCst),
        timer_finished: state.finished.load(Ordering::SeqCst),
        errors,
    }
}

#[tokio::test(start_paused = true)]
async fn stop_during_finite_backoff_allows_graceful_exit() {
    let observed = stopping_case(false, Duration::from_secs(10)).await;
    println!("finite backoff: {observed:?}");
    assert!(
        observed.timer_finished,
        "the original timer must actually have completed"
    );
    assert!(
        observed.finished,
        "worker must drain an already failed task after stop"
    );
    assert_eq!(observed.shutdown_errors, 1);
    assert_eq!(observed.errors.len(), 1);
    assert_eq!(observed.handler_calls, 1);
}

#[tokio::test(start_paused = true)]
async fn resolving_backoff_at_stop_also_requires_readiness_to_finish() {
    let observed = stopping_case(true, Duration::from_secs(3600)).await;
    println!("timer cancellation only: {observed:?}");
    assert!(observed.timer_finished);
    assert!(
        observed.finished,
        "resolving the waiting future must not leave shutdown stuck"
    );
    assert_eq!(observed.shutdown_errors, 1);
    assert_eq!(observed.errors.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn stopped_worker_keeps_handler_from_running_again() {
    let observed = stopping_case(false, Duration::from_secs(10)).await;
    println!("post-stop side effect probe: {observed:?}");
    assert_eq!(
        observed.attempts, 1,
        "do not silently add another handler execution after stop"
    );
    assert_eq!(observed.handler_calls, 1);
}

#[tokio::test(start_paused = true)]
async fn normal_retry_without_intervening_stop_still_completes() {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(())
        .task_id("normal-retry".to_owned().into())
        .build();
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let state = Arc::new(TimerState::default());
    let backoff = ObservedBackoff {
        state,
        delay: Duration::from_secs(10),
    };
    let worker = WorkerBuilder::new("normal-retry")
        .backend(storage)
        .retry(RetryPolicy::retries(1).with_backoff(backoff))
        .on_event(|worker, event| {
            if let Event::Error(e) = event {
                assert_eq!(e.to_string(), "original task failure");
                worker.stop().unwrap();
            }
        })
        .build(|(): ()| async { Err::<(), _>(io::Error::other("original task failure")) });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(30), worker.run()).await,
        Ok(Ok(()))
    ));
    assert_eq!(observed.attempt(), 2);
}

#[tokio::test(start_paused = true)]
async fn stop_before_retry_decision_preserves_the_original_error() {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(())
        .task_id("stop-before-decision".to_owned().into())
        .build();
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let events = errors.clone();
    let state = Arc::new(TimerState::default());
    let backoff = ObservedBackoff {
        state: state.clone(),
        delay: Duration::from_secs(10),
    };
    let worker = WorkerBuilder::new("stop-before-decision")
        .backend(storage)
        .retry(RetryPolicy::retries(1).with_backoff(backoff))
        .on_event(move |_, event| {
            if let Event::Error(e) = event {
                events.lock().unwrap().push(e.to_string());
            }
        })
        .build(|(): (), worker: WorkerContext| async move {
            worker.stop().unwrap();
            Err::<(), _>(io::Error::other("original task failure"))
        });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(30), worker.run()).await,
        Ok(Ok(()))
    ));
    assert_eq!(state.scheduled.load(Ordering::SeqCst), 0);
    assert_eq!(observed.attempt(), 1);
    assert_eq!(*errors.lock().unwrap(), ["original task failure"]);
}
