//! Probe stopping after backoff has already reached paused readiness.
#![cfg(feature = "retry")]
use miju::{layers::retry::backoff::Backoff, prelude::*};
use miju_core::backend::dequeue::VecDequeBackend;
use std::{
    future::{Ready, ready},
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tower::Service;
#[derive(Clone)]
struct ShortBackoff;
impl Backoff for ShortBackoff {
    type Future = tokio::time::Sleep;
    fn next_backoff(&mut self) -> Self::Future {
        tokio::time::sleep(Duration::from_secs(2))
    }
}
#[tokio::test(start_paused = true)]
async fn stop_after_backoff_reaches_paused_readiness() {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(())
        .task_id("paused-retry".to_owned().into())
        .build();
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let worker = WorkerBuilder::new("paused-retry")
        .backend(storage)
        .retry(RetryPolicy::retries(1).with_backoff(ShortBackoff))
        .build(|(): ()| async { Err::<(), _>(io::Error::other("failed")) });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        worker.run_with_ctx(|worker| async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            worker.pause().unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
            worker.stop().unwrap();
            Ok(())
        }),
    )
    .await;
    assert_eq!(observed.attempt(), 1);
    assert!(
        matches!(result, Ok(Ok(()))),
        "stopping paused readiness should drain: {result:?}"
    );
}

async fn control_parked_retries(resume: bool, count: usize) {
    let mut storage = MemoryStorage::new();
    let mut observed = Vec::new();
    for id in 0..count {
        let task = TaskBuilder::new(())
            .task_id(format!("parked-{id}").into())
            .build();
        observed.push(task.clone());
        storage.push_task(task).await.unwrap();
    }
    let errors = Arc::new(AtomicUsize::new(0));
    let seen = errors.clone();
    let worker = WorkerBuilder::new("many-parked")
        .backend(storage)
        .retry(RetryPolicy::retries(1).with_backoff(ShortBackoff))
        .on_event(move |worker, event| {
            if matches!(event, Event::Error(_))
                && seen.fetch_add(1, Ordering::SeqCst) + 1 == count
                && resume
            {
                worker.stop().unwrap();
            }
        })
        .build(|(): ()| async { Err::<(), _>(io::Error::other("failed")) });
    let snapshots = observed.clone();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        worker.run_with_ctx(move |worker| {
            let snapshots = snapshots.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                worker.pause().unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
                assert!(snapshots.iter().all(|task| task.attempt() == 1));
                if resume {
                    worker.resume().unwrap();
                } else {
                    worker.stop().unwrap();
                    worker.stop().unwrap();
                }
                Ok(())
            }
        }),
    )
    .await;
    assert!(
        matches!(result, Ok(Ok(()))),
        "parked retries must finish: {result:?}"
    );
    assert_eq!(errors.load(Ordering::SeqCst), count);
    assert!(
        observed
            .iter()
            .all(|task| task.attempt() == if resume { 2 } else { 1 })
    );
}

#[tokio::test(start_paused = true)]
async fn resume_wakes_every_parked_retry() {
    control_parked_retries(true, 8).await;
}

#[tokio::test(start_paused = true)]
async fn repeated_stop_wakes_every_parked_retry_without_new_execution() {
    control_parked_retries(false, 8).await;
}

#[tokio::test(start_paused = true)]
async fn monitor_shutdown_wakes_parked_retries_in_all_workers() {
    let mut monitor = Monitor::new();
    let mut observed = Vec::new();
    let exits = Arc::new(AtomicUsize::new(0));
    for id in 0..3 {
        let mut storage = VecDequeBackend::new();
        let task = TaskBuilder::new(())
            .task_id(format!("monitored-{id}").into())
            .build();
        observed.push(task.clone());
        storage.push_task(task).await.unwrap();
        let exits = exits.clone();
        monitor = monitor.register(move |_| {
            let exits = exits.clone();
            WorkerBuilder::new(format!("monitored-{id}"))
                .backend(storage.clone())
                .retry(RetryPolicy::retries(1).with_backoff(ShortBackoff))
                .on_event(move |_, event| {
                    if matches!(event, Event::Exit) {
                        exits.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .build(|(): ()| async { Err::<(), _>(io::Error::other("failed")) })
        });
    }
    let context = monitor.context();
    let control = async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for worker in context.workers() {
            worker.pause().unwrap();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        context.shutdown().unwrap();
    };
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let (result, ()) = tokio::join!(monitor.run(), control);
        result
    })
    .await;
    assert!(
        matches!(result, Ok(Ok(()))),
        "monitor must drain parked retries: {result:?}"
    );
    assert_eq!(exits.load(Ordering::SeqCst), 3);
    assert!(observed.iter().all(|task| task.attempt() == 1));
}

#[derive(Clone)]
struct PendingAfterCall {
    called: Arc<AtomicBool>,
}
impl Service<Task<()>> for PendingAfterCall {
    type Response = ();
    type Error = io::Error;
    type Future = Ready<Result<(), io::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if self.called.load(Ordering::SeqCst) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn call(&mut self, _: Task<()>) -> Self::Future {
        assert!(
            !self.called.swap(true, Ordering::SeqCst),
            "no call after Pending"
        );
        ready(Err(io::Error::other("failed")))
    }
}

#[tokio::test(start_paused = true)]
async fn stop_wakes_retry_parked_on_inner_readiness() {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(())
        .task_id("inner-pending".to_owned().into())
        .build();
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let worker = WorkerBuilder::new("inner-pending")
        .backend(storage)
        .retry(RetryPolicy::retries(1).with_backoff(ShortBackoff))
        .build(PendingAfterCall {
            called: Arc::new(AtomicBool::new(false)),
        });
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        worker.run_with_ctx(|worker| async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            worker.stop().unwrap();
            Ok(())
        }),
    )
    .await;
    assert!(
        matches!(result, Ok(Ok(()))),
        "pending inner readiness must not prevent stop: {result:?}"
    );
    assert_eq!(observed.attempt(), 1);
}
