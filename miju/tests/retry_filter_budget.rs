//! Exercise retry budgets across filtering, execution, and request replay.

#![cfg(all(feature = "retry", feature = "filter"))]

use std::{
    future::{Ready, ready},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use miju::{
    layers::retry::{
        BackoffRetryPolicy, FromTaskConfigPolicy, HasherRng, Policy, RetryConfig, RetryPolicy,
        backoff::{ExponentialBackoffMaker, MakeBackoff},
    },
    prelude::*,
};
use tower::{Service, ServiceExt, filter::FilterLayer, retry::Retry, service_fn};

async fn reject_then_fail<P>(
    policy: P,
    rejected_calls: usize,
    history: usize,
    metadata: Option<usize>,
) -> (usize, usize, usize)
where
    P: Policy<Task<()>, (), BoxDynError> + Clone + Send + Sync + 'static,
    P::Future: Send + 'static,
{
    let mut storage = MemoryStorage::new();
    let mut task = TaskBuilder::new(())
        .task_id("filter-budget".to_owned().into())
        .attempt(history)
        .build();
    if let Some(retries) = metadata {
        task.inject_metadata(&RetryConfig { retries }).unwrap();
    }
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let filter_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let filters = filter_calls.clone();
    let handlers = handler_calls.clone();
    let worker = WorkerBuilder::new("filter-budget")
        .backend(storage)
        .retry(policy)
        .layer(FilterLayer::new(move |task| {
            if filters.fetch_add(1, Ordering::SeqCst) < rejected_calls {
                Err::<_, BoxDynError>(io::Error::other("filter refused").into())
            } else {
                Ok(task)
            }
        }))
        .on_event(|worker, event| {
            if matches!(event, Event::Error(_)) {
                worker.stop().unwrap();
            }
        })
        .build(move |(): ()| {
            handlers.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("handler failed")) }
        });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    (
        filter_calls.load(Ordering::SeqCst),
        handler_calls.load(Ordering::SeqCst),
        observed.attempt(),
    )
}

fn backoff(
    retries: usize,
) -> BackoffRetryPolicy<impl tower::retry::backoff::Backoff<Future: Send> + Clone> {
    RetryPolicy::retries(retries).with_backoff(
        ExponentialBackoffMaker::new(
            Duration::from_millis(1),
            Duration::from_millis(10),
            0.0,
            HasherRng::default(),
        )
        .unwrap()
        .make_backoff(),
    )
}

#[tokio::test]
async fn immediate_filter_rejections_exhaust_the_budget() {
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(1), 3, 0, None).await,
        (2, 0, 0)
    );
}

#[tokio::test(start_paused = true)]
async fn backoff_filter_rejections_exhaust_the_budget() {
    assert_eq!(reject_then_fail(backoff(1), 3, 0, None).await, (2, 0, 0));
}

#[tokio::test]
async fn filter_rejections_and_handler_errors_share_the_budget() {
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(2), 1, 0, None).await,
        (3, 2, 2)
    );
}

#[tokio::test]
async fn metadata_applies_its_existing_total_attempt_cap_to_filter_errors() {
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(5).from_task_config(), 3, 0, Some(2)).await,
        (2, 0, 0)
    );
}

#[tokio::test(start_paused = true)]
async fn predicate_and_metadata_wrappers_preserve_the_filter_budget() {
    let first = backoff(5)
        .retry_if(|_: &BoxDynError| true)
        .from_task_config();
    assert_eq!(reject_then_fail(first, 3, 0, Some(2)).await, (2, 0, 0));
    let second = backoff(5)
        .from_task_config()
        .retry_if(|_: &BoxDynError| true);
    assert_eq!(reject_then_fail(second, 3, 0, Some(2)).await, (2, 0, 0));
}

#[tokio::test]
async fn predicate_rejection_and_zero_budget_still_stop_immediately() {
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(0), 3, 0, None).await,
        (1, 0, 0)
    );
    assert_eq!(
        reject_then_fail(
            RetryPolicy::retries(5).retry_if(|_: &BoxDynError| false),
            3,
            0,
            None
        )
        .await,
        (1, 0, 0)
    );
}

#[tokio::test]
async fn ordinary_handler_retries_and_history_remain_unchanged() {
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(2), 0, 0, None).await,
        (3, 3, 3)
    );
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(2), 0, 1, None).await,
        (2, 2, 3)
    );
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(5).from_task_config(), 0, 1, Some(3)).await,
        (2, 2, 3)
    );
}

#[test]
fn public_policy_can_be_reused_for_distinct_requests_without_cloning() {
    let mut policy = RetryPolicy::retries(1);
    for id in [Some("same"), Some("same"), None, None] {
        let mut builder = TaskBuilder::new(())
            .attempt(1)
            .data(WorkerContext::new("direct"));
        if let Some(id) = id {
            builder = builder.task_id(id.to_owned().into());
        }
        let mut request = builder.build();
        let mut result: Result<(), io::Error> = Err(io::Error::other("retry"));
        assert!(policy.retry(&mut request, &mut result).is_some());
        assert_eq!(request.attempt(), 1);
    }
}

#[derive(Clone)]
struct RetryEveryError;

impl Policy<Task<()>, (), BoxDynError> for RetryEveryError {
    type Future = Ready<()>;

    fn retry(
        &mut self,
        _: &mut Task<()>,
        result: &mut Result<(), BoxDynError>,
    ) -> Option<Self::Future> {
        result.is_err().then(|| ready(()))
    }

    fn clone_request(&mut self, req: &Task<()>) -> Option<Task<()>> {
        Some(req.clone())
    }
}

#[tokio::test]
async fn metadata_bounds_a_custom_inner_policy_without_lifecycle_progress() {
    assert_eq!(
        reject_then_fail(FromTaskConfigPolicy::new(RetryEveryError), 3, 0, Some(2)).await,
        (2, 0, 0)
    );
}

#[tokio::test]
async fn concurrent_retry_sessions_keep_independent_budgets() {
    let calls = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let seen = calls.clone();
    let mut service = Retry::new(
        RetryPolicy::retries(1),
        service_fn(move |task: Task<usize>| {
            let index = task.take().0;
            let count = seen[index].fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::task::yield_now().await;
                if count < 4 {
                    Err::<(), _>(io::Error::other("retry"))
                } else {
                    Ok(())
                }
            }
        }),
    );
    let first = service.ready().await.unwrap().call(
        TaskBuilder::new(0)
            .data(WorkerContext::new("first"))
            .build(),
    );
    let second = service.ready().await.unwrap().call(
        TaskBuilder::new(1)
            .data(WorkerContext::new("second"))
            .build(),
    );
    let (first, second) = tokio::join!(first, second);
    assert!(first.is_err());
    assert!(second.is_err());
    assert_eq!(calls[0].load(Ordering::SeqCst), 2);
    assert_eq!(calls[1].load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retry_service_reuse_gets_a_fresh_budget_for_each_request() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let service = Retry::new(
        RetryPolicy::retries(1),
        service_fn(move |_: Task<()>| {
            let count = seen.fetch_add(1, Ordering::SeqCst);
            // Bound broken implementations: after eight calls the service succeeds.
            ready(if count < 8 {
                Err::<(), _>(io::Error::other("retry"))
            } else {
                Ok(())
            })
        }),
    );
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("direct"))
        .build();
    for _ in 0..2 {
        assert!(service.clone().oneshot(original.clone()).await.is_err());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(original.attempt(), 0);
}

#[tokio::test]
async fn historical_attempts_also_reduce_filter_retry_budget() {
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(2), 3, 1, None).await,
        (2, 0, 1)
    );
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(5).from_task_config(), 3, 1, Some(3)).await,
        (2, 0, 1)
    );
}

#[tokio::test]
async fn replaying_a_task_keeps_the_existing_execution_history_budget() {
    let mut storage = MemoryStorage::new();
    storage
        .push_task(
            TaskBuilder::new(())
                .task_id("replay".to_owned().into())
                .build(),
        )
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None));
    let seen = calls.clone();
    let save = captured.clone();
    let first = WorkerBuilder::new("first-session")
        .backend(storage)
        .retry(RetryPolicy::retries(2))
        .layer(FilterLayer::new(move |task: Task<()>| {
            if seen.fetch_add(1, Ordering::SeqCst) < 2 {
                Err::<_, BoxDynError>(io::Error::other("try later").into())
            } else {
                *save.lock().unwrap() = Some(task.clone());
                Ok(task)
            }
        }))
        .on_event(|worker, event| {
            if matches!(event, Event::Success(_)) {
                worker.stop().unwrap();
            }
        })
        .build(|(): ()| async {});
    tokio::time::timeout(Duration::from_secs(5), first.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let replay = captured.lock().unwrap().take().unwrap();
    assert_eq!(replay.attempt(), 1);
    let observed = replay.clone();
    let mut storage = MemoryStorage::new();
    storage.push_task(replay).await.unwrap();
    let handlers = Arc::new(AtomicUsize::new(0));
    let seen = handlers.clone();
    let second = WorkerBuilder::new("second-session")
        .backend(storage)
        .retry(RetryPolicy::retries(2))
        .on_event(|worker, event| {
            if matches!(event, Event::Error(_)) {
                worker.stop().unwrap();
            }
        })
        .build(move |(): ()| {
            seen.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("handler failed")) }
        });
    tokio::time::timeout(Duration::from_secs(5), second.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(handlers.load(Ordering::SeqCst), 2);
    assert_eq!(observed.attempt(), 3);
}

#[test]
fn metadata_without_a_limit_preserves_the_custom_policy_at_overflow() {
    let mut policy = FromTaskConfigPolicy::new(RetryEveryError);
    let request = TaskBuilder::new(())
        .attempt(usize::MAX)
        .data(WorkerContext::new("unlimited-custom"))
        .build();
    let mut request = <FromTaskConfigPolicy<RetryEveryError> as Policy<
        Task<()>,
        (),
        BoxDynError,
    >>::clone_request(&mut policy, &request)
    .unwrap();
    let mut result = Err::<(), BoxDynError>(io::Error::other("retry").into());
    assert!(policy.retry(&mut request, &mut result).is_some());
    assert_eq!(request.attempt(), usize::MAX);
}

#[tokio::test]
async fn filter_budget_overflow_stops_without_changing_execution_history() {
    assert_eq!(
        reject_then_fail(RetryPolicy::retries(usize::MAX), 2, usize::MAX, None).await,
        (1, 0, usize::MAX)
    );
}

#[tokio::test]
async fn filter_failure_after_a_handler_attempt_spends_the_same_budget() {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(())
        .task_id("later-filter-error".to_owned().into())
        .build();
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let filters = calls.clone();
    let worker = WorkerBuilder::new("later-filter-error")
        .backend(storage)
        .retry(RetryPolicy::retries(2))
        .layer(FilterLayer::new(move |task| {
            if filters.fetch_add(1, Ordering::SeqCst) == 1 {
                Err::<_, BoxDynError>(io::Error::other("second call refused").into())
            } else {
                Ok(task)
            }
        }))
        .on_event(|worker, event| {
            if matches!(event, Event::Error(_)) {
                worker.stop().unwrap();
            }
        })
        .build(|(): ()| async { Err::<(), _>(io::Error::other("handler failed")) });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(observed.attempt(), 2);
}

#[test]
fn interleaved_clone_request_sessions_keep_independent_budgets() {
    let mut policy = RetryPolicy::retries(1);
    let first = TaskBuilder::new(())
        .data(WorkerContext::new("direct"))
        .build();
    let second = TaskBuilder::new(())
        .data(WorkerContext::new("direct"))
        .build();
    let mut first =
        <RetryPolicy as Policy<Task<()>, (), io::Error>>::clone_request(&mut policy, &first)
            .unwrap();
    let mut second =
        <RetryPolicy as Policy<Task<()>, (), io::Error>>::clone_request(&mut policy, &second)
            .unwrap();
    for req in [&mut first, &mut second] {
        assert!(
            policy
                .retry(req, &mut Err::<(), _>(io::Error::other("retry")))
                .is_some()
        );
        *req = <RetryPolicy as Policy<Task<()>, (), io::Error>>::clone_request(&mut policy, req)
            .unwrap();
    }
    for req in [&mut first, &mut second] {
        assert!(
            policy
                .retry(req, &mut Err::<(), _>(io::Error::other("retry")))
                .is_none()
        );
        assert_eq!(req.attempt(), 0);
    }
}

fn check_interleaved_policy<P>(mut policy: P)
where
    P: Policy<Task<()>, (), BoxDynError>,
{
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("interleaved"))
        .metadata(&RetryConfig { retries: 2 })
        .build();
    let mut first = policy.clone_request(&original).unwrap();
    let mut second = policy.clone_request(&original).unwrap();
    for req in [&mut first, &mut second] {
        let mut error = Err::<(), BoxDynError>(io::Error::other("retry").into());
        assert!(policy.retry(req, &mut error).is_some());
        // An unrelated context update must not detach the retry budget.
        req.inject_data("application data");
        *req = policy.clone_request(req).unwrap();
    }
    for req in [&mut first, &mut second] {
        let mut error = Err::<(), BoxDynError>(io::Error::other("retry").into());
        assert!(policy.retry(req, &mut error).is_none());
        assert_eq!(req.attempt(), 0);
        assert_eq!(req.data().get::<&str>(), Some(&"application data"));
    }
}

#[tokio::test(start_paused = true)]
async fn policies_isolate_interleaved_clones_of_the_same_task() {
    check_interleaved_policy(RetryPolicy::retries(1));
    check_interleaved_policy(backoff(1));
    check_interleaved_policy(FromTaskConfigPolicy::new(RetryEveryError));
    check_interleaved_policy(RetryPolicy::retries(5).from_task_config());
    check_interleaved_policy(backoff(5).from_task_config());
    check_interleaved_policy(
        backoff(5)
            .retry_if(|_: &BoxDynError| true)
            .from_task_config(),
    );
    check_interleaved_policy(
        backoff(5)
            .from_task_config()
            .retry_if(|_: &BoxDynError| true),
    );
}

#[tokio::test]
async fn nested_metadata_wrappers_do_not_reset_or_double_count_failures() {
    let policy = FromTaskConfigPolicy::new(RetryPolicy::retries(5).from_task_config());
    assert_eq!(reject_then_fail(policy, 5, 0, Some(3)).await, (3, 0, 0));
}

#[tokio::test]
async fn replaying_dispatched_task_on_same_service_gets_fresh_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None));
    let seen = calls.clone();
    let saved = captured.clone();
    let mut service = Retry::new(
        RetryPolicy::retries(1),
        service_fn(move |task: Task<()>| {
            *saved.lock().unwrap() = Some(task);
            let call = seen.fetch_add(1, Ordering::SeqCst);
            ready(if call < 10 {
                Err::<(), _>(io::Error::other("refused before execution"))
            } else {
                Ok(())
            })
        }),
    );
    let mut task = TaskBuilder::new(())
        .data(WorkerContext::new("same-worker"))
        .build();
    for expected in [2, 4, 6] {
        assert!(service.ready().await.unwrap().call(task).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), expected);
        task = captured.lock().unwrap().take().unwrap();
        assert_eq!(task.attempt(), 0);
    }
}

#[tokio::test]
async fn replay_while_original_retry_is_running_has_an_independent_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let release = Arc::new(tokio::sync::Notify::new());
    let gate = release.clone();
    let (saved, mut captured) = tokio::sync::mpsc::unbounded_channel();
    let service = Retry::new(
        RetryPolicy::retries(1),
        service_fn(move |task: Task<()>| {
            let call = seen.fetch_add(1, Ordering::SeqCst);
            if call == 1 {
                saved.send(task).unwrap();
            }
            let gate = gate.clone();
            async move {
                if call == 1 {
                    gate.notified().await;
                }
                if call < 10 {
                    Err::<(), _>(io::Error::other("failed"))
                } else {
                    Ok(())
                }
            }
        }),
    );
    let task = TaskBuilder::new(())
        .data(WorkerContext::new("live-replay"))
        .build();
    let original = tokio::spawn(service.clone().oneshot(task));
    let replay = tokio::time::timeout(Duration::from_secs(5), captured.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(service.oneshot(replay).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    release.notify_one();
    assert!(original.await.unwrap().is_err());
}

#[tokio::test]
async fn nested_retry_services_keep_their_own_call_budgets() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let service = Retry::new(
        RetryPolicy::retries(1),
        Retry::new(
            RetryPolicy::retries(1),
            service_fn(move |_: Task<()>| {
                let call = seen.fetch_add(1, Ordering::SeqCst);
                ready(if call < 10 {
                    Err::<(), _>(io::Error::other("rejected"))
                } else {
                    Ok(())
                })
            }),
        ),
    );
    let task = TaskBuilder::new(())
        .data(WorkerContext::new("nested"))
        .build();
    assert!(service.oneshot(task).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[test]
fn refusing_to_clone_keeps_metadata_policy_non_retryable() {
    #[derive(Clone)]
    struct NoClone;
    impl Policy<Task<()>, (), BoxDynError> for NoClone {
        type Future = Ready<()>;
        fn retry(
            &mut self,
            _: &mut Task<()>,
            _: &mut Result<(), BoxDynError>,
        ) -> Option<Self::Future> {
            Some(ready(()))
        }
        fn clone_request(&mut self, _: &Task<()>) -> Option<Task<()>> {
            None
        }
    }
    let mut policy = FromTaskConfigPolicy::new(NoClone);
    assert!(policy.clone_request(&Task::new(())).is_none());
}
