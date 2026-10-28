//! Verify retry ownership across aliases, cancellation, and policy composition.
#![cfg(feature = "retry")]

use std::{
    future::{Future, Ready, ready},
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use miju::{
    layers::retry::{FromTaskConfigPolicy, Policy, RetryConfig, RetryPolicy, backoff::Backoff},
    prelude::*,
};
use miju_core::task::metadata::MetadataStore;
use tokio::sync::{Notify, mpsc};
use tower::{ServiceExt, retry::Retry, service_fn};

#[derive(Clone)]
struct ObserveRetry<P> {
    inner: P,
    snapshot: mpsc::UnboundedSender<Task<()>>,
    observed: Arc<AtomicBool>,
    release: Arc<Notify>,
}

impl<P> Policy<Task<()>, (), io::Error> for ObserveRetry<P>
where
    P: Policy<Task<()>, (), io::Error>,
    P::Future: Send + 'static,
{
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        req: &mut Task<()>,
        result: &mut Result<(), io::Error>,
    ) -> Option<Self::Future> {
        let ready = self.inner.retry(req, result)?;
        let first = !self.observed.swap(true, Ordering::SeqCst);
        if first {
            self.snapshot.send(req.clone()).unwrap();
        }
        let release = self.release.clone();
        Some(Box::pin(async move {
            if first {
                release.notified().await;
            }
            ready.await;
        }))
    }

    fn clone_request(&mut self, req: &Task<()>) -> Option<Task<()>> {
        self.inner.clone_request(req)
    }
}

async fn retained_alias_case<P>(inner: P, cancel_original: bool, replay: bool) -> (usize, usize)
where
    P: Policy<Task<()>, (), io::Error> + Clone + Send + 'static,
    P::Future: Send + 'static,
{
    let (snapshot, mut receiver) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let first_calls = Arc::new(AtomicUsize::new(0));
    let seen = first_calls.clone();
    let policy = ObserveRetry {
        inner,
        snapshot,
        observed: Arc::new(AtomicBool::new(false)),
        release: release.clone(),
    };
    let original = Retry::new(
        policy,
        service_fn(move |_: Task<()>| {
            let count = seen.fetch_add(1, Ordering::SeqCst);
            async move {
                // A finite fuse: a broken policy must still reach the assertions.
                if count < 8 {
                    Err(io::Error::other("rejected"))
                } else {
                    Ok(())
                }
            }
        }),
    );
    let task = TaskBuilder::new(())
        .data(WorkerContext::new("alias-owner"))
        .metadata(&RetryConfig { retries: 2 })
        .build();
    let mut running = Some(tokio::spawn(original.oneshot(task)));
    let alias = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alias.attempt(), 0);
    if cancel_original {
        let cancelled = running.take().unwrap();
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
    }
    let replay_calls = Arc::new(AtomicUsize::new(0));
    if replay {
        let seen = replay_calls.clone();
        let service = Retry::new(
            RetryPolicy::retries(1),
            service_fn(move |_: Task<()>| {
                let count = seen.fetch_add(1, Ordering::SeqCst);
                async move {
                    if count < 8 {
                        Err(io::Error::other("rejected"))
                    } else {
                        Ok(())
                    }
                }
            }),
        );
        assert!(service.oneshot(alias).await.is_err());
    } else {
        drop(alias);
    }
    release.notify_one();
    if let Some(running) = running {
        let outcome = tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .unwrap();
        assert!(outcome.unwrap().is_err());
    }
    (
        first_calls.load(Ordering::SeqCst),
        replay_calls.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn retaining_an_observer_snapshot_without_replay_preserves_the_budget() {
    assert_eq!(
        retained_alias_case(RetryPolicy::retries(1), false, false).await,
        (2, 0)
    );
}

#[tokio::test]
async fn replaying_a_policy_snapshot_cannot_consume_another_requests_budget() {
    assert_eq!(
        retained_alias_case(RetryPolicy::retries(1), false, true).await,
        (2, 2)
    );
}

#[tokio::test]
async fn cancelled_retry_snapshot_starts_a_fresh_replay_budget() {
    assert_eq!(
        retained_alias_case(RetryPolicy::retries(1), true, true).await,
        (1, 2)
    );
}

#[tokio::test]
async fn cancelling_a_dispatched_call_keeps_its_replay_budget() {
    let captured = Arc::new(Mutex::new(None));
    let saved = captured.clone();
    let (sent, mut recv) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let gate = release.clone();
    let original = Retry::new(
        RetryPolicy::retries(1),
        service_fn(move |task: Task<()>| {
            *saved.lock().unwrap() = Some(task);
            sent.send(()).unwrap();
            let gate = gate.clone();
            async move {
                gate.notified().await;
                Err::<(), _>(io::Error::other("failed"))
            }
        }),
    );
    let running = tokio::spawn(
        original.oneshot(
            TaskBuilder::new(())
                .data(WorkerContext::new("dispatch-cancel"))
                .build(),
        ),
    );
    tokio::time::timeout(Duration::from_secs(5), recv.recv())
        .await
        .unwrap()
        .unwrap();
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    let task = captured.lock().unwrap().take().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let replay = Retry::new(
        RetryPolicy::retries(1),
        service_fn(move |_: Task<()>| {
            seen.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(io::Error::other("failed")) }
        }),
    );
    assert!(replay.oneshot(task).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[derive(Clone)]
struct ImmediateBackoff;
impl Backoff for ImmediateBackoff {
    type Future = Ready<()>;
    fn next_backoff(&mut self) -> Self::Future {
        ready(())
    }
}

#[derive(Clone)]
struct RetryEveryError;
impl Policy<Task<()>, (), io::Error> for RetryEveryError {
    type Future = Ready<()>;
    fn retry(
        &mut self,
        _: &mut Task<()>,
        result: &mut Result<(), io::Error>,
    ) -> Option<Self::Future> {
        result.is_err().then(|| ready(()))
    }
    fn clone_request(&mut self, req: &Task<()>) -> Option<Task<()>> {
        Some(req.clone())
    }
}

#[tokio::test]
async fn backoff_and_metadata_wrappers_keep_retained_aliases_independent() {
    assert_eq!(
        retained_alias_case(
            RetryPolicy::retries(1).with_backoff(ImmediateBackoff),
            false,
            true
        )
        .await,
        (2, 2)
    );
    assert_eq!(
        retained_alias_case(RetryPolicy::retries(5).from_task_config(), false, true).await,
        (2, 2)
    );
    assert_eq!(
        retained_alias_case(FromTaskConfigPolicy::new(RetryEveryError), false, true).await,
        (2, 2)
    );
    assert_eq!(
        retained_alias_case(
            RetryPolicy::retries(5)
                .with_backoff(ImmediateBackoff)
                .retry_if(|_: &io::Error| true)
                .from_task_config(),
            false,
            true
        )
        .await,
        (2, 2)
    );
    assert_eq!(
        retained_alias_case(
            RetryPolicy::retries(5)
                .from_task_config()
                .retry_if(|_: &io::Error| true),
            false,
            true
        )
        .await,
        (2, 2)
    );
    assert_eq!(
        retained_alias_case(
            FromTaskConfigPolicy::new(RetryPolicy::retries(5).from_task_config()),
            true,
            true
        )
        .await,
        (1, 2)
    );
}

fn cloned_request<P: Policy<Task<()>, (), io::Error>>(policy: &mut P, req: &Task<()>) -> Task<()> {
    policy.clone_request(req).unwrap()
}

fn failure() -> Result<(), io::Error> {
    Err(io::Error::other("before execution"))
}

fn assert_foreign_decision_does_not_revoke<P: Policy<Task<()>, (), io::Error>>(mut foreign: P) {
    let mut owner = RetryPolicy::retries(1);
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("decision-owner"))
        .metadata(&RetryConfig { retries: 1 })
        .build();
    let mut retained = cloned_request(&mut owner, &original);
    assert!(owner.retry(&mut retained, &mut failure()).is_some());
    let mut observer = retained.clone();
    assert!(foreign.retry(&mut observer, &mut failure()).is_none());
    let mut next = cloned_request(&mut owner, &retained);
    assert!(
        owner.retry(&mut next, &mut failure()).is_none(),
        "foreign decision must not reset the owner's budget"
    );
    assert_eq!(next.attempt(), 0);
}

#[test]
fn another_policy_cannot_revoke_a_pending_retry_through_an_alias() {
    assert_foreign_decision_does_not_revoke(RetryPolicy::retries(0));
    assert_foreign_decision_does_not_revoke(
        RetryPolicy::retries(5).retry_if(|_: &io::Error| false),
    );
    assert_foreign_decision_does_not_revoke(RetryPolicy::retries(5).from_task_config());
}

#[test]
fn cloning_an_active_policy_copies_its_budget_without_sharing_consumption() {
    let mut policy = RetryPolicy::retries(1);
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("clone-policy"))
        .build();
    let mut retained = cloned_request(&mut policy, &original);
    assert!(policy.retry(&mut retained, &mut failure()).is_some());
    let mut copied = policy.clone();
    let mut first = cloned_request(&mut policy, &retained);
    let mut second = cloned_request(&mut copied, &retained);
    assert!(policy.retry(&mut first, &mut failure()).is_none());
    assert!(copied.retry(&mut second, &mut failure()).is_none());
}

#[test]
fn a_predicate_reversal_cancels_the_inner_approval() {
    let allow = Arc::new(AtomicBool::new(true));
    let check = allow.clone();
    let mut policy =
        RetryPolicy::retries(1).retry_if(move |_: &io::Error| check.load(Ordering::SeqCst));
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("predicate-reversal"))
        .build();
    let mut retained = cloned_request(&mut policy, &original);
    assert!(policy.retry(&mut retained, &mut failure()).is_some());
    allow.store(false, Ordering::SeqCst);
    assert!(policy.retry(&mut retained, &mut failure()).is_none());
    allow.store(true, Ordering::SeqCst);
    let mut fresh = cloned_request(&mut policy, &retained);
    assert!(policy.retry(&mut fresh, &mut failure()).is_some());
    let mut last = cloned_request(&mut policy, &fresh);
    assert!(policy.retry(&mut last, &mut failure()).is_none());
}

#[test]
fn metadata_success_cancels_a_previously_approved_retry() {
    let mut policy = RetryPolicy::retries(5).from_task_config();
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("metadata-reversal"))
        .metadata(&RetryConfig { retries: 2 })
        .build();
    let mut retained = cloned_request(&mut policy, &original);
    assert!(policy.retry(&mut retained, &mut failure()).is_some());
    assert!(
        policy
            .retry(&mut retained, &mut Ok::<(), io::Error>(()))
            .is_none()
    );
    let mut fresh = cloned_request(&mut policy, &retained);
    assert!(policy.retry(&mut fresh, &mut failure()).is_some());
    let mut last = cloned_request(&mut policy, &fresh);
    assert!(policy.retry(&mut last, &mut failure()).is_none());
}

#[test]
fn pending_approvals_do_not_retain_cancelled_task_data() {
    let mut policy = RetryPolicy::retries(1);
    for _ in 0..128 {
        let data = Arc::new(());
        let observed = Arc::downgrade(&data);
        let original = TaskBuilder::new(())
            .data(WorkerContext::new("cancel-data"))
            .data(data)
            .build();
        let mut retained = cloned_request(&mut policy, &original);
        drop(original);
        assert!(policy.retry(&mut retained, &mut failure()).is_some());
        drop(retained);
        assert!(
            observed.upgrade().is_none(),
            "policy must not keep task data alive after cancellation"
        );
    }
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("after-cancellation"))
        .build();
    let mut retained = cloned_request(&mut policy, &original);
    assert!(policy.retry(&mut retained, &mut failure()).is_some());
    let mut last = cloned_request(&mut policy, &retained);
    assert!(policy.retry(&mut last, &mut failure()).is_none());
}

#[derive(Clone)]
struct ConditionalClone {
    allow: Arc<AtomicBool>,
}

impl Policy<Task<()>, (), io::Error> for ConditionalClone {
    type Future = Ready<()>;
    fn retry(
        &mut self,
        _: &mut Task<()>,
        result: &mut Result<(), io::Error>,
    ) -> Option<Self::Future> {
        result.is_err().then(|| ready(()))
    }
    fn clone_request(&mut self, req: &Task<()>) -> Option<Task<()>> {
        self.allow.load(Ordering::SeqCst).then(|| req.clone())
    }
}

#[test]
fn refusing_a_clone_does_not_leave_a_stale_approval() {
    let allow = Arc::new(AtomicBool::new(true));
    let mut policy = FromTaskConfigPolicy::new(ConditionalClone {
        allow: allow.clone(),
    });
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("refused-clone"))
        .metadata(&RetryConfig { retries: 2 })
        .build();
    let mut retained = cloned_request(&mut policy, &original);
    assert!(policy.retry(&mut retained, &mut failure()).is_some());
    allow.store(false, Ordering::SeqCst);
    assert!(policy.clone_request(&retained).is_none());
    allow.store(true, Ordering::SeqCst);
    let mut fresh = cloned_request(&mut policy, &retained);
    assert!(policy.retry(&mut fresh, &mut failure()).is_some());
    let mut last = cloned_request(&mut policy, &fresh);
    assert!(policy.retry(&mut last, &mut failure()).is_none());
}

#[test]
fn changing_to_backoff_keeps_an_approved_requests_budget() {
    let mut policy = RetryPolicy::retries(1);
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("convert-policy"))
        .build();
    let mut retained = cloned_request(&mut policy, &original);
    assert!(policy.retry(&mut retained, &mut failure()).is_some());
    let mut policy = policy.with_backoff(ImmediateBackoff);
    let mut last = cloned_request(&mut policy, &retained);
    assert!(policy.retry(&mut last, &mut failure()).is_none());
}

#[derive(Clone)]
struct FreshInner(RetryPolicy);

impl Policy<Task<()>, (), io::Error> for FreshInner {
    type Future = Ready<()>;

    fn retry(
        &mut self,
        req: &mut Task<()>,
        result: &mut Result<(), io::Error>,
    ) -> Option<Self::Future> {
        self.0.clone().retry(req, result)
    }

    fn clone_request(&mut self, req: &Task<()>) -> Option<Task<()>> {
        Some(cloned_request(&mut self.0.clone(), req))
    }
}

#[tokio::test]
async fn metadata_caps_an_inner_policy_that_renews_its_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let retry = Retry::new(
        FromTaskConfigPolicy::new(FreshInner(RetryPolicy::retries(10))),
        service_fn(move |_: Task<()>| {
            let count = seen.fetch_add(1, Ordering::SeqCst);
            // A broken outer cap still reaches the behavior assertions.
            ready(if count < 8 {
                Err::<(), _>(io::Error::other("pre-execution failure"))
            } else {
                Ok(())
            })
        }),
    );
    let result = retry
        .oneshot(
            TaskBuilder::new(())
                .data(WorkerContext::new("metadata-independent-cap"))
                .metadata(&RetryConfig { retries: 2 })
                .build(),
        )
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(result.is_err());
}

#[test]
fn adding_metadata_to_an_active_policy_preserves_its_approved_budget() {
    let mut policy = RetryPolicy::retries(1);
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("active-metadata"))
        .metadata(&RetryConfig { retries: 2 })
        .build();
    let mut retained = cloned_request(&mut policy, &original);
    assert!(policy.retry(&mut retained, &mut failure()).is_some());
    let mut policy = policy.from_task_config();
    let mut last = cloned_request(&mut policy, &retained);
    assert!(policy.retry(&mut last, &mut failure()).is_none());
}

#[test]
fn multiple_pending_approvals_survive_cloning_and_reverse_handoff() {
    let mut policy = RetryPolicy::retries(1);
    let original = TaskBuilder::new(())
        .data(WorkerContext::new("multiple-pending"))
        .build();
    let mut requests = (0..64)
        .map(|_| cloned_request(&mut policy, &original))
        .collect::<Vec<_>>();
    for request in &mut requests {
        assert!(policy.retry(request, &mut failure()).is_some());
    }
    let mut copied = policy.clone();
    for request in requests.iter().rev() {
        let mut last = cloned_request(&mut policy, request);
        assert!(policy.retry(&mut last, &mut failure()).is_none());
        let mut copied_last = cloned_request(&mut copied, request);
        assert!(copied.retry(&mut copied_last, &mut failure()).is_none());
    }
}

async fn succeeds_after_three_failures<P>(policy: P, metadata: MetadataStore) -> (usize, bool)
where
    P: Policy<Task<()>, (), io::Error> + Clone,
{
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let retry = Retry::new(
        policy,
        service_fn(move |_: Task<()>| {
            let count = seen.fetch_add(1, Ordering::SeqCst);
            ready(if count < 3 {
                Err::<(), _>(io::Error::other("pre-execution failure"))
            } else {
                Ok(())
            })
        }),
    );
    let result = retry
        .oneshot(
            TaskBuilder::new(())
                .data(WorkerContext::new("metadata-fallback"))
                .with_metadata(metadata)
                .build(),
        )
        .await;
    (calls.load(Ordering::SeqCst), result.is_ok())
}

#[tokio::test]
async fn missing_or_invalid_metadata_preserves_the_inner_policys_budget_choices() {
    let mut invalid = MetadataStore::new();
    invalid.insert("miju.retries.config", "invalid").unwrap();
    for metadata in [MetadataStore::new(), invalid] {
        let inner = FreshInner(RetryPolicy::retries(1));
        assert_eq!(
            succeeds_after_three_failures(inner.clone(), metadata.clone()).await,
            (4, true)
        );
        assert_eq!(
            succeeds_after_three_failures(FromTaskConfigPolicy::new(inner), metadata).await,
            (4, true)
        );
    }
}
