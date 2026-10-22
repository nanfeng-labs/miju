//! Verify the service contract even when the SDK client feature is disabled.

#![cfg(feature = "sentry")]

use std::{
    future::{Ready, ready},
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use miju::{
    layers::sentry::{SentryLayer, SentryTaskError},
    prelude::*,
};
use tower::{Service, ServiceExt, service_fn};

#[tokio::test]
async fn boxed_results_are_preserved_or_wrapped_without_requiring_a_client() {
    let service = SentryLayer::new().layer(service_fn(|task: Task<bool>| async move {
        if task.args {
            Err::<usize, BoxDynError>(io::Error::other("failed").into())
        } else {
            Ok(7)
        }
    }));
    for fail in [false, true] {
        let task = TaskBuilder::new(fail).task_id(1u64.into()).build();
        let result = service.clone().oneshot(task).await;
        if fail {
            let error = result.unwrap_err();
            let SentryTaskError::Inner(inner) = error.downcast_ref::<SentryTaskError>().unwrap()
            else {
                panic!("unexpected error")
            };
            assert!(inner.downcast_ref::<io::Error>().is_some());
        } else {
            assert_eq!(result.unwrap(), 7);
        }
    }
}

#[derive(Clone)]
struct NotReady;

impl Service<Task<()>> for NotReady {
    type Response = ();
    type Error = BoxDynError;
    type Future = Ready<Result<(), BoxDynError>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), BoxDynError>> {
        Poll::Ready(Err(
            io::Error::new(io::ErrorKind::WouldBlock, "not ready").into()
        ))
    }

    fn call(&mut self, _: Task<()>) -> Self::Future {
        panic!("must not be called")
    }
}

#[tokio::test]
async fn readiness_errors_are_not_wrapped_as_task_failures() {
    let error = SentryLayer::new()
        .layer(NotReady)
        .oneshot(Task::new(()))
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(error.downcast_ref::<SentryTaskError>().is_none());
}

#[test]
fn missing_task_id_panics_before_calling_the_inner_service() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut service = SentryLayer::new().layer(service_fn(move |_: Task<()>| {
        observed.fetch_add(1, Ordering::SeqCst);
        ready(Ok::<_, BoxDynError>(()))
    }));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| service.call(Task::new(()))))
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
