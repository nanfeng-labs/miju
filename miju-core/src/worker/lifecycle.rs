//! Task lifecycle tracking.
//!
//! This module provides middleware for tracking the lifecycle of tasks as they
//! are processed by a worker. The [`LifecycleLayer`] registers tasks with the
//! worker, tracks their execution state, records attempts, and removes them
//! from the worker when processing completes.
//!
//! The lifecycle service also handles task cancellation and maps task results
//! to their corresponding [`Status`] values.
use std::{
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};

use futures_util::FutureExt;
use tower_layer::Layer;
use tower_service::Service;

use crate::{
    error::{AbortError, BoxDynError, DeferredError, RetryAfterError},
    task::{
        Task,
        attempt::Attempt,
        status::{AtomicStatus, Status},
    },
    worker::context::WorkerContext,
};

#[cfg(feature = "task-context")]
use crate::task::context::{TaskContext, TaskStateError, WaitForExecutionFuture};

/// A layer that tracks the lifecycle of tasks processed by a worker.
///
/// The layer registers each task with the [`WorkerContext`] and wraps the
/// resulting task future so that its lifecycle state can be updated as the
/// future progresses.
#[derive(Debug, Clone)]
pub struct LifecycleLayer {
    worker: WorkerContext,
}

impl LifecycleLayer {
    pub(super) fn new(worker: WorkerContext) -> Self {
        Self { worker }
    }
}

impl<S> Layer<S> for LifecycleLayer {
    type Service = LifecycleService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LifecycleService {
            worker: self.worker.clone(),
            service,
        }
    }
}

/// A service that tracks the lifecycle of a task.
///
/// `LifecycleService` registers tasks with the worker before processing and
/// wraps the underlying service future in a [`LifecycleFuture`] to track task
/// state and completion.
#[derive(Debug, Clone)]
pub struct LifecycleService<S> {
    worker: WorkerContext,
    service: S,
}

impl<S, Args> Service<Task<Args>> for LifecycleService<S>
where
    S: Service<Task<Args>>,
    S::Error: Into<BoxDynError>,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = LifecycleFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.service.poll_ready(cx).map_err(Into::into);
        if self.worker.is_shutting_down() || self.worker.is_paused() || result.is_pending() {
            self.worker.is_ready.store(false, Ordering::SeqCst);
            return Poll::Pending;
        }

        match &result {
            Poll::Ready(Ok(_)) => self.worker.is_ready.store(true, Ordering::SeqCst),
            Poll::Pending | Poll::Ready(Err(_)) => {
                self.worker.is_ready.store(false, Ordering::SeqCst)
            }
        }

        result
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        let attempt = task.raw_attempt().clone();
        let status = task.raw_status().clone();
        #[allow(unused, clippy::let_unit_value)]
        let task_ctx = self
            .worker
            .register_task(task.ctx())
            .expect("Could not register task");

        // Cover unwinding before the future takes over registration cleanup.
        let mut registration = TaskRegistrationGuard {
            worker: Some(&self.worker),
            task_ctx: &task_ctx,
        };
        let fut = self.service.call(task);
        let worker = self.worker.clone();
        registration.worker = None;
        drop(registration);

        LifecycleFuture {
            fut,
            worker,
            #[cfg(feature = "task-context")]
            task_execution: task_ctx.executed(),
            #[cfg(feature = "task-context")]
            task_ctx,
            #[cfg(not(feature = "task-context"))]
            task_ctx: (),
            record_attempt: Some(attempt),
            task_status: status,
        }
    }
}

struct TaskRegistrationGuard<'a> {
    worker: Option<&'a WorkerContext>,
    #[cfg(feature = "task-context")]
    task_ctx: &'a TaskContext,
    #[cfg(not(feature = "task-context"))]
    task_ctx: &'a (),
}

impl Drop for TaskRegistrationGuard<'_> {
    fn drop(&mut self) {
        if let Some(worker) = self.worker {
            if worker.remove_task(self.task_ctx).is_err() {
                warn!("Dropped a task without removing it from the worker's context");
            }
            if worker.task_count() == 0 {
                worker.wake();
            }
        }
    }
}

/// A future that tracks the lifecycle of a task.
///
/// The future updates the task status when processing begins, handles
/// cancellation, and records the final task status when processing completes.
#[pin_project::pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct LifecycleFuture<Fut> {
    #[pin]
    fut: Fut,
    #[cfg(feature = "task-context")]
    task_ctx: TaskContext,
    #[cfg(feature = "task-context")]
    task_execution: WaitForExecutionFuture,
    #[cfg(not(feature = "task-context"))]
    task_ctx: (),
    worker: WorkerContext,
    record_attempt: Option<Attempt>,
    task_status: AtomicStatus,
}

/// Errors that can occur during the lifecycle of a task.
///
/// These errors indicate that a task could not transition through its
/// lifecycle as requested.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TaskLifecycleError {
    /// The task does not have an identifier.
    ///
    /// A task identifier is required for lifecycle operations that need to
    /// track or associate the task with its execution state.
    #[error("task doesn't have an id")]
    MissingTaskId,

    /// The task already has a token associated with it.
    ///
    /// A task can only have one lifecycle token. Attempting to assign another
    /// token results in this error.
    #[error("task already has a token")]
    Duplicate,

    #[cfg(feature = "task-context")]
    /// The task exited after it was manually canceled.
    ///
    /// Contains the [`TaskContext`] associated with the task at the time it
    /// exited.
    #[error("task exited after manually being canceled")]
    Exit(TaskContext),

    #[cfg(feature = "task-context")]
    /// The task is in an invalid state for the requested lifecycle operation.
    ///
    /// Contains the underlying [`TaskStateError`] describing the invalid
    /// state transition.
    #[error("task state is invalid: {0}")]
    StateError(#[from] TaskStateError),
}

impl<Fut, T, E> Future for LifecycleFuture<Fut>
where
    Fut: Future<Output = Result<T, E>>,
    E: Into<BoxDynError>,
{
    type Output = Result<T, BoxDynError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Some(attempt) = this.record_attempt.take() {
            let _current = attempt.increment();
            this.task_status.store(Status::Running);
            trace!("task started with prev attempts: {}", _current);
        }

        #[cfg(feature = "task-context")]
        let _ = this.task_execution.poll_unpin(cx);

        #[cfg(feature = "task-context")]
        if this.task_ctx.is_cancelled() {
            trace!("task.cancelled");
            this.task_status.store(Status::Killed);
            return Poll::Ready(Err(Box::new(AbortError::new(TaskLifecycleError::Exit(
                this.task_ctx.clone(),
            )))));
        }

        match this.fut.poll_unpin(cx).map(|res| res.map_err(Into::into)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                let new_status = match res {
                    Ok(_) => Status::Done,
                    Err(ref e) => match e {
                        e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
                        e if (e.downcast_ref::<RetryAfterError>().is_some()
                            || e.downcast_ref::<DeferredError>().is_some()) =>
                        {
                            Status::Pending
                        }
                        _ => Status::Failed,
                    },
                };

                #[cfg(feature = "task-context")]
                if let Err(e) = this.task_ctx.complete() {
                    this.task_status.store(Status::Killed);
                    return Poll::Ready(Err(e.into()));
                }
                this.task_status.store(new_status);
                Poll::Ready(res)
            }
        }
    }
}

#[pin_project::pinned_drop]
impl<Fut> PinnedDrop for LifecycleFuture<Fut> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        drop(TaskRegistrationGuard {
            worker: Some(this.worker),
            task_ctx: this.task_ctx,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "task-context")]
    use crate::task::context::TerminalState;
    use crate::task::{builder::TaskBuilder, task_fn::task_fn};
    use std::{
        future::{Future, Ready, pending, ready},
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Arc, atomic::AtomicUsize},
        task::{Wake, Waker},
        time::Duration,
    };

    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn task() -> Task<u32> {
        TaskBuilder::new(7)
            .task_id("tracked".to_owned().into())
            .build()
    }

    #[test]
    fn synchronous_call_panic_releases_registration_and_wakes_worker() {
        let worker = WorkerContext::new("call-panic");
        let observer = worker.clone();
        let mut service = LifecycleLayer::new(worker.clone()).layer(task_fn(
            move |_: u32| -> Ready<Result<(), BoxDynError>> {
                assert_eq!(observer.task_count(), 1);
                #[cfg(feature = "task-context")]
                assert!(observer.get_task("tracked").is_ok());
                panic!("call failed");
            },
        ));
        let wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = wake.clone().into();
        worker.register_waker(&Context::from_waker(&waker));

        for expected_wakes in 1..=3 {
            let task = task();
            let panic = catch_unwind(AssertUnwindSafe(|| service.call(task.clone()))).unwrap_err();
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"call failed"));
            assert_eq!(worker.task_count(), 0);
            assert_eq!(wake.0.load(Ordering::SeqCst), expected_wakes);
            assert_eq!(task.attempt(), 0);
            assert_eq!(task.status(), Status::Pending);
            #[cfg(feature = "task-context")]
            assert!(worker.get_task("tracked").is_err());
        }
    }

    #[test]
    fn synchronous_call_panic_keeps_other_tasks_registered() {
        let worker = WorkerContext::new("call-panic-with-pending");
        let mut pending_service = LifecycleLayer::new(worker.clone())
            .layer(task_fn(|_: u32| pending::<Result<(), BoxDynError>>()));
        let other = task()
            .into_builder()
            .task_id("other".to_owned().into())
            .build();
        let pending = pending_service.call(other);
        let observer = worker.clone();
        let mut service = LifecycleLayer::new(worker.clone()).layer(task_fn(
            move |_: u32| -> Ready<Result<(), BoxDynError>> {
                assert_eq!(observer.task_count(), 2);
                panic!("call failed");
            },
        ));
        let wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = wake.clone().into();
        worker.register_waker(&Context::from_waker(&waker));

        let panic = catch_unwind(AssertUnwindSafe(|| service.call(task()))).unwrap_err();
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"call failed"));
        assert_eq!(worker.task_count(), 1);
        assert_eq!(wake.0.load(Ordering::SeqCst), 0);
        #[cfg(feature = "task-context")]
        assert!(worker.get_task("other").is_ok());
        drop(pending);
        assert_eq!(worker.task_count(), 0);
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn registration_first_poll_and_drop_have_distinct_effects() {
        let worker = WorkerContext::new("lifecycle");
        let mut service = LifecycleLayer::new(worker.clone())
            .layer(task_fn(|_: u32| pending::<Result<(), BoxDynError>>()));
        let task = task();
        let mut future = Box::pin(service.call(task.clone()));
        assert_eq!(worker.task_count(), 1);
        assert_eq!(task.attempt(), 0);
        assert_eq!(task.status(), Status::Pending);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert_eq!(task.attempt(), 1);
        assert_eq!(task.status(), Status::Running);
        drop(future);
        assert_eq!(worker.task_count(), 0);
        assert_eq!(task.status(), Status::Running);

        let unpolled = task.into_builder().attempt(0).build();
        let future = service.call(unpolled.clone());
        assert_eq!(worker.task_count(), 1);
        drop(future);
        assert_eq!(worker.task_count(), 0);
        assert_eq!(unpolled.attempt(), 0);
    }

    #[test]
    fn result_classification_updates_status_without_reexecuting() {
        let cases: [(Result<(), BoxDynError>, Status); 5] = [
            (Ok(()), Status::Done),
            (Err(std::io::Error::other("failure").into()), Status::Failed),
            (Err(AbortError::new("abort").into()), Status::Killed),
            (
                Err(RetryAfterError::new("retry", Duration::from_secs(10)).into()),
                Status::Pending,
            ),
            (Err(DeferredError::new("defer").into()), Status::Pending),
        ];
        for (result, expected) in cases {
            let worker = WorkerContext::new("classification");
            let mut result = Some(result);
            let mut service = LifecycleLayer::new(worker.clone())
                .layer(task_fn(move |_: u32| ready(result.take().unwrap())));
            let task = task();
            let mut future = Box::pin(service.call(task.clone()));
            let output = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            assert!(output.is_ready());
            assert_eq!(task.status(), expected);
            assert_eq!(task.attempt(), 1);
            assert_eq!(worker.task_count(), 1);
            drop(future);
            assert_eq!(worker.task_count(), 0);
        }
    }

    #[cfg(feature = "task-context")]
    #[test]
    fn cancellation_is_observed_on_poll_and_registration_checks_ids() {
        let worker = WorkerContext::new("cancel");
        let mut service = LifecycleLayer::new(worker.clone())
            .layer(task_fn(|_: u32| pending::<Result<(), BoxDynError>>()));
        let task = task();
        let mut future = Box::pin(service.call(task.clone()));
        assert!(matches!(
            worker.register_task(task.ctx()),
            Err(TaskLifecycleError::Duplicate)
        ));
        assert!(matches!(
            worker.register_task(Task::new(()).ctx()),
            Err(TaskLifecycleError::MissingTaskId)
        ));
        let context = worker.get_task("tracked").unwrap();
        worker.cancel_task(&context).unwrap();
        let Poll::Ready(Err(error)) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("cancelled task must finish on its next poll");
        };
        assert!(error.is::<AbortError>());
        assert_eq!(task.status(), Status::Killed);
        assert_eq!(task.attempt(), 1);
        drop(future);
        assert_eq!(worker.task_count(), 0);
        assert!(worker.get_task("tracked").is_err());
    }

    #[cfg(feature = "task-context")]
    #[test]
    fn stale_context_cannot_cancel_a_new_execution() {
        for complete_first in [false, true] {
            let worker = WorkerContext::new("replayed");
            let mut service = LifecycleLayer::new(worker.clone())
                .layer(task_fn(|_: u32| ready(Ok::<_, BoxDynError>(()))));
            let task = task();
            let first = service.call(task.clone());
            let old = worker.get_task("tracked").unwrap();
            if complete_first {
                first.now_or_never().unwrap().unwrap();
            } else {
                drop(first);
            }
            assert_eq!(worker.task_count(), 0);

            let second = service.call(task.clone());
            let current = worker.get_task("tracked").unwrap();
            assert!(Arc::ptr_eq(
                &old.execution_context().unwrap(),
                &current.execution_context().unwrap()
            ));
            assert_eq!(worker.cancel_task(&old), Err(TaskStateError::TaskNotFound));
            assert!(!current.is_executed());
            assert_eq!(old.is_completed(), complete_first);
            second.now_or_never().unwrap().unwrap();
            assert!(current.is_completed());
            assert_eq!(task.status(), Status::Done);
            assert_eq!(worker.task_count(), 0);
        }
    }

    #[cfg(feature = "task-context")]
    #[test]
    fn cancellation_requires_a_context_from_the_same_worker_execution() {
        let worker = WorkerContext::new("local");
        let other = WorkerContext::new("other");
        let handler = task_fn(|_: u32| ready(Ok::<_, BoxDynError>(())));
        let mut service = LifecycleLayer::new(worker.clone()).layer(handler);
        let mut other_service = LifecycleLayer::new(other.clone()).layer(handler);
        let task = task();
        let local_future = service.call(task.clone());
        let other_future = other_service.call(task);
        let local = worker.get_task("tracked").unwrap();
        let foreign = other.get_task("tracked").unwrap();
        assert_eq!(local, foreign);
        assert_eq!(
            worker.cancel_task(&foreign),
            Err(TaskStateError::TaskNotFound)
        );
        assert!(!local.is_executed());
        assert!(!foreign.is_executed());

        worker.cancel_task(&local).unwrap();
        assert_eq!(
            worker.cancel_task(&local),
            Err(TaskStateError::AlreadyExecuted(TerminalState::Cancelled))
        );
        assert!(
            local_future
                .now_or_never()
                .unwrap()
                .unwrap_err()
                .is::<AbortError>()
        );
        other_future.now_or_never().unwrap().unwrap();
        assert_eq!(worker.task_count(), 0);
        assert_eq!(other.task_count(), 0);
        assert_eq!(
            worker.cancel_task(&local),
            Err(TaskStateError::TaskNotFound)
        );
    }
}
