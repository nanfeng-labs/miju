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
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
};

use futures_util::{FutureExt, task::AtomicWaker};
use tower_layer::Layer;
use tower_service::Service;

use crate::{
    error::{AbortError, BoxDynError, DeferredError, RetryAfterError, WorkerStateError},
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
            readiness_waker: None,
        }
    }
}

/// A service that tracks the lifecycle of a task.
///
/// `LifecycleService` registers tasks with the worker before processing and
/// wraps the underlying service future in a [`LifecycleFuture`] to track task
/// state and completion.
#[derive(Debug)]
pub struct LifecycleService<S> {
    worker: WorkerContext,
    service: S,
    readiness_waker: Option<Arc<AtomicWaker>>,
}

impl<S: Clone> Clone for LifecycleService<S> {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            service: self.service.clone(),
            readiness_waker: None,
        }
    }
}

impl<S> Drop for LifecycleService<S> {
    fn drop(&mut self) {
        if let Some(waker) = &self.readiness_waker {
            self.worker
                .readiness_waiters
                .lock()
                .unwrap()
                .retain(|registered| !Arc::ptr_eq(registered, waker));
        }
    }
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
        if let Some(waker) = &self.readiness_waker {
            waker.take();
        }
        let result = self.service.poll_ready(cx).map_err(Into::into);
        if let Poll::Ready(Err(error)) = result {
            self.worker.is_ready.store(false, Ordering::SeqCst);
            return Poll::Ready(Err(error));
        }
        let mut paused = self.worker.is_paused();
        if paused || result.is_pending() {
            let waker = self.readiness_waker.get_or_insert_with(|| {
                let waker = Arc::new(AtomicWaker::new());
                self.worker
                    .readiness_waiters
                    .lock()
                    .unwrap()
                    .push(waker.clone());
                waker
            });
            waker.register(cx.waker());
            // Register before rechecking state so resume/stop cannot be missed.
            paused = self.worker.is_paused();
        }
        if self.worker.is_shutting_down() {
            self.worker.is_ready.store(false, Ordering::SeqCst);
            return Poll::Ready(Err(Box::new(AbortError::new(
                WorkerStateError::ShuttingDown,
            ))));
        }
        if paused || result.is_pending() {
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
    use crate::{
        backend::memory::MemoryStorage,
        task::{builder::TaskBuilder, task_fn::task_fn},
        worker::{builder::WorkerBuilder, event::Event},
    };
    use futures_util::StreamExt;
    use std::{
        future::{Future, Ready, pending, ready},
        io,
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

    struct ReadinessFailure {
        worker: WorkerContext,
        before_error: fn(&WorkerContext),
        error: Option<io::Error>,
    }

    impl Service<Task<u32>> for ReadinessFailure {
        type Response = ();
        type Error = io::Error;
        type Future = Ready<Result<(), io::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            (self.before_error)(&self.worker);
            Poll::Ready(Err(self.error.take().expect("failed service polled again")))
        }

        fn call(&mut self, _: Task<u32>) -> Self::Future {
            panic!("service with failed readiness must not receive tasks");
        }
    }

    #[tokio::test]
    async fn readiness_errors_survive_pause_and_stop() {
        let cases: [(bool, fn(&WorkerContext)); 3] = [
            (true, |_| {}),
            (false, |worker| worker.pause().unwrap()),
            (false, |worker| worker.stop().unwrap()),
        ];
        for (paused_before, before_error) in cases {
            let ctx = WorkerContext::new("readiness-error");
            let service = ReadinessFailure {
                worker: ctx.clone(),
                before_error,
                error: Some(io::Error::new(io::ErrorKind::PermissionDenied, "readiness")),
            };
            let mut events = WorkerBuilder::new(&ctx)
                .backend(MemoryStorage::new())
                .build(service)
                .stream();
            assert!(matches!(events.next().await, Some(Ok(Event::Start))));
            if paused_before {
                ctx.pause().unwrap();
            }
            let Poll::Ready(Some(Ok(Event::Error(error)))) =
                events.poll_next_unpin(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("worker must report the readiness error");
            };
            let error = error.downcast_ref::<io::Error>().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            assert_eq!(error.to_string(), "readiness");
            assert!(!ctx.is_ready());
            assert_eq!(ctx.task_count(), 0);
            if ctx.is_paused() {
                ctx.resume().unwrap();
            }
            ctx.stop().unwrap();
            assert!(matches!(events.next().await, Some(Ok(Event::Exit))));
            assert!(events.next().await.is_none());
        }
    }

    #[test]
    fn pause_and_resume_preserve_readiness() {
        let mut worker = WorkerContext::new("pause-readiness");
        worker.start().unwrap();
        let mut service = LifecycleLayer::new(worker.clone())
            .layer(task_fn(|_: u32| ready(Ok::<_, BoxDynError>(()))));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(worker.is_ready());
        worker.pause().unwrap();
        assert!(service.poll_ready(&mut cx).is_pending());
        assert!(!worker.is_ready());
        worker.resume().unwrap();
        assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(worker.is_ready());
    }

    #[test]
    fn cloned_readiness_waiters_wake_independently_and_release_on_drop() {
        let mut worker = WorkerContext::new("cloned-readiness");
        worker.start().unwrap();
        worker.pause().unwrap();
        let parent = Arc::new(CountWake(AtomicUsize::new(0)));
        let parent_waker = Waker::from(parent.clone());
        worker.register_waker(&Context::from_waker(&parent_waker));
        let mut first = LifecycleLayer::new(worker.clone())
            .layer(task_fn(|_: u32| ready(Ok::<_, BoxDynError>(()))));
        let first_wake = Arc::new(CountWake(AtomicUsize::new(0)));
        assert!(
            first
                .poll_ready(&mut Context::from_waker(&first_wake.clone().into()))
                .is_pending()
        );
        // Clone after registration: it must not overwrite the first child's waker.
        let mut second = first.clone();
        let second_wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let second_waker = Waker::from(second_wake.clone());
        assert!(
            second
                .poll_ready(&mut Context::from_waker(&second_waker))
                .is_pending()
        );
        worker.resume().unwrap();
        assert_eq!(first_wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(second_wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(parent.0.load(Ordering::SeqCst), 1);
        worker.pause().unwrap();
        let dropped = Arc::new(CountWake(AtomicUsize::new(0)));
        let dropped_weak = Arc::downgrade(&dropped);
        assert!(
            first
                .poll_ready(&mut Context::from_waker(&dropped.into()))
                .is_pending()
        );
        assert!(
            second
                .poll_ready(&mut Context::from_waker(&second_waker))
                .is_pending()
        );
        drop(first);
        assert!(
            dropped_weak.upgrade().is_none(),
            "dropped service must release its waiter"
        );
        worker.stop().unwrap();
        assert_eq!(second_wake.0.load(Ordering::SeqCst), 2);
        assert!(matches!(
            second.poll_ready(&mut Context::from_waker(&second_waker)),
            Poll::Ready(Err(_))
        ));
    }

    #[test]
    fn readiness_notifications_do_not_recurse_through_remaining_waiters() {
        struct ReenterStop {
            worker: WorkerContext,
            depth: Arc<AtomicUsize>,
            maximum: Arc<AtomicUsize>,
            calls: Arc<AtomicUsize>,
        }
        impl Wake for ReenterStop {
            fn wake(self: Arc<Self>) {
                let depth = self.depth.fetch_add(1, Ordering::SeqCst) + 1;
                self.maximum.fetch_max(depth, Ordering::SeqCst);
                self.calls.fetch_add(1, Ordering::SeqCst);
                assert!(depth <= 64);
                self.worker.stop().unwrap();
                self.depth.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let mut worker = WorkerContext::new("reentrant-readiness");
        worker.start().unwrap();
        worker.register_waker(&Context::from_waker(Waker::noop()));
        worker.pause().unwrap();
        let depth = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut services = Vec::new();
        for _ in 0..64 {
            let mut service = LifecycleLayer::new(worker.clone())
                .layer(task_fn(|_: u32| ready(Ok::<_, BoxDynError>(()))));
            let waker = Waker::from(Arc::new(ReenterStop {
                worker: worker.clone(),
                depth: depth.clone(),
                maximum: maximum.clone(),
                calls: calls.clone(),
            }));
            assert!(
                service
                    .poll_ready(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            services.push(service);
        }
        worker.stop().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 64);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        assert_eq!(depth.load(Ordering::SeqCst), 0);
        drop(services);
        assert!(worker.readiness_waiters.lock().unwrap().is_empty());
    }

    #[test]
    fn repolling_readiness_replaces_its_waker() {
        let mut worker = WorkerContext::new("replace-readiness");
        worker.start().unwrap();
        worker.pause().unwrap();
        let mut service = LifecycleLayer::new(worker.clone())
            .layer(task_fn(|_: u32| ready(Ok::<_, BoxDynError>(()))));
        let old = Arc::new(CountWake(AtomicUsize::new(0)));
        let old_weak = Arc::downgrade(&old);
        assert!(
            service
                .poll_ready(&mut Context::from_waker(&old.into()))
                .is_pending()
        );
        let latest = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = Waker::from(latest.clone());
        for _ in 0..32 {
            assert!(
                service
                    .poll_ready(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        assert!(old_weak.upgrade().is_none());
        worker.stop().unwrap();
        assert_eq!(latest.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shutdown_finishes_readiness_even_when_inner_service_is_pending() {
        struct NotReady;
        impl Service<Task<u32>> for NotReady {
            type Response = ();
            type Error = BoxDynError;
            type Future = Ready<Result<(), BoxDynError>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Pending
            }

            fn call(&mut self, _: Task<u32>) -> Self::Future {
                panic!("a pending service must not receive a task");
            }
        }

        let mut worker = WorkerContext::new("pending-at-stop");
        worker.start().unwrap();
        let mut service = LifecycleLayer::new(worker.clone()).layer(NotReady);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(service.poll_ready(&mut cx).is_pending());
        worker.stop().unwrap();
        let Poll::Ready(Err(error)) = service.poll_ready(&mut cx) else {
            panic!("stopped readiness must finish even if the inner service is pending");
        };
        assert!(error.is::<AbortError>());
        assert!(!worker.is_ready());
        assert_eq!(worker.task_count(), 0);
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
