use std::{
    fmt,
    task::{Context, Poll},
};

use futures_util::{
    TryFutureExt,
    future::{Either, MapOk, Ready, ready},
};
use miju_core::task::{
    Task,
    task_fn::{TaskFn, task_fn},
};
use tower::Service;

use super::Decision;

/// Wraps a task function so the worker decides whether to handle each task.
///
/// `predicate` runs on the decoded arguments inside the worker's service call,
/// in the order tasks are fetched. It should be cheap, non-blocking and pure:
/// it runs on the worker's task, and a retry layer around the worker calls it
/// again. When it returns `false` the handler does not run and the task
/// completes with [`Decision::Skipped`]; otherwise the handler's output is
/// returned as [`Decision::Handled`]. Middleware added to the worker still sees
/// every task, including skipped ones.
///
/// Use [`When::new`] to wrap a Tower service instead of a task function.
#[must_use]
pub fn when<P, F, Args, FnArgs>(predicate: P, handler: F) -> When<P, TaskFn<F, Args, FnArgs>> {
    When::new(predicate, task_fn(handler))
}

/// A service that calls its inner service only when its predicate accepts the
/// task. See [`when`].
#[derive(Clone)]
pub struct When<P, S> {
    predicate: P,
    inner: S,
}

impl<P, S> When<P, S> {
    /// Wraps `service` with `predicate`.
    ///
    /// Readiness is delegated to `service` before every task, including tasks
    /// the predicate then skips. A service that reserves capacity in
    /// `poll_ready` keeps that reservation for its next call.
    #[must_use]
    pub fn new(predicate: P, service: S) -> Self {
        Self {
            predicate,
            inner: service,
        }
    }
}

impl<P, S, Args> Service<Task<Args>> for When<P, S>
where
    P: FnMut(&Args) -> bool,
    S: Service<Task<Args>>,
{
    type Response = Decision<S::Response>;
    type Error = S::Error;
    type Future = Either<
        Ready<Result<Self::Response, Self::Error>>,
        MapOk<S::Future, fn(S::Response) -> Self::Response>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        if (self.predicate)(&task.args) {
            Either::Right(
                self.inner
                    .call(task)
                    .map_ok(Decision::Handled as fn(S::Response) -> Self::Response),
            )
        } else {
            Either::Left(ready(Ok(Decision::Skipped)))
        }
    }
}

impl<P, S: fmt::Debug> fmt::Debug for When<P, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("When")
            .field("predicate", &std::any::type_name::<P>())
            .field("inner", &self.inner)
            .finish()
    }
}
