use std::{
    fmt, mem,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures_channel::oneshot;
use futures_util::{
    FutureExt, TryFutureExt,
    future::{Either, MapOk, Ready, Shared, ready},
};
use miju_core::task::{
    Task,
    task_fn::{TaskFn, task_fn},
};
use tower::{Service, ServiceExt, util::Oneshot};

use super::Decision;

/// Wraps a task function so the worker decides whether to handle each task.
///
/// `predicate` runs on the decoded arguments inside the worker's service call.
/// It should be cheap, non-blocking and pure: it runs on the worker's task, and
/// a retry layer around the worker calls it again. When it returns `false` the
/// handler does not run and the task completes with [`Decision::Skipped`];
/// otherwise the handler's output is returned as [`Decision::Handled`].
/// Middleware added to the worker still sees every task, including skipped
/// ones.
///
/// The predicate sees tasks in the order they reach this service, not the order
/// they were published. The two match only with a FIFO backend and middleware
/// that keeps order: an asynchronous filter, for example, can let a later task
/// through first.
///
/// The handler must be `Clone`; see [`When::new`]. Use [`When::new`] to wrap a
/// Tower service instead of a task function.
#[must_use]
pub fn when<P, F, Args, FnArgs>(predicate: P, handler: F) -> When<P, TaskFn<F, Args, FnArgs>> {
    // TaskFn reserves nothing in poll_ready, so its synchronous call can run
    // immediately without allocating a dispatch channel.
    When {
        direct: true,
        ..When::new(predicate, task_fn(handler))
    }
}

/// A service that calls its inner service only when its predicate accepts the
/// task. See [`when`].
#[derive(Clone)]
pub struct When<P, S> {
    predicate: P,
    inner: S,
    direct: bool,
    dispatching: Option<PendingDispatch<S>>,
}

impl<P, S> When<P, S> {
    /// Wraps `service` with `predicate`.
    ///
    /// A skipped task never polls the handler for readiness. An accepted task
    /// waits for readiness in its response future. Each `When` handle admits
    /// at most one accepted task waiting to call the handler. As soon as that
    /// call starts, later tasks can enter while its response is still running.
    ///
    /// That call starts from the accepted task's response future, so the next
    /// `poll_ready` waits until that future has been polled. A caller that
    /// collects several responses before polling any of them waits forever on
    /// the second `poll_ready`. miju workers poll responses while they wait for
    /// readiness, and [`when`] calls task functions directly, so neither is
    /// affected.
    ///
    /// The handle keeps an unreserved clone while dispatch is pending. If the
    /// response future is dropped before dispatch or readiness fails, the
    /// waiting service is dropped and the next task uses that clone. If a
    /// handle is cloned during dispatch, the new handle's readiness waits for
    /// the handler's state after the call.
    #[must_use]
    pub fn new(predicate: P, service: S) -> Self {
        Self {
            predicate,
            inner: service,
            direct: false,
            dispatching: None,
        }
    }
}

struct PendingDispatch<S> {
    returned: Shared<oneshot::Receiver<Arc<Mutex<S>>>>,
    owner: bool,
}

impl<S> Clone for PendingDispatch<S> {
    fn clone(&self) -> Self {
        Self {
            returned: self.returned.clone(),
            owner: false,
        }
    }
}

impl<S: Clone> PendingDispatch<S> {
    // Tower Retry clones immediately after call. A clone made during dispatch
    // must inherit the state *after* the handler has received that request.
    fn service(&self, returned: Arc<Mutex<S>>) -> S {
        if !self.owner {
            return returned.lock().unwrap().clone();
        }
        match Arc::try_unwrap(returned) {
            Ok(service) => service.into_inner().unwrap(),
            Err(shared) => {
                let mut service = shared.lock().unwrap();
                let snapshot = service.clone();
                mem::replace(&mut *service, snapshot)
            }
        }
    }
}

impl<P, S, Args> Service<Task<Args>> for When<P, S>
where
    P: FnMut(&Args) -> bool,
    S: Service<Task<Args>> + Clone,
{
    type Response = Decision<S::Response>;
    type Error = S::Error;
    type Future = Either<
        Ready<Result<Self::Response, Self::Error>>,
        MapOk<
            Either<S::Future, Oneshot<Dispatch<S>, Task<Args>>>,
            fn(S::Response) -> Self::Response,
        >,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.direct {
            return self.inner.poll_ready(cx);
        }
        if let Some(dispatching) = &mut self.dispatching {
            if let Ok(returned) = std::task::ready!(dispatching.returned.poll_unpin(cx)) {
                self.inner = dispatching.service(returned);
            }
            self.dispatching = None;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        assert!(
            self.dispatching.is_none(),
            "When::call requires readiness after the previous dispatch"
        );
        if !(self.predicate)(&task.args) {
            return Either::Left(ready(Ok(Decision::Skipped)));
        }

        let response = if self.direct {
            Either::Left(self.inner.call(task))
        } else {
            let spare = self.inner.clone();
            let service = mem::replace(&mut self.inner, spare);
            let (dispatched, dispatching) = oneshot::channel();
            self.dispatching = Some(PendingDispatch {
                returned: dispatching.shared(),
                owner: true,
            });
            Either::Right(Dispatch(Some((service, dispatched))).oneshot(task))
        };
        Either::Right(response.map_ok(Decision::Handled as fn(S::Response) -> Self::Response))
    }
}

// Oneshot owns the service until dispatch. A canceled or failed dispatch
// drops its sender and service, releasing any partial readiness reservation.
#[doc(hidden)]
#[derive(Debug)]
pub struct Dispatch<S>(Option<(S, oneshot::Sender<Arc<Mutex<S>>>)>);

impl<S: Service<R>, R> Service<R> for Dispatch<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let (mut service, dispatched) = self.0.take().expect("dispatch already finished");
        let ready = service.poll_ready(cx);
        if !matches!(ready, Poll::Ready(Err(_))) {
            self.0 = Some((service, dispatched));
        }
        ready
    }

    fn call(&mut self, request: R) -> Self::Future {
        let (mut service, dispatched) = self.0.take().expect("dispatch already finished");
        let future = service.call(request);
        let _ = dispatched.send(Arc::new(Mutex::new(service)));
        future
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
