//! # Middleware for catching panics
//!
//! The [`CatchPanicLayer`] allows you to catch panics in your task handlers and convert them into errors. This helps prevent panics from crashing your worker process and enables custom error handling or reporting.
//!
//! ## Usage
//!
//! ### Basic Worker Example
//!
//! ```rust
//! # use miju::prelude::*;
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(42).await.unwrap();
//!
//!     async fn task(task: u32) {
//!         if task == 42 {
//!             panic!("I am a panic, catch me!");
//!         }
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .catch_panic()
//!         .on_event(|worker, ev| {
//!             if matches!(ev, Event::Error(_)) {
//!                 worker.stop().unwrap();
//!             }
//!         })
//!         .build(task);
//!     tokio::time::timeout(std::time::Duration::from_secs(5), worker.run())
//!         .await
//!         .unwrap()
//!         .unwrap();
//! }
//! ```
//!
//! ### Worker With Custom Panic Handler
//!
//! ```rust
//! # use miju::layers::catch_panic::CatchPanicLayer;
//! # use miju::layers::catch_panic::PanicError;
//! # use miju::prelude::*;
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(42).await.unwrap();
//!
//!     async fn task(task: u32) {
//!         if task == 42 {
//!             panic!("I am a panic, catch me!");
//!         }
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .layer(CatchPanicLayer::with_panic_handler(|e| {
//!             println!("Caught panic: {:?}", e);
//!             PanicError("Custom panic handler".to_string())
//!         }))
//!         .on_event(|worker, ev| {
//!             if matches!(ev, Event::Error(_)) {
//!                 worker.stop().unwrap();
//!             }
//!         })
//!         .build(task);
//!     tokio::time::timeout(std::time::Duration::from_secs(5), worker.run())
//!         .await
//!         .unwrap()
//!         .unwrap();
//! }
//! ```
//! ### Combining a Custom Handler with Retries
//!
//! With both `retry` and `catch-panic` enabled, an explicit predicate can exclude
//! panic errors while retrying other failures. Add the retry layer first so it
//! receives errors produced by the panic handler.
//!
//! ```rust
//! # #[cfg(feature = "retry")]
//! # {
//! use miju::layers::catch_panic::{CatchPanicLayer, PanicError};
//! use miju::prelude::*;
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), BoxDynError> {
//!     let mut storage = MemoryStorage::new();
//!     storage.push(42u32).await?;
//!     async fn task(_: u32) {
//!         panic!("task panicked");
//!     }
//!     let worker = WorkerBuilder::new("panic-with-retry")
//!         .backend(storage)
//!         .retry(RetryPolicy::retries(1).retry_if(|error: &BoxDynError| {
//!             error.downcast_ref::<PanicError>().is_none()
//!         }))
//!         .layer(CatchPanicLayer::with_panic_handler(|_| {
//!             PanicError("Custom panic handler".to_owned())
//!         }))
//!         .on_event(|worker, event| {
//!             if matches!(event, Event::Error(_)) {
//!                 worker.stop().unwrap();
//!             }
//!         })
//!         .build(task);
//!     tokio::time::timeout(std::time::Duration::from_secs(5), worker.run()).await??;
//!     Ok(())
//! }
//! # main().unwrap();
//! # }
//! ```
//!
//! ## Capture boundary
//!
//! The layer catches unwinding panics from [`Service::call`] and polling the
//! returned future. It forwards readiness checks without catching panics and
//! does not catch panics in a custom panic handler or separately spawned tasks.
//! Rust's panic hook still runs before a panic is caught. With `panic = "abort"`,
//! the process terminates instead of unwinding.
//!
//! The default handler wraps [`PanicError`] in [`AbortError`]. Custom handlers
//! return their own error type; the layer does not add an abort wrapper to it.
use futures_util::FutureExt;
use futures_util::future::CatchUnwind;
use miju_core::error::{AbortError, BoxDynError};
use miju_core::task::Task;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::ready;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

/// Layer that catches panics in the service.
#[derive(Clone, Debug)]
pub struct CatchPanicLayer<F, Err> {
    on_panic: F,
    _marker: std::marker::PhantomData<Err>,
}

impl<Err> CatchPanicLayer<fn(&Box<dyn Any + Send + 'static>) -> AbortError, Err> {
    /// Creates a new `CatchPanicLayer` with a default panic handler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            on_panic: default_handler,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Err> Default for CatchPanicLayer<fn(&Box<dyn Any + Send>) -> AbortError, Err> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F, Err> CatchPanicLayer<F, Err>
where
    F: FnMut(&Box<dyn Any + Send>) -> Err + Clone,
{
    /// Creates a new `CatchPanicLayer` with a custom panic handler.
    #[must_use]
    pub fn with_panic_handler(on_panic: F) -> Self {
        Self {
            on_panic,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, F, Err> Layer<S> for CatchPanicLayer<F, Err>
where
    F: FnMut(&Box<dyn Any + Send>) -> Err + Clone,
{
    type Service = CatchPanicService<S, F>;

    fn layer(&self, service: S) -> Self::Service {
        CatchPanicService {
            service,
            on_panic: self.on_panic.clone(),
        }
    }
}

/// Service that catches panics.
#[derive(Clone, Debug)]
pub struct CatchPanicService<S, F> {
    service: S,
    on_panic: F,
}

impl<S, Args, Res, F, PanicErr> Service<Task<Args>> for CatchPanicService<S, F>
where
    S: Service<Task<Args>, Response = Res>,
    F: FnMut(&Box<dyn Any + Send>) -> PanicErr + Clone,
    S::Error: Into<BoxDynError>,
    PanicErr: Into<BoxDynError>,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = CatchPanicFuture<S::Future, F, PanicErr>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        match std::panic::catch_unwind(AssertUnwindSafe(|| self.service.call(task))) {
            Ok(future) => CatchPanicFuture {
                kind: Kind::Future {
                    future: AssertUnwindSafe(future).catch_unwind(),
                    panic_handler: Some(self.on_panic.clone()),
                },
                _marker: std::marker::PhantomData,
            },
            Err(panic_err) => CatchPanicFuture {
                kind: Kind::Panicked {
                    panic_err: Some(panic_err),
                    panic_handler: Some(self.on_panic.clone()),
                },
                _marker: std::marker::PhantomData,
            },
        }
    }
}

#[pin_project::pin_project(project = KindProj)]
enum Kind<F, T> {
    Panicked {
        panic_err: Option<Box<dyn Any + Send + 'static>>,
        panic_handler: Option<T>,
    },
    Future {
        #[pin]
        future: CatchUnwind<AssertUnwindSafe<F>>,
        panic_handler: Option<T>,
    },
}

/// A wrapper that catches panics during execution
#[pin_project::pin_project]
pub struct CatchPanicFuture<Fut, F, Err> {
    #[pin]
    kind: Kind<Fut, F>,
    _marker: std::marker::PhantomData<Err>,
}

impl<Fut, F, Err> fmt::Debug for CatchPanicFuture<Fut, F, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatchPanicFuture")
            .field("kind", &"<hidden>")
            .field("_marker", &std::any::type_name::<Err>())
            .finish()
    }
}

/// An error generated from a panic
#[derive(Debug, Clone)]
pub struct PanicError(
    /// Message extracted from the panic payload.
    pub String,
);

impl std::error::Error for PanicError {}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PanicError: {}", self.0)
    }
}

impl<Fut, Res, F, Err, PanicErr> Future for CatchPanicFuture<Fut, F, PanicErr>
where
    Fut: Future<Output = Result<Res, Err>>,
    F: FnMut(&Box<dyn Any + Send>) -> PanicErr,
    Err: Into<BoxDynError>,
    PanicErr: Into<BoxDynError>,
{
    type Output = Result<Res, BoxDynError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::Panicked {
                panic_err,
                panic_handler,
            } => {
                let mut panic_handler = panic_handler
                    .take()
                    .expect("future polled after completion");
                let panic_err = panic_err.take().expect("future polled after completion");
                Poll::Ready(Err(panic_handler(&panic_err).into()))
            }
            KindProj::Future {
                future,
                panic_handler,
            } => match ready!(future.poll(cx)) {
                Ok(Ok(res)) => Poll::Ready(Ok(res)),
                Ok(Err(svc_err)) => Poll::Ready(Err(svc_err.into())),
                Err(panic_err) => {
                    let mut panic_handler = panic_handler
                        .take()
                        .expect("future polled after completion");
                    Poll::Ready(Err(panic_handler(&panic_err).into()))
                }
            },
        }
    }
}

fn default_handler(e: &Box<dyn Any + Send>) -> AbortError {
    let panic_info = if let Some(s) = e.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_owned()
    };
    // miju assumes service functions are pure
    // therefore a panic should ideally abort
    AbortError::new(PanicError(panic_info))
}
