//! Worker extension for parallel execution and spawning child tasks.
//!
//! This module provides functionality to parallelize task execution within a worker
//! and spawn child tasks. It includes the [`ParallelizeLayer`] middleware for parallel execution
//! and the [`ParallelizeService`] service.
//!
//! # Example
//! ```rust,no_run
//! # use miju_core::worker::builder::WorkerBuilder;
//! # use miju_core::backend::memory::MemoryStorage;
//! # use miju_core::worker::ext::parallelize::ParallelizeExt;
//! # use miju_core::worker::ext::event_listener::EventListenerExt;
//! #[tokio::main]
//! async fn main() {
//!     async fn task(task: u32) {
//!         println!("Processing task: {task}");
//!     }
//!     let in_memory = MemoryStorage::new();
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .parallelize(tokio::spawn)
//!         .on_event(|wrk, ev| {
//!             println!("CTX {:?}, On Event = {:?}", wrk.name(), ev);
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! # Executor ownership, timeouts and retries
//!
//! [`ParallelizeService`] hands the future of the layers added after it, and of
//! the handler, to the executor. Dropping the future it returns drops only the
//! executor's handle: with `tokio::spawn` the spawned task keeps running. An
//! outer timeout therefore does not stop a handler that runs on the executor.
//! With `.retry(..).timeout(..).parallelize(tokio::spawn)`, a timed-out attempt
//! keeps running while the retry starts the next one, so two attempts of the
//! same task can run at once and the late one can still change task state.
//!
//! Add `parallelize` before `retry` and `timeout` so that the timeout owns each
//! attempt's handler future and drops it before the next attempt starts:
//!
//! ```text
//! .concurrency(1)
//! .parallelize(tokio::spawn)
//! .retry(RetryPolicy::retries(1))
//! .timeout(deadline)
//! ```
//!
//! This order does not cancel tasks that the handler spawns itself, and it
//! does not make results durable when the worker is aborted.
use std::future::ready;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::TryFutureExt;
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{backend::Backend, error::BoxDynError, task::Task, worker::builder::WorkerBuilder};

/// Worker extension for parallel execution
pub trait ParallelizeExt<Args, Source, Middleware, Executor>: Sized {
    /// Register the executor for parallel task execution.
    fn parallelize(
        self,
        f: Executor,
    ) -> WorkerBuilder<Args, Source, Stack<ParallelizeLayer<Executor>, Middleware>>;
}

/// Middleware for emitting events
#[derive(Debug, Clone, Default)]
pub struct ParallelizeLayer<Executor> {
    executor: Executor,
}

impl<Executor> ParallelizeLayer<Executor> {
    /// Create a new event listener layer
    pub fn new(executor: Executor) -> Self {
        Self { executor }
    }
}

impl<S, Executor: Clone> Layer<S> for ParallelizeLayer<Executor> {
    type Service = ParallelizeService<S, Executor>;

    fn layer(&self, service: S) -> Self::Service {
        ParallelizeService {
            service,
            executor: self.executor.clone(),
        }
    }
}

/// Service to allow spawning tasks with utils like `tokio::spawn`
#[derive(Debug, Clone)]
pub struct ParallelizeService<S, Executor> {
    service: S,
    executor: Executor,
}

impl<S, Args, Fut, T, Executor, ExecErr> Service<Task<Args>> for ParallelizeService<S, Executor>
where
    S: Service<Task<Args>, Future = Fut>,
    Executor: Fn(Fut) -> T + Send + 'static,
    Fut: Future<Output = Result<S::Response, S::Error>> + Send + 'static,
    T: Future<Output = Result<Result<S::Response, S::Error>, ExecErr>> + Send + 'static,
    S::Error: Into<BoxDynError> + Send + 'static,
    ExecErr: Into<BoxDynError>,
    S::Response: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, request: Task<Args>) -> Self::Future {
        (self.executor)(self.service.call(request))
            .map_err(|e| e.into())
            .and_then(|s| ready(s.map_err(|e| e.into())))
            .boxed()
    }
}

impl<Args, P, M, Executor> ParallelizeExt<Args, P, M, Executor> for WorkerBuilder<Args, P, M>
where
    P: Backend,
    M: Layer<ParallelizeLayer<Executor>>,
{
    fn parallelize(
        self,
        f: Executor,
    ) -> WorkerBuilder<Args, P, Stack<ParallelizeLayer<Executor>, M>> {
        self.layer(ParallelizeLayer::new(f))
    }
}
