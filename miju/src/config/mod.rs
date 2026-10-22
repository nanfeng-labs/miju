//! Construct workers and middleware stacks from configuration.
//!
//! [`WorkerConfig`] combines a worker name, backend-specific configuration, and a
//! set of [`Middleware`] descriptors. [`WorkerFromConfig::try_config`] constructs
//! the backend through [`TryNewBackend`] and returns a [`WorkerBuilder`], which
//! can then receive application data, event listeners, and a task handler.
//! Configuration parsing is separate from backend construction: applications
//! choose a Serde-compatible format and supply its parser.
//!
//! # Building a worker
//!
//! This JSON example requires `serde_json` in the application. All three fields
//! are required. The in-memory backend uses unit configuration (`null` in JSON)
//! and creates an empty queue; construction does not start processing tasks.
//!
//! ```rust
//! use miju::{
//!     config::{WorkerConfig, WorkerFromConfig},
//!     prelude::{BoxDynError, MemoryStorage, WorkerBuilder},
//! };
//!
//! let config: WorkerConfig<MemoryStorage<usize>> = serde_json::from_str(r#"{
//!     "name": "configured-worker",
//!     "backend": null,
//!     "middleware": []
//! }"#)?;
//! let _worker = WorkerBuilder::try_config(config)?
//!     .build(|value: usize| async move { value * 2 });
//! # Ok::<(), BoxDynError>(())
//! ```
//!
//! A backend may return a decorated backend from `try_new`; the returned builder
//! uses that backend's argument type. Backend construction errors are returned
//! unchanged. Construction delegates to the backend, including any I/O or other
//! side effects performed by its constructor.
//!
//! # Middleware features and order
//!
//! `config` enables the configuration types. Enable each middleware feature
//! separately to make its Rust and serialized variants available:
//!
//! | Feature | Variants |
//! | --- | --- |
//! | `timeout` | `Timeout { duration }` |
//! | `limit` | `RateLimit { num, per }`, `Concurrency { max }` |
//! | `catch-panic` | `CatchPanic` |
//! | `tracing` | `Tracing` |
//!
//! With none of these features enabled, the middleware set must be empty.
//! Retry and filter middleware are not configuration variants. Retry requires a
//! cloneable service, while this stack uses [`BoxService`].
//!
//! Descriptors are stored in a [`HashSet`], so identical entries collapse. Entries
//! with different parameters remain distinct. The stack sorts by middleware
//! kind, then wraps the service in this order from outermost to innermost:
//! tracing, rate limit, concurrency limit, timeout, panic capture, task handler.
//! Ordering among entries of the same kind is unspecified. Serialized array
//! order does not control execution order.
//!
//! [`MiddlewareStack`] can also decorate an existing backend. With `config` and
//! `timeout` enabled, the following worker applies a one-second deadline:
//!
//! ```rust
//! # #[cfg(feature = "timeout")]
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), miju::error::BoxDynError> {
//! use miju::{config::{Middleware, MiddlewareStack}, prelude::*};
//! use std::{collections::HashSet, time::Duration};
//!
//! let mut backend = MemoryStorage::new();
//! backend.push(7usize).await?;
//! let middleware = HashSet::from([
//!     Middleware::Timeout { duration: Duration::from_secs(1) },
//! ]);
//! let worker = WorkerBuilder::new("configured-timeout")
//!     .backend(backend)
//!     .layer(MiddlewareStack::<MemoryStorage<usize>>::new(middleware))
//!     .build(|value: usize, worker: WorkerContext| async move {
//!         worker.stop().unwrap();
//!         value * 2
//!     });
//! worker.run().await?;
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "timeout"))]
//! # fn main() {}
//! ```
//!
//! # Construction boundaries
//!
//! Parsing does not validate middleware settings. `try_config` creates the
//! backend and stores descriptors; the middleware is constructed when the stack
//! is layered, including when the worker starts. Rate limits require both `num`
//! and `per` to be nonzero and otherwise panic during layer construction. A zero
//! concurrency limit never becomes ready. Service responses must implement
//! [`Debug`] even when tracing is disabled; service errors become [`BoxDynError`].

#[cfg(any(feature = "timeout", feature = "limit"))]
use std::time::Duration;
use std::{collections::HashSet, fmt::Debug, marker::PhantomData};

use miju_core::{
    backend::{Backend, BackendConfig, TryNewBackend},
    error::BoxDynError,
    layers::{Identity, Stack},
    task::Task,
    worker::builder::WorkerBuilder,
};

use tower::{
    Layer, Service, ServiceBuilder,
    util::{BoxLayer, BoxService},
};

#[cfg(feature = "tracing")]
use crate::layers::tracing::TraceLayer;

/// A standalone config that allows quickly bootstrapping workers
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig<B>
where
    B: BackendConfig,
{
    /// The name of the worker
    pub name: String,
    /// The config for the backend
    pub backend: B::Config,
    /// Middleware to be applied in the stack's fixed order, not insertion order.
    pub middleware: HashSet<Middleware>,
}

/// Configuration for worker middlewares
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum Middleware {
    /// A simple timeout middleware that cancels jobs running longer than the specified duration
    #[cfg(feature = "timeout")]
    Timeout {
        /// The duration after which the job should be cancelled
        duration: Duration,
    },
    /// A rate limiting middleware that limits the number of tasks processed per unit of time
    #[cfg(feature = "limit")]
    RateLimit {
        /// Maximum number of tasks to process
        num: u64,
        /// Time window for the rate limit
        per: Duration,
    },
    /// A concurrency middleware that limits the number of tasks processed concurrently
    #[cfg(feature = "limit")]
    Concurrency {
        /// Maximum number of concurrent tasks
        max: usize,
    },
    /// A middleware that catches panics and prevents them from crashing the worker
    #[cfg(feature = "catch-panic")]
    CatchPanic,

    // Retries need S: Clone.
    /// Enable tracing
    #[cfg(feature = "tracing")]
    Tracing,
}

impl Middleware {
    /// Convert this middleware descriptor into a boxed `tower::Layer`.
    ///
    /// Errors produced by the wrapped service are converted to [`BoxDynError`].
    /// Rate limits with a zero count or period panic when this method constructs
    /// the layer. A zero concurrency limit leaves the service pending.
    #[must_use]
    pub fn to_layer<S, Args>(self) -> BoxLayer<S, Task<Args>, S::Response, BoxDynError>
    where
        S: Service<Task<Args>> + Send + Sync + 'static,
        S::Response: Send + 'static,
        S::Error: Into<BoxDynError> + Send + Sync + 'static,
        S::Future: Send + 'static,
        Args: Send + 'static,
        S::Response: Debug, // Specifically only `tracing`
    {
        match self {
            #[cfg(feature = "timeout")]
            Self::Timeout { duration } => {
                let layer = ServiceBuilder::new()
                    .map_err(Into::into)
                    .timeout(duration)
                    .into_inner();
                BoxLayer::new(layer)
            }
            #[cfg(feature = "limit")]
            Self::RateLimit { num, per } => {
                let layer = ServiceBuilder::new()
                    .rate_limit(num, per)
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }
            #[cfg(feature = "limit")]
            Self::Concurrency { max } => {
                let layer = ServiceBuilder::new()
                    .concurrency_limit(max)
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }

            #[cfg(feature = "catch-panic")]
            Self::CatchPanic => {
                use crate::layers::catch_panic::CatchPanicLayer;

                let layer = ServiceBuilder::new()
                    .layer(CatchPanicLayer::new())
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }

            #[cfg(feature = "tracing")]
            Self::Tracing => {
                let layer = ServiceBuilder::new()
                    .layer(TraceLayer::new())
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }
        }
    }
}
type ConfiguredWorker<B> =
    WorkerBuilder<<B as BackendConfig>::Args, B, Stack<MiddlewareStack<B>, Identity>>;

/// Builds a [`WorkerBuilder`] from a serialized/external configuration.
pub trait WorkerFromConfig<B>
where
    B::Backend: Backend + BackendConfig,
    B: TryNewBackend,
{
    /// Attempt to build a [`WorkerBuilder`] from the given [`WorkerConfig`].
    ///
    /// Backend construction errors are returned unchanged. Middleware descriptors
    /// are stored for later construction and are not validated here.
    fn try_config(config: WorkerConfig<B>) -> Result<ConfiguredWorker<B::Backend>, B::Error>;
}

impl<B> WorkerFromConfig<B> for WorkerBuilder<(), (), Identity>
where
    B: TryNewBackend,
    B::Backend: BackendConfig + Backend,
{
    fn try_config(config: WorkerConfig<B>) -> Result<ConfiguredWorker<B::Backend>, B::Error> {
        let backend = B::try_new(config.backend)?;

        Ok(Self::new(config.name)
            .backend(backend)
            .layer(MiddlewareStack::new(config.middleware)))
    }
}

// The enum's size depends on which middleware features are enabled.
#[allow(unreachable_patterns, clippy::trivially_copy_pass_by_ref)]
fn middleware_priority(m: &Middleware) -> u8 {
    match m {
        #[cfg(feature = "catch-panic")]
        Middleware::CatchPanic => 0,
        #[cfg(feature = "timeout")]
        Middleware::Timeout { .. } => 1,
        #[cfg(feature = "limit")]
        Middleware::Concurrency { .. } => 3,
        #[cfg(feature = "limit")]
        Middleware::RateLimit { .. } => 4,
        #[cfg(feature = "tracing")]
        Middleware::Tracing => 6,
        _ => unreachable!(),
    }
}

/// A stack containing all the middleware to apply.
///
/// Middleware kinds have a fixed wrapping order described in the
/// [module documentation](self#middleware-features-and-order). Multiple entries
/// of the same kind with different parameters have no guaranteed relative order.
#[derive(Debug, Clone)]
pub struct MiddlewareStack<B> {
    layers: HashSet<Middleware>,
    backend: PhantomData<B>,
}

impl<B> MiddlewareStack<B> {
    /// Build a new Middleware stack
    #[must_use]
    pub fn new(layers: HashSet<Middleware>) -> Self {
        Self {
            backend: PhantomData,
            layers,
        }
    }
}

impl<S, B> Layer<S> for MiddlewareStack<B>
where
    B: Backend + BackendConfig,
    S: Service<Task<B::Args>> + Send + 'static,
    S::Response: Send + Debug + 'static,
    S::Error: Into<BoxDynError> + Send + Sync + 'static,
    S::Future: Send + 'static,
    B::Args: Send + 'static,
{
    type Service = BoxService<Task<B::Args>, S::Response, BoxDynError>;

    fn layer(&self, inner: S) -> Self::Service {
        let mut ordered: Vec<Middleware> = self.layers.iter().copied().collect();
        ordered.sort_by_key(middleware_priority);

        let mut boxed = BoxService::new(ServiceBuilder::new().map_err(Into::into).service(inner));

        for mw in ordered {
            let layer = mw.to_layer();

            boxed = layer.layer(boxed);
        }

        boxed
    }
}
