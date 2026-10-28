#![cfg_attr(docsrs, feature(doc_cfg))]
//! Task backends and worker execution for background processing in Rust.
//!
//! `miju-core` provides task data structures, backend protocols, workers, and
//! monitoring. Workers execute handlers with middleware and lifecycle tracking;
//! backends supply tasks and define their storage capabilities.
//!
//! # Core Concepts
//!
//! - **[`Tasks`](#tasks)**: Type-safe task data structures with processing metadata
//! - **[`Backends`](#backends)**: Task sources, submission, and storage contracts
//! - **[`Workers`](#workers)**: Handler execution and lifecycle control
//! - **[`Monitors`](#monitors)**: Coordinated workers, shutdown, and restart policies
//!
//! ## Tasks
//!
//! The task struct provides type-safe components for task data and metadata:
//! - [`Args`](crate::task::task_fn) - The primary structure for the task
//! - [`ExecutionContext`](crate::task::ExecutionContext) - Backend persisted info on task execution.
//! - [`Status`](crate::task::status::Status) - Represents the current state of a task
//! - [`TaskId`](crate::task::task_id::TaskId) - Unique identifier for task tracking
//! - [`Attempt`](crate::task::attempt::Attempt) - Retry tracking and attempt information
//! - [`Extensions`](crate::task::extensions::Extensions) - Type-safe storage for additional task data
//! - [`Metadata`](crate::task::metadata) - metadata associated with the task
//!
//! ### Example: Using `TaskBuilder`
//!
//! ```rust
//! use miju_core::task::{Task, builder::TaskBuilder};
//!
//! let task: Task<String> = TaskBuilder::new("my-task".to_string())
//!     .task_id("task-123".to_string().into())
//!     .max_attempts(3)
//!     .run_in_minutes(10)
//!     .build();
//! ```
//! Specific documentation for tasks can be found in the [`task`] and [`task::builder`] modules.
//!
//! #### Relevant Guides:
//! - [**Defining Task arguments**](crate::task::task_fn) - Creating effective task arguments that are scalable and type-safe
//!
//! ## Backends
//!
//! The [`backend`] module defines [`Backend`](backend::Backend) for polling tasks,
//! [`TaskSink`](backend::TaskSink) for submission, and management capabilities.
//! [`MemoryStorage`](backend::memory::MemoryStorage),
//! [`VecDequeBackend`](backend::dequeue::VecDequeBackend), and
//! [`CustomBackend`](backend::custom::CustomBackend) provide task sources.
//! [`Codec`](backend::codec::Codec) and [`FinalizeBackend`](backend::finalize::FinalizeBackend)
//! connect encoded payloads with typed handlers.
//!
//! [`BackendExt`](backend::ext::BackendExt) adds error inspection and mapping,
//! lifecycle callbacks, task piping, stream interleaving, and polling strategies.
//! Strategies supply wakeups and collect polling statistics; they do not gate
//! calls to the underlying backend. Enable `sleep` for interval and backoff
//! strategies, `tracing` for span instrumentation, and `shared` for cloneable
//! backend handles.
//!
//! ### Backend capabilities
//!
//! `Backend` defines the consumption contract: readiness, task delivery, and
//! shutdown cleanup. Workers poll that contract and drive in-flight execution.
//! The additional interfaces below are representative capabilities, not a
//! requirement that every backend provide every operation.
//!
//! - **Submission and management.** `TaskSink` submits tasks and drives submission
//!   buffers; `Update` and `Reschedule` manage existing tasks. These interfaces
//!   alone do not establish durable storage guarantees.
//! - **Task and result queries.** `FetchById` retrieves a task by ID.
//!   `WaitForCompletion` streams execution results or checks their current status;
//!   it does not execute tasks or store results itself.
//! - **Acknowledgement.** An optional `AcknowledgeLayer` wraps a service and passes
//!   its outcome and execution context to `Acknowledge::ack`. The acknowledger and
//!   storage integration determine whether and how results are recorded. Adding
//!   this layer does not automatically provide result-query capabilities.
//!
//! ### Persistence integration
//!
//! With `sleep`, `backend::persistence` provides the `Persistence` provider
//! contract and the `Persisted<P>` driver. The driver coordinates registration,
//! heartbeats, task batches, event processing, and shutdown cleanup; it does not
//! implement `Backend` itself.
//!
//! Storage integrations implement `Backend`, delegate polling to the driver,
//! and attach its `TaskPersistLayer` through backend middleware configuration.
//! The event layer's task-locking and result-persistence options are disabled by
//! default. Enabling them emits `Lock` and `Complete` events respectively; tasks
//! must have IDs even when both options are disabled.
//!
//! Sending an event only queues it. The driver must continue being polled to apply
//! it through the provider. A queued lock event does not establish exclusive task
//! ownership. Providers implement the actual storage operations and are responsible
//! for atomic task claims, consistency, and durability.
//!
//! For repeated worker middleware initialization, `Persisted::shared_layer` reuses
//! one driver's event channel. Cloning `Persisted` resets its local polling,
//! submission, and event-channel state; storage sharing depends on the provider's
//! own `Clone` implementation.
//!
//! ## Workers
//!
//! Workers are the execution engines that process tasks from backends. They are
//! configured with a backend, middleware, and a task handler using
//! [`WorkerBuilder`](worker::builder::WorkerBuilder).
//!
//! ```rust
//! # use miju_core::backend::{memory::MemoryStorage, TaskSink};
//! # use miju_core::worker::{builder::WorkerBuilder, context::WorkerContext};
//! # #[tokio::main]
//! # async fn main() {
//! let mut backend = MemoryStorage::new();
//! backend.push(42u32).await.unwrap();
//! let worker = WorkerBuilder::new("example")
//!     .backend(backend)
//!     .build(|value: u32, worker: WorkerContext| async move {
//!         assert_eq!(value, 42);
//!         worker.stop().unwrap();
//!     });
//! worker.run().await.unwrap();
//! # }
//! ```
//!
//! [`WorkerContext`](worker::context::WorkerContext) controls pause, resume, and
//! shutdown. Lifecycle tracking records attempts and task status. Handler errors
//! become worker events; retry-classified errors update status without automatically
//! requeuing tasks. On backend EOF, the worker drains in-flight work and
//! completes backend cleanup before stopping its context and finishing shutdown.
//!
//! ## Monitors
//!
//! [`Monitor`](monitor::Monitor) manages multiple workers, coordinates shutdown,
//! and optionally restarts failed workers according to a supplied policy.
//! Normal shutdown drains tracked work. A configured terminator can end the wait
//! early; see [`Monitor::run_with_signal`](monitor::Monitor::run_with_signal).
//!
//! Enable `task-context` for individual task tracking and cancellation, `sleep`
//! for timer-based task and shutdown limits, and `tracing` for lifecycle logs.
//! Enable `test-utils` for task function signature validation helpers.
//! Default features remain empty.
//!
//! ### Example: Using `Monitor` with a Worker
//!
//! ```rust
//! # use miju_core::monitor::Monitor;
//! # use miju_core::worker::builder::WorkerBuilder;
//! # use miju_core::task::Task;
//! # use miju_core::backend::{TaskSink, dequeue::VecDequeBackend};
//! # use tower::service_fn;
//! # use std::time::Duration;
//! # use miju_core::worker::context::WorkerContext;
//! #[tokio::main]
//! async fn main() {
//!     let mut storage = VecDequeBackend::new();
//!     storage.push(1u32).await.unwrap();
//!
//!     let monitor = Monitor::new()
//!         .on_event(|worker, event| println!("{}: {:?}", worker.name(), event))
//!         .register(move |_| {
//!             WorkerBuilder::new("demo-worker")
//!                 .backend(storage.clone())
//!                 .build(|req: u32, worker: WorkerContext| async move {
//!                     println!("Processing task: {:?}", req);
//! #                   worker.stop().unwrap();
//!                     Ok::<_, std::io::Error>(req)
//!                 })
//!         });
//!
//!     // Start monitor and run all registered workers
//!     monitor.run().await.unwrap();
//! }
//! ```
//!
//! ## Middleware
//!
//! Built on the `tower` ecosystem, `miju-core` supports middleware for error handling, timeouts, rate limiting, and observability.
//!
//! ### Core Middleware
//!
//! The following middleware layers are included with their worker extensions:
//! - [`AcknowledgeLayer`](crate::worker::ext::ack::AcknowledgeLayer) - Task acknowledgment after processing
//! - [`EventListenerLayer`](crate::worker::ext::event_listener) - Worker event emission and handling
//! - [`CircuitBreakerLayer`](crate::worker::ext::circuit_breaker) - Circuit breaker pattern for failure handling
//! - [`LongRunningLayer`](crate::worker::ext::long_running) - Support for tracking long-running tasks
//!
//! ### Extending with middleware
//!
//! You can write your own middleware to run code before or after a task is processed.
//!
//! <details>
//! <summary>Creating Custom Middleware</summary>
//!
//! Here's a simple example of a logging middleware layer:
//!
//! ```rust
//! use miju_core::task::Task;
//! use tower::{Layer, Service};
//! use std::task::{Context, Poll};
//!
//! // Define a logging service that wraps another service
//! pub struct LoggingService<S> {
//!     inner: S,
//! }
//!
//! impl<S, Args, Res, Err> Service<Task<Args>> for LoggingService<S>
//! where
//!     S: Service<Task<Args>, Response = Res, Error = Err>,
//!     Args: std::fmt::Debug,
//! {
//!     type Response = Res;
//!     type Error = Err;
//!     type Future = S::Future;
//!
//!     fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//!         self.inner.poll_ready(cx)
//!     }
//!
//!     fn call(&mut self, req: Task<Args>) -> Self::Future {
//!         println!("Processing task: {:?}", req.args);
//!         self.inner.call(req)
//!     }
//! }
//!
//! // Define a layer that wraps services with LoggingService
//! pub struct LoggingLayer;
//!
//! impl<S> Layer<S> for LoggingLayer {
//!     type Service = LoggingService<S>;
//!
//!     fn layer(&self, service: S) -> Self::Service {
//!         LoggingService { inner: service }
//!     }
//! }
//! ```
//! </details>
//!
//! If you want your middleware to do more than just intercept requests and responses, you can use extension traits. See the [`worker::ext`] module for examples.
//!
//! ## Error Handling
//!
//! Errors classify the result of a handler for lifecycle tracking:
//! - [`AbortError`](error::AbortError) marks a task as killed.
//! - [`RetryAfterError`](error::RetryAfterError) marks a task as pending and carries a retry delay.
//! - [`DeferredError`](error::DeferredError) marks a task as pending.
//!
//! Backends and middleware implement the corresponding rescheduling policy.
//! These error types do not themselves requeue tasks.
//!
//! ## Graceful Shutdown
//!
//! Workers track in-flight tasks and drain tracked work during shutdown.
//! A shared [`Shutdown`](monitor::shutdown::Shutdown) token coordinates workers.
//! A configured [`Monitor::with_terminator`](monitor::Monitor::with_terminator)
//! can end the drain early; provider integrations determine task recovery.
//!
//! # Serialization
//!
//! Enable `serde` to serialize a [`TaskBuilder`](task::builder::TaskBuilder),
//! [`ExecutionContext`](task::ExecutionContext), identifiers, states, metadata,
//! and recorded results. Process-local extensions are skipped. Deserialization
//! creates independent status and attempt cells; it does not reconnect observers
//! of an existing task. A [`Task`](task::Task) itself is not serializable.
//!
//! # Task identifiers
//!
//! [`GenerateId`](task::task_id::GenerateId) provides ID generation with
//! [`RandomId`](task::task_id::RandomId) or `u64`. Enable `uuid` or `ulid` for
//! the corresponding [`TaskId`](task::task_id::TaskId) variants and generators.
//! Constructing a task does not automatically assign an ID.
//!
//! `serde` adds serialization support for the enabled ID types without enabling
//! either ID feature. `TaskId` uses an untagged representation: UUID and ULID
//! strings deserialize as its `String` variant, while string parsing recognizes
//! the enabled ID formats.
//!
//! The default and `serde` configurations support Rust 1.85. Enabling `uuid` or
//! `ulid` requires Rust 1.89 because of the UUID dependency.
//!
//! # Tracing metadata
//!
//! With `tracing`, `task::metadata::TracingContext` stores explicit trace fields
//! under `miju_core.tracing.context`. The value is a semicolon-separated string
//! with decimal flags and `-` for absent fields. It does not capture the current
//! span or validate W3C trace IDs. Supply a subscriber separately for runtime logs.
//!
//! ```rust
//! # #[cfg(feature = "tracing")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use miju_core::task::{builder::TaskBuilder, metadata::TracingContext};
//!
//! let trace = TracingContext::new()
//!     .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
//!     .with_span_id("00f067aa0ba902b7")
//!     .with_trace_flags(1);
//! let task = TaskBuilder::new("send-email").metadata(&trace).build();
//! let restored = task.metadata().extract_as::<TracingContext>()?;
//! assert_eq!(restored.to_string(), trace.to_string());
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "tracing"))]
//! # fn main() {}
//! ```
//!
//! # Feature flags
//!
//! All features are opt-in. Enable `docsrs` when building documentation to
//! generate the feature reference from the package manifest.
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]
//!
//! # Development
//!
//! - [**Using CustomBackend**](crate::backend::custom) - using custom backend to integrate with already existing systems
//! - [**Extending Workers using extension traits**](crate::worker::ext#creating-a-custom-worker-extension-trait) - implementing custom worker functionality via extension traits

#[macro_use]
pub(crate) mod macros;

pub mod backend;
/// Includes internal error types.
pub mod error;
pub mod task;

/// Actively manage and observe workers.
pub mod monitor;
pub mod worker;

/// Layers for building middleware stacks
pub mod layers {
    pub use tower_layer::*;
    pub use tower_service::Service;
}
/// Timing and delaying utilities
#[cfg(feature = "sleep")]
pub mod timer {
    pub use futures_timer::Delay;
    /// Runtime agnostic sleep function based on [Delay]
    pub async fn sleep(duration: std::time::Duration) {
        futures_timer::Delay::new(duration).await;
    }
}
