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
//! With `sleep`, `backend::persistence` provides the `Persistence` provider
//! contract and the `Persisted` driver for registration, heartbeats, task
//! batches, and lifecycle events. Storage integrations bridge that driver to
//! `Backend` and configure its event layer; the driver itself is not a backend
//! implementation. Providers supply storage operations and atomic task claims.
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
//! requeuing tasks. Backend exhaustion alone does not stop the worker context.
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
