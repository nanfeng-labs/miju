//! Task models and storage contracts for background processing in Rust.
//!
//! `miju-core` provides the task data structures and storage types used by miju.
//! Applications can construct tasks and carry execution state and metadata with them;
//! execution and persistence are supplied by integrations.
//!
//! # Core Concepts
//!
//! - **[`Tasks`](#tasks)**: Type-safe task data structures with processing metadata
//! - **[`Backends`](#backends)**: Payload conversion, queue names, and recorded results
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
//! The [`backend`] module provides [`Codec`](backend::codec::Codec),
//! [`Queue`](backend::queue::Queue), and [`TaskResult`](backend::TaskResult)
//! for representing task payloads, destinations, and execution results.
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

pub mod backend;
/// Includes internal error types.
pub mod error;
pub mod task;
