//! An application entry point for miju's background task APIs.
//!
//! Use this crate to describe work with typed arguments, attach application data,
//! and configure the execution information that a storage integration will use.
//! It exposes [`Task`] directly and groups the remaining APIs under [`task`] and
//! [`backend`].
//!
//! # Usage
//!
//! Start with a payload describing the work, then use [`TaskBuilder`] to choose
//! a queue and attach any context needed by the application:
//!
//! ```
//! use miju::{Task, task::{builder::TaskBuilder, status::Status}};
//!
//! #[derive(Debug)]
//! struct RefreshIndex {
//!     collection: String,
//! }
//!
//! let task: Task<RefreshIndex> = TaskBuilder::new(RefreshIndex {
//!     collection: "catalog".to_owned(),
//! })
//!     .queue("maintenance".into())
//!     .max_attempts(3)
//!     .build();
//!
//! assert_eq!(task.args.collection, "catalog");
//! assert_eq!(task.status(), Status::Pending);
//! assert!(task.task_id().is_none());
//! assert_eq!(task.queue().map(AsRef::as_ref), Some("maintenance"));
//! ```
//!
//! The resulting value is ready to pass to an integration. Constructing it does
//! not enqueue or execute work, allocate an identifier, or enforce scheduling
//! settings; those operations belong to the execution and storage layers.
//!
//! # Task Data
//!
//! A task separates the payload in [`Task::args`] from its [`ExecutionContext`].
//! The context carries the task's identity, progress, and requested execution
//! settings, and provides two ways to attach application information:
//!
//! - [`Extensions`] stores process-local values by their concrete Rust type.
//! - [`MetadataStore`] stores string pairs; implement [`Metadata`] to read and
//!   write them as an application-defined type.
//!
//! Task clones share status and attempt tracking. Changes to attached data use
//! copy-on-write; see the [`task`] module for examples of that behavior.
//!
//! # Storage Integration
//!
//! The [`backend`] module provides [`Codec`] for payload conversion, [`Queue`]
//! for destination names, and [`TaskResult`] for recorded outcomes. A storage
//! integration combines these contracts with its own persistence and delivery
//! mechanisms.
//!
//! Libraries that only need these foundational types can depend on `miju-core`
//! directly. The types re-exported here are the same types, so values can cross
//! between application code and such libraries without conversion.
//!
//! [`TaskBuilder`]: task::builder::TaskBuilder
//! [`ExecutionContext`]: task::ExecutionContext
//! [`Extensions`]: task::extensions::Extensions
//! [`MetadataStore`]: task::metadata::MetadataStore
//! [`Metadata`]: task::metadata::Metadata
//! [`Codec`]: backend::codec::Codec
//! [`Queue`]: backend::queue::Queue
//! [`TaskResult`]: backend::TaskResult

pub use miju_core::task::Task;
pub use miju_core::{backend, task};
