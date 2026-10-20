//! Task models and storage contracts for background processing in Rust.
//!
//! `miju-core` provides the task model and storage contracts used by miju. It lets
//! applications describe work with typed arguments, carry execution state and
//! metadata alongside those arguments, and convert payloads for storage without
//! coupling the task model to a particular database or asynchronous runtime.
//!
//! This documentation covers the core APIs and their integration points. Application
//! code can access them through `miju`; libraries implementing task producers or
//! storage adapters can depend on `miju-core` directly.
//!
//! # Core Concepts
//!
//! The core API brings together two parts of a task system:
//!
//! - **[Tasks](#tasks)** describe the work to perform and the execution information
//!   that travels with it.
//! - **[Backends](#backends)** provide storage-facing contracts for representing
//!   payloads, selecting queues, and recording results.
//!
//! A producer constructs a task, and a storage integration uses its arguments and
//! context to prepare it for delivery. These types describe the work and its state;
//! running handlers and delivering queued tasks require an execution layer.
//!
//! ## Tasks
//!
//! A [`Task`](task::Task) is the unit of work exchanged between application code
//! and a backend. Its arguments retain their concrete Rust type, while execution
//! information is carried in a separate context. This lets payloads be transformed
//! without losing the identity or state associated with the work.
//!
//! ### Task Components
//!
//! - **Arguments** contain the application data needed to perform the work.
//! - **[Execution context](task::ExecutionContext)** carries identification, shared
//!   status and attempt tracking, and requested execution settings.
//! - **Attached data** uses [`Extensions`](task::extensions::Extensions) for
//!   process-local values and [`MetadataStore`](task::metadata::MetadataStore)
//!   for string metadata that an integration can persist with the task.
//!
//! The [`TaskBuilder`](task::builder::TaskBuilder) brings these parts together.
//! Queue selection, priority, scheduling, and attempt limits describe the desired
//! execution; a backend and worker decide how to honor those settings.
//!
//! ### Example: Preparing a Task
//!
//! ```
//! use miju_core::task::builder::TaskBuilder;
//!
//! #[derive(Debug)]
//! struct ReportRequest {
//!     customer_id: u64,
//! }
//!
//! let task = TaskBuilder::new(ReportRequest { customer_id: 42 })
//!     .queue("reports")
//!     .max_attempts(3)
//!     .build();
//!
//! assert_eq!(task.args.customer_id, 42);
//! assert_eq!(task.queue().map(AsRef::as_ref), Some("reports"));
//! ```
//!
//! Building a task does not enqueue it or assign an identifier automatically.
//! The task is ready for an integration to accept and store.
//!
//! ### Context and Metadata
//!
//! Observers of a task share its status and attempt counter. Task clones initially
//! share the context too; changing attached data or metadata uses copy-on-write.
//! This allows execution progress to remain visible while task-specific data can
//! be changed independently, according to each stored value's `Clone` behavior.
//!
//! Process-local data is accessed by Rust type. Persistable metadata uses string
//! keys and values, with typed extraction and injection rules supplied by the
//! application's [`Metadata`](task::metadata::Metadata) implementations.
//! Application resources stay in the process, while a storage adapter can carry
//! metadata across executions.
//!
//! See [`task`] for ownership and context behavior, and [`task::builder`] for the
//! available construction options.
//!
//! ## Backends
//!
//! Backend integrations connect the task model to a storage system. The
//! [`backend`] module supplies the data contracts for that connection, allowing
//! application arguments to stay independent of a database's representation.
//!
//! ### Storage Contracts
//!
//! - **Payload conversion** uses [`Codec`](backend::codec::Codec) to encode and
//!   decode arguments with a representation and error type chosen by the adapter.
//! - **Queue selection** uses [`Queue`](backend::queue::Queue) to carry a
//!   destination name for the backend to interpret.
//! - **Execution outcomes** use [`TaskResult`](backend::results::TaskResult) to
//!   associate a task and attempt with a recorded status and either an output
//!   value or a failure description.
//!
//! These contracts can be used to implement an adapter; the adapter is responsible
//! for persistence, task delivery, and writing execution results.
//!
//! ### Example: Defining a Storage Representation
//!
//! A codec can use any representation accepted by its storage system. This one
//! converts an integer payload to decimal text and reports invalid stored text as
//! a parse error.
//!
//! ```
//! use std::num::ParseIntError;
//! use miju_core::backend::codec::Codec;
//!
//! #[derive(Debug)]
//! struct DecimalCodec;
//!
//! impl Codec<u64> for DecimalCodec {
//!     type Compact = String;
//!     type Error = ParseIntError;
//!
//!     fn encode(&self, value: &u64) -> Result<String, Self::Error> {
//!         Ok(value.to_string())
//!     }
//!
//!     fn decode(&self, value: &String) -> Result<u64, Self::Error> {
//!         value.parse()
//!     }
//! }
//!
//! let codec = DecimalCodec;
//! let stored = codec.encode(&42)?;
//! assert_eq!(codec.decode(&stored)?, 42);
//! assert!(codec.decode(&"invalid".to_owned()).is_err());
//! # Ok::<(), ParseIntError>(())
//! ```
//!
//! See [`backend`] for the storage contracts and [`backend::codec`] for the
//! conversion interface. These types do not select a storage engine or runtime;
//! integrations supply those choices.

pub mod backend;
pub mod task;
