//! Payload conversion, queue names, and outcomes for storage integrations.
//!
//! These contracts connect the typed [`Task`](crate::task::Task) model to a
//! storage representation. They let an integration choose its own format and
//! destination naming while keeping application arguments independent of those
//! choices.
//!
//! # Overview
//!
//! - [`Codec`](codec::Codec) converts between a Rust value and the representation
//!   accepted by a store. The implementation defines its format and errors.
//! - [`Queue`](queue::Queue) carries a destination name without interpreting it
//!   or creating a queue in a storage system.
//! - [`TaskResult`](results::TaskResult) records one attempt's identity, status,
//!   and output or failure description.
//!
//! # Integrating with the Task Model
//!
//! A producer can select a queue through
//! [`TaskBuilder`](crate::task::builder::TaskBuilder). An integration then reads
//! the task's arguments and context, encodes the payload, and stores the data
//! needed to reconstruct it. [`Task::map_args`](crate::task::Task::map_args) and
//! [`Task::try_map_args`](crate::task::Task::try_map_args) preserve the context
//! when the payload representation changes.
//!
//! After processing, the integration can associate an output or error with an
//! attempt using `TaskResult`. Recording that value does not update a task's
//! shared status. Persistence, task delivery, scheduling, and consistency between
//! recorded outcomes and live state remain the integration's responsibility.
//!
//! See [`codec`] for an encoding example and [`results`] for a recorded outcome.

pub mod codec;
pub mod queue;
pub mod results;
