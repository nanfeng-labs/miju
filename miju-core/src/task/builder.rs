//! Configuring task arguments and execution context.
//!
//! [`TaskBuilder`] collects a payload and its execution settings before creating
//! a [`Task`]. Start with [`TaskBuilder::new`], apply the options needed by an
//! integration, and call [`build`](TaskBuilder::build) to obtain the task.
//!
//! # Configuration
//!
//! A builder can:
//!
//! - Set an identifier, destination queue, priority, and requested execution time.
//! - Attach process-local data and typed string metadata.
//! - Supply a recorded status, attempt count, and requested attempt limit.
//! - Convert the argument type while retaining the configured context.
//!
//! Unspecified fields keep the [`ExecutionContext`] defaults. The builder records
//! settings for an integration to apply; it does not allocate IDs, enqueue work,
//! or enforce scheduling and retry policies.
//!
//! # Example
//!
//! ```
//! use miju_core::task::builder::TaskBuilder;
//!
//! let task = TaskBuilder::new("archive report")
//!     .queue("archives")
//!     .run_at_timestamp(1_800_000_000)
//!     .max_attempts(4)
//!     .data(vec!["monthly", "finance"])
//!     .build();
//!
//! assert_eq!(task.run_at(), Some(1_800_000_000));
//! assert_eq!(task.max_attempts(), Some(4));
//! assert_eq!(task.data().get::<Vec<&str>>().unwrap().len(), 2);
//! ```
//!
//! # Reconfiguring an Existing Task
//!
//! [`Task::into_builder`] retains the existing configuration, cloning the context
//! if it is shared. Status and attempts continue to share their underlying cells
//! until explicitly set on the builder. Setting either creates an independent
//! cell, leaving existing observers unchanged:
//!
//! ```
//! use miju_core::task::{builder::TaskBuilder, status::Status};
//!
//! let original = TaskBuilder::new("archive report")
//!     .attempt(2)
//!     .status(Status::Failed)
//!     .build();
//! let updated = original.clone().into_builder().priority(5).build();
//! updated.raw_attempt().increment();
//! assert_eq!(original.attempt(), 3);
//!
//! let reset = updated.into_builder()
//!     .attempt(0)
//!     .status(Status::Pending)
//!     .build();
//! assert_eq!(reset.attempt(), 0);
//! assert_eq!(reset.status(), Status::Pending);
//! assert_eq!(original.attempt(), 3);
//! assert_eq!(original.status(), Status::Failed);
//! ```

use std::sync::Arc;

use super::{
    ExecutionContext, Task, attempt::Attempt, metadata::Metadata, status::AtomicStatus,
    status::Status, task_id::TaskId,
};
use crate::backend::queue::Queue;

/// A builder for a task's payload, attached data, and execution settings.
///
/// `Args` may be any payload type; construction does not require [`Clone`].
/// Builder methods consume and return the builder, and [`build`](Self::build)
/// completes construction. See the [module documentation](self) for a full
/// example and the behavior of rebuilding a shared task.
#[derive(Debug)]
#[must_use = "build the configured task to use it"]
pub struct TaskBuilder<Args> {
    pub(super) args: Args,
    pub(super) ctx: ExecutionContext,
}

impl<Args> TaskBuilder<Args> {
    /// Starts with a fresh context and the supplied payload.
    ///
    /// The task starts pending with zero attempts. Optional settings, including
    /// its identifier and queue, remain unset until supplied by the caller.
    pub fn new(args: Args) -> Self {
        Self {
            args,
            ctx: ExecutionContext::default(),
        }
    }

    /// Sets an explicit task identifier.
    ///
    /// This stores the provided ID without generating or checking uniqueness.
    /// See [`TaskId`] for the distinction between integer and string identifiers.
    pub fn task_id(mut self, id: impl Into<TaskId>) -> Self {
        self.ctx.task_id = Some(id.into());
        self
    }

    /// Inserts data, replacing any previous value of the same type.
    ///
    /// Each distinct Rust type has its own entry. Use separate wrapper types
    /// when multiple values have the same underlying representation; see
    /// [`Extensions`](super::extensions::Extensions).
    pub fn data<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.ctx.data.insert(value);
        self
    }

    /// Writes typed metadata into this builder.
    ///
    /// # Errors
    ///
    /// Returns the injection error and consumes the builder on failure.
    /// For an update that retains an existing task on failure, use
    /// [`Task::inject_metadata`].
    pub fn metadata<M: Metadata>(mut self, value: &M) -> Result<Self, M::Error> {
        value.inject(&mut self.ctx.metadata)?;
        Ok(self)
    }

    /// Replaces the attempt counter with a new cell, leaving existing observers alone.
    ///
    /// The count is stored verbatim. It is not checked against
    /// [`max_attempts`](Self::max_attempts).
    pub fn attempt(mut self, count: usize) -> Self {
        self.ctx.attempt = Attempt::new_with_value(count);
        self
    }

    /// Replaces the status with a new cell, leaving existing observers alone.
    ///
    /// No lifecycle transition is validated, and no execution is triggered.
    pub fn status(mut self, status: Status) -> Self {
        self.ctx.status = AtomicStatus::new(status);
        self
    }

    /// Records an attempt limit, including zero; enforcement belongs to the worker.
    ///
    /// Calling this method always stores `Some(count)`. The builder does not
    /// interpret zero as unlimited or compare the value with the current count.
    pub fn max_attempts(mut self, count: usize) -> Self {
        self.ctx.max_attempts = Some(count);
        self
    }

    /// Records a priority value for the backend to interpret.
    ///
    /// The builder does not define which end of the numeric range runs first.
    pub fn priority(mut self, value: usize) -> Self {
        self.ctx.priority = Some(value);
        self
    }

    /// Selects a queue name.
    ///
    /// Names are stored without normalization or validation; see [`Queue`].
    pub fn queue(mut self, queue: impl Into<Queue>) -> Self {
        self.ctx.queue = Some(queue.into());
        self
    }

    /// Records an execution time in seconds since the Unix epoch.
    ///
    /// This is an absolute timestamp, not a delay. Past timestamps are accepted,
    /// and the builder does not wait until the requested time.
    pub fn run_at_timestamp(mut self, timestamp: u64) -> Self {
        self.ctx.run_at = Some(timestamp);
        self
    }

    /// Converts the payload while retaining all configured context.
    ///
    /// The mapping receives ownership of the arguments and may return a new
    /// type, for example an encoded representation for storage.
    pub fn map_args<T>(self, map: impl FnOnce(Args) -> T) -> TaskBuilder<T> {
        TaskBuilder {
            args: map(self.args),
            ctx: self.ctx,
        }
    }

    /// Converts the payload while retaining all configured context on success.
    ///
    /// # Errors
    ///
    /// Returns the conversion error and consumes the builder on failure.
    pub fn try_map_args<T, E>(
        self,
        map: impl FnOnce(Args) -> Result<T, E>,
    ) -> Result<TaskBuilder<T>, E> {
        Ok(TaskBuilder {
            args: map(self.args)?,
            ctx: self.ctx,
        })
    }

    /// Produces a task with the configured context.
    ///
    /// The returned task owns a new context handle. Any status or attempt cells
    /// retained from an existing task still share updates with their observers.
    #[must_use]
    pub fn build(self) -> Task<Args> {
        Task::new_with_ctx(self.args, Arc::new(self.ctx))
    }
}
