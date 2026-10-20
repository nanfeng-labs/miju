//! Tasks and the execution information carried with them.
//!
//! A [`Task`] combines application arguments with an [`ExecutionContext`]. The
//! arguments describe the work; the context holds its identity, execution
//! progress, attached data, and requested scheduling settings.
//!
//! # Overview
//!
//! Use [`Task::new`] for a task with default context or [`TaskBuilder`] to
//! configure it before passing it to an integration. With default context, a
//! task is pending, has zero attempts, and has no identifier or scheduling
//! preferences.
//!
//! Context accessors are available directly on a task through [`Deref`]. For
//! example, `task.status()` reads the same state as `task.ctx().status()`.
//!
//! ## Preparing a Task
//!
//! ```
//! use miju_core::task::{builder::TaskBuilder, status::Status};
//!
//! let task = TaskBuilder::new("generate invoice")
//!     .task_id(81)
//!     .queue("billing")
//!     .max_attempts(3)
//!     .build();
//!
//! assert_eq!(task.task_id().and_then(|id| id.as_int()), Some(81));
//! assert_eq!(task.status(), Status::Pending);
//! assert_eq!(task.attempt(), 0);
//! ```
//!
//! Queue, priority, time, and attempt limits are configuration values. Building
//! a task does not persist it or start processing; an integration decides how to
//! apply those settings.
//!
//! # Attached Data
//!
//! - [`Extensions`] holds process-local values, addressed by their Rust type.
//!   Use it for application resources or data that does not need a storage format.
//! - [`MetadataStore`] holds string pairs that can accompany a stored task.
//!   Implement [`Metadata`] to define the keys and validation for a typed value.
//!
//! Both stores belong to the execution context. See [`extensions`] for typed
//! storage operations and [`metadata`] for extraction and injection examples.
//!
//! # Sharing and Updating Context
//!
//! Cloning a task clones its arguments and initially shares the same context.
//! [`Task::inject_data`] and [`Task::inject_metadata`] detach that context when
//! it is shared. Metadata is copied, and extension values follow their own
//! [`Clone`] implementations. The status cell and attempt counter remain shared,
//! so observers still see progress after the attached data changes:
//!
//! ```
//! use miju_core::task::{builder::TaskBuilder, status::Status};
//!
//! let original = TaskBuilder::new("send digest").data(10_u32).build();
//! let mut copy = original.clone();
//! copy.inject_data(20_u32);
//! copy.raw_status().store(Status::Running);
//! copy.raw_attempt().increment();
//!
//! assert_eq!(original.data().get::<u32>(), Some(&10));
//! assert_eq!(copy.data().get::<u32>(), Some(&20));
//! assert_eq!(original.status(), Status::Running);
//! assert_eq!(original.attempt(), 1);
//! ```
//!
//! To reconfigure an existing task, use [`Task::into_builder`]. Explicitly
//! setting its status or attempt count replaces that shared cell; the
//! [`builder`] module demonstrates this distinction.
//!
//! # Converting Arguments
//!
//! [`Task::map_args`] and [`Task::try_map_args`] change the payload type while
//! carrying the original context handle into the new task. This lets an adapter
//! prepare a storage representation without rebuilding identity and execution
//! information. [`Task::take`] separates the payload and context when an
//! integration needs to handle them independently.

use std::{ops::Deref, sync::Arc};

use self::{
    attempt::Attempt,
    builder::TaskBuilder,
    extensions::Extensions,
    metadata::{Metadata, MetadataStore},
    status::{AtomicStatus, Status},
    task_id::TaskId,
};
use crate::backend::queue::Queue;

pub mod attempt;
pub mod builder;
pub mod extensions;
pub mod metadata;
pub mod status;
pub mod task_id;

/// A unit of work with typed arguments and a shared execution context.
///
/// `Args` is the application's payload type. Construction and payload mapping
/// do not require it to implement [`Clone`]; cloning a task does.
///
/// Context accessors are available through [`Deref`]. Clones initially share
/// that context, and data injection uses copy-on-write while preserving shared
/// status and attempt tracking. See the [module documentation](self) for examples.
#[derive(Clone, Debug)]
pub struct Task<Args> {
    /// Arguments passed to the task handler.
    pub args: Args,
    ctx: Arc<ExecutionContext>,
}

/// Identity, progress, and execution settings associated with a task.
///
/// The default context has [`Status::Pending`], zero attempts, empty data and
/// metadata stores, and no identifier, queue, time, priority, or attempt limit.
/// Use [`TaskBuilder`] to supply those optional values.
///
/// Cloning copies data using each stored value's `Clone` implementation, copies
/// metadata and scheduling fields, and shares the status and attempt cells.
/// A context clone is therefore not a snapshot of execution progress.
#[derive(Clone, Debug, Default)]
pub struct ExecutionContext {
    task_id: Option<TaskId>,
    data: Extensions,
    metadata: MetadataStore,
    attempt: Attempt,
    status: AtomicStatus,
    run_at: Option<u64>,
    max_attempts: Option<usize>,
    priority: Option<usize>,
    queue: Option<Queue>,
}

impl ExecutionContext {
    /// Returns the assigned identifier, if any.
    #[must_use]
    pub fn task_id(&self) -> Option<&TaskId> {
        self.task_id.as_ref()
    }

    /// Borrows process-local typed data.
    ///
    /// See [`Extensions`] for exact-type lookup and cloning behavior.
    #[must_use]
    pub fn data(&self) -> &Extensions {
        &self.data
    }

    /// Borrows persistable string metadata.
    ///
    /// Use [`Metadata::extract`] to interpret entries as an application type.
    #[must_use]
    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }

    /// Reads the current attempt count.
    #[must_use]
    pub fn attempt(&self) -> usize {
        self.attempt.current()
    }

    /// Borrows the shared attempt counter.
    ///
    /// Updates through this counter are visible to every context sharing it.
    #[must_use]
    pub fn raw_attempt(&self) -> &Attempt {
        &self.attempt
    }

    /// Reads the current status.
    #[must_use]
    pub fn status(&self) -> Status {
        self.status.load()
    }

    /// Borrows the shared status cell.
    ///
    /// Writing this cell updates observers; it does not execute or cancel a task.
    #[must_use]
    pub fn raw_status(&self) -> &AtomicStatus {
        &self.status
    }

    /// Returns the requested execution time in seconds since the Unix epoch.
    ///
    /// Returns `None` when no time was configured. The value is a request for an
    /// integration to interpret, not a timer managed by this context.
    #[must_use]
    pub fn run_at(&self) -> Option<u64> {
        self.run_at
    }

    /// Returns the requested attempt limit; its meaning is enforced by a worker.
    ///
    /// `None` means no limit was configured here. `Some(0)` is retained as an
    /// explicit value, without assigning it a special meaning.
    #[must_use]
    pub fn max_attempts(&self) -> Option<usize> {
        self.max_attempts
    }

    /// Returns the priority value for the backend to interpret.
    ///
    /// Returns `None` when unspecified. This type does not define whether larger
    /// or smaller values take precedence.
    #[must_use]
    pub fn priority(&self) -> Option<usize> {
        self.priority
    }

    /// Borrows the requested queue name.
    ///
    /// Returns `None` when no destination was selected.
    #[must_use]
    pub fn queue(&self) -> Option<&Queue> {
        self.queue.as_ref()
    }
}

impl<Args> Task<Args> {
    /// Creates a pending task without assigning an identifier.
    ///
    /// The context starts with zero attempts and no scheduling preferences.
    /// Use [`TaskBuilder`] when the task needs additional configuration.
    ///
    /// # Example
    ///
    /// ```
    /// use miju_core::task::{Task, status::Status};
    ///
    /// let task = Task::new("rebuild summary");
    /// assert_eq!(task.args, "rebuild summary");
    /// assert_eq!(task.status(), Status::Pending);
    /// assert!(task.task_id().is_none());
    /// ```
    #[must_use]
    pub fn new(args: Args) -> Self {
        Self::new_with_ctx(args, Arc::new(ExecutionContext::default()))
    }

    /// Attaches a payload to an existing context without copying the context.
    ///
    /// Use this when reconstructing a task from [`take`](Self::take) or when an
    /// integration already owns the context. The supplied context is retained
    /// as-is, including its identifier and shared progress cells.
    #[must_use]
    pub fn new_with_ctx(args: Args, ctx: Arc<ExecutionContext>) -> Self {
        Self { args, ctx }
    }

    /// Borrows the shared context handle.
    ///
    /// Cloning this handle keeps observing the current context. A later
    /// copy-on-write data update may give the task a different context handle;
    /// see the [module documentation](self#sharing-and-updating-context).
    #[must_use]
    pub fn ctx(&self) -> &Arc<ExecutionContext> {
        &self.ctx
    }

    /// Consumes the task and returns its payload and context.
    #[must_use]
    pub fn take(self) -> (Args, Arc<ExecutionContext>) {
        (self.args, self.ctx)
    }

    /// Converts the payload while preserving the exact context handle.
    ///
    /// The mapping consumes the arguments, so the payload need not be cloneable.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use miju_core::task::builder::TaskBuilder;
    ///
    /// let task = TaskBuilder::new(81_u64).task_id(12).build();
    /// let context = Arc::clone(task.ctx());
    /// let encoded = task.map_args(|value| value.to_string());
    ///
    /// assert_eq!(encoded.args, "81");
    /// assert!(Arc::ptr_eq(encoded.ctx(), &context));
    /// ```
    #[must_use]
    pub fn map_args<T>(self, map: impl FnOnce(Args) -> T) -> Task<T> {
        Task::new_with_ctx(map(self.args), self.ctx)
    }

    /// Converts the payload while preserving its context on success.
    ///
    /// # Errors
    ///
    /// Returns the conversion error, consuming the original task in either case.
    ///
    /// # Example
    ///
    /// ```
    /// use miju_core::task::Task;
    ///
    /// let parsed = Task::new("81").try_map_args(str::parse::<u64>)?;
    /// assert_eq!(parsed.args, 81);
    /// assert!(Task::new("invalid").try_map_args(str::parse::<u64>).is_err());
    /// # Ok::<(), std::num::ParseIntError>(())
    /// ```
    pub fn try_map_args<T, E>(self, map: impl FnOnce(Args) -> Result<T, E>) -> Result<Task<T>, E> {
        Ok(Task::new_with_ctx(map(self.args)?, self.ctx))
    }

    /// Opens the task for configuration, cloning its context only if shared.
    ///
    /// Existing configuration is retained. Status and attempts remain shared
    /// until explicitly replaced through [`TaskBuilder::status`] or
    /// [`TaskBuilder::attempt`].
    pub fn into_builder(self) -> TaskBuilder<Args> {
        TaskBuilder {
            args: self.args,
            ctx: Arc::unwrap_or_clone(self.ctx),
        }
    }

    /// Inserts process-local data and returns any previous value of its type.
    ///
    /// If the context is shared, its data is cloned before insertion. Other task
    /// clones retain their entries; stored handles may still share resources
    /// according to their own [`Clone`] implementations. See [`Extensions`].
    pub fn inject_data<T: Clone + Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        Arc::make_mut(&mut self.ctx).data.insert(value)
    }

    /// Adds typed metadata, committing the changes only if injection succeeds.
    ///
    /// # Errors
    ///
    /// Returns the injection error while leaving the stored metadata and context
    /// handle unchanged. Even an implementation that writes some entries before
    /// returning an error cannot publish those entries through this method.
    /// See [`Metadata`] for an implementation example.
    pub fn inject_metadata<M: Metadata>(&mut self, value: &M) -> Result<(), M::Error> {
        let mut metadata = self.ctx.metadata.clone();
        value.inject(&mut metadata)?;
        // Detach the context only after all metadata writes have succeeded.
        Arc::make_mut(&mut self.ctx).metadata = metadata;
        Ok(())
    }
}

impl<Args> Deref for Task<Args> {
    type Target = ExecutionContext;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}
