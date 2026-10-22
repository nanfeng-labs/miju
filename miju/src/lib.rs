#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
//! ## Feature flags
//!
//! Enable `docsrs` when building documentation to generate the feature reference.
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]

/// Middleware built on Tower's [`Layer`](prelude::Layer) trait.
pub mod layers;

/// Worker configuration utilities allowing easier worker decoration.
#[cfg(feature = "config")]
#[cfg_attr(docsrs, doc(cfg(feature = "config")))]
pub mod config;

pub use miju_core::task::Task;
pub use miju_core::{backend, error, task};

/// A "prelude" of common imports
pub mod prelude {
    pub use crate::layers::WorkerBuilderExt;
    #[cfg(feature = "retry")]
    pub use crate::layers::retry::{
        BackoffRetryPolicy, FromTaskConfigPolicy, RetryIfPolicy, RetryPolicy,
    };
    #[cfg(feature = "task-context")]
    pub use miju_core::task::context::{SubTaskFuture, TaskContext, WaitForExecutionFuture};

    #[cfg(feature = "shared")]
    pub use miju_core::backend::ext::shared::Shared;

    pub use miju_core::{
        backend::{
            Backend, Expose, FetchById, Filter, ListAllTasks, ListQueues, ListTasks, ListWorkers,
            Metrics, QueueInfo, RegisterWorker, Reschedule, ResumeAbandoned, ResumeById,
            RunningWorker, StatType, Statistic, TaskResult, TaskSink, TaskSinkError, Update,
            WaitForCompletion, ext::BackendExt, ext::PollNextArgsError,
        },
        backend::{
            codec::*, custom::*, ext::pipe::*, ext::poll_strategy::*, factory::*, memory::*,
        },
        error::*,
        layers::*,
        monitor::{
            ExitError, Monitor, MonitorError, MonitoredWorkerError, context::MonitorContext,
            shutdown::Shutdown,
        },
        task::{
            ExecutionContext, Task,
            attempt::Attempt,
            builder::TaskBuilder,
            data::{AddExtension, Data, MissingDataError},
            extensions::Extensions,
            from_request::FromRequest,
            into_response::IntoResponse,
            metadata::{Meta, Metadata, MetadataError, MetadataStore},
            status::Status,
            task_fn::TaskFn,
            task_fn::task_fn,
            task_id::{RandomId, TaskId, TaskIdError},
        },
        worker::builder::*,
        worker::ext::{
            ack::*, circuit_breaker::*, event_listener::*, long_running::*, parallelize::*,
        },
        worker::{
            Worker, context::WorkerContext, event::Event, lifecycle::*, service::IntoWorkerService,
            service::WorkerService,
        },
    };
}
