//! Span factories for task instrumentation.
//!
//! [`MakeSpan`] creates a span from a task. [`DefaultMakeSpan`] and
//! [`ContextualTaskSpan`] record its ID and attempt count under the current span.
//! A fixed [`tracing::Span`] or a closure can also implement the factory role.
//! Applications configure their own tracing subscriber to collect spans.
//!
//! # Instrumenting a task
//!
//! ```rust
//! use miju::{layers::tracing::{DefaultMakeSpan, MakeSpan}, prelude::*};
//! use tracing::Instrument;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let task = TaskBuilder::new(7usize)
//!     .task_id("example-task".to_owned().into())
//!     .build();
//! let span = DefaultMakeSpan::new().make_span(&task);
//! let result = async move { task.args * 2 }.instrument(span).await;
//! assert_eq!(result, 14);
//! # }
//! ```
//!
//! A custom factory can select different fields or work with tasks that do not
//! yet have IDs:
//!
//! ```rust
//! use miju::{layers::tracing::MakeSpan, Task};
//!
//! let mut make_span = |task: &Task<usize>| {
//!     tracing::info_span!("calculate", amount = task.args)
//! };
//! let task = Task::new(7usize);
//! let span = make_span.make_span(&task);
//! span.in_scope(|| tracing::info!("processing task"));
//! ```
//!
//! # Context and attempt fields
//!
//! [`TracingContext`] is the core metadata type. Storing it on a task does not
//! configure a subscriber or restore a remote parent; the span factories use
//! the currently active span. They read the attempt count when called and do
//! not advance it. Both built-in factories require a task ID even when tracing
//! is disabled by the subscriber.

mod contextual_span;
mod make_span;

pub use self::{
    contextual_span::ContextualTaskSpan,
    make_span::{DefaultMakeSpan, MakeSpan},
};
pub use miju_core::task::metadata::TracingContext;
pub use tracing::{debug, debug_span, error, error_span, info, info_span, trace, trace_span};

const DEFAULT_MESSAGE_LEVEL: tracing::Level = tracing::Level::DEBUG;
