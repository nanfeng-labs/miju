//! Report task failures and performance transactions through Sentry.
//!
//! Enable `sentry` to use [`SentryLayer`]. This feature enables UUID task-ID
//! conversion and requires Rust 1.89; builds without it retain their existing
//! compiler requirements. The application initializes its Sentry SDK client,
//! transport, sampling, and shutdown handling. The library depends on the core
//! instrumentation API with default features disabled, so enabling this layer
//! alone does not enable a client or send events.
//!
//! # Adding the layer
//!
//! ```rust
//! use miju::{layers::sentry::SentryLayer, prelude::*};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), BoxDynError> {
//! let mut backend = MemoryStorage::new();
//! backend.push(7usize).await?;
//! WorkerBuilder::new("reported-worker")
//!     .backend(backend)
//!     .layer(SentryLayer::new())
//!     .on_event(|worker, event| {
//!         if matches!(event, Event::Success(_)) {
//!             worker.stop().unwrap();
//!         }
//!     })
//!     .build(|value: usize| async move { value * 2 })
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Direct Tower services must return a Future whose error is [`BoxDynError`].
//! Convert a concrete error in the inner service or use Tower's error mapping.
//! Readiness errors pass through as boxed errors. Errors from the task Future
//! are wrapped in [`SentryTaskError::Inner`]; the response value is unchanged.
//!
//! ```compile_fail
//! use miju::{layers::sentry::SentryLayer, prelude::*};
//! use tower::{ServiceExt, service_fn};
//! let service = SentryLayer::new().layer(service_fn(|_: Task<()>| async {
//!     Err::<(), std::io::Error>(std::io::Error::other("failed"))
//! }));
//! let _future = service.oneshot(Task::new(()));
//! ```
//!
//! # Task details and completion
//!
//! Calling the service requires a task ID and converts it with
//! [`TaskId::to_uuid`](miju_core::task::task_id::TaskId::to_uuid). Integer and
//! string IDs receive deterministic UUIDs; existing UUID IDs are retained.
//! The attempt count is read at call time and cast to `i32` without clamping.
//!
//! On the first poll, the layer sets the `queue` tag to the Rust argument type,
//! adds a `task` context containing `task_id` and `current_attempt`, and installs
//! an event processor that assigns that UUID to events. It starts a fresh
//! transaction named after the argument type with operation `miju.task`.
//! It does not restore distributed trace metadata or inherit the previous span
//! as a parent. Transaction timing starts at the first poll, after the inner
//! service's synchronous call.
//!
//! On completion, an unset status becomes `Ok` or `InternalError`. A returned
//! error is captured only while setting that previously unset status. An
//! explicitly set status is preserved and skips this automatic capture. The
//! transaction finishes and the previous scope span is restored. Readiness
//! errors do not start transactions.
//!
//! # Scope isolation and cancellation
//!
//! The layer uses the current hub and does not create a scope per task. Tags,
//! context and event processors remain on that scope after completion. Bind a
//! separate hub to each task Future, for example with an outer `NewSentryLayer`
//! from `sentry-tower`, or with [`SentryFutureExt::bind_hub`] and an
//! application-created hub. Merely binding one shared hub to an entire worker
//! does not isolate concurrently running tasks.
//!
//! Dropping an unpolled Future starts no transaction. Dropping it after polling,
//! or unwinding through it, does not finish its transaction or restore its scope
//! span. Isolating the task hub contains scope changes but does not finish a
//! cancelled transaction. Put timeout or panic-capture middleware inside this
//! layer when their returned errors should complete the transaction. Retry
//! placement similarly controls whether each attempt gets its own transaction.
//!
//! [`SentryFutureExt::bind_hub`]: sentry_core::SentryFutureExt::bind_hub

use miju_core::error::BoxDynError;
use sentry_core::protocol;
use std::fmt::{self, Debug};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

use miju_core::task::Task;

/// Sentry integration Layer.
///
/// The service starts a fresh transaction on the first poll and reports task
/// details through the application's current hub. See the [module documentation](self)
/// for client setup, error conversion, scope isolation, and completion boundaries.
#[derive(Clone, Default, Debug)]
pub struct SentryLayer;

impl SentryLayer {
    /// Creates a layer for task details, errors, and performance transactions.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Task Service for Sentry integration.
///
/// The inner Future must return [`BoxDynError`]. Calling the service without a
/// task ID panics before calling the inner service. Transaction names and queue
/// tags use the Rust task argument type.
#[derive(Clone, Debug)]
pub struct SentryTaskService<S> {
    service: S,
}

impl<S> Layer<S> for SentryLayer {
    type Service = SentryTaskService<S>;

    fn layer(&self, service: S) -> Self::Service {
        Self::Service { service }
    }
}

struct Request {
    id: uuid::Uuid,
    current_attempt: i32,
    queue: String,
}

/// The Future returned from [`SentryTaskService`].
///
/// Starts instrumentation on the first poll and finishes it on completion.
/// Dropping a pending Future does not finish or restore its transaction.
#[pin_project::pin_project]
pub struct SentryHttpFuture<F> {
    on_first_poll: Option<(Request, sentry_core::TransactionContext)>,
    transaction: Option<(
        sentry_core::TransactionOrSpan,
        Option<sentry_core::TransactionOrSpan>,
    )>,
    #[pin]
    future: F,
}

/// Error type for Sentry task processing.
///
/// Returned inside [`BoxDynError`] when the inner task Future fails. Readiness
/// errors are boxed directly without this wrapper.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SentryTaskError {
    /// Error occurred in the inner service.
    #[error("Inner service error: {0}")]
    Inner(BoxDynError),
}

impl<F> fmt::Debug for SentryHttpFuture<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SentryHttpFuture")
            .field("on_first_poll", &"<Request, TransactionContext>")
            .field(
                "transaction",
                &self.transaction.as_ref().map(|(_, maybe_span)| {
                    let has_child = maybe_span.is_some();
                    format!(
                        "<TransactionOrSpan, child: {}>",
                        if has_child { "Some" } else { "None" }
                    )
                }),
            )
            .field("future", &self.future)
            .finish()
    }
}

impl<F, Res, Err> Future for SentryHttpFuture<F>
where
    F: Future<Output = Result<Res, Err>> + 'static,
    Err: Into<BoxDynError> + 'static,
{
    type Output = Result<Res, BoxDynError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Some((task_details, trx_ctx)) = this.on_first_poll.take() {
            let tid = task_details.id;
            sentry_core::configure_scope(|scope| {
                scope.add_event_processor(move |mut event| {
                    event.event_id = tid;
                    Some(event)
                });
                scope.set_tag("queue", task_details.queue.clone());
                let mut details = std::collections::BTreeMap::new();
                details.insert(String::from("task_id"), task_details.id.to_string().into());
                details.insert(
                    String::from("current_attempt"),
                    task_details.current_attempt.into(),
                );
                scope.set_context("task", sentry_core::protocol::Context::Other(details));

                let transaction: sentry_core::TransactionOrSpan =
                    sentry_core::start_transaction(trx_ctx).into();
                let parent_span = scope.get_span();
                scope.set_span(Some(transaction.clone()));
                *this.transaction = Some((transaction, parent_span));
            });
        }
        match this
            .future
            .poll(cx)
            .map_err(|e| SentryTaskError::Inner(e.into()))
        {
            Poll::Ready(res) => {
                if let Some((transaction, parent_span)) = this.transaction.take() {
                    if transaction.get_status().is_none() {
                        let status = match &res {
                            Ok(_) => protocol::SpanStatus::Ok,
                            Err(err) => {
                                sentry_core::capture_error(&err);
                                protocol::SpanStatus::InternalError
                            }
                        };
                        transaction.set_status(status);
                    }
                    transaction.finish();
                    sentry_core::configure_scope(|scope| scope.set_span(parent_span));
                }
                Poll::Ready(Ok(res?))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Svc, Args, Fut, Res, Err> Service<Task<Args>> for SentryTaskService<Svc>
where
    Svc: Service<Task<Args>, Response = Res, Error = Err, Future = Fut>,
    Fut: Future<Output = Result<Res, BoxDynError>> + 'static,
    Err: Into<BoxDynError> + 'static,
{
    type Response = Svc::Response;
    type Error = BoxDynError;
    type Future = SentryHttpFuture<Svc::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        let task_type = std::any::type_name::<Args>().to_owned();
        let attempt = task.attempt() as i32;
        let task_id = task.task_id().expect("Task ID is missing").to_uuid();
        let trx_ctx =
            sentry_core::TransactionContext::new(std::any::type_name::<Args>(), "miju.task");

        let task_details = Request {
            id: task_id,
            current_attempt: attempt,
            queue: task_type,
        };

        SentryHttpFuture {
            on_first_poll: Some((task_details, trx_ctx)),
            transaction: None,
            future: self.service.call(task),
        }
    }
}
