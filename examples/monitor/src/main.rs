//! Register worker factories, collect events, and bound graceful shutdown.

use std::{fmt, time::Duration};

use email_service::{Email, example_good_email};
use miju::{
    layers::tracing::{ContextualTaskSpan, OtelTraceContext, TraceLayer, TracingContext},
    prelude::*,
};
use miju_file_storage::JsonStorage;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct DeliveryRejected(String);

impl fmt::Display for DeliveryRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "simulated delivery rejected for {}", self.0)
    }
}

impl std::error::Error for DeliveryRejected {}

async fn deliver(email: Email, worker: WorkerContext) -> Result<(), BoxDynError> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!(
        worker = worker.name(),
        "Stopping after one simulated delivery"
    );
    worker.stop()?;
    // A handler error emits an event; it does not fail the worker future.
    Err(DeliveryRejected(email.to).into())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();

    // A fresh owned directory keeps each run independent and removes these
    // demonstration files on normal exit.
    let directory = tempfile::tempdir()?;
    let mut plain = JsonStorage::new(directory.path().join("plain.json"))?;
    let mut contextual = JsonStorage::new(directory.path().join("contextual.json"))?;
    plain.push(example_good_email()).await?;
    // This shows metadata wiring. Without an OpenTelemetry provider and
    // propagator the current context is empty; see the tracing example for both.
    let context = TracingContext::from(OtelTraceContext::current());
    contextual
        .push_task(
            TaskBuilder::new(example_good_email())
                .metadata(&context)
                .build(),
        )
        .await?;

    Monitor::new()
        .register(move |restarts| {
            tracing::info!(restarts, "Building plain worker");
            WorkerBuilder::new("plain")
                .backend(plain.clone())
                .enable_tracing()
                .build(deliver)
        })
        .register_n(5, move |index, restarts| {
            tracing::info!(index, restarts, "Building contextual worker");
            // Cloned file backends have separate read cursors. These workers
            // can see the same task; this is not a competing-consumer queue.
            WorkerBuilder::new(format!("contextual-{index}"))
                .backend(contextual.clone())
                .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
                .build(deliver)
        })
        .on_event(|worker, event| tracing::info!(worker = worker.name(), %event, "Worker event"))
        .should_restart(|worker, error| {
            // This predicate handles worker-level failures, not DeliveryRejected.
            // Match the indexed names and stop retrying after five restarts.
            worker.name().starts_with("contextual-")
                && error.to_string().contains("recoverable backend error")
                && worker.restarts() < 5
        })
        .shutdown_timeout(Duration::from_secs(5))
        .run_with_signal(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}
