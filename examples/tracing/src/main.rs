//! Compare worker-local traces with context captured by a task producer.

use std::{fmt, time::Duration};

use email_service::{Email, example_good_email};
use miju::{
    layers::tracing::{ContextualTaskSpan, OtelTraceContext, TraceLayer, TracingContext},
    prelude::*,
};
use opentelemetry::{
    global,
    trace::{TraceContextExt, TracerProvider},
};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
use tracing::{Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::prelude::*;

#[derive(Debug)]
struct DeliveryRejected(String);

impl fmt::Display for DeliveryRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "simulated address lookup failed for {}", self.0)
    }
}

impl std::error::Error for DeliveryRejected {}

fn show_context(phase: &str) {
    let context = Span::current().context();
    let span = context.span();
    let span_context = span.span_context();
    tracing::info!(phase, trace_id = %span_context.trace_id(), span_id = %span_context.span_id(), "Trace context");
}

async fn deliver(email: Email, worker: WorkerContext) -> Result<(), BoxDynError> {
    show_context(&email.subject);
    tokio::time::sleep(Duration::from_secs(1)).await;
    worker.stop()?;
    Err(DeliveryRejected(email.to).into())
}

#[tracing::instrument(skip(storage))]
async fn produce_plain(storage: &mut MemoryStorage<Email>) -> Result<(), BoxDynError> {
    show_context("plain-producer");
    storage
        .push(Email {
            subject: "plain-consumer".to_owned(),
            ..example_good_email()
        })
        .await?;
    Ok(())
}

#[tracing::instrument(skip(storage))]
async fn produce_contextual(storage: &mut MemoryStorage<Email>) -> Result<(), BoxDynError> {
    show_context("contextual-producer");
    // Capture while the producer span is active, before handing work to the queue.
    let metadata = TracingContext::from(OtelTraceContext::current());
    let task = TaskBuilder::new(Email {
        subject: "contextual-consumer".to_owned(),
        ..example_good_email()
    })
    .metadata(&metadata)
    .build();
    storage.push_task(task).await?;
    Ok(())
}

async fn run_workers() -> Result<(), BoxDynError> {
    let mut plain = MemoryStorage::new().instrumented(tracing::info_span!("plain-backend"));
    produce_plain(&mut plain).await?;
    let mut contextual =
        MemoryStorage::new().instrumented(tracing::info_span!("contextual-backend"));
    produce_contextual(&mut contextual).await?;

    // Instrumenting a backend or worker adds local spans. It does not carry a
    // producer's context across the queue by itself.
    let plain_worker = WorkerBuilder::new("plain")
        .backend(plain)
        .enable_tracing()
        .build(deliver)
        .run()
        .instrument(tracing::info_span!("plain-worker"));
    let contextual_worker = WorkerBuilder::new("contextual")
        .backend(contextual)
        .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
        .build(deliver)
        .run()
        .instrument(tracing::info_span!("contextual-worker"));
    tokio::try_join!(plain_worker, contextual_worker)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let provider = SdkTracerProvider::builder().build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(provider.clone());
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("tracing-example")))
        .init();
    // No remote exporter is configured: inspect the trace and span IDs in stdout.
    let result = run_workers().await;
    provider.shutdown()?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use tracing::instrument::WithSubscriber;

    #[tokio::test]
    async fn queued_metadata_restores_the_producer_as_parent() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_text_map_propagator(TraceContextPropagator::new());
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("propagation-test")));
        run_workers().with_subscriber(subscriber).await.unwrap();
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let producer = spans
            .iter()
            .find(|span| span.name == "produce_contextual")
            .unwrap();
        let plain_worker = spans
            .iter()
            .find(|span| span.name == "plain-worker")
            .unwrap();
        let plain_producer = spans
            .iter()
            .find(|span| span.name == "produce_plain")
            .unwrap();
        let tasks: Vec<_> = spans.iter().filter(|span| span.name == "task").collect();
        assert_eq!(tasks.len(), 2);
        let contextual = tasks
            .iter()
            .find(|span| span.parent_span_id == producer.span_context.span_id())
            .unwrap();
        assert_eq!(
            contextual.span_context.trace_id(),
            producer.span_context.trace_id()
        );
        let plain = tasks
            .iter()
            .find(|span| span.parent_span_id == plain_worker.span_context.span_id())
            .unwrap();
        assert_ne!(
            plain.span_context.trace_id(),
            plain_producer.span_context.trace_id()
        );
        provider.shutdown().unwrap();
    }
}
