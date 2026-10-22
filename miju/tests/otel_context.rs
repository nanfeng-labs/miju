//! Verify exported parent IDs with independent producer and consumer lifetimes.

#![cfg(all(feature = "tracing", feature = "opentelemetry"))]

use std::{sync::Once, time::Duration};

use miju::{
    layers::tracing::{
        ContextualTaskSpan, DefaultMakeSpan, MakeSpan, OtelTraceContext, TraceLayer, TracingContext,
    },
    prelude::*,
};
use opentelemetry::{
    global,
    trace::{TraceContextExt, TracerProvider},
};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{InMemorySpanExporter, SdkTracerProvider},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::prelude::*;

fn subscriber() -> (SdkTracerProvider, InMemorySpanExporter, tracing::Dispatch) {
    static INIT: Once = Once::new();
    INIT.call_once(|| global::set_text_map_propagator(TraceContextPropagator::new()));
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("context-tests")));
    (provider, exporter, tracing::Dispatch::new(subscriber))
}

#[tokio::test]
async fn worker_restores_producer_parent_after_producer_has_closed() {
    let (provider, exporter, dispatch) = subscriber();
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    let metadata = {
        let span = tracing::info_span!("producer");
        let _entered = span.enter();
        TracingContext::from(OtelTraceContext::current())
    };
    assert!(metadata.trace_id().is_some());
    let mut backend = MemoryStorage::new();
    backend
        .push_task(TaskBuilder::new(7usize).metadata(&metadata).build())
        .await
        .unwrap();
    let worker = WorkerBuilder::new("propagation")
        .backend(backend)
        .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
        .on_event(|worker, event| {
            if matches!(event, Event::Success(_)) {
                worker.stop().unwrap();
            }
        })
        .build(|value: usize| async move { value * 2 });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let producer = spans.iter().find(|s| s.name == "producer").unwrap();
    let consumer = spans.iter().find(|s| s.name == "task").unwrap();
    assert_eq!(
        consumer.span_context.trace_id(),
        producer.span_context.trace_id()
    );
    assert_eq!(consumer.parent_span_id, producer.span_context.span_id());
    assert_ne!(
        consumer.span_context.span_id(),
        producer.span_context.span_id()
    );
}

#[test]
fn remote_context_overrides_ambient_parent_and_preserves_tracestate() {
    let (provider, exporter, dispatch) = subscriber();
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    let metadata = TracingContext::new()
        .with_trace_id("0123456789abcdef0123456789abcdef")
        .with_span_id("0123456789abcdef")
        .with_trace_flags(1)
        .with_trace_state("vendor=value");
    let task = TaskBuilder::new(())
        .task_id("remote".to_owned().into())
        .metadata(&metadata)
        .build();
    let ambient = tracing::info_span!("ambient");
    let _ambient = ambient.enter();
    let span = ContextualTaskSpan::new().make_span(&task);
    let context = span.context();
    let observed = context.span();
    assert_eq!(
        observed.span_context().trace_id().to_string(),
        metadata.trace_id().as_deref().unwrap()
    );
    assert_eq!(
        observed.span_context().trace_state().header(),
        "vendor=value"
    );
    assert!(observed.span_context().is_sampled());
    let recaptured = span.in_scope(|| TracingContext::from(OtelTraceContext::current()));
    assert_eq!(recaptured.trace_state().as_deref(), Some("vendor=value"));
    assert_eq!(recaptured.trace_flags(), &Some(1));
    drop(span);
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let consumer = spans.iter().find(|s| s.name == "task").unwrap();
    assert_eq!(
        consumer.parent_span_id.to_string(),
        metadata.span_id().as_deref().unwrap()
    );
}

#[test]
fn absent_malformed_and_zero_context_keep_the_ambient_parent() {
    let (provider, exporter, dispatch) = subscriber();
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    let ambient = tracing::info_span!("ambient");
    let ambient_context = ambient.context();
    let _ambient = ambient.enter();
    let invalid = TracingContext::new()
        .with_trace_id("invalid")
        .with_span_id("invalid");
    let zero = TracingContext::new()
        .with_trace_id("00000000000000000000000000000000")
        .with_span_id("0000000000000000")
        .with_trace_flags(1);
    for metadata in [None, Some(invalid), Some(zero)] {
        let mut builder = TaskBuilder::new(()).task_id("invalid".to_owned().into());
        if let Some(metadata) = metadata {
            builder = builder.metadata(&metadata);
        }
        drop(ContextualTaskSpan::new().make_span(&builder.build()));
    }
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 3);
    for span in spans {
        assert_eq!(
            span.parent_span_id,
            ambient_context.span().span_context().span_id()
        );
        assert_eq!(
            span.span_context.trace_id(),
            ambient_context.span().span_context().trace_id()
        );
    }
}

#[test]
fn default_factory_ignores_remote_metadata_and_conversion_preserves_fields() {
    let (_provider, _exporter, dispatch) = subscriber();
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    let metadata = TracingContext::new()
        .with_trace_id("0123456789abcdef0123456789abcdef")
        .with_span_id("0123456789abcdef")
        .with_trace_flags(1)
        .with_trace_state("vendor=value");
    let converted = TracingContext::from(OtelTraceContext::from(metadata.clone()));
    assert_eq!(converted.to_string(), metadata.to_string());
    let task = TaskBuilder::new(())
        .task_id("default".to_owned().into())
        .metadata(&converted)
        .build();
    let span = DefaultMakeSpan::new().make_span(&task);
    assert_ne!(
        span.context().span().span_context().trace_id().to_string(),
        metadata.trace_id().as_deref().unwrap()
    );
}

#[test]
fn capture_without_an_active_span_is_empty() {
    let (_provider, _exporter, dispatch) = subscriber();
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    let metadata = TracingContext::from(OtelTraceContext::current());
    assert_eq!(metadata.trace_id(), &None);
    assert_eq!(metadata.span_id(), &None);
}
