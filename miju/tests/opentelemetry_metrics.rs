//! Exercise the SDK's exported measurements, including worker and future boundaries.

#![cfg(feature = "opentelemetry")]

use std::{collections::BTreeMap, future::poll_fn, io, sync::OnceLock, task::Poll, time::Duration};

use miju::{layers::opentelemetry::OpenTelemetryMetricsLayer, prelude::*};
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData},
};
use tower::{Service, ServiceExt, service_fn};

fn metrics() -> &'static (SdkMeterProvider, InMemoryMetricExporter) {
    static METRICS: OnceLock<(SdkMeterProvider, InMemoryMetricExporter)> = OnceLock::new();
    METRICS.get_or_init(|| {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_secs(3600))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        global::set_meter_provider(provider.clone());
        (provider, exporter)
    })
}

fn task<T>(args: T, worker: &str) -> Task<T> {
    TaskBuilder::new(args)
        .data(WorkerContext::new(worker))
        .build()
}

fn assert_metrics(worker: &str, queue: &str, expected: &[(&str, u64)], minimum_seconds: f64) {
    let (provider, exporter) = metrics();
    provider.force_flush().unwrap();
    let exports = exporter.get_finished_metrics().unwrap();
    let latest = exports.last().unwrap();
    let mut counts = BTreeMap::new();
    let mut durations = BTreeMap::new();
    let mut seconds = 0.0;
    let status = |attributes: Vec<&KeyValue>| {
        let find = |key: &str| {
            attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.to_string())
        };
        if find("messaging.destination.partition.id").as_deref() != Some(worker) {
            return None;
        }
        assert_eq!(attributes.len(), 5);
        assert_eq!(find("messaging.system").as_deref(), Some("miju"));
        assert_eq!(find("messaging.operation.name").as_deref(), Some("process"));
        assert_eq!(find("messaging.destination.name").as_deref(), Some(queue));
        find("miju.status")
    };
    for scope in latest.scope_metrics() {
        assert_eq!(scope.scope().name(), "miju");
        for metric in scope.metrics() {
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    assert_eq!(metric.name(), "messaging.client.consumed.messages");
                    assert!(sum.is_monotonic());
                    for point in sum.data_points() {
                        if let Some(status) = status(point.attributes().collect()) {
                            counts.insert(status, point.value());
                        }
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    assert_eq!(metric.name(), "messaging.process.duration");
                    assert_eq!(metric.unit(), "s");
                    for point in histogram.data_points() {
                        if let Some(status) = status(point.attributes().collect()) {
                            assert_eq!(
                                point.bounds().collect::<Vec<_>>(),
                                [
                                    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                                    15.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0,
                                ]
                            );
                            assert_eq!(point.bucket_counts().sum::<u64>(), point.count());
                            durations.insert(status, point.count());
                            seconds += point.sum();
                        }
                    }
                }
                other => panic!("unexpected metric: {other:?}"),
            }
        }
    }
    let expected = expected.iter().map(|(s, n)| (s.to_string(), *n)).collect();
    assert_eq!(counts, expected);
    assert_eq!(durations, expected);
    assert!(seconds >= minimum_seconds);
}

#[tokio::test]
async fn typed_completions_export_attributes_and_include_time_before_poll() {
    metrics();
    let mut service =
        OpenTelemetryMetricsLayer::default().layer(service_fn(|t: Task<bool>| async move {
            if t.args {
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"))
            } else {
                Ok(42usize)
            }
        }));
    let pending = service.ready().await.unwrap().call(task(false, "typed"));
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(pending.await.unwrap(), 42);
    let error = service.oneshot(task(true, "typed")).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "denied");
    assert_metrics("typed", "bool", &[("Ok", 1), ("Err", 1)], 0.01);
}

#[tokio::test]
async fn worker_supplies_context_and_exports_completion() {
    metrics();
    let mut backend = MemoryStorage::new();
    backend.push(7usize).await.unwrap();
    let worker = WorkerBuilder::new("worker")
        .backend(backend)
        .layer(OpenTelemetryMetricsLayer::default())
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
    assert_metrics("worker", "usize", &[("Ok", 1)], 0.0);
}

#[tokio::test]
async fn missing_context_and_dropped_futures_do_not_emit_completions() {
    metrics();
    let mut service = OpenTelemetryMetricsLayer::default().layer(service_fn(|_: Task<()>| {
        std::future::pending::<io::Result<()>>()
    }));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| service.call(Task::new(()))))
            .is_err()
    );
    let mut future = Box::pin(service.call(task((), "dropped")));
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(future);
    assert_metrics("dropped", "()", &[], 0.0);
}

#[cfg(feature = "timeout")]
#[tokio::test(start_paused = true)]
async fn timeout_order_controls_observed_errors() {
    metrics();
    let pending = service_fn(|_: Task<()>| std::future::pending::<io::Result<()>>());
    let timeout = tower::timeout::TimeoutLayer::new(Duration::from_secs(1));
    let observed = OpenTelemetryMetricsLayer::default().layer(timeout.layer(pending));
    assert!(
        observed
            .oneshot(task((), "timeout-observed"))
            .await
            .is_err()
    );
    let dropped = timeout.layer(OpenTelemetryMetricsLayer::default().layer(pending));
    assert!(dropped.oneshot(task((), "timeout-dropped")).await.is_err());
    assert_metrics("timeout-observed", "()", &[("Err", 1)], 0.0);
    assert_metrics("timeout-dropped", "()", &[], 0.0);
}

#[cfg(feature = "retry")]
#[tokio::test]
async fn retries_can_measure_each_attempt_or_one_combined_completion() {
    metrics();
    for per_attempt in [false, true] {
        let name = if per_attempt { "attempts" } else { "combined" };
        let mut backend = MemoryStorage::new();
        backend.push(1usize).await.unwrap();
        let builder = WorkerBuilder::new(name)
            .backend(backend)
            .on_event(|worker, event| {
                if matches!(event, Event::Success(_)) {
                    worker.stop().unwrap();
                }
            });
        let handler = |_: usize, attempt: Attempt| async move {
            if attempt.current() < 3 {
                Err(io::Error::other("retry"))
            } else {
                Ok(())
            }
        };
        let metrics = OpenTelemetryMetricsLayer::default();
        let policy = RetryPolicy::retries(2);
        if per_attempt {
            builder
                .retry(policy)
                .layer(metrics)
                .build(handler)
                .run()
                .await
                .unwrap();
        } else {
            builder
                .layer(metrics)
                .retry(policy)
                .build(handler)
                .run()
                .await
                .unwrap();
        }
    }
    assert_metrics("attempts", "usize", &[("Err", 2), ("Ok", 1)], 0.0);
    assert_metrics("combined", "usize", &[("Ok", 1)], 0.0);
}
