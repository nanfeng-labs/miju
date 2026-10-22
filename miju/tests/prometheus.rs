//! Verify task metrics through a scoped recorder and the Prometheus text exporter.

#![cfg(feature = "prometheus")]

use std::{
    future::{Ready, poll_fn},
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};
use miju::{layers::prometheus::PrometheusLayer, prelude::*};
use tower::{Service, ServiceExt, service_fn};

fn recorder() -> PrometheusRecorder {
    PrometheusBuilder::new()
        .set_buckets(&[0.005, 0.05, 0.5, 5.0])
        .unwrap()
        .build_recorder()
}

fn task<T>(args: T, worker: &str) -> Task<T> {
    TaskBuilder::new(args)
        .data(WorkerContext::new(worker))
        .build()
}

fn sample(output: &str, metric: &str, worker: &str, queue: &str, status: &str) -> f64 {
    let line = output
        .lines()
        .find(|line| {
            line.starts_with(&format!("{metric}{{"))
                && line.contains(&format!("worker={worker:?}"))
                && line.contains(&format!("queue={queue:?}"))
                && line.contains(&format!("status={status:?}"))
        })
        .unwrap_or_else(|| panic!("missing {metric}/{worker}/{queue}/{status}:\n{output}"));
    line.rsplit_once(' ').unwrap().1.parse().unwrap()
}

#[tokio::test]
async fn completions_preserve_values_errors_and_export_labels() {
    let recorder = recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let inner = service_fn(|request: Task<bool>| async move {
        if request.args {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"))
        } else {
            Ok(42usize)
        }
    });
    let service = PrometheusLayer::default().layer(inner);
    for _ in 0..2 {
        assert_eq!(
            service.clone().oneshot(task(false, "typed")).await.unwrap(),
            42
        );
    }
    let error = service.oneshot(task(true, "typed")).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "denied");

    let output = recorder.handle().render();
    assert!(output.contains("# TYPE tasks_total counter"));
    assert!(output.contains("# TYPE task_duration_seconds histogram"));
    for (status, expected) in [("Ok", 2.0), ("Err", 1.0)] {
        assert_eq!(
            sample(&output, "tasks_total", "typed", "bool", status),
            expected
        );
        assert_eq!(
            sample(
                &output,
                "task_duration_seconds_count",
                "typed",
                "bool",
                status
            ),
            expected
        );
    }
}

#[tokio::test]
async fn worker_supplies_context_and_records_success_and_failure() {
    let recorder = recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let mut storage = MemoryStorage::new();
    storage.push(0usize).await.unwrap();
    storage.push(1usize).await.unwrap();
    let completed = Arc::new(Mutex::new(0));
    let count = completed.clone();
    let worker = WorkerBuilder::new("measured-worker")
        .backend(storage)
        .layer(PrometheusLayer::default())
        .on_event(move |worker, event| {
            if matches!(event, Event::Success(_) | Event::Error(_)) {
                let mut count = count.lock().unwrap();
                *count += 1;
                if *count == 2 {
                    worker.stop().unwrap();
                }
            }
        })
        .build(|value: usize| async move {
            if value == 0 {
                Err(io::Error::other("failed"))
            } else {
                Ok(value * 2)
            }
        });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*completed.lock().unwrap(), 2);
    let output = recorder.handle().render();
    for status in ["Ok", "Err"] {
        assert_eq!(
            sample(&output, "tasks_total", "measured-worker", "usize", status),
            1.0
        );
        assert_eq!(
            sample(
                &output,
                "task_duration_seconds_count",
                "measured-worker",
                "usize",
                status
            ),
            1.0
        );
    }
}

#[tokio::test]
async fn duration_includes_time_before_the_first_poll() {
    let recorder = recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let mut service = PrometheusLayer::default()
        .layer(service_fn(|_: Task<()>| async { Ok::<_, io::Error>(()) }));
    let future = service.ready().await.unwrap().call(task((), "latency"));
    assert!(recorder.handle().render().is_empty());
    tokio::time::sleep(Duration::from_millis(20)).await;
    future.await.unwrap();
    let output = recorder.handle().render();
    assert!(sample(&output, "task_duration_seconds_sum", "latency", "()", "Ok") >= 0.02);
}

struct ReadinessError;

impl Service<Task<()>> for ReadinessError {
    type Response = ();
    type Error = io::Error;
    type Future = Ready<io::Result<()>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("not ready")))
    }

    fn call(&mut self, _: Task<()>) -> Self::Future {
        panic!("call must not follow failed readiness");
    }
}

#[tokio::test]
async fn readiness_errors_and_dropped_futures_emit_no_completions() {
    let recorder = recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let error = PrometheusLayer::default()
        .layer(ReadinessError)
        .oneshot(task((), "not-ready"))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "not ready");
    let mut service = PrometheusLayer::default().layer(service_fn(|_: Task<()>| {
        std::future::pending::<io::Result<()>>()
    }));
    let mut future = Box::pin(service.ready().await.unwrap().call(task((), "cancelled")));
    poll_fn(|cx| {
        assert!(std::future::Future::poll(future.as_mut(), cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(future);
    assert!(recorder.handle().render().is_empty());
}

#[cfg(feature = "timeout")]
#[tokio::test(start_paused = true)]
async fn timeout_order_determines_whether_a_failure_is_counted() {
    let recorder = recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let pending = service_fn(|_: Task<()>| std::future::pending::<io::Result<()>>());
    let timeout = tower::timeout::TimeoutLayer::new(Duration::from_secs(1));
    let observed = PrometheusLayer::default().layer(timeout.layer(pending));
    assert!(observed.oneshot(task((), "observed")).await.is_err());
    let cancelled = timeout.layer(PrometheusLayer::default().layer(pending));
    assert!(cancelled.oneshot(task((), "cancelled")).await.is_err());
    let output = recorder.handle().render();
    assert_eq!(sample(&output, "tasks_total", "observed", "()", "Err"), 1.0);
    assert!(!output.contains("cancelled"));
}

#[cfg(feature = "catch-panic")]
#[tokio::test]
async fn an_outer_metrics_layer_observes_converted_panics() {
    let recorder = recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let panic = service_fn(|_: Task<()>| async {
        panic!("observed panic");
        #[allow(unreachable_code)]
        Ok::<_, io::Error>(())
    });
    let service = PrometheusLayer::default()
        .layer(miju::layers::catch_panic::CatchPanicLayer::new().layer(panic));
    assert!(
        service
            .oneshot(task((), "panic"))
            .await
            .unwrap_err()
            .downcast_ref::<AbortError>()
            .is_some()
    );
    assert_eq!(
        sample(
            &recorder.handle().render(),
            "tasks_total",
            "panic",
            "()",
            "Err"
        ),
        1.0
    );
}

#[cfg(feature = "retry")]
#[tokio::test]
async fn retry_order_selects_attempts_or_the_combined_result() {
    let recorder = recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    for per_attempt in [false, true] {
        let name = if per_attempt { "attempts" } else { "combined" };
        let mut storage = MemoryStorage::new();
        storage.push(1usize).await.unwrap();
        let builder = WorkerBuilder::new(name)
            .backend(storage)
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
        if per_attempt {
            let worker = builder
                .retry(RetryPolicy::retries(2))
                .layer(PrometheusLayer::default())
                .build(handler);
            tokio::time::timeout(Duration::from_secs(5), worker.run())
                .await
                .unwrap()
                .unwrap();
        } else {
            let worker = builder
                .layer(PrometheusLayer::default())
                .retry(RetryPolicy::retries(2))
                .build(handler);
            tokio::time::timeout(Duration::from_secs(5), worker.run())
                .await
                .unwrap()
                .unwrap();
        }
    }
    let output = recorder.handle().render();
    assert_eq!(
        sample(&output, "tasks_total", "attempts", "usize", "Err"),
        2.0
    );
    assert_eq!(
        sample(&output, "tasks_total", "attempts", "usize", "Ok"),
        1.0
    );
    assert_eq!(
        sample(&output, "tasks_total", "combined", "usize", "Ok"),
        1.0
    );
    assert!(
        !output
            .lines()
            .any(|line| line.contains("worker=\"combined\"") && line.contains("status=\"Err\""))
    );
}
