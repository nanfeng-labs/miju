//! Record task completions and processing durations through the `metrics` facade.
//!
//! Add [`PrometheusLayer`] to a worker and install a metrics recorder in the
//! application. The layer emits metrics when the inner service future completes;
//! it does not install a recorder or start a scrape endpoint. Applications using
//! the exporter directly should declare `metrics` and `metrics-exporter-prometheus`
//! as dependencies as well as enabling miju's `prometheus` feature.
//!
//! # Metrics and labels
//!
//! | Metric | Kind | Value |
//! | --- | --- | --- |
//! | `tasks_total` | Counter | Incremented once for each observed completion |
//! | `task_duration_seconds` | Histogram | Seconds elapsed from service call to completion |
//!
//! Both metrics carry `worker` (the worker name), `queue` (the Rust argument type
//! name), and `status` (`Ok` or `Err` from the service result). The `queue` label
//! is not a backend queue identifier. Stable worker names help keep the number of
//! label combinations bounded. Rust type names are diagnostic names and may
//! change when application types are renamed.
//!
//! # Collecting and rendering
//!
//! Configure histogram buckets for `task_duration_seconds` before installing the
//! recorder. Without a bucket configuration, the exporter uses summaries. This
//! example builds a recorder and renders its text output without opening a port:
//!
//! ```rust
//! use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
//! use miju::{layers::prometheus::PrometheusLayer, prelude::*};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), BoxDynError> {
//! let recorder = PrometheusBuilder::new()
//!     .set_buckets_for_metric(
//!         Matcher::Full("task_duration_seconds".to_owned()),
//!         &[0.005, 0.05, 0.5, 5.0],
//!     )?
//!     .build_recorder();
//! let handle = recorder.handle();
//! metrics::set_global_recorder(recorder)?;
//!
//! let mut backend = MemoryStorage::new();
//! backend.push(7usize).await?;
//! WorkerBuilder::new("measured-worker")
//!     .backend(backend)
//!     .layer(PrometheusLayer::default())
//!     .on_event(|worker, event| {
//!         if matches!(event, Event::Success(_)) {
//!             worker.stop().unwrap();
//!         }
//!     })
//!     .build(|value: usize| async move { value * 2 })
//!     .run()
//!     .await?;
//!
//! handle.run_upkeep();
//! let output = handle.render();
//! assert!(output.contains("tasks_total"));
//! # Ok(())
//! # }
//! ```
//!
//! A process-wide recorder can be installed only once. For a long-running
//! application using `build_recorder`, retain the handle and schedule
//! `run_upkeep` regularly. Serve `handle.render()` through the application's
//! chosen HTTP stack. The exporter dependency's default features are disabled;
//! enable its listener or push-gateway features explicitly if those are needed.
//!
//! # Completion boundaries
//!
//! Readiness errors, panics, and futures dropped before completion do not emit
//! completion metrics. Duration uses a monotonic wall clock; it includes time
//! after `call`, including time before the future is first polled, but excludes
//! readiness waiting. No recorder means no metrics are retained.
//!
//! Layer order determines what a completion represents. Placing this layer
//! outside retry middleware records the combined operation; placing it inside
//! records individual attempts. Place it outside timeout or panic-capture
//! middleware to observe the errors those layers produce.
//!
//! Workers supply [`WorkerContext`] in task data. Direct service callers must
//! supply that data themselves; a missing context panics before calling the
//! inner service.

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use miju_core::{task::Task, worker::context::WorkerContext};
use tower::{Layer, Service};

/// A layer to support prometheus metrics
///
/// Emits through the application's installed metrics recorder. See the
/// [module documentation](self) for labels, exporter setup, and layer ordering.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct PrometheusLayer {}

impl<S> Layer<S> for PrometheusLayer {
    type Service = PrometheusService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PrometheusService { service }
    }
}

/// This service implements the metric collection behavior
///
/// Readiness, response values, and error types are delegated unchanged. Calling
/// this service requires a [`WorkerContext`] in the task's data and panics if it
/// is missing. [`WorkerBuilder`](miju_core::worker::builder::WorkerBuilder)
/// supplies this context when running a worker.
#[derive(Debug, Clone)]
pub struct PrometheusService<S> {
    service: S,
}

impl<Svc, Fut, Args, Res, Err> Service<Task<Args>> for PrometheusService<Svc>
where
    Svc: Service<Task<Args>, Response = Res, Error = Err, Future = Fut>,
    Fut: Future<Output = Result<Res, Err>> + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future = ResponseFuture<Fut>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Task<Args>) -> Self::Future {
        let start = Instant::now();
        let worker = request
            .data()
            .get::<WorkerContext>()
            .map(|w| w.name())
            .expect("worker context not found in task data");

        let req = self.service.call(request);
        let job_type = std::any::type_name::<Args>().to_owned();

        ResponseFuture {
            inner: req,
            start,
            job_type,
            worker,
        }
    }
}

/// Response for prometheus service
///
/// Records the completion counter and duration after the inner future resolves.
/// Dropping a pending future records neither metric. Like other futures, this
/// future must not be polled again after returning `Poll::Ready`.
#[pin_project::pin_project]
pub struct ResponseFuture<F> {
    #[pin]
    pub(crate) inner: F,
    pub(crate) start: Instant,
    pub(crate) job_type: String,
    pub(crate) worker: String,
}

impl<F> fmt::Debug for ResponseFuture<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elapsed = self.start.elapsed();

        f.debug_struct("ResponseFuture")
            .field("inner", &self.inner)
            .field("elapsed_since_start", &elapsed)
            .field("job_type", &self.job_type)
            .field("worker", &self.worker)
            .finish()
    }
}

impl<Fut, Res, Err> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Res, Err>>,
{
    type Output = Result<Res, Err>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let response = std::task::ready!(this.inner.poll(cx));

        let latency = this.start.elapsed().as_secs_f64();
        let status = response
            .as_ref()
            .ok()
            .map(|_res| "Ok".to_owned())
            .unwrap_or_else(|| "Err".to_owned());

        let labels = [
            ("worker", this.worker.to_owned()),
            ("queue", this.job_type.to_owned()),
            ("status", status),
        ];
        let counter = metrics::counter!("tasks_total", &labels);
        counter.increment(1);
        let hist = metrics::histogram!("task_duration_seconds", &labels);
        hist.record(latency);
        Poll::Ready(response)
    }
}
