//! Record task completions with the application's OpenTelemetry meter provider.
//!
//! [`OpenTelemetryMetricsLayer`] creates instruments in the `miju` scope when
//! it wraps a service. Install the global meter provider, including its readers
//! and exporters, before building the worker. Existing services retain their
//! instruments if the global provider is later replaced. Without a configured
//! provider, those instruments are no-ops. The application owns flushing and
//! shutting down its provider.
//!
//! # Metrics and attributes
//!
//! | Instrument | Kind | Measurement |
//! | --- | --- | --- |
//! | `messaging.client.consumed.messages` | Counter | One per observed completion |
//! | `messaging.process.duration` | Histogram | Seconds from service call to completion |
//!
//! Both carry `messaging.system = "miju"`, `messaging.operation.name = "process"`,
//! `messaging.destination.partition.id` (worker name),
//! `messaging.destination.name` (Rust argument type), and `miju.status` (`Ok` or
//! `Err`). These names do not imply that the storage queue or partition is read.
//! Keep worker names stable to limit the number of attribute combinations.
//! The histogram requests explicit boundaries from 0.005 to 600 seconds; the
//! application's SDK views can override aggregation.
//!
//! # Adding metrics to a worker
//!
//! Configure the provider before this worker is built. Enabling the feature
//! alone does not install an SDK, exporter, or tracing subscriber.
//!
//! ```rust
//! use miju::{layers::opentelemetry::OpenTelemetryMetricsLayer, prelude::*};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), BoxDynError> {
//! let mut backend = MemoryStorage::new();
//! backend.push(7usize).await?;
//! WorkerBuilder::new("measured-worker")
//!     .backend(backend)
//!     .layer(OpenTelemetryMetricsLayer::default())
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
//! # Completion and context boundaries
//!
//! Readiness errors, uncaught panics, and futures dropped before completion emit
//! neither metric. Timing includes synchronous work and time before the first
//! poll, but excludes readiness waiting. Placing this layer outside retries
//! measures the combined operation; placing it inside counts individual attempts.
//! Place it outside timeout or panic-capture layers to count their returned errors.
//!
//! Workers supply [`WorkerContext`] in task data. Direct service callers must
//! supply it themselves; missing context panics before the inner service is called.
//! Distributed trace propagation is separate: enable `tracing` as well and use
//! `OtelTraceContext` with `ContextualTaskSpan` in `miju::layers::tracing`.

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use miju_core::{task::Task, worker::context::WorkerContext};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};
use tower::{Layer, Service};

/// A layer to support OpenTelemetry metrics
///
/// Creates instruments using the global provider when [`Layer::layer`] is called.
/// See the [module documentation](self) for setup, attributes, and layer order.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct OpenTelemetryMetricsLayer {}

impl<S> Layer<S> for OpenTelemetryMetricsLayer {
    type Service = OpenTelemetryMetricsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        let meter = global::meter("miju");

        let task_counter = meter
            .u64_counter("messaging.client.consumed.messages")
            .build();

        let duration_histogram = meter
            .f64_histogram("messaging.process.duration")
            .with_unit("s")
            .with_boundaries(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0,
                60.0, 120.0, 300.0, 600.0,
            ])
            .build();

        OpenTelemetryMetricsService {
            service,
            task_counter,
            duration_histogram,
        }
    }
}

/// This service implements the metric collection behavior
///
/// Delegates readiness, responses, and errors unchanged. Calling the service
/// requires a [`WorkerContext`] in the task's data.
#[derive(Debug, Clone)]
pub struct OpenTelemetryMetricsService<S> {
    service: S,
    task_counter: Counter<u64>,
    duration_histogram: Histogram<f64>,
}

impl<Svc, Fut, Args, Res, Err> Service<Task<Args>> for OpenTelemetryMetricsService<Svc>
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
            .map(|ns| ns.name().to_owned())
            .expect("worker context not found in task data");

        let req = self.service.call(request);
        let task_type = std::any::type_name::<Args>().to_owned();

        ResponseFuture {
            inner: req,
            start,
            task_type,
            worker,
            task_counter: self.task_counter.clone(),
            duration_histogram: self.duration_histogram.clone(),
        }
    }
}

/// Response for opentelemetry service
///
/// Records both instruments after the inner future resolves. Dropping a pending
/// future emits nothing. A completed future must not be polled again.
#[pin_project::pin_project]
pub struct ResponseFuture<F> {
    #[pin]
    pub(crate) inner: F,
    pub(crate) start: Instant,
    pub(crate) task_type: String,
    pub(crate) worker: String,
    pub(crate) task_counter: Counter<u64>,
    pub(crate) duration_histogram: Histogram<f64>,
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
            .field("task_type", &self.task_type)
            .field("worker", &self.worker)
            .field("task_counter", &self.task_counter)
            .field("duration_histogram", &self.duration_histogram)
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

        let attributes = [
            KeyValue::new("messaging.system", "miju"),
            KeyValue::new("messaging.operation.name", "process"),
            KeyValue::new("messaging.destination.partition.id", this.worker.clone()),
            KeyValue::new("messaging.destination.name", this.task_type.clone()),
            KeyValue::new("miju.status", status),
        ];

        this.task_counter.add(1, &attributes);
        this.duration_histogram.record(latency, &attributes);

        Poll::Ready(response)
    }
}
