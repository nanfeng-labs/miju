# miju

miju is an application entry point for background task processing. It brings
task, backend, worker, and monitor APIs into one [`prelude`] and extends
[`WorkerBuilder`](prelude::WorkerBuilder) with middleware composition methods.
The underlying types are shared with `miju-core`, so applications and backend
integrations can exchange tasks without conversion.

## Getting started

Add `miju` and an async runtime to your application. This example uses Tokio:

```toml
[dependencies]
miju = "0.1.0"
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Enqueue a typed task, attach application data, and run a worker. The handler
stops the worker after processing the task so the example terminates.

```rust
use miju::prelude::*;
use std::{
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

async fn count(
    amount: usize,
    total: Data<Arc<AtomicUsize>>,
    worker: WorkerContext,
) -> Result<(), BoxDynError> {
    total.fetch_add(amount, Ordering::SeqCst);
    worker.stop()?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxDynError> {
    let mut storage = MemoryStorage::new();
    storage.push(7usize).await?;
    let total = Arc::new(AtomicUsize::new(0));

    let worker = WorkerBuilder::new("counter")
        .backend(storage)
        .data(total.clone())
        .map_request(|mut task: Task<usize>| {
            task.args *= 2;
            task
        })
        .build(count);

    tokio::time::timeout(Duration::from_secs(5), worker.run()).await??;
    assert_eq!(total.load(Ordering::SeqCst), 14);
    Ok(())
}
```

[`MemoryStorage`](prelude::MemoryStorage) is an in-process queue and does not
persist tasks. Use a backend with the capabilities your application needs.
Backend traits describe those capabilities; importing a trait does not make
every backend implement it.

## Task processing

- [`Task`](prelude::Task) holds the payload and execution context.
  [`TaskBuilder`](prelude::TaskBuilder) sets queue and execution metadata before
  a task is submitted through [`TaskSink`](prelude::TaskSink).
- [`WorkerBuilder`](prelude::WorkerBuilder) connects a backend to an async
  handler. [`Data`](prelude::Data) makes application state available to the
  handler alongside its arguments and [`WorkerContext`](prelude::WorkerContext).
- [`WorkerBuilderExt`](layers::WorkerBuilderExt) adds optional layers, function
  layers, and transformations for requests, responses, errors, and futures.
  Earlier layers wrap later layers: requests traverse them in insertion order,
  and results return in reverse order.
- [`Monitor`](prelude::Monitor) coordinates multiple workers and their shutdown.
  [`EventListenerExt`](prelude::EventListenerExt) observes worker lifecycle and
  processing events.

Use `miju::prelude::*` for application code. The [`task`], [`backend`], and
[`error`] modules remain available for explicit imports. Libraries implementing
backends or worker internals can depend on `miju-core` directly.

## Execution controls

Enable the middleware features your worker needs:

| Feature | Builder methods | Purpose |
| --- | --- | --- |
| `limit` | `concurrency`, `rate_limit` | Bound in-flight calls and calls per time period |
| `timeout` | `timeout` | Return an error when a call exceeds its time limit |
| `filter` | `filter`, `filter_async` | Accept, modify, or reject a task before the handler runs |

For example, enable all three with:

```toml
miju = { version = "0.1.0", features = ["limit", "timeout", "filter"] }
```

The following worker allows up to four in-flight calls, starts at most ten calls
per second, and rejects zero-valued arguments. The timeout wraps the filter and
handler because it is added before them.

```rust
# #[cfg(all(feature = "limit", feature = "timeout", feature = "filter"))]
# {
use miju::prelude::*;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxDynError> {
    let mut storage = MemoryStorage::new();
    storage.push(7usize).await?;

    let worker = WorkerBuilder::new("controlled")
        .backend(storage)
        .concurrency(4)
        .rate_limit(10, Duration::from_secs(1))
        .timeout(Duration::from_secs(1))
        .filter(|task: Task<usize>| {
            if task.args == 0 {
                Err::<_, BoxDynError>("amount must be nonzero".into())
            } else {
                Ok(task)
            }
        })
        .build(|amount: usize, worker: WorkerContext| async move {
            assert_eq!(amount, 7);
            worker.stop().unwrap();
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run()).await??;
    Ok(())
}
# main().unwrap();
# }
```

`rate_limit` and `timeout` use Tokio timers and require an enabled Tokio time
driver. A rejected predicate returns an error without calling the handler.
The timeout drops its inner future when the deadline expires; it cannot preempt
blocking code or stop work that the handler has spawned independently.

For explicit layer composition, `layers::limit` exposes
`ConcurrencyLimitLayer`, `GlobalConcurrencyLimitLayer`, and `RateLimitLayer`;
`layers::TimeoutLayer` provides the timeout layer. Clones of a
`GlobalConcurrencyLimitLayer` share a concurrency budget across workers.

## Retries

Enable `retry` to use `.retry(policy)` and the retry policies exported by the
prelude. `RetryPolicy::retries(n)` allows up to `n` retries after the first
execution; `RetryPolicy::default()` allows five. A successful result, a killed
task, or a worker already shutting down prevents another retry decision.

```rust
# #[cfg(feature = "retry")]
# {
use miju::prelude::*;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxDynError> {
    let mut storage = MemoryStorage::new();
    storage.push(7usize).await?;

    let worker = WorkerBuilder::new("retrying")
        .backend(storage)
        .retry(RetryPolicy::retries(2))
        .on_event(|worker, event| match event {
            Event::Success(_) => worker.stop().unwrap(),
            Event::Error(error) => panic!("{error}"),
            _ => {}
        })
        .build(|value: usize, attempt: Attempt| async move {
            if attempt.current() < 3 {
                Err(std::io::Error::other("temporarily unavailable"))
            } else {
                Ok(value)
            }
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run()).await??;
    Ok(())
}
# main().unwrap();
# }
```

Use `.with_backoff(...)` to wait between retries and `.retry_if(...)` to select
errors. Put `.retry(...)` before a layer whose errors should be retried. The
`layers::retry` module re-exports Tower's retry and backoff APIs.

`RetryMetadataExt::retries` stores `RetryConfig` on a task. Enable its use with
`.from_task_config()`. This wrapper compares the configured value against the
total attempt count, including the first execution, and also applies the inner
policy's limit. Missing or invalid metadata uses the inner policy unchanged.

When combining retries with panic handling, use `.retry_if(...)` to explicitly
exclude the boxed errors that should stop retries. The panic module includes
a custom-handler example. A shutdown request during an existing backoff can
leave a retry waiting for readiness; bound worker execution when shutdown must
finish within a deadline.

## Panic handling

Enable `catch-panic` and add `.catch_panic()` to the worker builder to convert
unwinding panics during service calls and future polling into `AbortError`
values containing a `PanicError`. Successful responses and ordinary service
errors pass through. Use `CatchPanicLayer::with_panic_handler` to choose the
error produced from a panic payload. The `layers::catch_panic` module includes
examples of both forms.

Panics from readiness checks, the custom panic handler, or separately spawned
tasks are outside this layer's capture boundary. Rust's panic hook still runs,
and builds using `panic = "abort"` cannot recover through this middleware.

## Task tracing

Enable `tracing` and call `enable_tracing()` to wrap task processing in spans
with start, success, and failure events. Configure a tracing subscriber in the
application to collect these events and core lifecycle logs.

```rust
# #[cfg(feature = "tracing")]
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use miju::prelude::*;

let mut backend = MemoryStorage::new();
backend.push(7usize).await?;
let worker = WorkerBuilder::new("traced-worker")
    .backend(backend)
    .enable_tracing()
    .build(|value: usize, worker: WorkerContext| async move {
        tracing::info!(value, "processing task");
        worker.stop().unwrap();
        value * 2
    });
worker.run().await?;
# Ok(())
# }
# #[cfg(not(feature = "tracing"))]
# fn main() {}
```

Use `layer(TraceLayer::new()...)` to customize the span factory and callbacks.
Default events are `task.start` and `task.done` at DEBUG and `task.failed` at ERROR.
Completion events record latency and the result; callbacks can choose different
fields or levels. Readiness errors and unfinished futures dropped before
completion do not produce completion events. The `layers::tracing` module
documents callback timing and how layer order changes retry reporting.

`DefaultMakeSpan` and `ContextualTaskSpan` create spans under the current span,
recording the task ID and its attempt count. Both require an assigned task ID.
`MakeSpan` also accepts a fixed span or a closure for custom fields.

Use `tracing::Instrument` to attach a generated span to an async operation;
the module includes runnable examples. `TracingContext` re-exports the core
metadata type for storing explicit trace fields. With `opentelemetry` also
enabled, `ContextualTaskSpan` can restore a stored remote parent. The default
factory continues to use the active span.

## Prometheus metrics

Enable `prometheus` and add
`miju::layers::prometheus::PrometheusLayer::default()` with the worker builder's
`.layer(...)` method to record completed tasks and their durations. The layer
emits `tasks_total` and `task_duration_seconds`, labeled by worker name, Rust
argument type (`queue`), and result (`status`).

The application installs the metrics recorder and exposes its output. The
`miju::layers::prometheus` module contains a runnable example
using `metrics-exporter-prometheus`, including histogram buckets and recorder
upkeep. Enable the `prometheus` feature to access that module. Exporter transport
features are disabled by default, so enabling this middleware does not start an
HTTP listener or push gateway.

## OpenTelemetry

Enable `opentelemetry` and add
`miju::layers::opentelemetry::OpenTelemetryMetricsLayer::default()` with
`.layer(...)` to record completed task counts and processing durations. Install
the application's global meter provider before constructing the worker; the
layer creates its instruments when it wraps the service. Exporters and their
flush/shutdown lifecycle remain application-owned.

For distributed traces, also enable `tracing`, configure a tracing subscriber
with `tracing-opentelemetry`, and install a compatible text-map propagator.
Capture `OtelTraceContext::current()` on the producer, convert it into
`TracingContext`, and store it using `TaskBuilder::metadata`. On the consumer,
use `TraceLayer::new().make_span_with(ContextualTaskSpan::new())`. The
`OtelTraceContext` documentation contains a runnable propagation example.
Enabling `opentelemetry` alone does not enable the tracing module or switch the
default span factory.

## Worker configuration

Enable `config` to construct a worker from `WorkerConfig` using
`WorkerFromConfig::try_config`. The configuration contains the worker name,
the backend's own configuration, and a set of middleware descriptors. Parsing
and backend construction are separate; applications choose a Serde-compatible
format when loading configuration.

```rust
# #[cfg(feature = "config")]
# fn main() -> Result<(), miju::error::BoxDynError> {
use miju::{config::{WorkerConfig, WorkerFromConfig}, prelude::*};
use std::collections::HashSet;

let config = WorkerConfig::<MemoryStorage<usize>> {
    name: "configured-worker".to_owned(),
    backend: (),
    middleware: HashSet::new(),
};
let _worker = WorkerBuilder::try_config(config)?
    .build(|value: usize| async move { value * 2 });
# Ok(())
# }
# #[cfg(not(feature = "config"))]
# fn main() {}
```

This constructs a worker with a new, empty in-memory queue. Middleware variants
require their corresponding `timeout`, `limit`, `catch-panic`, or `tracing`
feature. `config` does not enable them automatically. `MiddlewareStack` can also
decorate an existing backend; the `config` module documents its fixed ordering,
serialized fields, and construction requirements.

## Optional core capabilities

`task-context` exposes `TaskContext` and its completion futures through the
prelude. `shared` exposes `Shared` for sharing a backend through cloneable
handles.
