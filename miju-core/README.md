# miju-core

Task backends and worker execution for background processing in Rust.

`miju-core` provides task data structures, backend protocols, workers, and
monitoring. Workers execute handlers with middleware and lifecycle tracking;
backends supply tasks and define their storage capabilities.

## Core Concepts

- **[`Tasks`](#tasks)**: Type-safe task data structures with processing metadata
- **[`Backends`](#backends)**: Task sources, submission, and storage contracts
- **[`Workers`](#workers)**: Handler execution and lifecycle control
- **[`Monitors`](#monitors)**: Coordinated workers, shutdown, and restart policies

### Tasks

The task struct provides type-safe components for task data and metadata:

- [`Args`](src/task/task_fn.rs) - The primary structure for the task
- [`ExecutionContext`](src/task/mod.rs) - Backend persisted info on task execution.
- [`Status`](src/task/status.rs) - Represents the current state of a task
- [`TaskId`](src/task/task_id.rs) - Unique identifier for task tracking
- [`Attempt`](src/task/attempt.rs) - Retry tracking and attempt information
- [`Extensions`](src/task/extensions.rs) - Type-safe storage for additional task data
- [`Metadata`](src/task/metadata.rs) - metadata associated with the task

#### Example: Using `TaskBuilder`

```rust
use miju_core::task::{Task, builder::TaskBuilder};

let task: Task<String> = TaskBuilder::new("my-task".to_string())
    .task_id("task-123".to_string().into())
    .max_attempts(3)
    .run_in_minutes(10)
    .build();
```
Specific documentation for tasks can be found in the [`task`](src/task/mod.rs) and [`task::builder`](src/task/builder.rs) modules.

##### Relevant Guides:

- [**Defining Task arguments**](src/task/task_fn.rs) - Creating effective task arguments that are scalable and type-safe

### Backends

The [`backend`](src/backend/mod.rs) module defines [`Backend`](src/backend/mod.rs) for polling tasks,
[`TaskSink`](src/backend/mod.rs) for submission, and management capabilities.
[`MemoryStorage`](src/backend/impls/memory.rs),
[`VecDequeBackend`](src/backend/impls/dequeue.rs), and
[`CustomBackend`](src/backend/custom.rs) provide task sources.
[`Codec`](src/backend/codec.rs) and [`FinalizeBackend`](src/backend/finalize.rs)
connect encoded payloads with typed handlers.

[`BackendExt`](src/backend/ext/mod.rs) adds error inspection and mapping,
lifecycle callbacks, task piping, stream interleaving, and polling strategies.
Strategies supply wakeups and collect polling statistics; they do not gate
calls to the underlying backend. Enable `sleep` for interval and backoff
strategies, `tracing` for span instrumentation, and `shared` for cloneable
backend handles.

With `sleep`, `backend::persistence` provides the `Persistence` provider
contract and the `Persisted` driver for registration, heartbeats, task
batches, and lifecycle events. Storage integrations bridge that driver to
`Backend` and configure its event layer; the driver itself is not a backend
implementation. Providers supply storage operations and atomic task claims.

### Workers

Workers are the execution engines that process tasks from backends. They are
configured with a backend, middleware, and a task handler using
[`WorkerBuilder`](src/worker/builder.rs).

```rust
use miju_core::backend::{TaskSink, memory::MemoryStorage};
use miju_core::worker::{builder::WorkerBuilder, context::WorkerContext};
#[tokio::main]
async fn main() {
    let mut backend = MemoryStorage::new();
    backend.push(42u32).await.unwrap();
    let worker = WorkerBuilder::new("example").backend(backend).build(
        |value: u32, worker: WorkerContext| async move {
            assert_eq!(value, 42);
            worker.stop().unwrap();
        },
    );
    worker.run().await.unwrap();
}
```

[`WorkerContext`](src/worker/context.rs) controls pause, resume, and
shutdown. Lifecycle tracking records attempts and task status. Handler errors
become worker events; retry-classified errors update status without automatically
requeuing tasks. Backend exhaustion alone does not stop the worker context.

### Monitors

[`Monitor`](src/monitor/mod.rs) manages multiple workers, coordinates shutdown,
and optionally restarts failed workers according to a supplied policy.
Normal shutdown drains tracked work. A configured terminator can end the wait
early; see [`Monitor::run_with_signal`](src/monitor/mod.rs).

Enable `task-context` for individual task tracking and cancellation, `sleep`
for timer-based task and shutdown limits, and `tracing` for lifecycle logs.
Enable `test-utils` for task function signature validation helpers.
Default features remain empty.

#### Example: Using `Monitor` with a Worker

```rust
use miju_core::backend::{TaskSink, dequeue::VecDequeBackend};
use miju_core::monitor::Monitor;
use miju_core::task::Task;
use miju_core::worker::builder::WorkerBuilder;
use miju_core::worker::context::WorkerContext;
use std::time::Duration;
use tower::service_fn;
#[tokio::main]
async fn main() {
    let mut storage = VecDequeBackend::new();
    storage.push(1u32).await.unwrap();

    let monitor = Monitor::new()
        .on_event(|worker, event| println!("{}: {:?}", worker.name(), event))
        .register(move |_| {
            WorkerBuilder::new("demo-worker")
                .backend(storage.clone())
                .build(|req: u32, worker: WorkerContext| async move {
                    println!("Processing task: {:?}", req);
                    worker.stop().unwrap();
                    Ok::<_, std::io::Error>(req)
                })
        });

    // Start monitor and run all registered workers
    monitor.run().await.unwrap();
}
```

### Middleware

Built on the `tower` ecosystem, `miju-core` supports middleware for error handling, timeouts, rate limiting, and observability.

#### Core Middleware

The following middleware layers are included with their worker extensions:

- [`AcknowledgeLayer`](src/worker/ext/ack/mod.rs) - Task acknowledgment after processing
- [`EventListenerLayer`](src/worker/ext/event_listener/mod.rs) - Worker event emission and handling
- [`CircuitBreakerLayer`](src/worker/ext/circuit_breaker/mod.rs) - Circuit breaker pattern for failure handling
- [`LongRunningLayer`](src/worker/ext/long_running/mod.rs) - Support for tracking long-running tasks

#### Extending with middleware

You can write your own middleware to run code before or after a task is processed.

<details>
<summary>Creating Custom Middleware</summary>

Here's a simple example of a logging middleware layer:

```rust
use miju_core::task::Task;
use std::task::{Context, Poll};
use tower::{Layer, Service};

// Define a logging service that wraps another service
pub struct LoggingService<S> {
    inner: S,
}

impl<S, Args, Res, Err> Service<Task<Args>> for LoggingService<S>
where
    S: Service<Task<Args>, Response = Res, Error = Err>,
    Args: std::fmt::Debug,
{
    type Response = Res;
    type Error = Err;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Task<Args>) -> Self::Future {
        println!("Processing task: {:?}", req.args);
        self.inner.call(req)
    }
}

// Define a layer that wraps services with LoggingService
pub struct LoggingLayer;

impl<S> Layer<S> for LoggingLayer {
    type Service = LoggingService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LoggingService { inner: service }
    }
}
```
</details>

If you want your middleware to do more than just intercept requests and responses, you can use extension traits. See the [`worker::ext`](src/worker/ext/mod.rs) module for examples.

### Error Handling

Errors classify the result of a handler for lifecycle tracking:

- [`AbortError`](src/error.rs) marks a task as killed.
- [`RetryAfterError`](src/error.rs) marks a task as pending and carries a retry delay.
- [`DeferredError`](src/error.rs) marks a task as pending.

Backends and middleware implement the corresponding rescheduling policy.
These error types do not themselves requeue tasks.

### Graceful Shutdown

Workers track in-flight tasks and drain tracked work during shutdown.
A shared [`Shutdown`](src/monitor/shutdown.rs) token coordinates workers.
A configured [`Monitor::with_terminator`](src/monitor/mod.rs)
can end the drain early; provider integrations determine task recovery.

## Serialization

Enable `serde` to serialize a [`TaskBuilder`](src/task/builder.rs),
[`ExecutionContext`](src/task/mod.rs), identifiers, states, metadata,
and recorded results. Process-local extensions are skipped. Deserialization
creates independent status and attempt cells; it does not reconnect observers
of an existing task. A [`Task`](src/task/mod.rs) itself is not serializable.

## Task identifiers

[`GenerateId`](src/task/task_id.rs) provides ID generation with
[`RandomId`](src/task/task_id.rs) or `u64`. Enable `uuid` or `ulid` for
the corresponding [`TaskId`](src/task/task_id.rs) variants and generators.
Constructing a task does not automatically assign an ID.

`serde` adds serialization support for the enabled ID types without enabling
either ID feature. `TaskId` uses an untagged representation: UUID and ULID
strings deserialize as its `String` variant, while string parsing recognizes
the enabled ID formats.

The default and `serde` configurations support Rust 1.85. Enabling `uuid` or
`ulid` requires Rust 1.89 because of the UUID dependency.

## Tracing metadata

With `tracing`, `task::metadata::TracingContext` stores explicit trace fields
under `miju_core.tracing.context`. The value is a semicolon-separated string
with decimal flags and `-` for absent fields. It does not capture the current
span or validate W3C trace IDs. Supply a subscriber separately for runtime logs.

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use miju_core::task::{builder::TaskBuilder, metadata::TracingContext};

    let trace = TracingContext::new()
        .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
        .with_span_id("00f067aa0ba902b7")
        .with_trace_flags(1);
    let task = TaskBuilder::new("send-email").metadata(&trace).build();
    let restored = task.metadata().extract_as::<TracingContext>()?;
    assert_eq!(restored.to_string(), trace.to_string());
    Ok(())
}
```

## Feature flags

All features are opt-in; the default feature set is empty.

| Feature | Capability |
| --- | --- |
| `sleep` | Runtime-independent timers, polling strategies, and persistence driver |
| `tracing` | Runtime logs, backend instrumentation, and explicit tracing metadata |
| `serde` | Serialization of contexts, metadata, results, and enabled ID types |
| `task-context` | Individual task tracking and cancellation |
| `shared` | Cloneable backend handles |
| `test-utils` | Task handler signature validation helpers |
| `uuid` / `ulid` | Additional task IDs and generators; requires Rust 1.89 |
| `docsrs` | Generate the feature reference for rustdoc |

Examples use Tokio for execution and Tower for middleware. From the workspace,
run `cargo doc -p miju-core --all-features --no-deps` to build the full API reference.
The optional `docsrs` feature works on stable Rust; docs.rs also uses nightly
with `--cfg docsrs`.

## Development

- [**Using CustomBackend**](src/backend/custom.rs) - using custom backend to integrate with already existing systems
- [**Extending Workers using extension traits**](src/worker/ext/mod.rs) - implementing custom worker functionality via extension traits
