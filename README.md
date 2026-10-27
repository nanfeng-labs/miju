<p align="center">
  <img src="assets/miju-header.svg" alt="miju" width="480" />
</p>

<h1 align="center">miju</h1>

<div align="center">
  <strong>Typed background task processing and workflow composition for Rust</strong>
</div>

<br />

<div align="center">
  <a href="https://crates.io/crates/miju">
    <img src="https://img.shields.io/crates/v/miju.svg" alt="Crates.io version" />
  </a>
  <a href="https://docs.rs/miju">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg" alt="API documentation" />
  </a>
  <a href="https://github.com/nanfeng-labs/miju/actions/workflows/ci.yaml">
    <img src="https://github.com/nanfeng-labs/miju/actions/workflows/ci.yaml/badge.svg?branch=main" alt="CI" />
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/default%20MSRV-1.85-orange.svg" alt="Default MSRV 1.85" />
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/github/license/nanfeng-labs/miju" alt="License" />
  </a>
</div>

<br />

<div align="center">
  <a href="README.zh-CN.md">简体中文</a>
</div>

## Features

- **Typed task handlers**: write asynchronous functions whose arguments are extracted from task data and worker context.
- **Worker lifecycle management**: configure task sources, middleware, shutdown, and worker coordination.
- **Composable middleware**: apply concurrency and rate limits, timeouts, retries, panic capture, and task transformations.
- **Workflow composition**: build sequential flows and dependency graphs from typed steps.
- **Codec and storage utilities**: encode task data as JSON, MessagePack, or Bincode, or use JSON and CSV file adapters.
- **Optional observability**: integrate tracing, Prometheus metrics, OpenTelemetry metrics and context propagation, or Sentry reporting.

## Crate ecosystem

### Application and core

| Crate | Responsibility |
| --- | --- |
| [`miju`](miju) | Application facade with worker extensions, middleware, configuration, and observability integrations. |
| [`miju-core`](miju-core) | Task types, backend contracts, worker execution, and lifecycle management. |

### Workflow

| Crate | Responsibility |
| --- | --- |
| [`miju-workflow`](miju-workflow) | Typed sequential and graph workflow execution. |

### Data formats and storage

| Crate | Responsibility |
| --- | --- |
| [`miju-codec`](utils/miju-codec) | JSON, MessagePack, and Bincode codecs for task payloads and results. |
| [`miju-file-storage`](utils/miju-file-storage) | JSON and CSV file-backed storage adapters. |

File-backed storage has documented persistence and concurrency limitations. Review the crate documentation before choosing it for an application.

## Installation

Add the facade and an async runtime to your application's `Cargo.toml`:

```toml
[dependencies]
miju = { version = "0.1.1" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

# Optional: workflows, codecs, and file storage
# miju-workflow = { version = "0.1.1" }
# miju-codec = { version = "0.1.1" }
# miju-file-storage = { version = "0.1.1" }
```

## Usage

Submit a typed task to an in-memory backend and stop the worker after it handles the task:

```rust
use miju::prelude::*;

async fn handle(value: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
    assert_eq!(value, 42);
    worker.stop()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let mut backend = MemoryStorage::new();
    backend.push(42u32).await?;

    WorkerBuilder::new("example")
        .backend(backend)
        .build(handle)
        .run()
        .await?;

    Ok(())
}
```

`MemoryStorage` is process-local. Choose a backend based on the durability, task management, and lifecycle guarantees your application needs.

## Workflows

`miju-workflow` adds sequential flows and dependency graphs. Sequential steps pass typed results to the next step; graph workflows schedule dependent nodes and collect results when branches converge. See the [`miju-workflow` guide](miju-workflow/README.md) for examples and current behavior.

## Examples

Start with [`basic`](examples/basic) to see task processing and middleware in action. Run it from the repository root:

```sh
cargo run -p basic --locked
```

Browse the [example guide](examples/README.md) for dependency injection, worker context, panic handling, and shutdown scenarios, with instructions for running and stopping each example.

## How miju works

```mermaid
sequenceDiagram
    participant App as Application
    participant Backend
    participant Worker
    participant Handler

    App->>Backend: Submit a typed task
    Worker->>Backend: Poll for work
    Backend-->>Worker: Return task and execution context
    Worker->>Handler: Invoke extracted handler
    Handler-->>Worker: Return response or error
    Worker->>Backend: Complete task or enqueue continuation
    Worker-->>App: Emit lifecycle events
```

Backend capabilities determine how tasks are stored, completed, observed, and queried. Middleware wraps task execution, while a monitor coordinates multiple workers.

### Task execution in detail

The worker passes each task from its backend through middleware to the application handler. The following diagram follows one task through a worker configured with rate limiting, concurrency control, and retries.

```mermaid
sequenceDiagram
    participant Backend
    participant Worker
    participant Middleware
    participant Handler

    Worker->>Middleware: Check execution readiness
    Middleware-->>Worker: Ready to accept work
    Worker->>Backend: Fetch a task
    Backend-->>Worker: Task data and context
    Worker->>+Middleware: Dispatch task

    loop Execution attempts
        Middleware->>+Handler: Extract arguments and call handler
        Handler-->>-Middleware: Response or error
        Middleware->>Middleware: Check retry policy
    end

    Middleware-->>-Worker: Return final outcome
    opt If supported
        Worker->>Backend: Acknowledge outcome
    end
    Worker->>Worker: Notify event listeners
```

**Fetching work.** The worker fetches a task only when both the backend and the execution pipeline are ready. Rate limiting controls how frequently tasks start, while concurrency limiting bounds the number being processed at once. When either limit is reached, the worker waits before fetching more work.

**Execution and retries.** Requests enter middleware in the order the layers were added; responses return in reverse order. On failure, the retry layer checks the error, remaining attempts, and task and worker state before calling the inner handler again. These retries happen within the worker without re-enqueuing the task. In [`basic`](examples/basic/src/main.rs), rate and concurrency limits wrap retry, while tracing sits inside it: retries retain the task's concurrency slot, and each attempt gets its own span.

**Completion and shutdown.** Once processing reaches a final outcome, the backend performs any acknowledgement or result storage it supports, and the worker reports success or failure through `on_event`. During normal shutdown, the worker stops fetching tasks and waits for active work and backend cleanup. Handlers that need to save progress can check `WorkerContext::is_shutting_down()` and finish their cleanup, as shown in [`graceful-shutdown`](examples/graceful-shutdown/src/main.rs).

## Observability

Enable the facade feature for the telemetry system used by your application:

| Feature | Integration |
| --- | --- |
| `tracing` | Worker lifecycle events and task-processing spans. |
| `prometheus` | Task counts and processing durations. |
| `opentelemetry` | Metrics and tracing-context propagation. |
| `sentry` | Task error and transaction reporting. |

The default feature set includes `tracing`. The other integrations are opt-in. The `sentry` and `full` features require Rust 1.89 because they enable UUID task IDs. The default workspace MSRV is Rust 1.85; UUID and ULID task-ID support in `miju-core` also requires Rust 1.89. Core serialization is enabled with `miju-core/serde`.

## License

miju is licensed under the [MIT License](LICENSE).
