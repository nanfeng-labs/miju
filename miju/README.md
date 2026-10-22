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

## Optional core capabilities

`task-context` exposes `TaskContext` and its completion futures through the
prelude. `shared` exposes `Shared` for sharing a backend through cloneable
handles.
