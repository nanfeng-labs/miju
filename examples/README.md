# Examples

[miju](../README.md) · [简体中文](README.zh-CN.md)

Each runnable example contains its own `Cargo.toml` and `src/main.rs`, with the required features and comments explaining execution. Shared support libraries live alongside them.

## Running an example

From the repository root, run:

```sh
cargo run -p basic --locked
```

Replace `basic` with any example name below. Some examples exit automatically; others keep running until **Ctrl+C**. See [runtime behavior](#runtime-behavior) for details.

## Task handlers and context

| Example | Scenario |
| --- | --- |
| [`basic`](basic) | Process an in-memory task with rate limiting, concurrency, retries, panic capture, task spawning, and tracing. |
| [`deps-injection`](deps-injection) | Extract task metadata, share a counter through `Data`, and provide a custom `FromRequest` argument. |
| [`task-context`](task-context) | Cancel a task through a cloned `TaskContext`, await execution notification, and observe its `Killed` status. |
| [`worker-context`](worker-context) | Look up a running task and observe its worker through a shared `WorkerContext`. |
| [`advanced`](advanced) | Compose client-injecting middleware, shared API quota and cache, deferred maintenance, panic capture, and retries. |

## Execution and shutdown

| Example | Scenario |
| --- | --- |
| [`catch-panic`](catch-panic) | Capture deliberate email-task panics, exclude abort errors from retries, and observe worker error events. |
| [`graceful-shutdown`](graceful-shutdown) | Respond to Ctrl+C, finish a batch, and simulate saving task state before shutdown. |
| [`long-running`](long-running) | Run concurrent data exports with `TaskRunner`, collect their results, and drain active work on shutdown. |
| [`retries`](retries) | Apply three retries with exponential backoff to ordinary errors while excluding `AbortError`. |
| [`unique-jobs`](unique-jobs) | Submit two different payloads with the same idempotency key and process only the first. |
| [`unmonitored-worker`](unmonitored-worker) | Run a standalone worker with 20 concurrent tasks and stop after its finite batch drains. |

## Workflows

| Example | Scenario |
| --- | --- |
| [`workflow-stepped`](workflow-stepped) | Pass typed results through three steps, configure a continuation delay, and inject a shared backend into each handler. |
| [`workflow-graph`](workflow-graph) | Distribute entry inputs to three graph nodes, collect a typed dependency tuple, and print the graph as DOT. |
| [`workflow-graph-dynamic`](workflow-graph-dynamic) | Generate 1,000 dependent nodes, validate the DAG, and collect their results through a vector of node handles. |

## Shared example support

[`email-service`](email-service) is a library shared by the email examples, not a runnable binary. It provides a serializable payload, three validation outcomes, sample messages, application errors, and an HTML submission form. Delivery is simulated: no mail is sent. Run its checks with `cargo test -p email-service --locked`.

## Runtime behavior

- **`basic`** exits after processing its task.
- **`advanced`** queues ten emails; its ten-call budget permits five validated deliveries before readiness blocks. The handler then cancels its task, producing expected task errors. Cache refresh runs independently after execution notification, including cancellation. There is no quota refill; wait for the refresh logs, then press Ctrl+C. See its [guide](advanced/README.md) for cache paths, cancellation semantics, and shutdown limits.
- **`deps-injection`** exits after ten tasks.
- **`task-context`** requests cancellation after three seconds, waits for the worker to record `Killed`, then stops automatically. The cancellation produces an expected task error event; the rest of the cancelled handler never runs.
- **`worker-context`** observes one task for five seconds, then stops and waits for the external observer to finish.
- **`catch-panic`** runs until Ctrl+C. It deliberately prints panic-hook messages before reporting captured errors; it does not send email.
- **`graceful-shutdown`** runs until Ctrl+C. It checks for shutdown between five-second batches, then spends five seconds simulating a checkpoint without writing data.
- **`long-running`** simulates three concurrent exports lasting eight to twelve seconds. The worker stays running until Ctrl+C, which waits for any active export to finish and its results to be collected.
- **`retries`** processes three addresses: one succeeds once, one fails on four attempts, and one aborts on its first attempt. Errors are intentional. Backoff uses a one-second minimum, a five-second maximum, and jitter; press Ctrl+C after the attempts finish to stop the worker.
- **`unique-jobs`** accepts two submissions for one recipient but processes only the first subject. The in-memory backend remembers the key for its lifetime, including after completion; this is not durable deduplication across application restarts. Press Ctrl+C to exit.
- **`unmonitored-worker`** processes 100 five-second tasks with a concurrency limit of 20, then stops its worker. Its `run_until` call still waits for Ctrl+C before the process exits. Pressing Ctrl+C earlier drains active tasks and leaves any unstarted tasks unprocessed.
- **`workflow-stepped`** runs welcome, campaign, and completion steps with IDs 1, 2, and 3, then stops automatically. `delay_for(1s)` records a scheduled time, but the in-memory backend does not enforce it: the next step runs immediately. Actual waiting requires a backend that honors scheduling. The worker spawns task futures with a concurrency limit of two; step dependencies still determine execution order.
- **`workflow-graph`** sends 42, 43, and 44 to the name, age, and address-code entry nodes respectively. These are three inputs to one graph run. The collector receives `("42", 63, 144)`, prints a total of 249, and stops the worker. Its DOT output can be rendered with Graphviz.
- **`workflow-graph-dynamic`** starts with one source value of 1. Each of 1,000 generated branches adds one; the collector receives 1,000 results, prints their sum of 2,000, and stops automatically. Graph construction happens before execution; this does not demonstrate editing a running graph.
