# Examples

[miju](../README.md) · [简体中文](README.zh-CN.md)

Each runnable example contains its own `Cargo.toml` and `src/main.rs`, with the required features and comments explaining execution. Shared support libraries live alongside them.

## Running an example

From the repository root, run:

```sh
cargo run -p basic --locked
```

Use the package names and any `--bin` arguments listed under [runtime behavior](#runtime-behavior). Some examples exit automatically; others keep running until **Ctrl+C**.

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
| [`catch-panic`](catch-panic) | Capture deliberate email-task panics, retry each task once, and observe worker error events. |
| [`graceful-shutdown`](graceful-shutdown) | Respond to Ctrl+C, finish a batch, and simulate saving task state before shutdown. |
| [`long-running`](long-running) | Run concurrent data exports with `TaskRunner`, collect their results, and drain active work on shutdown. |
| [`retries`](retries) | Apply three retries with exponential backoff to ordinary errors while excluding `AbortError`. |
| [`unique-jobs`](unique-jobs) | Submit two different payloads with the same idempotency key and process only the first. |
| [`unmonitored-worker`](unmonitored-worker) | Run a standalone worker with 20 concurrent tasks and stop after its finite batch drains. |
| [`monitor`](monitor) | Register one worker and five indexed workers, aggregate events, and configure bounded restart and shutdown policies. |
| [`monitor-context`](monitor-context) | Observe worker state externally while continuous producers and cancellable tasks run. |

## Workflows

| Example | Scenario |
| --- | --- |
| [`workflow-stepped`](workflow-stepped) | Pass typed results through three steps, configure a continuation delay, and inject a shared backend into each handler. |
| [`workflow-graph`](workflow-graph) | Distribute entry inputs to three graph nodes, collect a typed dependency tuple, and print the graph as DOT. |
| [`workflow-graph-dynamic`](workflow-graph-dynamic) | Generate 1,000 dependent nodes, validate the DAG, and collect their results through a vector of node handles. |

## Observability

| Example | Scenario |
| --- | --- |
| [`tracing`](tracing) | Compare local backend/worker spans with a producer trace restored from task metadata. |
| [`prometheus`](prometheus) | Submit email jobs through an HTTP form and expose task counters and duration histograms through the metrics recorder. |
| [`opentelemetry`](opentelemetry) | Export OpenTelemetry SDK counters and histograms through a Prometheus registry and HTTP scrape endpoint. |

## Shared example support

[`email-service`](email-service) is a library shared by the email examples, not a runnable binary. It provides a serializable payload, three validation outcomes, sample messages, application errors, and an HTML submission form. Delivery is simulated: no mail is sent. Run its checks with `cargo test -p email-service --locked`.

## Runtime behavior

- **`basic`** exits after processing its task.
- **`advanced`** queues ten emails; its ten-call budget permits five validated deliveries before readiness blocks. The handler then cancels its task, producing expected task errors. Cache refresh runs independently after execution notification, including cancellation. There is no quota refill; wait for the refresh logs, then press Ctrl+C. See its [guide](advanced/README.md) for cache paths, cancellation semantics, and shutdown limits.
- **`deps-injection`** exits after ten tasks.
- **`task-context`** requests cancellation after three seconds, waits for the worker to record `Killed`, then stops automatically. The cancellation produces an expected task error event; the rest of the cancelled handler never runs.
- **`worker-context`** observes one task for five seconds, then stops and waits for the external observer to finish.
- **`catch-panic`** runs until Ctrl+C. Each of its two tasks panics twice: panic capture boxes the error, and this retry policy does not unpack it. The output contains four panic-hook messages and two final worker errors; no email is sent.
- **`graceful-shutdown`** runs until Ctrl+C. It checks for shutdown between five-second batches, then spends five seconds simulating a checkpoint without writing data.
- **`long-running`** simulates three concurrent exports lasting eight to twelve seconds. The worker stays running until Ctrl+C, which waits for any active export to finish and its results to be collected.
- **`retries`** processes three addresses: one succeeds once, one fails on four attempts, and one aborts on its first attempt. Errors are intentional. Backoff uses a one-second minimum, a five-second maximum, and jitter; press Ctrl+C after the attempts finish to stop the worker.
- **`unique-jobs`** accepts two submissions for one recipient but processes only the first subject. The in-memory backend remembers the key for its lifetime, including after completion; this is not durable deduplication across application restarts. Press Ctrl+C to exit.
- **`unmonitored-worker`** processes 100 five-second tasks with a concurrency limit of 20, then stops its worker. Its `run_until` call still waits for Ctrl+C before the process exits. Pressing Ctrl+C earlier drains active tasks and leaves any unstarted tasks unprocessed.
- **`workflow-stepped`** runs welcome, campaign, and completion steps with IDs 1, 2, and 3, then stops automatically. `delay_for(1s)` records a scheduled time, but the in-memory backend does not enforce it: the next step runs immediately. Actual waiting requires a backend that honors scheduling. The worker spawns task futures with a concurrency limit of two; step dependencies still determine execution order.
- **`workflow-graph`** sends 42, 43, and 44 to the name, age, and address-code entry nodes respectively. These are three inputs to one graph run. The collector receives `("42", 63, 144)`, prints a total of 249, and stops the worker. Its DOT output can be rendered with Graphviz.
- **`workflow-graph-dynamic`** starts with one source value of 1. Each of 1,000 generated branches adds one; the collector receives 1,000 results, prints their sum of 2,000, and stops automatically. Graph construction happens before execution; this does not demonstrate editing a running graph.

- **`monitor`** submits one task to each of two temporary JSON backends. The five contextual workers use independent file cursors and can process the same entry; this is not a competing-consumer queue. Each handler stops its worker after one second and emits an intentional business error. All six workers normally exit automatically; Ctrl+C can also initiate shutdown with a five-second limit. Business errors do not trigger the worker restart predicate. Context metadata is wired without a provider here; the `tracing` example supplies full trace propagation.

- **`monitor-context`** queues jobs every 500 ms into two temporary files, with concurrency limits of 30 and 20. Tasks choose a duration of 1–20 seconds: values above seven seconds cancel immediately; shorter tasks wait and report an intentional error. A separate future refreshes the terminal panel every second, showing names, running/ready state, task counts, restarts, and elapsed time. Redirected output contains plain snapshots. Ctrl+C allows up to five seconds to drain, drops the producers and display, and restores the terminal. The twelve-second `run_until_executed` sub-task checks cancellation on its next poll; runtime shutdown can drop it earlier. Trace metadata is wired without a provider.

- **`tracing`** runs as `cargo run -p tracing-example --locked`. It prints four trace/span ID pairs and exits after two intentional task errors. The contextual consumer shares the producer's trace ID and has its own span ID; the ordinary consumer belongs to its worker's trace. Keep the default debug filter (or set `RUST_LOG=debug`) to see the task spans. The SDK and W3C propagator are installed, but no remote exporter is configured. Its inline test also verifies the actual exported parent span IDs.
- **`prometheus`** and **`opentelemetry`** run as `cargo run -p prometheus-example --locked` and `cargo run -p opentelemetry-example --locked`. Run one at a time: both bind `127.0.0.1:3000`. Open `/` to submit simulated email jobs and `/metrics` to scrape results. A 201 response means queued; address validation happens in the worker, so invalid addresses become error metrics. The first example exposes `tasks_total` and `task_duration_seconds`; the second exposes `messaging_client_consumed_messages_total` and `messaging_process_duration_seconds`, with an OpenTelemetry resource and reader. Both use a single-thread runtime to serialize the file backend's synchronous operations while handling HTTP requests concurrently. They create a fresh temporary JSON file, poll for later submissions, and clean up on Ctrl+C. Files are for the demonstration, not durable storage across restarts.
