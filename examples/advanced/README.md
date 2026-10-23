# Advanced worker composition

Run `RUST_LOG=debug cargo run -p advanced --locked` from the workspace root. Ten queued emails share a simulated client and validation cache. No email or network request is sent.

The example combines:

- A custom `Layer` and `Service` that inject `EmailClient` into task data and use `poll_ready` to pause consumption when its shared quota reaches zero. It is placed outside spawning and retries, so it gates new admissions in the worker driver rather than parking a retry in a separate task.
- An `Arc`-backed client with an atomic budget of ten calls. Validation plus delivery costs two calls; a valid cache hit costs one; an invalid cache hit returns a typed error.
- A shared cache that copies entries out of its mutex before any asynchronous work.
- `TaskContext::executed()` inside a detached task, followed by a simulated one-second cache refresh. The follow-up carries the current tracing span and does not delay the handler's return.
- CPU-based concurrency, spawned task futures, three retries, a custom panic-to-`AbortError` conversion, tracing, and worker event handling.
- Ctrl+C shutdown while the service is waiting for more API capacity.

The initial batch contains ten different addresses and an empty cache, so it exercises cache misses. Its ten-call budget allows five validated deliveries. Already admitted calls can report quota errors; further work waits once the middleware observes an empty budget. No refill occurs. A real refill implementation must retain and wake the readiness waker.

The handler deliberately calls `TaskContext::cancel()` after its simulated delivery. The worker then reports `task cannot be completed: already cancelled`; those task errors are expected. `executed()` wakes on cancellation as well as completion; it does not prove successful delivery, and cancelling a task cannot roll back external side effects. Cache refresh is independent of worker shutdown: exiting before it finishes may discard it. Press Ctrl+C after the refresh messages to see the complete demonstration.

`cargo test -p advanced --locked` checks the shared quota under concurrent calls and exercises both valid and invalid cache hits through a worker.
