# Changelog

## [Unreleased]

## [0.3.0](https://github.com/nanfeng-labs/miju/releases/tag/v0.3.0)

- bump: to v0.3.0 ([#146](https://github.com/nanfeng-labs/miju/pull/146))

### Upgrading to v0.3.0

This release changes stored data formats and several error and readiness contracts. Check the following before upgrading.

#### Stored formats that older versions cannot read

- **Workflow metadata.** Graph dependencies and root IDs, filter collectors and repeater predecessors now store task IDs as tagged JSON with an encoding marker, so ID variants and commas round-trip. New readers still read marker-free legacy metadata, with its existing ambiguity. Older `miju-workflow` versions cannot read the new metadata. Drain or separate queues before mixing versions, and do not roll back across this boundary. ([#110](https://github.com/nanfeng-labs/miju/pull/110))
- **CSV file storage task IDs.** New and empty CSV files store task IDs in a `task_id_v1` column that preserves integer, string and typed variants. Older versions accept such files, ignore the column and assign random IDs, and a write from an older version drops it. Upgrade every reader and writer of a file before any of them creates a new-format file. Existing files keep their legacy format and limits; they are not migrated. ([#143](https://github.com/nanfeng-labs/miju/pull/143))

#### Error and behaviour changes

- **Task registration.** A duplicate or missing task ID now returns a typed `TaskLifecycleError` before the handler runs instead of panicking, and a rejected duplicate is not acknowledged or persisted. `LifecycleService`'s associated `Future` type is now an `Either`. ([#113](https://github.com/nanfeng-labs/miju/pull/113))
- **`FileStorage::wait_for`.** Transient lock contention no longer yields `WouldBlockLock`; the query waits and retries. A poisoned lock yields the new `FileStorageError::PoisonedLock` once and ends the query. Other operations keep their lock classification. ([#142](https://github.com/nanfeng-labs/miju/pull/142))
- **`MemorySink` readiness.** A successful `poll_ready` reserves the next send for that handle across its clones until it sends, flushes, closes or drops. Code that polls one clone ready and then sends through another must release the first handle first. ([#135](https://github.com/nanfeng-labs/miju/pull/135))
- **TaskPersist.** A task without an ID now fails with `MissingTaskId` instead of panicking. ([#132](https://github.com/nanfeng-labs/miju/pull/132))
- **Extraction.** Missing task extraction data returns an error instead of panicking. ([#108](https://github.com/nanfeng-labs/miju/pull/108))
- **Memory queues.** A worker's close flushes instead of closing the shared sender, so pushes after the only worker stops succeed and stay queued instead of failing. ([#118](https://github.com/nanfeng-labs/miju/pull/118))
- **Shared backends.** Closing one handle no longer ends the backend or signals end of input to workers; stop workers explicitly. Building middleware waits for the backend lock instead of panicking under contention. ([#120](https://github.com/nanfeng-labs/miju/pull/120)) ([#124](https://github.com/nanfeng-labs/miju/pull/124))
- **Memory idempotency keys.** Keys of failed sends are released, so a retry can reuse them. A sink that stores a task and still reports an error stores the retry again unless it deduplicates. ([#122](https://github.com/nanfeng-labs/miju/pull/122))
- **File storage locks and reopening.** Under contention, `flush` and pushes may wait briefly for a clone's write instead of returning `WouldBlockLock`. Reopening a file resumes only unfinished tasks; records with a stored result, successful or failed, are not delivered again. ([#123](https://github.com/nanfeng-labs/miju/pull/123)) ([#126](https://github.com/nanfeng-labs/miju/pull/126))
- **`prometheus` feature.** It no longer pulls in `metrics-exporter-prometheus`; applications that use the exporter declare it themselves, as the module docs already describe. ([#136](https://github.com/nanfeng-labs/miju/pull/136))

### Features

- `miju` gains a `sleep` feature that forwards to `miju-core/sleep`, enabling shutdown timeouts, interval and backoff polling, and persistence through the facade. It is included in `full`. ([#127](https://github.com/nanfeng-labs/miju/pull/127))

### Fixes

- **Workers and lifecycle:** keep worker errors during forced shutdown, make `stop` and `kill` repeatable, notify every shutdown waiter and worker, finalize worker state after execution teardown, and track running tasks by `TaskId` rather than its text. ([#107](https://github.com/nanfeng-labs/miju/pull/107)) ([#114](https://github.com/nanfeng-labs/miju/pull/114)) ([#115](https://github.com/nanfeng-labs/miju/pull/115)) ([#116](https://github.com/nanfeng-labs/miju/pull/116)) ([#130](https://github.com/nanfeng-labs/miju/pull/130))
- **Circuit breaker:** keep readiness until dispatch, and run calls admitted before the circuit opened. ([#111](https://github.com/nanfeng-labs/miju/pull/111)) ([#129](https://github.com/nanfeng-labs/miju/pull/129))
- **Shared backends:** relay backend notifications to every handle, keep shared backends open when one handle closes, wait for the shared lock when building middleware, wake handles after management operations, and keep one waiter per polling operation. ([#112](https://github.com/nanfeng-labs/miju/pull/112)) ([#120](https://github.com/nanfeng-labs/miju/pull/120)) ([#124](https://github.com/nanfeng-labs/miju/pull/124)) ([#131](https://github.com/nanfeng-labs/miju/pull/131)) ([#134](https://github.com/nanfeng-labs/miju/pull/134))
- **Memory backend:** keep queues open after worker shutdown, return errors for repeated failed sends and release their idempotency keys. ([#118](https://github.com/nanfeng-labs/miju/pull/118)) ([#119](https://github.com/nanfeng-labs/miju/pull/119)) ([#122](https://github.com/nanfeng-labs/miju/pull/122))
- **Persistence:** maintain persistence while buffered tasks drain. ([#117](https://github.com/nanfeng-labs/miju/pull/117))
- **Adapters:** remember exhausted adapter inputs. ([#109](https://github.com/nanfeng-labs/miju/pull/109))
- **File storage:** return errors for repeated shared-store send failures, wait for clone locks when flushing, and skip finished records when delivering tasks. ([#121](https://github.com/nanfeng-labs/miju/pull/121)) ([#123](https://github.com/nanfeng-labs/miju/pull/123)) ([#126](https://github.com/nanfeng-labs/miju/pull/126))
- **Workflows:** run graph joins in the deepest predecessor's task. ([#125](https://github.com/nanfeng-labs/miju/pull/125))

### Documentation

- Explain how an idle `Persisted` driver fetches again. ([#128](https://github.com/nanfeng-labs/miju/pull/128))
- Align the graph context and catch-panic guides with current behaviour. ([#137](https://github.com/nanfeng-labs/miju/pull/137))
- Document `CustomBackend` fetcher, `Interleave` and `WakeOnPush` contracts, and executor ownership with `parallelize`, `timeout` and `retry`. ([#140](https://github.com/nanfeng-labs/miju/pull/140)) ([#141](https://github.com/nanfeng-labs/miju/pull/141))
- Document `Filter` page 0, acknowledgement error precedence and MsgPack trailing bytes. ([#144](https://github.com/nanfeng-labs/miju/pull/144))

### CI and tooling

- Append full changelog links to releases, check each feature and the declared rust-version (1.85, and 1.89 for typed IDs and Sentry), and limit the crates.io token to the publish steps. ([#133](https://github.com/nanfeng-labs/miju/pull/133)) ([#138](https://github.com/nanfeng-labs/miju/pull/138)) ([#139](https://github.com/nanfeng-labs/miju/pull/139))
- Stay warning-free on Rust 1.100. ([#136](https://github.com/nanfeng-labs/miju/pull/136)) ([#145](https://github.com/nanfeng-labs/miju/pull/145))

### Dependencies

- Update the workspace lockfile to sentry, sentry-tower and sentry-tracing 0.49.5. ([#104](https://github.com/nanfeng-labs/miju/pull/104)) ([#105](https://github.com/nanfeng-labs/miju/pull/105)) ([#106](https://github.com/nanfeng-labs/miju/pull/106))

#### PR breakdown

- fix: clear the remaining docsrs and rustdoc warnings ([#145](https://github.com/nanfeng-labs/miju/pull/145))
- docs: document Filter page 0, ack error precedence and MsgPack trailing bytes ([#144](https://github.com/nanfeng-labs/miju/pull/144))
- fix(file-storage): keep CSV task ID variants across reopening ([#143](https://github.com/nanfeng-labs/miju/pull/143))
- fix(file-storage): wait through lock contention in wait\_for ([#142](https://github.com/nanfeng-labs/miju/pull/142))
- docs: explain executor ownership with parallelize, timeout and retry ([#141](https://github.com/nanfeng-labs/miju/pull/141))
- docs(core): document backend wrapper and custom fetcher contracts ([#140](https://github.com/nanfeng-labs/miju/pull/140))
- ci: limit the registry token to publish steps ([#139](https://github.com/nanfeng-labs/miju/pull/139))
- ci: check each feature and the declared rust-version ([#138](https://github.com/nanfeng-labs/miju/pull/138))
- docs: align graph context and catch-panic guides ([#137](https://github.com/nanfeng-labs/miju/pull/137))
- fix(core): reserve memory sink readiness across clones ([#135](https://github.com/nanfeng-labs/miju/pull/135))
- fix(core): keep shared waiters per polling operation ([#134](https://github.com/nanfeng-labs/miju/pull/134))
- fix(core): return typed errors for missing persistence task IDs ([#132](https://github.com/nanfeng-labs/miju/pull/132))
- fix: keep the workspace warning-free on Rust 1.100 ([#136](https://github.com/nanfeng-labs/miju/pull/136))
- ci: append full changelog links to releases ([#133](https://github.com/nanfeng-labs/miju/pull/133))
- fix(core): wake shared handles after management operations ([#131](https://github.com/nanfeng-labs/miju/pull/131))
- fix(core): track running tasks by ID instead of ID text ([#130](https://github.com/nanfeng-labs/miju/pull/130))
- fix(core): run calls admitted before the circuit opened ([#129](https://github.com/nanfeng-labs/miju/pull/129))
- fix(core)!: return atomic task registration errors ([#113](https://github.com/nanfeng-labs/miju/pull/113))
- fix(workflow)!: preserve task ID identity in metadata ([#110](https://github.com/nanfeng-labs/miju/pull/110))
- fix(core): maintain persistence while buffered tasks drain ([#117](https://github.com/nanfeng-labs/miju/pull/117))
- fix(core): finalize worker state after execution teardown ([#116](https://github.com/nanfeng-labs/miju/pull/116))
- fix(core): notify every shutdown waiter and worker ([#115](https://github.com/nanfeng-labs/miju/pull/115))
- fix(core): make worker stop and kill repeatable ([#114](https://github.com/nanfeng-labs/miju/pull/114))
- fix(core): relay backend notifications to shared handles ([#112](https://github.com/nanfeng-labs/miju/pull/112))
- fix(core): preserve circuit readiness until dispatch ([#111](https://github.com/nanfeng-labs/miju/pull/111))
- fix(core): remember exhausted adapter inputs ([#109](https://github.com/nanfeng-labs/miju/pull/109))
- fix(core): return errors for missing task extraction data ([#108](https://github.com/nanfeng-labs/miju/pull/108))
- fix(core): retain worker errors during forced shutdown ([#107](https://github.com/nanfeng-labs/miju/pull/107))
- docs(core): explain how an idle Persisted driver fetches again ([#128](https://github.com/nanfeng-labs/miju/pull/128))
- feat(miju): expose the core sleep feature ([#127](https://github.com/nanfeng-labs/miju/pull/127))
- fix(file-storage): skip finished records when delivering tasks ([#126](https://github.com/nanfeng-labs/miju/pull/126))
- fix(workflow): run joins in the deepest predecessor's task ([#125](https://github.com/nanfeng-labs/miju/pull/125))
- fix(core): wait for the shared backend lock when building middleware ([#124](https://github.com/nanfeng-labs/miju/pull/124))
- fix(file-storage): wait for clone locks when flushing ([#123](https://github.com/nanfeng-labs/miju/pull/123))
- fix(core): release idempotency keys of failed memory sends ([#122](https://github.com/nanfeng-labs/miju/pull/122))
- fix(file-storage): return errors for repeated shared store send failures ([#121](https://github.com/nanfeng-labs/miju/pull/121))
- fix(core): keep shared backends open when one handle closes ([#120](https://github.com/nanfeng-labs/miju/pull/120))
- fix(core): return errors for repeated failed memory sends ([#119](https://github.com/nanfeng-labs/miju/pull/119))
- fix(core): keep memory queues open after worker shutdown ([#118](https://github.com/nanfeng-labs/miju/pull/118))
- deps(deps): bump sentry-tracing from 0.49.3 to 0.49.5 ([#106](https://github.com/nanfeng-labs/miju/pull/106))
- deps(deps): bump sentry-tower from 0.49.3 to 0.49.5 ([#105](https://github.com/nanfeng-labs/miju/pull/105))
- deps(deps): bump sentry from 0.49.3 to 0.49.5 ([#104](https://github.com/nanfeng-labs/miju/pull/104))

## [0.2.1](https://github.com/nanfeng-labs/miju/releases/tag/v0.2.1)

- bump: to v0.2.1 ([#103](https://github.com/nanfeng-labs/miju/pull/103))

### Fixes

- Preserve observed shutdown signal errors when workers finish before the shutdown timeout, and report them ahead of worker errors. ([#100](https://github.com/nanfeng-labs/miju/pull/100))
- Yield between monitored worker restarts so other tasks, workers, and shutdown signals can make progress during immediate failures. ([#102](https://github.com/nanfeng-labs/miju/pull/102))

### Upgrading to v0.2.1

`Monitor::run_with_signal` can now return `ShutdownSignal` on paths that previously returned success or a worker error after the signal had failed. Graceful shutdown still waits for workers or the configured terminator. Worker restarts yield execution without adding a timed backoff; `should_restart` continues to control the restart budget.

### Dependencies

- Update the workspace lockfile to sentry-core and sentry-types 0.49.5. ([#97](https://github.com/nanfeng-labs/miju/pull/97))
- Update the workspace lockfile to uuid 1.29.0. ([#98](https://github.com/nanfeng-labs/miju/pull/98))

## [0.2.0](https://github.com/nanfeng-labs/miju/releases/tag/v0.2.0)

- bump: to v0.2.0 ([#96](https://github.com/nanfeng-labs/miju/pull/96))

### Upgrading to v0.2.0

When upgrading from 0.1.x, check code that uses tuple inputs in graph workflows.

#### Workflow codecs

For tuples with two to twelve elements, `GraphCodec::encode` and `decode` now use `GraphFlowError` as their associated error type. ([#91](https://github.com/nanfeng-labs/miju/pull/91))

- Update error annotations and handling that expect the underlying codec error type.
- Ensure custom codec errors implement `Into<BoxDynError>`.
- Follow the error source chain when downcasting to an underlying codec error.

One-element tuples retain their existing error type. Invalid tuple input counts return `GraphFlowError::InputCountMismatch` before individual elements are decoded or the node handler is called.

#### PR breakdown

- fix(file-storage): avoid shared receiver panics during writes ([#94](https://github.com/nanfeng-labs/miju/pull/94))
- docs: fix README links and improve reading flow ([#93](https://github.com/nanfeng-labs/miju/pull/93))
- fix(core): return errors on memory sink lock contention ([#92](https://github.com/nanfeng-labs/miju/pull/92))
- fix(workflow)!: return errors for invalid tuple input counts ([#91](https://github.com/nanfeng-labs/miju/pull/91))
- fix(core): renew exhausted circuit probe budgets after cooldown ([#90](https://github.com/nanfeng-labs/miju/pull/90))
- fix(core): keep backoff polling wakeups active ([#89](https://github.com/nanfeng-labs/miju/pull/89))
- docs: add bilingual architecture guides ([#88](https://github.com/nanfeng-labs/miju/pull/88))
- fix(workflow): reject unregistered sequential steps ([#87](https://github.com/nanfeng-labs/miju/pull/87))
- fix(workflow): escape DOT node labels ([#86](https://github.com/nanfeng-labs/miju/pull/86))
- fix(workflow): propagate filter collector errors ([#85](https://github.com/nanfeng-labs/miju/pull/85))
- fix(core): preserve selected monitor termination errors ([#84](https://github.com/nanfeng-labs/miju/pull/84))
- fix(sentry): finish transactions when task futures are dropped ([#83](https://github.com/nanfeng-labs/miju/pull/83))
- fix(workflow): reject overflowing graph progress ([#82](https://github.com/nanfeng-labs/miju/pull/82))
- fix(retry): bound failures before task execution ([#81](https://github.com/nanfeng-labs/miju/pull/81))
- fix(core): finish retry readiness during shutdown ([#80](https://github.com/nanfeng-labs/miju/pull/80))
- fix(file-storage): preserve CSV completion results ([#79](https://github.com/nanfeng-labs/miju/pull/79))
- fix(core): preserve shared persistence event channels ([#78](https://github.com/nanfeng-labs/miju/pull/78))
- fix(core): finalize interrupted task executions ([#77](https://github.com/nanfeng-labs/miju/pull/77))
- fix(core): share worker names across restarts ([#76](https://github.com/nanfeng-labs/miju/pull/76))
- fix(workflow): reject invalid graph continuation indices ([#75](https://github.com/nanfeng-labs/miju/pull/75))
- fix(workflow): complete empty folds with their accumulator ([#74](https://github.com/nanfeng-labs/miju/pull/74))
- fix(core): preserve the full pagination offset range ([#73](https://github.com/nanfeng-labs/miju/pull/73))
- fix(core): accept single-extractor handler validation ([#72](https://github.com/nanfeng-labs/miju/pull/72))
- fix(workflow): preserve predecessor IDs in fan-out joins ([#71](https://github.com/nanfeng-labs/miju/pull/71))
- fix(workflow): preserve predecessor IDs for single-edge joins ([#70](https://github.com/nanfeng-labs/miju/pull/70))
- fix(file-storage): preserve CSV metadata keys on reopen ([#69](https://github.com/nanfeng-labs/miju/pull/69))
- fix(file-storage): preserve application line metadata ([#68](https://github.com/nanfeng-labs/miju/pull/68))

### Breaking Changes

- workflow: Tuple `GraphCodec` implementations with two to twelve elements now return `GraphFlowError` and require codec errors convertible to `BoxDynError`. ([#91](https://github.com/nanfeng-labs/miju/pull/91))

## [0.1.1](https://github.com/nanfeng-labs/miju/releases/tag/v0.1.1)

- bump: to v0.1.1 ([#67](https://github.com/nanfeng-labs/miju/pull/67))

### Fixes

- **Backends**: Restore wakeups under contention, keep empty deque consumers waiting, drain accepted tasks before flush and close, respect pipe readiness, and serialize in-memory idempotency checks.
- **Task lifecycle**: Wake execution waiters and canceled tasks, scope cancellation to each execution, release task registrations after handler panics, and finish worker shutdown after source exhaustion.
- **Middleware and scheduling**: Honor circuit breaker thresholds and recovery timers, stop retries for boxed abort errors, preserve readiness errors, and reject overflowing scheduling units.
- **Workflow execution**: Honor step and graph-node readiness, start graphs at their entry node, finish terminal delays, reuse fold accumulators, and validate step indices, repeat counters, and graph flags.
- **Workflow results**: Preserve notifications and fan-out task IDs, finish waits with duplicate IDs, release canceled waiters, and report decoding and serialization errors.
- **Storage and observability**: Preserve completion queries under contention, report failed or undecodable file-storage results accurately, and retain Sentry error chains and full attempt counts.

## [0.1.0](https://github.com/nanfeng-labs/miju/releases/tag/v0.1.0)

- bump: to v0.1.0 ([#18](https://github.com/nanfeng-labs/miju/pull/18))

### Initial release

- **Tasks**: Typed async handlers, worker lifecycle management, and monitor coordination.
- **Middleware**: Retries, timeouts, concurrency and rate limits, and panic handling.
- **Workflows**: Sequential steps and dependency graph execution.
- **Data and storage**: JSON, MessagePack, and Bincode codecs, with JSON and CSV file backends.
- **Observability**: Tracing, Prometheus, OpenTelemetry, and Sentry integrations.

---

> This **CHANGELOG** is a distilled view of the many commits shown in the log. If you need more granular details (e.g., every single merge or patch commit), you can expand each section to list additional commits or consult the full Git history.
