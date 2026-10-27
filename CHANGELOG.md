# Changelog

## [Unreleased]

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
