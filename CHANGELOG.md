# Changelog

## [Unreleased]

- fix(workflow)!: return errors for invalid tuple input counts ([#91](https://github.com/nanfeng-labs/miju/pull/91))
- docs: add bilingual architecture guides ([#88](https://github.com/nanfeng-labs/miju/pull/88))
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
- fix(workflow): reject unregistered sequential steps ([#87](https://github.com/nanfeng-labs/miju/pull/87))
- fix(workflow): escape DOT node labels ([#86](https://github.com/nanfeng-labs/miju/pull/86))

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
