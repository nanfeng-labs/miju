# Changelog

## [Unreleased]

- fix(core): honor inner sink readiness after startup hook ([#23](https://github.com/nanfeng-labs/miju/pull/23))
- fix(core): wake contended memory sink operations ([#64](https://github.com/nanfeng-labs/miju/pull/64))
- fix(core): drain accepted tasks before persisted flush completes ([#39](https://github.com/nanfeng-labs/miju/pull/39))
- fix(core): finish worker shutdown after source exhaustion ([#29](https://github.com/nanfeng-labs/miju/pull/29))
- fix(core): wake pending tasks on cancellation ([#62](https://github.com/nanfeng-labs/miju/pull/62))
- fix(core): wake subtasks when their parent finishes ([#60](https://github.com/nanfeng-labs/miju/pull/60))
- fix(core): honor configured circuit breaker thresholds ([#63](https://github.com/nanfeng-labs/miju/pull/63))
- fix(core): wake circuit readiness after recovery timeout ([#61](https://github.com/nanfeng-labs/miju/pull/61))
- fix(core): notify every task execution waiter ([#46](https://github.com/nanfeng-labs/miju/pull/46))
- fix(sentry): preserve the task error source chain ([#37](https://github.com/nanfeng-labs/miju/pull/37))
- fix(sentry): preserve the full task attempt count ([#34](https://github.com/nanfeng-labs/miju/pull/34))
- fix(retry): stop retries for boxed abort errors ([#27](https://github.com/nanfeng-labs/miju/pull/27))
- fix(file-storage): return completion decode errors ([#56](https://github.com/nanfeng-labs/miju/pull/56))
- fix(file-storage): preserve completion queries during contention ([#55](https://github.com/nanfeng-labs/miju/pull/55))
- fix(file-storage): report failed completion results accurately ([#24](https://github.com/nanfeng-labs/miju/pull/24))
- fix(core): release task registration on synchronous call panic ([#22](https://github.com/nanfeng-labs/miju/pull/22))
- fix(core): honor destination readiness when polling pipes ([#21](https://github.com/nanfeng-labs/miju/pull/21))
- fix(core): restore shared backend wakeups under contention ([#20](https://github.com/nanfeng-labs/miju/pull/20))
- fix(core): drain shared buffers before flush and close ([#19](https://github.com/nanfeng-labs/miju/pull/19))

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
