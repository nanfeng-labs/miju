# Changelog

## [Unreleased]

- fix(core): preserve readiness errors during pause and stop ([#44](https://github.com/nanfeng-labs/miju/pull/44))
- fix(workflow): store output serialization failures ([#43](https://github.com/nanfeng-labs/miju/pull/43))
- fix(core): scope task cancellation to its execution ([#42](https://github.com/nanfeng-labs/miju/pull/42))
- fix(core): avoid missed deque readiness notifications ([#41](https://github.com/nanfeng-labs/miju/pull/41))
- fix(workflow): check readiness on new and cloned sequential steps ([#40](https://github.com/nanfeng-labs/miju/pull/40))
- fix(workflow): return filter element decoding errors ([#38](https://github.com/nanfeng-labs/miju/pull/38))
- fix(workflow): reuse the queued fold accumulator ([#36](https://github.com/nanfeng-labs/miju/pull/36))
- fix(workflow): retain fresh task IDs after fan-out ([#35](https://github.com/nanfeng-labs/miju/pull/35))
- fix(workflow): await selected graph node readiness ([#33](https://github.com/nanfeng-labs/miju/pull/33))
- fix(workflow): start fresh graphs at their actual entry node ([#32](https://github.com/nanfeng-labs/miju/pull/32))
- fix(workflow): finish terminal delays without scheduling a successor ([#31](https://github.com/nanfeng-labs/miju/pull/31))
- fix(workflow): release canceled result waiter registrations ([#30](https://github.com/nanfeng-labs/miju/pull/30))
- fix(workflow): finish result waits with duplicate task IDs ([#28](https://github.com/nanfeng-labs/miju/pull/28))
- fix(workflow): preserve result notifications during waiter registration ([#26](https://github.com/nanfeng-labs/miju/pull/26))
- fix(core): keep interval polling timers registered ([#25](https://github.com/nanfeng-labs/miju/pull/25))
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
