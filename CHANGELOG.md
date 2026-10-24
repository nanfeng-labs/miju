# Changelog

## [Unreleased]

- fix(retry): stop retries for boxed abort errors ([#27](https://github.com/nanfeng-labs/miju/pull/27))
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
