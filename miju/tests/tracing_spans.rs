//! Verify task spans, event callbacks, and scoped middleware instrumentation.

#![cfg(feature = "tracing")]

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use miju::{
    layers::tracing::{
        ContextualTaskSpan, DefaultMakeSpan, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse,
        LatencyUnit, MakeSpan, OnFailure, OnRequest, OnResponse, Trace, TraceLayer, TracingContext,
    },
    prelude::*,
};
use tower::{Layer, Service, ServiceExt};
use tracing::{
    Instrument, Level, Metadata as SpanMetadata, Span, Subscriber,
    field::{Field, Visit},
    instrument::WithSubscriber,
    span::{Attributes, Id, Record},
};
use tracing_core::span::Current;

#[derive(Clone, Debug)]
struct RecordedSpan {
    metadata: &'static SpanMetadata<'static>,
    parent: Option<Id>,
    fields: Fields,
}

#[derive(Clone, Debug)]
struct RecordedEvent {
    level: Level,
    parent: Option<Id>,
    fields: Fields,
}

#[derive(Clone, Debug, Default)]
struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

#[derive(Default)]
struct State {
    spans: Vec<(Id, RecordedSpan)>,
    entered: Vec<Id>,
    events: Vec<RecordedEvent>,
}

// Each recorder is scoped to one test thread, including current-thread async tests.
#[derive(Default)]
struct Recorder(Mutex<State>);

impl Recorder {
    fn spans(&self) -> Vec<(Id, RecordedSpan)> {
        self.0.lock().unwrap().spans.clone()
    }

    fn events(&self) -> Vec<RecordedEvent> {
        self.0.lock().unwrap().events.clone()
    }

    fn span(&self, id: &Id) -> RecordedSpan {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .find(|(key, _)| key == id)
            .unwrap()
            .1
            .clone()
    }
}

impl Subscriber for Recorder {
    fn enabled(&self, _: &SpanMetadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut state = self.0.lock().unwrap();
        let id = Id::from_u64(state.spans.len() as u64 + 1);
        let parent = if attributes.is_contextual() {
            state.entered.last().cloned()
        } else {
            attributes.parent().cloned()
        };
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        state.spans.push((
            id.clone(),
            RecordedSpan {
                metadata: attributes.metadata(),
                parent,
                fields,
            },
        ));
        id
    }

    fn record(&self, id: &Id, values: &Record<'_>) {
        let mut state = self.0.lock().unwrap();
        let (_, span) = state.spans.iter_mut().find(|(key, _)| key == id).unwrap();
        values.record(&mut span.fields);
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut state = self.0.lock().unwrap();
        let parent = if event.is_contextual() {
            state.entered.last().cloned()
        } else {
            event.parent().cloned()
        };
        let mut fields = Fields::default();
        event.record(&mut fields);
        state.events.push(RecordedEvent {
            level: *event.metadata().level(),
            parent,
            fields,
        });
    }
    fn enter(&self, id: &Id) {
        self.0.lock().unwrap().entered.push(id.clone());
    }
    fn exit(&self, id: &Id) {
        assert_eq!(self.0.lock().unwrap().entered.pop().as_ref(), Some(id));
    }

    fn current_span(&self) -> Current {
        let state = self.0.lock().unwrap();
        match state.entered.last() {
            Some(id) => {
                let (_, span) = state.spans.iter().find(|(key, _)| key == id).unwrap();
                Current::new(id.clone(), span.metadata)
            }
            None => Current::none(),
        }
    }
}

#[test]
fn factories_record_task_fields_levels_and_current_parent() {
    let recorder = Arc::new(Recorder::default());
    tracing::subscriber::with_default(recorder.clone(), || {
        let parent = tracing::info_span!("parent");
        let _entered = parent.enter();
        for attempt in [0, 1, 4] {
            let task = TaskBuilder::new(())
                .task_id("span-task".to_owned().into())
                .attempt(attempt)
                .build();
            for level in [
                Level::ERROR,
                Level::WARN,
                Level::INFO,
                Level::DEBUG,
                Level::TRACE,
            ] {
                let spans = [
                    DefaultMakeSpan::new().level(level).make_span(&task),
                    ContextualTaskSpan::new().level(level).make_span(&task),
                ];
                for span in spans {
                    let recorded = recorder.span(&span.id().unwrap());
                    assert_eq!(recorded.metadata.name(), "task");
                    assert_eq!(*recorded.metadata.level(), level);
                    assert_eq!(recorded.parent, parent.id());
                    assert_eq!(recorded.fields.0["task_id"], "span-task");
                    assert_eq!(recorded.fields.0["attempt"], attempt.max(1).to_string());
                }
            }
            for span in [
                DefaultMakeSpan::default().make_span(&task),
                ContextualTaskSpan::default().make_span(&task),
            ] {
                assert_eq!(*span.metadata().unwrap().level(), Level::DEBUG);
            }
            assert_eq!(task.attempt(), attempt);
        }
    });
}

#[test]
fn fixed_spans_and_mutable_closures_are_factories() {
    let recorder = Arc::new(Recorder::default());
    tracing::subscriber::with_default(recorder.clone(), || {
        let task = Task::new(7usize);
        let mut fixed = tracing::info_span!("fixed");
        assert_eq!(fixed.make_span(&task).id(), fixed.id());
        let mut calls = 0;
        let mut factory = |task: &Task<usize>| {
            calls += 1;
            tracing::info_span!("custom", amount = task.args, call = calls)
        };
        for expected in [1, 2] {
            let span = factory.make_span(&task);
            let recorded = recorder.span(&span.id().unwrap());
            assert_eq!(recorded.fields.0["amount"], "7");
            assert_eq!(recorded.fields.0["call"], expected.to_string());
        }
        assert_eq!(calls, 2);
    });
}

#[test]
fn metadata_remains_available_while_contextual_spans_use_the_active_parent() {
    let recorder = Arc::new(Recorder::default());
    tracing::subscriber::with_default(recorder.clone(), || {
        let context: miju_core::task::metadata::TracingContext = TracingContext::new()
            .with_trace_id("remote-trace")
            .with_span_id("remote-span");
        let task = TaskBuilder::new(())
            .task_id("context-task".to_owned().into())
            .metadata(&context)
            .build();
        let parent = tracing::info_span!("local-parent");
        let span = parent.in_scope(|| ContextualTaskSpan::new().make_span(&task));
        let recorded = recorder.span(&span.id().unwrap());
        assert_eq!(recorded.parent, parent.id());
        assert_eq!(recorded.fields.0.len(), 2);
        let extracted = TracingContext::extract(task.metadata()).unwrap();
        assert_eq!(extracted.trace_id().as_deref(), Some("remote-trace"));
        assert_eq!(extracted.span_id().as_deref(), Some("remote-span"));
    });
}

#[test]
fn built_in_factories_require_ids_even_without_a_subscriber() {
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        assert!(
            std::panic::catch_unwind(|| DefaultMakeSpan::new().make_span(&Task::new(()))).is_err()
        );
        assert!(
            std::panic::catch_unwind(|| ContextualTaskSpan::new().make_span(&Task::new(())))
                .is_err()
        );
    });
}

#[tokio::test]
async fn generated_span_is_current_on_each_poll_and_exits_afterwards() {
    let recorder = Arc::new(Recorder::default());
    async {
        let task = TaskBuilder::new(7usize)
            .task_id("async-task".to_owned().into())
            .build();
        let span = DefaultMakeSpan::new().make_span(&task);
        let id = span.id();
        let result = async {
            assert_eq!(Span::current().id(), id);
            tokio::task::yield_now().await;
            assert_eq!(Span::current().id(), id);
            task.args * 2
        }
        .instrument(span)
        .await;
        assert_eq!(result, 14);
        assert!(Span::current().id().is_none());
    }
    .with_subscriber(recorder)
    .await;
}

#[test]
fn default_callbacks_emit_the_selected_levels_units_and_results() {
    use std::{io, time::Duration};

    let recorder = Arc::new(Recorder::default());
    tracing::subscriber::with_default(recorder.clone(), || {
        let span = tracing::info_span!("callbacks");
        let _entered = span.enter();
        let task = Task::new(());
        let error = io::Error::other("failed");
        let duration = Duration::from_nanos(1_250_000_000);
        let levels = [
            Level::ERROR,
            Level::WARN,
            Level::INFO,
            Level::DEBUG,
            Level::TRACE,
        ];
        for level in levels {
            DefaultOnRequest::new()
                .level(level)
                .on_request(&task, &span);
            for unit in [
                LatencyUnit::Seconds,
                LatencyUnit::Millis,
                LatencyUnit::Micros,
                LatencyUnit::Nanos,
            ] {
                DefaultOnResponse::new()
                    .level(level)
                    .latency_unit(unit)
                    .on_response(&"ok", duration, &span);
                DefaultOnFailure::new()
                    .level(level)
                    .latency_unit(unit)
                    .on_failure(&error, duration, &span);
            }
        }
        let events = recorder.events();
        assert_eq!(events.len(), 45);
        for (group, level) in events.chunks_exact(9).zip(levels) {
            assert!(
                group
                    .iter()
                    .all(|event| event.level == level && event.parent == span.id())
            );
            assert_eq!(group[0].fields.0["message"], "task.start");
            for (pair, expected) in
                group[1..]
                    .chunks_exact(2)
                    .zip(["1.25s", "1250ms", "1250000μs", "1250000000ns"])
            {
                assert_eq!(pair[0].fields.0["message"], "task.done");
                assert_eq!(pair[0].fields.0["result"], "\"ok\"");
                assert_eq!(pair[1].fields.0["message"], "task.failed");
                assert_eq!(pair[1].fields.0["result"], "failed");
                assert!(
                    pair.iter()
                        .all(|event| event.fields.0["done_in"] == expected)
                );
            }
        }
        DefaultOnRequest::default().on_request(&task, &span);
        DefaultOnResponse::default().on_response(&7, duration, &span);
        DefaultOnFailure::default().on_failure(&error, duration, &span);
        let events = recorder.events();
        assert_eq!(events[45].level, Level::DEBUG);
        assert_eq!(events[46].level, Level::DEBUG);
        assert_eq!(events[47].level, Level::ERROR);
        assert_eq!(events[46].fields.0["done_in"], "1250ms");
        assert_eq!(events[47].fields.0["done_in"], "1250ms");
    });
}

#[tokio::test]
async fn middleware_scopes_call_and_polls_and_preserves_success_and_error() {
    use std::{io, time::Duration};

    let recorder = Arc::new(Recorder::default());
    async {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let make_log = |stage: &'static str| {
            let calls = calls.clone();
            move |span: &Span| {
                assert_eq!(Span::current().id(), span.id());
                calls.lock().unwrap().push((stage, span.id()));
            }
        };
        let request_log = make_log("request");
        let response_log = make_log("response");
        let failure_log = make_log("failure");
        let inner_calls = calls.clone();
        let mut request_count = 0;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_counts = requests.clone();
        let layer = TraceLayer::new()
            .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
            .on_request(move |_: &Task<bool>, span: &Span| {
                request_count += 1;
                request_counts.lock().unwrap().push(request_count);
                request_log(span);
            })
            .on_response(move |result: &u32, latency: Duration, span: &Span| {
                assert_eq!(*result, 14);
                assert!(latency >= Duration::from_millis(2));
                response_log(span);
            })
            .on_failure(move |error: &io::Error, latency: Duration, span: &Span| {
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                assert_eq!(error.to_string(), "denied");
                assert!(latency >= Duration::from_millis(2));
                failure_log(span);
            });
        let mut service = layer.layer(tower::service_fn(move |task: Task<bool>| {
            let span = Span::current();
            inner_calls.lock().unwrap().push(("call", span.id()));
            let calls = inner_calls.clone();
            async move {
                assert_eq!(Span::current().id(), span.id());
                calls.lock().unwrap().push(("poll", span.id()));
                tokio::task::yield_now().await;
                assert_eq!(Span::current().id(), span.id());
                calls.lock().unwrap().push(("resumed", span.id()));
                if task.args {
                    Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"))
                } else {
                    Ok(14)
                }
            }
        }));
        for fail in [false, true] {
            service.ready().await.unwrap();
            assert!(Span::current().id().is_none());
            let future = service.call(
                TaskBuilder::new(fail)
                    .task_id(fail.to_string().into())
                    .build(),
            );
            assert!(Span::current().id().is_none());
            tokio::time::sleep(Duration::from_millis(2)).await;
            let result = future.await;
            assert!(Span::current().id().is_none());
            if fail {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
            } else {
                assert_eq!(result.unwrap(), 14);
            }
        }
        assert_eq!(*requests.lock().unwrap(), [1, 2]);
        let calls = calls.lock().unwrap();
        for (group, final_stage) in calls.chunks_exact(5).zip(["response", "failure"]) {
            assert_eq!(
                group.iter().map(|(stage, _)| *stage).collect::<Vec<_>>(),
                ["request", "call", "poll", "resumed", final_stage]
            );
            assert!(group[0].1.is_some());
            assert!(group.iter().all(|(_, id)| *id == group[0].1));
        }
        assert_eq!(calls.len(), 10);
        assert_ne!(calls[0].1, calls[5].1);
    }
    .with_subscriber(recorder)
    .await;
}

#[tokio::test]
async fn readiness_errors_and_dropped_futures_skip_completion_hooks() {
    use std::{
        future::{Future, Ready, pending},
        io,
        pin::Pin,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    struct ReadinessError;
    impl Service<Task<()>> for ReadinessError {
        type Response = ();
        type Error = io::Error;
        type Future = Ready<Result<(), io::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err(io::Error::other("not ready")))
        }
        fn call(&mut self, _: Task<()>) -> Self::Future {
            panic!("a readiness error must prevent call")
        }
    }
    let recorder = Arc::new(Recorder::default());
    async {
        let error = Trace::new(ReadinessError).ready().await.err().unwrap();
        assert_eq!(error.to_string(), "not ready");
        assert!(recorder.spans().is_empty());
        assert!(recorder.events().is_empty());
        let completions = Arc::new(Mutex::new(0));
        let response_count = completions.clone();
        let failure_count = completions.clone();
        let mut traced = Trace::new(tower::service_fn(|_: Task<()>| {
            pending::<Result<(), io::Error>>()
        }))
        .on_response(move |_: &(), _: Duration, _: &Span| {
            *response_count.lock().unwrap() += 1;
        })
        .on_failure(move |_: &io::Error, _: Duration, _: &Span| {
            *failure_count.lock().unwrap() += 1;
        });
        let mut future = Box::pin(
            traced.call(
                TaskBuilder::new(())
                    .task_id("dropped".to_owned().into())
                    .build(),
            ),
        );
        assert!(
            Pin::as_mut(&mut future)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(Span::current().id().is_none());
        drop(future);
        assert_eq!(*completions.lock().unwrap(), 0);
        let events = recorder.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields.0["message"], "task.start");
        assert!(Span::current().id().is_none());
    }
    .with_subscriber(recorder.clone())
    .await;
}

#[tokio::test]
async fn direct_trace_supports_consuming_response_hooks_and_silent_callbacks() {
    use std::{convert::Infallible, time::Duration};

    struct Response(u32);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let output = observed.clone();
    let label = String::from("consumed");
    let mut service = Trace::new(tower::service_fn(|task: Task<u32>| async move {
        Ok::<_, Infallible>(Response(task.args))
    }))
    .make_span_with(|_: &Task<u32>| Span::none())
    .on_request(|_: &Task<u32>, _: &Span| {})
    .on_failure(())
    .on_response(move |response: &Response, _: Duration, _: &Span| {
        output.lock().unwrap().push((label, response.0));
    });
    for value in [7, 9] {
        let result = service
            .ready()
            .await
            .unwrap()
            .call(Task::new(value))
            .await
            .unwrap();
        assert_eq!(result.0, value);
    }
    assert_eq!(
        *observed.lock().unwrap(),
        [("consumed".to_owned(), 7), ("consumed".to_owned(), 9)]
    );
    let mut silent = service.on_response(());
    assert_eq!(
        silent
            .ready()
            .await
            .unwrap()
            .call(Task::new(11))
            .await
            .unwrap()
            .0,
        11
    );
    assert_eq!(observed.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn worker_builder_tracing_reports_success_and_failure() {
    use std::{io, time::Duration};

    for fail in [false, true] {
        let recorder = Arc::new(Recorder::default());
        async {
            let mut storage = MemoryStorage::new();
            let task = TaskBuilder::new(fail)
                .task_id("worker-tracing".to_owned().into())
                .build();
            let observed = task.clone();
            storage.push_task(task).await.unwrap();
            let worker = WorkerBuilder::new("traced")
                .backend(storage)
                .enable_tracing()
                .on_event(move |worker, event| match event {
                    Event::Success(value) => {
                        assert!(!fail);
                        assert_eq!(value.downcast_ref::<u32>(), Some(&14));
                        worker.stop().unwrap();
                    }
                    Event::Error(error) => {
                        assert!(fail);
                        assert!(error.downcast_ref::<io::Error>().is_some());
                        worker.stop().unwrap();
                    }
                    _ => {}
                })
                .build(|fail: bool| async move {
                    let span = Span::current();
                    assert_eq!(span.metadata().unwrap().name(), "task");
                    tokio::task::yield_now().await;
                    assert_eq!(Span::current().id(), span.id());
                    if fail {
                        Err(io::Error::other("worker failed"))
                    } else {
                        Ok(14u32)
                    }
                });
            tokio::time::timeout(Duration::from_secs(5), worker.run())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(observed.attempt(), 1);
            assert_eq!(
                observed.status(),
                if fail { Status::Failed } else { Status::Done }
            );
            let spans = recorder.spans();
            let (id, span) = spans
                .iter()
                .find(|(_, span)| span.metadata.name() == "task")
                .unwrap();
            assert_eq!(span.fields.0["task_id"], "worker-tracing");
            let events: Vec<_> = recorder
                .events()
                .into_iter()
                .filter(|event| {
                    event.fields.0.get("message").is_some_and(|m| {
                        matches!(m.as_str(), "task.start" | "task.done" | "task.failed")
                    })
                })
                .collect();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].fields.0["message"], "task.start");
            assert_eq!(
                events[1].fields.0["message"],
                if fail { "task.failed" } else { "task.done" }
            );
            assert_eq!(
                events[1].fields.0["result"],
                if fail { "worker failed" } else { "14" }
            );
            assert!(events.iter().all(|event| event.parent.as_ref() == Some(id)));
            assert!(Span::current().id().is_none());
        }
        .with_subscriber(recorder.clone())
        .await;
    }
}

#[tokio::test]
async fn custom_worker_callbacks_record_fields_and_observe_results() {
    use std::time::Duration;

    let recorder = Arc::new(Recorder::default());
    async {
        let mut storage = MemoryStorage::new();
        storage.push(7usize).await.unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let requests = observed.clone();
        let responses = observed.clone();
        let worker = WorkerBuilder::new("custom-tracing")
            .backend(storage)
            .layer(
                TraceLayer::new()
                    .make_span_with(|_: &Task<usize>| {
                        tracing::info_span!("calculation", value = tracing::field::Empty)
                    })
                    .on_request(move |task: &Task<usize>, span: &Span| {
                        span.record("value", task.args);
                        requests.lock().unwrap().push(task.args);
                    })
                    .on_response(move |value: &usize, _: Duration, span: &Span| {
                        assert_eq!(Span::current().id(), span.id());
                        responses.lock().unwrap().push(*value);
                    })
                    .on_failure(|error: &BoxDynError, _: Duration, _: &Span| {
                        panic!("unexpected failure: {error}");
                    }),
            )
            .build(|value: usize, worker: WorkerContext| async move {
                worker.stop().unwrap();
                value * 2
            });
        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*observed.lock().unwrap(), [7, 14]);
        let spans = recorder.spans();
        let (_, span) = spans
            .iter()
            .find(|(_, span)| span.metadata.name() == "calculation")
            .unwrap();
        assert_eq!(span.fields.0["value"], "7");
    }
    .with_subscriber(recorder.clone())
    .await;
}

#[cfg(feature = "retry")]
#[tokio::test]
async fn retry_layer_order_controls_per_attempt_or_combined_reporting() {
    use std::{io, time::Duration};

    for per_attempt in [false, true] {
        let recorder = Arc::new(Recorder::default());
        async {
            let mut storage = MemoryStorage::new();
            let task = TaskBuilder::new(())
                .task_id("retried".to_owned().into())
                .build();
            let observed = task.clone();
            storage.push_task(task).await.unwrap();
            let builder = WorkerBuilder::new("retry-tracing")
                .backend(storage)
                .on_event(|worker, event| match event {
                    Event::Success(_) => worker.stop().unwrap(),
                    Event::Error(error) => panic!("unexpected final error: {error}"),
                    _ => {}
                });
            let handler = |(): (), attempt: Attempt| async move {
                if attempt.current() < 3 {
                    Err(io::Error::other("try again"))
                } else {
                    Ok(())
                }
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                if per_attempt {
                    builder
                        .retry(RetryPolicy::retries(2))
                        .enable_tracing()
                        .build(handler)
                        .run()
                        .await
                        .unwrap();
                } else {
                    builder
                        .enable_tracing()
                        .retry(RetryPolicy::retries(2))
                        .build(handler)
                        .run()
                        .await
                        .unwrap();
                }
            })
            .await
            .unwrap();
            assert_eq!(observed.attempt(), 3);
            assert_eq!(observed.status(), Status::Done);
            let spans = recorder.spans();
            assert_eq!(
                spans
                    .iter()
                    .filter(|(_, span)| span.metadata.name() == "task")
                    .count(),
                if per_attempt { 3 } else { 1 }
            );
            let events: Vec<_> = recorder
                .events()
                .into_iter()
                .filter_map(|event| {
                    event
                        .fields
                        .0
                        .get("message")
                        .filter(|m| {
                            matches!(m.as_str(), "task.start" | "task.done" | "task.failed")
                        })
                        .cloned()
                })
                .collect();
            let expected = if per_attempt {
                vec![
                    "task.start",
                    "task.failed",
                    "task.start",
                    "task.failed",
                    "task.start",
                    "task.done",
                ]
            } else {
                vec!["task.start", "task.done"]
            };
            assert_eq!(events, expected);
        }
        .with_subscriber(recorder.clone())
        .await;
    }
}
