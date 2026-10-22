//! Verify task span fields, parentage and scoped async instrumentation.

#![cfg(feature = "tracing")]

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use miju::{
    layers::tracing::{ContextualTaskSpan, DefaultMakeSpan, MakeSpan, TracingContext},
    prelude::*,
};
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
}

// Each recorder is scoped to one test thread, including current-thread async tests.
#[derive(Default)]
struct Recorder(Mutex<State>);

impl Recorder {
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
    fn event(&self, _: &tracing::Event<'_>) {}
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
