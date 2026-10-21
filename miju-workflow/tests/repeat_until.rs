//! Repetition state, payload preservation, and continuation through public APIs.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Context,
    time::Duration,
};

use futures_util::{future::poll_fn, task::noop_waker_ref};
use miju_core::{
    backend::{Backend, TaskSink, ext::shared::Shared},
    error::BoxDynError,
    task::{
        Task,
        builder::TaskBuilder,
        metadata::{Meta, Metadata, MetadataStore},
        task_id::{GenerateId, RandomId, TaskId},
    },
    worker::{builder::WorkerBuilder, context::WorkerContext, service::IntoWorkerService},
};
use miju_workflow::{
    SteppedFlow,
    in_memory::InMemoryWorkflow,
    sequential::{
        GoTo, WorkflowContext,
        repeat_until::{RepeaterState, RepeaterStateError},
    },
};
use serde_json::json;
use tower::ServiceExt;

type BackendHandle = Shared<InMemoryWorkflow<u32>>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn next_task(backend: &mut BackendHandle) -> Task<Vec<u8>> {
    let worker = WorkerContext::new("inspect-repeat");
    tokio::time::timeout(
        DEADLINE,
        poll_fn(|cx| Backend::poll_next(backend, cx, &worker)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

fn assert_queue_empty(backend: &mut BackendHandle) {
    assert!(
        Backend::poll_next(
            backend,
            &mut Context::from_waker(noop_waker_ref()),
            &WorkerContext::new("empty"),
        )
        .is_pending()
    );
}

#[tokio::test]
async fn consecutive_repeaters_reset_progress_and_compose_with_typed_steps() {
    let backend = InMemoryWorkflow::create();
    let mut producer = backend.clone();
    producer.push(6u32).await.unwrap();
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let first = first_calls.clone();
    let second = second_calls.clone();
    let flow = SteppedFlow::new("repeat-chain")
        .and_then(async |value: u32| value + 1)
        .repeat_until(move |value: u32, Meta(state): Meta<RepeaterState>| {
            assert_eq!(state.iterations(), first.fetch_add(1, Ordering::SeqCst));
            assert_eq!(value, 7);
            async move { (state.iterations() >= 2).then_some(value + 1) }
        })
        .repeat_until(move |value: u32, Meta(state): Meta<RepeaterState>| {
            assert_eq!(state.iterations(), second.fetch_add(1, Ordering::SeqCst));
            assert_eq!(value, 8);
            async move { (state.iterations() >= 1).then_some(value) }
        })
        .and_then(async |value: u32| value.to_string())
        .and_then(async |value: String, worker: WorkerContext| {
            assert_eq!(value, "8");
            worker.stop().unwrap();
        });
    let worker = WorkerBuilder::new("repeat-chain")
        .backend(backend)
        .build(flow);
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_calls.load(Ordering::SeqCst), 3);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn repetition_preserves_wire_input_and_tracks_ids_before_advancing() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut reader = backend.clone();
    let flow = SteppedFlow::new("repeat-wire")
        .and_then(async |value: u32| value)
        .repeat_until(async |value: u32, Meta(state): Meta<RepeaterState>| {
            (state.iterations() >= 2).then_some(value + 10)
        })
        .and_then(async |value: u32| value.to_string());
    let service = flow.into_service(backend).service;
    let id = RandomId::generate();
    let bytes = b" 7 \n".to_vec();
    let mut metadata = MetadataStore::new();
    metadata.insert("request-tag", "original").unwrap();
    let mut task = TaskBuilder::new(bytes.clone())
        .task_id(id.clone())
        .with_metadata(metadata)
        .metadata(&WorkflowContext { step_index: 1 })
        .build();
    let mut unique = HashSet::from([id]);
    for iteration in 1..=2 {
        let previous_id = task.task_id().cloned().unwrap();
        let GoTo::Break(response) = service.clone().oneshot(task).await.unwrap() else {
            panic!("expected a repetition")
        };
        assert_eq!(response.result, json!(null));
        task = next_task(&mut reader).await;
        assert_eq!(task.task_id(), response.next_task_id.as_ref());
        assert!(unique.insert(task.task_id().cloned().unwrap()));
        assert_eq!(task.args, bytes);
        assert_eq!(
            WorkflowContext::extract(task.metadata())
                .unwrap()
                .step_index,
            1
        );
        let state = RepeaterState::extract(task.metadata()).unwrap();
        assert_eq!(state.iterations(), iteration);
        assert_eq!(state.previous_task_id(), Some(&previous_id));
        assert!(task.metadata().get("request-tag").is_none());
        assert_queue_empty(&mut reader);
    }
    let GoTo::Next(response) = service.clone().oneshot(task).await.unwrap() else {
        panic!("expected the next workflow step")
    };
    assert_eq!(response.result, json!(17));
    let next = next_task(&mut reader).await;
    assert_eq!(next.task_id(), response.next_task_id.as_ref());
    assert!(unique.insert(next.task_id().cloned().unwrap()));
    assert_eq!(serde_json::from_slice::<u32>(&next.args).unwrap(), 17);
    assert_eq!(
        WorkflowContext::extract(next.metadata())
            .unwrap()
            .step_index,
        2
    );
    assert_eq!(
        RepeaterState::extract(next.metadata()).unwrap(),
        RepeaterState::default()
    );
    assert!(
        next.metadata()
            .get("miju_workflow.repeater.iterations")
            .is_none()
    );
    assert!(matches!(service.oneshot(next).await.unwrap(), GoTo::Done));
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn terminal_repeater_retains_a_different_output_type_without_enqueueing() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut reader = backend.clone();
    let flow = SteppedFlow::new("terminal")
        .repeat_until(async |value: u32| Some(format!("value-{value}")));
    let service = flow.into_service(backend).service;
    let GoTo::Break(response) = service.oneshot(Task::new(b"9".to_vec())).await.unwrap() else {
        panic!("expected a terminal result")
    };
    assert_eq!(response.result, json!("value-9"));
    assert!(response.next_task_id.is_none());
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn decoding_and_handler_errors_do_not_schedule_another_iteration() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut reader = backend.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let flow = SteppedFlow::new("errors")
        .repeat_until(move |_: u32| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Err::<Option<u32>, BoxDynError>("repeat failed".into()) }
        })
        .and_then(async |value: u32| value);
    let service = flow.into_service(backend).service;
    assert!(
        service
            .clone()
            .oneshot(Task::new(b"not-json".to_vec()))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let error = service.oneshot(Task::new(b"7".to_vec())).await.unwrap_err();
    assert_eq!(error.to_string(), "repeat failed");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn malformed_state_resets_the_service_but_fails_a_requested_extractor() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut reader = backend.clone();
    let flow = SteppedFlow::new("reset").repeat_until(async |_: u32| None::<u32>);
    let service = flow.into_service(backend.clone()).service;
    let mut metadata = MetadataStore::new();
    metadata
        .insert("miju_workflow.repeater.iterations", "invalid")
        .unwrap();
    let id = RandomId::generate();
    let task = TaskBuilder::new(b"7".to_vec())
        .task_id(id.clone())
        .with_metadata(metadata.clone())
        .build();
    assert!(matches!(
        service.oneshot(task).await.unwrap(),
        GoTo::Break(_)
    ));
    let repeated = next_task(&mut reader).await;
    let state = RepeaterState::extract(repeated.metadata()).unwrap();
    assert_eq!(state.iterations(), 1);
    assert_eq!(state.previous_task_id(), Some(&id));

    let flow = SteppedFlow::new("extract")
        .repeat_until(async |value: u32, _: Meta<RepeaterState>| Some(value));
    let service = flow.into_service(backend).service;
    let task = TaskBuilder::new(b"7".to_vec())
        .with_metadata(metadata)
        .build();
    let error = service.oneshot(task).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RepeaterStateError>(),
        Some(RepeaterStateError::ParseIterations(_))
    ));
    assert_queue_empty(&mut reader);
}

#[test]
fn repeater_metadata_defaults_round_trips_and_reports_invalid_or_duplicate_entries() {
    assert_eq!(
        RepeaterState::extract(&MetadataStore::new()).unwrap(),
        RepeaterState::default()
    );
    for (iterations, previous) in [
        (0, None),
        (3, Some(TaskId::from_int(17))),
        (usize::MAX, Some(TaskId::from_string("task-one"))),
    ] {
        let state: RepeaterState = serde_json::from_value(json!({
            "iterations": iterations,
            "prev_task_id": previous,
        }))
        .unwrap();
        let mut metadata = MetadataStore::new();
        state.inject(&mut metadata).unwrap();
        let extracted = RepeaterState::extract(&metadata).unwrap();
        assert_eq!(extracted, state);
        assert_eq!(extracted.iterations(), iterations);
        assert_eq!(extracted.previous_task_id(), previous.as_ref());
        assert!(matches!(
            state.inject(&mut metadata),
            Err(RepeaterStateError::DuplicateEntry(_))
        ));
        assert_eq!(RepeaterState::extract(&metadata).unwrap(), state);
    }
    for value in ["", "-1", "invalid", "18446744073709551616"] {
        let mut metadata = MetadataStore::new();
        metadata
            .insert("miju_workflow.repeater.iterations", value)
            .unwrap();
        assert!(matches!(
            RepeaterState::extract(&metadata),
            Err(RepeaterStateError::ParseIterations(_))
        ));
    }
    let mut metadata = MetadataStore::new();
    metadata
        .insert("miju_workflow.repeater.prev_task_id", "")
        .unwrap();
    assert!(matches!(
        RepeaterState::extract(&metadata),
        Err(RepeaterStateError::ParseTaskId)
    ));
}
