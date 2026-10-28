//! Collection fan-out, result gathering, and metadata through public APIs.

use std::{
    collections::HashSet,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Context,
    time::Duration,
};

use futures_util::{FutureExt, future::poll_fn, task::noop_waker_ref};
use miju_core::{
    backend::{Backend, BackendConfig, TaskResult, TaskSink, ext::shared::Shared},
    error::BoxDynError,
    task::{
        Task,
        builder::TaskBuilder,
        data::Data,
        metadata::{Metadata, MetadataStore},
        status::Status,
        task_id::{GenerateId, RandomId},
    },
    worker::{builder::WorkerBuilder, context::WorkerContext, service::IntoWorkerService},
};
use miju_workflow::{
    SteppedFlow,
    in_memory::InMemoryWorkflow,
    sequential::{
        GoTo, StepResponse, WorkflowContext,
        filter_map::{FilterContext, FilterContextError, FilterState, FilterStateError},
    },
};
use serde_json::{Value, json};
use tower::{Layer, ServiceExt};

const DEADLINE: Duration = Duration::from_secs(5);
type BackendHandle = Shared<InMemoryWorkflow<Vec<u32>>>;

async fn next_task(backend: &mut BackendHandle) -> Task<Vec<u8>> {
    let worker = WorkerContext::new("inspect-filter");
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
async fn filters_compose_between_typed_steps_in_a_worker() {
    let backend = InMemoryWorkflow::create();
    let mut producer = backend.clone();
    producer.push(5u32).await.unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let seen = observed.clone();
    let flow = SteppedFlow::new("filter-chain")
        .and_then(async |count: u32| (1..=count).collect::<Vec<_>>())
        .filter_map(async |n: u32| (n % 2 == 0).then(|| n.to_string()))
        .filter_map(async |label: String| (label == "4").then_some(label))
        .and_then(move |labels: Vec<String>, worker: WorkerContext| {
            let seen = seen.clone();
            async move {
                assert_eq!(labels, ["4"]);
                seen.store(labels.len(), Ordering::SeqCst);
                worker.stop().unwrap();
            }
        });
    let worker = WorkerBuilder::new("filter-chain")
        .backend(backend)
        .build(flow);
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn collector_waits_for_distinct_element_tasks_and_enqueues_mapped_values() {
    let mut backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let layer = backend.middleware(&mut WorkerContext::new("collect"));
    let flow = SteppedFlow::new("collect")
        .filter_map(async |n: u32| (n % 2 == 0).then(|| format!("item-{n}")))
        .and_then(async |values: Vec<String>| values.len());
    let service = layer.layer(flow.into_service(backend).service);
    let entry_id = RandomId::generate();
    let result = service
        .clone()
        .oneshot(
            TaskBuilder::new(serde_json::to_vec(&[1u32, 2, 4]).unwrap())
                .task_id(entry_id.clone())
                .build(),
        )
        .await
        .unwrap();
    assert!(matches!(result, GoTo::Done));

    let mut elements = Vec::new();
    let mut ids = Vec::new();
    for input in [1u32, 2, 4] {
        let task = next_task(&mut reader).await;
        assert_eq!(serde_json::from_slice::<u32>(&task.args).unwrap(), input);
        assert!(matches!(
            FilterState::extract(task.metadata()).unwrap(),
            FilterState::SingleStep
        ));
        assert_eq!(
            WorkflowContext::extract(task.metadata())
                .unwrap()
                .step_index,
            0
        );
        ids.push(task.task_id().cloned().unwrap());
        elements.push(task);
    }
    let collector = next_task(&mut reader).await;
    assert!(matches!(
        FilterState::extract(collector.metadata()).unwrap(),
        FilterState::Collector
    ));
    assert_eq!(
        WorkflowContext::extract(collector.metadata())
            .unwrap()
            .step_index,
        0
    );
    let context = FilterContext::extract(collector.metadata()).unwrap();
    assert_eq!(
        serde_json::to_value(context).unwrap(),
        json!({"task_ids": ids})
    );
    let mut unique: HashSet<_> = ids.into_iter().collect();
    assert_eq!(unique.len(), 3);
    assert!(unique.insert(entry_id));
    assert!(unique.insert(collector.task_id().cloned().unwrap()));
    assert_queue_empty(&mut reader);

    // Poll the collector before any element result is stored.
    let pending = service.clone().oneshot(collector);
    tokio::pin!(pending);
    poll_fn(|cx| {
        assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    for task in elements {
        let expected = match serde_json::from_slice::<u32>(&task.args).unwrap() {
            1 => json!(null),
            n => json!(format!("item-{n}")),
        };
        let GoTo::Break(result) = service.clone().oneshot(task).await.unwrap() else {
            panic!("expected an element result")
        };
        assert_eq!(result.result, expected);
        assert!(result.next_task_id.is_none());
    }
    let GoTo::Next(response) = tokio::time::timeout(DEADLINE, pending)
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("expected collected outputs")
    };
    let mut values: Vec<String> = serde_json::from_value(response.result).unwrap();
    values.sort();
    assert_eq!(values, ["item-2", "item-4"]);
    let next = next_task(&mut reader).await;
    assert_eq!(next.task_id(), response.next_task_id.as_ref());
    assert!(unique.insert(next.task_id().cloned().unwrap()));
    assert_eq!(
        WorkflowContext::extract(next.metadata())
            .unwrap()
            .step_index,
        1
    );
    assert!(matches!(
        FilterState::extract(next.metadata()),
        Err(FilterStateError::MissingKey)
    ));
    assert!(matches!(service.oneshot(next).await.unwrap(), GoTo::Done));
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn empty_and_all_filtered_collections_stop_before_the_next_step() {
    for inputs in [vec![], vec![1u32, 3]] {
        let mut backend = InMemoryWorkflow::<Vec<u32>>::create();
        let mut reader = backend.clone();
        let layer = backend.middleware(&mut WorkerContext::new("empty-filter"));
        let flow = SteppedFlow::new("empty-filter")
            .filter_map(async |n: u32| (n % 2 == 0).then_some(n))
            .and_then(async |values: Vec<u32>| values.len());
        let service = layer.layer(flow.into_service(backend).service);
        service
            .clone()
            .oneshot(
                TaskBuilder::new(serde_json::to_vec(&inputs).unwrap())
                    .task_id(RandomId::generate())
                    .build(),
            )
            .await
            .unwrap();
        for _ in inputs {
            let task = next_task(&mut reader).await;
            service.clone().oneshot(task).await.unwrap();
        }
        let collector = next_task(&mut reader).await;
        let result = tokio::time::timeout(DEADLINE, service.oneshot(collector))
            .await
            .unwrap()
            .unwrap();
        let GoTo::Break(response) = result else {
            panic!("expected an empty collection to stop")
        };
        assert_eq!(response.result, json!([]));
        assert!(response.next_task_id.is_none());
        assert_queue_empty(&mut reader);
    }
}

#[tokio::test]
async fn invalid_collection_context_and_element_failures_are_reported() {
    let mut backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let layer = backend.middleware(&mut WorkerContext::new("filter-error"));
    let flow = SteppedFlow::new("filter-error")
        .filter_map(async |n: u32, _: Data<usize>| Some(n))
        .and_then(async |values: Vec<u32>| values.len());
    let service = layer.layer(flow.into_service(backend).service);
    let malformed = TaskBuilder::new(b"not-json".to_vec())
        .task_id(RandomId::generate())
        .build();
    assert!(service.clone().oneshot(malformed).await.is_err());
    assert_queue_empty(&mut reader);
    let missing_context = TaskBuilder::new(b"[]".to_vec())
        .task_id(RandomId::generate())
        .metadata(&FilterState::Collector)
        .build();
    let error = service.clone().oneshot(missing_context).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<FilterContextError>(),
        Some(FilterContextError::MissingKey)
    ));
    service
        .clone()
        .oneshot(
            TaskBuilder::new(b"[7]".to_vec())
                .task_id(RandomId::generate())
                .build(),
        )
        .await
        .unwrap();
    let task = next_task(&mut reader).await;
    let error = service.clone().oneshot(task).await.unwrap_err();
    let collector = next_task(&mut reader).await;
    let result = tokio::time::timeout(DEADLINE, service.oneshot(collector))
        .await
        .unwrap()
        .unwrap_err();
    assert!(result.to_string().contains(&error.to_string()));
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn element_decode_failures_are_returned_without_running_the_handler() {
    for payload in [b"not-json".to_vec(), b"\"not-a-number\"".to_vec()] {
        let mut backend = InMemoryWorkflow::<Vec<u32>>::create();
        let mut reader = backend.clone();
        let layer = backend.middleware(&mut WorkerContext::new("decode-error"));
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let flow = SteppedFlow::new("decode-error")
            .filter_map(move |n: u32| {
                observed.fetch_add(1, Ordering::SeqCst);
                async move { Some(n) }
            })
            .and_then(async |values: Vec<u32>| values.len());
        let service = layer.layer(flow.into_service(backend).service);
        service
            .clone()
            .oneshot(
                TaskBuilder::new(b"[7]".to_vec())
                    .task_id(RandomId::generate())
                    .build(),
            )
            .await
            .unwrap();
        let element = next_task(&mut reader).await.map_args(|_| payload);
        let collector = next_task(&mut reader).await;
        let result = AssertUnwindSafe(service.clone().oneshot(element))
            .catch_unwind()
            .await;
        assert!(result.is_ok(), "an element decode failure must not panic");
        let error = result.unwrap().unwrap_err();
        assert!(error.downcast_ref::<serde_json::Error>().is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let error_from_collector = tokio::time::timeout(DEADLINE, service.oneshot(collector))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            error_from_collector
                .to_string()
                .contains(&error.to_string())
        );
        assert_queue_empty(&mut reader);
    }
}

#[test]
fn filter_metadata_round_trips_and_reports_missing_invalid_and_duplicate_values() {
    for state in [
        FilterState::Init,
        FilterState::SingleStep,
        FilterState::Collector,
    ] {
        let mut metadata = MetadataStore::new();
        state.inject(&mut metadata).unwrap();
        assert_eq!(
            FilterState::extract(&metadata).unwrap().to_string(),
            state.to_string()
        );
        assert!(matches!(
            state.inject(&mut metadata),
            Err(FilterStateError::DuplicateEntry(_))
        ));
    }
    let mut metadata = MetadataStore::new();
    assert!(matches!(
        FilterState::extract(&metadata),
        Err(FilterStateError::MissingKey)
    ));
    metadata
        .insert("miju_workflow.filter.state", "invalid")
        .unwrap();
    assert!(matches!(
        FilterState::extract(&metadata),
        Err(FilterStateError::Parse(_))
    ));
    for ids in [json!([]), json!([17, "task-one"])] {
        let context: FilterContext = serde_json::from_value(json!({"task_ids": ids})).unwrap();
        let mut metadata = MetadataStore::new();
        context.inject(&mut metadata).unwrap();
        let extracted = FilterContext::extract(&metadata).unwrap();
        assert_eq!(
            serde_json::to_value(extracted).unwrap(),
            json!({"task_ids": ids})
        );
        assert!(matches!(
            context.inject(&mut metadata),
            Err(FilterContextError::DuplicateEntry(_))
        ));
    }
    let mut metadata = MetadataStore::new();
    assert!(matches!(
        FilterContext::extract(&metadata),
        Err(FilterContextError::MissingKey)
    ));
    metadata
        .insert("miju_workflow.filter.task_ids", "task-one,,task-two")
        .unwrap();
    assert!(matches!(
        FilterContext::extract(&metadata),
        Err(FilterContextError::ParseTaskId)
    ));
}

async fn collect_stored_results(
    results: Vec<Result<Value, String>>,
) -> (Result<GoTo<StepResponse>, BoxDynError>, BackendHandle) {
    let mut backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let layer = backend.middleware(&mut WorkerContext::new("stored-filter-results"));
    let store = layer.store();
    let flow = SteppedFlow::new("stored-filter-results")
        .filter_map(async |n: u32| Some(n))
        .and_then(async |values: Vec<u32>| values.len());
    let service = layer.layer(flow.into_service(backend).service);
    service
        .clone()
        .oneshot(
            TaskBuilder::new(serde_json::to_vec(&vec![2u32; results.len()]).unwrap())
                .task_id(RandomId::generate())
                .build(),
        )
        .await
        .unwrap();
    for result in results {
        let task = next_task(&mut reader).await;
        let task_id = task.task_id().unwrap().clone();
        store.insert(
            task_id.clone(),
            TaskResult {
                task_id,
                attempt: 0,
                status: if result.is_ok() {
                    Status::Done
                } else {
                    Status::Failed
                },
                result,
            },
        );
    }
    let collector = next_task(&mut reader).await;
    let result = tokio::time::timeout(DEADLINE, service.oneshot(collector))
        .await
        .unwrap();
    (result, reader)
}

#[tokio::test]
async fn collector_reports_child_result_and_output_decode_errors() {
    for (bad_result, message) in [
        (json!({"not-a-transition": 7}), "unknown variant"),
        (
            json!({"Break": {"result": "wrong-type", "next_task_id": null}}),
            "expected u32",
        ),
    ] {
        let bad = Ok(bad_result);
        let some = Ok(json!({"Break": {"result": 2, "next_task_id": null}}));
        let none = Ok(json!({"Break": {"result": null, "next_task_id": null}}));
        for results in [
            vec![bad.clone()],
            vec![bad.clone(), none.clone()],
            vec![none, bad.clone()],
            vec![bad.clone(), some.clone()],
            vec![some, bad.clone()],
        ] {
            let (result, mut reader) = collect_stored_results(results).await;
            let error =
                result.expect_err("an explicit error must not become a successful omission");
            assert!(error.to_string().contains(message), "{error}");
            assert_queue_empty(&mut reader);
        }
    }
}

#[tokio::test]
async fn collector_keeps_outer_stream_error_priority() {
    for unreadable in [
        json!({"not-a-transition": 7}),
        json!({"Break": {"result": "wrong-type", "next_task_id": null}}),
    ] {
        let (result, mut reader) = collect_stored_results(vec![
            Ok(unreadable),
            Err("outer result-stream failure".into()),
        ])
        .await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "Failed to add task to storage: outer result-stream failure"
        );
        assert_queue_empty(&mut reader);
    }
}

#[tokio::test]
async fn collector_preserves_none_and_non_break_omissions() {
    for omitted in [
        GoTo::Break(StepResponse {
            result: Value::Null,
            next_task_id: None,
        }),
        GoTo::Done,
        GoTo::Next(StepResponse {
            result: json!(4),
            next_task_id: None,
        }),
        GoTo::DelayFor(
            Duration::from_secs(1),
            StepResponse {
                result: json!(4),
                next_task_id: None,
            },
        ),
    ] {
        let stored = serde_json::to_value(omitted).unwrap();
        let (result, mut reader) = collect_stored_results(vec![Ok(stored.clone())]).await;
        let GoTo::Break(response) = result.unwrap() else {
            panic!("a collection with no retained values keeps its existing empty result");
        };
        assert_eq!(response.result, json!([]));
        assert_queue_empty(&mut reader);
        let (result, mut reader) = collect_stored_results(vec![
            Ok(json!({"Break": {"result": 2, "next_task_id": null}})),
            Ok(stored),
        ])
        .await;
        let GoTo::Next(response) = result.unwrap() else {
            panic!("the retained value must continue");
        };
        assert_eq!(response.result, json!([2]));
        let next = next_task(&mut reader).await;
        assert_eq!(
            serde_json::from_slice::<Value>(&next.args).unwrap(),
            json!([2])
        );
        assert_eq!(next.task_id(), response.next_task_id.as_ref());
        assert_queue_empty(&mut reader);
    }
}
