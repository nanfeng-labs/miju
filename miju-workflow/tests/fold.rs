//! Accumulator tasks and workflow continuation through public APIs.

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
    backend::{Backend, TaskSink, ext::shared::Shared},
    error::BoxDynError,
    task::{
        Task,
        builder::TaskBuilder,
        metadata::{Metadata, MetadataStore},
        task_id::{GenerateId, RandomId},
    },
    worker::{builder::WorkerBuilder, context::WorkerContext, service::IntoWorkerService},
};
use miju_workflow::{
    SteppedFlow,
    in_memory::InMemoryWorkflow,
    sequential::{
        GoTo, WorkflowContext,
        fold::{FoldState, FoldStateError},
        service::WorkflowService,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower::ServiceExt;

type BackendHandle = Shared<InMemoryWorkflow<Vec<u32>>>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn next_task(backend: &mut BackendHandle) -> Task<Vec<u8>> {
    let worker = WorkerContext::new("inspect-fold");
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
async fn fold_composes_with_mapping_delays_and_typed_steps_in_a_worker() {
    let backend = InMemoryWorkflow::create();
    let mut producer = backend.clone();
    producer.push(5u32).await.unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    let flow = SteppedFlow::new("sum")
        .and_then(async |count: u32| (1..=count).collect::<Vec<_>>())
        .filter_map(async |n: u32| (n % 2 == 0).then_some(n))
        .delay_for(Duration::ZERO)
        .fold(async |(sum, n): (u64, u32)| sum + u64::from(n))
        .and_then(move |sum: u64, worker: WorkerContext| {
            let observed = observed.clone();
            async move {
                assert_eq!(sum, 6);
                observed.fetch_add(1, Ordering::SeqCst);
                worker.stop().unwrap();
            }
        });
    let worker = WorkerBuilder::new("sum").backend(backend).build(flow);
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fold_carries_accumulator_in_order_and_advances_after_the_last_item() {
    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let flow = SteppedFlow::new("ordered")
        .and_then(async |items: Vec<u32>| items)
        .fold(async |(acc, n): (String, u32)| format!("{acc}{n}"))
        .and_then(async |text: String| text.len());
    let service = flow.into_service(backend).service;
    let entry_id = RandomId::generate();
    let response = service
        .clone()
        .oneshot(
            TaskBuilder::new(b"[1,3,2]".to_vec())
                .task_id(entry_id.clone())
                .metadata(&WorkflowContext { step_index: 1 })
                .build(),
        )
        .await
        .unwrap();
    let GoTo::Next(mut response) = response else {
        panic!("expected initialization")
    };
    assert_eq!(response.result, json!(""));
    let mut unique = HashSet::from([entry_id]);
    let inputs = [1u32, 3, 2];
    let mut accumulated = String::new();
    for (index, input) in inputs.iter().enumerate() {
        let task = next_task(&mut reader).await;
        assert_eq!(task.task_id(), response.next_task_id.as_ref());
        assert!(unique.insert(task.task_id().cloned().unwrap()));
        assert_eq!(
            WorkflowContext::extract(task.metadata())
                .unwrap()
                .step_index,
            1
        );
        assert_eq!(
            FoldState::extract(task.metadata()).unwrap(),
            FoldState::Collection
        );
        let payload: (String, Vec<u32>) = serde_json::from_slice(&task.args).unwrap();
        assert_eq!(payload, (accumulated.clone(), inputs[index..].to_vec()));
        assert_queue_empty(&mut reader);
        accumulated.push_str(&input.to_string());
        let result = service.clone().oneshot(task).await.unwrap();
        response = match result {
            GoTo::Next(response) if index < 2 => response,
            GoTo::Break(response) if index == 2 => response,
            other => panic!("unexpected transition: {other:?}"),
        };
        assert_eq!(response.result, json!(accumulated));
        assert!(response.next_task_id.is_some());
    }
    let next = next_task(&mut reader).await;
    assert_eq!(serde_json::from_slice::<String>(&next.args).unwrap(), "132");
    assert_eq!(next.task_id(), response.next_task_id.as_ref());
    assert!(unique.insert(next.task_id().cloned().unwrap()));
    assert_eq!(
        WorkflowContext::extract(next.metadata())
            .unwrap()
            .step_index,
        2
    );
    assert!(matches!(
        FoldState::extract(next.metadata()),
        Err(FoldStateError::MissingKey)
    ));
    assert!(matches!(service.oneshot(next).await.unwrap(), GoTo::Done));
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn terminal_fold_uses_the_accumulator_default_and_retains_the_result() {
    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct Total(u64);

    impl Default for Total {
        fn default() -> Self {
            Self(10)
        }
    }

    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let flow = SteppedFlow::new("terminal")
        .fold(async |(sum, n): (Total, u32)| Ok::<_, BoxDynError>(Total(sum.0 + u64::from(n))));
    let service: WorkflowService<_, _, Vec<u32>> = flow.into_service(backend).service;
    let GoTo::Next(initial) = service
        .clone()
        .oneshot(Task::new(b"[7]".to_vec()))
        .await
        .unwrap()
    else {
        panic!("expected initialization")
    };
    assert_eq!(initial.result, json!(10));
    let task = next_task(&mut reader).await;
    assert_eq!(task.task_id(), initial.next_task_id.as_ref());
    let GoTo::Break(result) = service.oneshot(task).await.unwrap() else {
        panic!("expected terminal accumulator")
    };
    assert_eq!(result.result, json!(17));
    assert!(result.next_task_id.is_none());
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn malformed_payloads_and_handler_errors_do_not_enqueue_continuations() {
    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let flow = SteppedFlow::new("failure")
        .fold(move |_: (u64, u32)| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Err::<u64, BoxDynError>("fold failed".into()) }
        })
        .and_then(async |n: u64| n);
    let service: WorkflowService<_, _, Vec<u32>> = flow.into_service(backend).service;
    for state in [FoldState::Init, FoldState::Collection] {
        let task = TaskBuilder::new(b"not-json".to_vec())
            .metadata(&state)
            .build();
        assert!(service.clone().oneshot(task).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_queue_empty(&mut reader);
    }
    let task = TaskBuilder::new(b"[5,[1,2]]".to_vec())
        .metadata(&FoldState::Collection)
        .build();
    let error = service.oneshot(task).await.unwrap_err();
    assert_eq!(error.to_string(), "fold failed");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn empty_collection_returns_default_without_calling_the_handler() {
    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let flow = SteppedFlow::new("empty").fold(move |(sum, n): (u64, u32)| {
        observed.fetch_add(1, Ordering::SeqCst);
        async move { sum + u64::from(n) }
    });
    let service: WorkflowService<_, _, Vec<u32>> = flow.into_service(backend).service;
    assert!(matches!(
        service
            .clone()
            .oneshot(Task::new(b"[]".to_vec()))
            .await
            .unwrap(),
        GoTo::Next(_)
    ));
    let task = next_task(&mut reader).await;
    assert_eq!(
        serde_json::from_slice::<(u64, Vec<u32>)>(&task.args).unwrap(),
        (0, vec![])
    );
    let outcome = AssertUnwindSafe(service.oneshot(task)).catch_unwind().await;
    assert!(outcome.is_ok(), "accepted empty fold must not panic");
    let GoTo::Break(result) = outcome.unwrap().unwrap() else {
        panic!("expected terminal accumulator")
    };
    assert_eq!(result.result, json!(0));
    assert!(result.next_task_id.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_queue_empty(&mut reader);
}

#[tokio::test]
async fn empty_fold_forwards_its_custom_default_in_a_worker() {
    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct Total(u64);

    impl Default for Total {
        fn default() -> Self {
            Self(13)
        }
    }

    let backend = InMemoryWorkflow::create();
    let mut producer = backend.clone();
    producer.push(Vec::<u32>::new()).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    let flow = SteppedFlow::new("empty-worker")
        .and_then(async |items: Vec<u32>| items)
        .fold(move |(sum, item): (Total, u32)| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, BoxDynError>(Total(sum.0 + u64::from(item))) }
        })
        .and_then(move |sum: Total, worker: WorkerContext| {
            let observed = observed.clone();
            async move {
                assert_eq!(sum.0, 13);
                observed.fetch_add(1, Ordering::SeqCst);
                worker.stop().unwrap();
            }
        });
    let worker = WorkerBuilder::new("empty-worker")
        .backend(backend)
        .build(flow);
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn empty_restored_collection_forwards_its_existing_accumulator() {
    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    let flow = SteppedFlow::new("restored-empty")
        .fold(move |(sum, item): (u64, u32)| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async move { sum + u64::from(item) }
        })
        .and_then(move |sum: u64| {
            let observed = observed.clone();
            async move {
                assert_eq!(sum, 41);
                observed.fetch_add(1, Ordering::SeqCst);
                sum
            }
        });
    let service: WorkflowService<_, _, Vec<u32>> = flow.into_service(backend).service;
    let task = TaskBuilder::new(b"[41,[]]".to_vec())
        .metadata(&FoldState::Collection)
        .build();
    let GoTo::Break(result) = service.clone().oneshot(task).await.unwrap() else {
        panic!("empty restored collection must complete")
    };
    assert_eq!(result.result, json!(41));
    let next = next_task(&mut reader).await;
    assert_eq!(next.task_id(), result.next_task_id.as_ref());
    assert_eq!(serde_json::from_slice::<u64>(&next.args).unwrap(), 41);
    assert_eq!(
        WorkflowContext::extract(next.metadata())
            .unwrap()
            .step_index,
        1
    );
    assert!(matches!(
        FoldState::extract(next.metadata()),
        Err(FoldStateError::MissingKey)
    ));
    assert_queue_empty(&mut reader);
    assert!(matches!(service.oneshot(next).await.unwrap(), GoTo::Done));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_queue_empty(&mut reader);
}

#[test]
fn fold_metadata_round_trips_and_preserves_initialization_fallback() {
    for state in [FoldState::Init, FoldState::Collection] {
        let mut metadata = MetadataStore::new();
        state.inject(&mut metadata).unwrap();
        assert_eq!(FoldState::extract(&metadata).unwrap(), state);
        assert!(matches!(
            state.inject(&mut metadata),
            Err(FoldStateError::DuplicateEntry(_))
        ));
        assert_eq!(FoldState::extract(&metadata).unwrap(), state);
    }
    assert!(matches!(
        FoldState::extract(&MetadataStore::new()),
        Err(FoldStateError::MissingKey)
    ));
    for value in ["", "invalid", "collection"] {
        let mut metadata = MetadataStore::new();
        metadata.insert("miju_workflow.fold.state", value).unwrap();
        assert_eq!(FoldState::extract(&metadata).unwrap(), FoldState::Init);
    }
}

#[tokio::test]
async fn fold_reports_the_same_default_accumulator_that_it_queues() {
    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct Seed(usize);

    impl Default for Seed {
        fn default() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(1);
            Self(NEXT.fetch_add(1, Ordering::SeqCst))
        }
    }

    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let flow = SteppedFlow::new("default-identity")
        .fold(async |(seed, item): (Seed, u32)| Ok::<_, BoxDynError>(Seed(seed.0 + item as usize)));
    let service: WorkflowService<_, _, Vec<u32>> = flow.into_service(backend).service;
    let GoTo::Next(initial) = service
        .clone()
        .oneshot(Task::new(b"[7]".to_vec()))
        .await
        .unwrap()
    else {
        panic!("expected initialization")
    };
    let task = next_task(&mut reader).await;
    assert_eq!(task.task_id(), initial.next_task_id.as_ref());
    let (seed, items): (Seed, Vec<u32>) = serde_json::from_slice(&task.args).unwrap();
    assert_eq!(items, vec![7]);
    assert_eq!(initial.result, json!(seed.0));
    let GoTo::Break(result) = service.oneshot(task).await.unwrap() else {
        panic!("expected terminal accumulator")
    };
    assert_eq!(result.result, json!(seed.0 + 7));
    assert!(result.next_task_id.is_none());
    assert_queue_empty(&mut reader);
}
