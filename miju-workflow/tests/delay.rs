//! Scheduling metadata and payload preservation for delay steps.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Context,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{future::poll_fn, task::noop_waker_ref};
use miju_core::{
    backend::{Backend, TaskSink, ext::shared::Shared},
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
    sequential::{GoTo, WorkflowContext},
};
use tower::ServiceExt;

type BackendHandle = Shared<InMemoryWorkflow<u32>>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn next_task(backend: &mut BackendHandle) -> Task<Vec<u8>> {
    let worker = WorkerContext::new("inspect-delay");
    tokio::time::timeout(
        DEADLINE,
        poll_fn(|cx| Backend::poll_next(backend, cx, &worker)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn fixed_delay_preserves_payload_and_schedules_the_next_step() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut reader = backend.clone();
    let delay = Duration::from_secs(60);
    let flow = SteppedFlow::new("fixed")
        .delay_for(delay)
        .and_then(async |n: u32| n + 1);
    let service = flow.into_service(backend).service;
    let input_id = RandomId::generate();
    let bytes = serde_json::to_vec(&7u32).unwrap();
    let before = now();
    let response = service
        .oneshot(
            TaskBuilder::new(bytes.clone())
                .task_id(input_id.clone())
                .build(),
        )
        .await
        .unwrap();
    let after = now();
    let GoTo::DelayFor(duration, response) = response else {
        panic!("expected a delay")
    };
    assert_eq!(duration, delay);
    assert_eq!(response.result, serde_json::to_value(delay).unwrap());
    let task = next_task(&mut reader).await;
    assert_eq!(task.args, bytes);
    assert_eq!(task.task_id(), response.next_task_id.as_ref());
    assert_ne!(task.task_id(), Some(&input_id));
    assert_eq!(
        WorkflowContext::extract(task.metadata())
            .unwrap()
            .step_index,
        1
    );
    let scheduled = task.ctx().run_at().unwrap();
    assert!((before + 60..=after + 60).contains(&scheduled));
}

#[tokio::test]
async fn computed_delay_receives_decoded_arguments_and_task_context() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut reader = backend.clone();
    let id = RandomId::generate();
    let expected_id = id.clone();
    let flow = SteppedFlow::new("computed")
        .and_then(async |n: u32| n)
        .delay_with(move |task: Task<u32>| {
            assert_eq!(task.task_id(), Some(&expected_id));
            assert_eq!(task.metadata().get("request-tag"), Some("scheduled"));
            Duration::from_secs(u64::from(task.args))
        })
        .and_then(async |n: u32| n);
    let service = flow.into_service(backend).service;
    let mut metadata = MetadataStore::new();
    metadata.insert("request-tag", "scheduled").unwrap();
    let task = TaskBuilder::new(serde_json::to_vec(&9u32).unwrap())
        .task_id(id)
        .with_metadata(metadata)
        .metadata(&WorkflowContext { step_index: 1 })
        .build();
    let before = now();
    let response = service.oneshot(task).await.unwrap();
    let after = now();
    let GoTo::DelayFor(duration, response) = response else {
        panic!("expected a delay")
    };
    assert_eq!(duration, Duration::from_secs(9));
    let task = next_task(&mut reader).await;
    assert_eq!(serde_json::from_slice::<u32>(&task.args).unwrap(), 9);
    assert_eq!(task.task_id(), response.next_task_id.as_ref());
    assert_eq!(
        WorkflowContext::extract(task.metadata())
            .unwrap()
            .step_index,
        2
    );
    assert!((before + 9..=after + 9).contains(&task.ctx().run_at().unwrap()));
}

#[tokio::test]
async fn decode_failure_does_not_invoke_the_delay_function_or_enqueue() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut reader = backend.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let flow = SteppedFlow::new("invalid")
        .delay_with(move |_: Task<u32>| {
            observed.fetch_add(1, Ordering::SeqCst);
            Duration::ZERO
        })
        .and_then(async |n: u32| n);
    let service = flow.into_service(backend).service;
    assert!(
        service
            .oneshot(Task::new(b"not-json".to_vec()))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut cx = Context::from_waker(noop_waker_ref());
    assert!(Backend::poll_next(&mut reader, &mut cx, &WorkerContext::new("empty")).is_pending());
}

#[tokio::test]
async fn zero_duration_delays_compose_with_worker_execution() {
    let mut backend = InMemoryWorkflow::create();
    backend.push(12u32).await.unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    let flow = SteppedFlow::new("zero")
        .delay_for(Duration::ZERO)
        .delay_with(|task: Task<u32>| {
            assert_eq!(task.args, 12);
            Duration::ZERO
        })
        .and_then(move |n: u32, worker: WorkerContext| {
            observed.store(n as usize, Ordering::SeqCst);
            async move {
                worker.stop().unwrap();
            }
        });
    let worker = WorkerBuilder::new("zero").backend(backend).build(flow);
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 12);
}
