//! In-memory execution and result observation through public APIs.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

use futures_util::StreamExt;
use miju_core::{
    backend::{BackendConfig, TaskSink, WaitForCompletion},
    error::BoxDynError,
    task::{
        Task,
        builder::TaskBuilder,
        task_id::{GenerateId, RandomId},
    },
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_workflow::{
    SteppedFlow,
    in_memory::InMemoryWorkflow,
    sequential::{GoTo, StepResponse},
};
use serde_json::json;
use tower::{Layer, ServiceExt, service_fn};

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn shared_backend_executes_and_records_each_workflow_step() {
    let backend = InMemoryWorkflow::create();
    let mut producer = backend.clone();
    let mut observer = backend.clone();
    let start = RandomId::generate();
    producer
        .push_task(TaskBuilder::new(7u32).task_id(start.clone()).build())
        .await
        .unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let observed = seen.clone();
    let flow = SteppedFlow::new("memory")
        .and_then(async |input: u32| input + 1)
        .and_then(move |input: u32, worker: WorkerContext| {
            observed.store(input as usize, Ordering::SeqCst);
            async move {
                worker.stop().unwrap();
            }
        });
    let worker = WorkerBuilder::new("memory").backend(backend).build(flow);
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 8);
    let mut results = WaitForCompletion::<GoTo<StepResponse>>::wait_for(&mut observer, [start]);
    let result = tokio::time::timeout(DEADLINE, results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let GoTo::Next(response) = result.result.unwrap() else {
        panic!("expected next step")
    };
    assert_eq!(response.result, json!(8));
    let mut last = WaitForCompletion::<GoTo<StepResponse>>::wait_for(
        &mut observer,
        [response.next_task_id.unwrap()],
    );
    let result = tokio::time::timeout(DEADLINE, last.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(result.result, Ok(GoTo::Done)));
    assert!(results.next().await.is_none());
    assert!(last.next().await.is_none());
}

#[tokio::test]
async fn result_insertion_wakes_a_pending_waiter_and_decodes_the_result() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let layer = backend.middleware(&mut WorkerContext::new("results"));
    let id = RandomId::generate();
    let mut stream = WaitForCompletion::<u32>::wait_for(&mut backend, [id.clone()]);
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(counter.clone());
    assert!(
        stream
            .poll_next_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let service = layer.layer(service_fn(async |task: Task<u32>| {
        Ok::<_, BoxDynError>(task.args + 1)
    }));
    assert_eq!(
        service
            .oneshot(TaskBuilder::new(8).task_id(id.clone()).build())
            .await
            .unwrap(),
        9
    );
    assert!(counter.0.load(Ordering::SeqCst) > 0);
    let result = tokio::time::timeout(DEADLINE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.task_id, id);
    assert_eq!(result.result, Ok(9));
    assert!(stream.next().await.is_none());
    assert_eq!(layer.store().get(&id).unwrap().result, Ok(json!(9)));
    let snapshot = WaitForCompletion::<u32>::check_status(&mut backend, [id, RandomId::generate()])
        .await
        .unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].result, Ok(9));
}

#[tokio::test]
async fn handler_failure_is_stored_and_reported_as_a_stream_error() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let layer = backend.middleware(&mut WorkerContext::new("failure"));
    let id = RandomId::generate();
    let service = layer.layer(service_fn(async |_: Task<u32>| {
        Err::<u32, BoxDynError>("handler failed".into())
    }));
    let error = service
        .oneshot(TaskBuilder::new(8).task_id(id.clone()).build())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "handler failed");
    assert_eq!(
        layer.store().get(&id).unwrap().result,
        Err("handler failed".to_owned())
    );
    let mut stream = WaitForCompletion::<u32>::wait_for(&mut backend, [id.clone()]);
    assert!(
        tokio::time::timeout(DEADLINE, stream.next())
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(stream.next().await.is_none());
    assert!(
        WaitForCompletion::<u32>::check_status(&mut backend, [id])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn empty_wait_finishes_and_incompatible_result_types_are_reported() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let mut empty = WaitForCompletion::<u32>::wait_for(&mut backend, []);
    assert!(
        tokio::time::timeout(DEADLINE, empty.next())
            .await
            .unwrap()
            .is_none()
    );
    let layer = backend.middleware(&mut WorkerContext::new("decode"));
    let id = RandomId::generate();
    let service = layer.layer(service_fn(async |_: Task<u32>| {
        Ok::<_, BoxDynError>("text")
    }));
    service
        .oneshot(TaskBuilder::new(8).task_id(id.clone()).build())
        .await
        .unwrap();
    let mut stream = WaitForCompletion::<u32>::wait_for(&mut backend, [id.clone()]);
    let result = tokio::time::timeout(DEADLINE, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(result.result.is_err());
    assert!(stream.next().await.is_none());
    let snapshot = WaitForCompletion::<u32>::check_status(&mut backend, [id])
        .await
        .unwrap();
    assert!(snapshot[0].result.is_err());
}

#[test]
fn dropping_a_pending_result_stream_releases_its_latest_waker() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let mut results = WaitForCompletion::<u32>::wait_for(&mut backend, [RandomId::generate()]);
    let old_counter = Arc::new(WakeCounter::default());
    let old_weak = Arc::downgrade(&old_counter);
    let old_waker = Waker::from(old_counter);
    assert!(
        results
            .poll_next_unpin(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    drop(old_waker);
    assert!(old_weak.upgrade().is_some());

    let counter = Arc::new(WakeCounter::default());
    let weak = Arc::downgrade(&counter);
    let waker = Waker::from(counter);
    assert!(
        results
            .poll_next_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(old_weak.upgrade().is_none());
    drop(waker);
    assert!(weak.upgrade().is_some());
    drop(results);
    assert!(
        weak.upgrade().is_none(),
        "cancelling the observer must release its registered waker"
    );
}

#[tokio::test]
async fn dropping_one_result_observer_preserves_another_observer() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let layer = backend.middleware(&mut WorkerContext::new("cancellation"));
    let id = RandomId::generate();
    let mut canceled = WaitForCompletion::<u32>::wait_for(&mut backend, [id.clone()]);
    let mut active = WaitForCompletion::<u32>::wait_for(&mut backend, [id.clone()]);
    let canceled_counter = Arc::new(WakeCounter::default());
    let canceled_waker = Waker::from(canceled_counter.clone());
    let active_counter = Arc::new(WakeCounter::default());
    let active_waker = Waker::from(active_counter.clone());
    assert!(
        canceled
            .poll_next_unpin(&mut Context::from_waker(&canceled_waker))
            .is_pending()
    );
    assert!(
        active
            .poll_next_unpin(&mut Context::from_waker(&active_waker))
            .is_pending()
    );
    drop(canceled);
    let service = layer.layer(service_fn(async |task: Task<u32>| {
        Ok::<_, BoxDynError>(task.args + 1)
    }));
    service
        .oneshot(TaskBuilder::new(8).task_id(id.clone()).build())
        .await
        .unwrap();
    assert_eq!(canceled_counter.0.load(Ordering::SeqCst), 0);
    assert!(active_counter.0.load(Ordering::SeqCst) > 0);
    let result = tokio::time::timeout(DEADLINE, active.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.task_id, id);
    assert_eq!(result.result, Ok(9));
    assert!(active.next().await.is_none());
}
