//! In-memory execution and result observation through public APIs.

use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use futures_util::StreamExt;
use miju_core::{
    backend::{BackendConfig, TaskResult, TaskSink, WaitForCompletion},
    error::BoxDynError,
    task::{
        Task,
        builder::TaskBuilder,
        status::Status,
        task_id::{GenerateId, RandomId, TaskId},
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

#[test]
fn concurrent_result_insertion_does_not_leave_a_waiter_without_a_wakeup() {
    const ROUNDS: usize = 512;
    let mut backend = InMemoryWorkflow::<u32>::create();
    let store = backend.middleware(&mut WorkerContext::new("race")).store();
    let barrier = Arc::new(Barrier::new(2));
    let pending_ids: Vec<_> = (0..256)
        .map(|i| TaskId::from_string(format!("pending-{i}")))
        .collect();
    let mut missed_wakeups = 0;

    std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer_store = store.clone();
        scope.spawn(move || {
            for i in 0..ROUNDS {
                let id = TaskId::from_string(format!("result-{i}"));
                writer_barrier.wait();
                std::thread::yield_now();
                writer_store.insert(
                    id.clone(),
                    TaskResult {
                        task_id: id,
                        attempt: 0,
                        status: Status::Done,
                        result: Ok(json!(7)),
                    },
                );
                writer_barrier.wait();
            }
        });

        for i in 0..ROUNDS {
            let id = TaskId::from_string(format!("result-{i}"));
            let ids = std::iter::once(id).chain(pending_ids.iter().cloned());
            let mut results = WaitForCompletion::<u32>::wait_for(&mut backend, ids);
            let counter = Arc::new(WakeCounter::default());
            let waker = Waker::from(counter.clone());
            barrier.wait();
            let polled = results.poll_next_unpin(&mut Context::from_waker(&waker));
            barrier.wait();
            if polled.is_pending() && counter.0.load(Ordering::SeqCst) == 0 {
                missed_wakeups += 1;
            }
        }
    });

    assert_eq!(
        missed_wakeups, 0,
        "a completed result must be returned or wake its pending observer"
    );
}

#[test]
fn result_waiting_replaces_the_waker_and_does_not_rewake_for_completed_ids() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let store = backend
        .middleware(&mut WorkerContext::new("results"))
        .store();
    let ids = [RandomId::generate(), RandomId::generate()];
    let mut results = WaitForCompletion::<u32>::wait_for(&mut backend, ids.clone());
    let old_counter = Arc::new(WakeCounter::default());
    let old_waker = Waker::from(old_counter.clone());
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(counter.clone());
    assert!(
        results
            .poll_next_unpin(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    assert!(
        results
            .poll_next_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );

    for (index, id) in ids.into_iter().enumerate() {
        store.insert(
            id.clone(),
            TaskResult {
                task_id: id.clone(),
                attempt: 0,
                status: Status::Done,
                result: Ok(json!(7)),
            },
        );
        assert_eq!(old_counter.0.load(Ordering::SeqCst), 0);
        assert!(counter.0.swap(0, Ordering::SeqCst) > 0);
        assert!(matches!(
            results.poll_next_unpin(&mut Context::from_waker(&waker)),
            Poll::Ready(Some(Ok(result))) if result.task_id == id && result.result == Ok(7)
        ));
        let next = results.poll_next_unpin(&mut Context::from_waker(&waker));
        if index == 0 {
            assert!(next.is_pending());
        } else {
            assert!(matches!(next, Poll::Ready(None)));
        }
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    }
    assert!(matches!(
        results.poll_next_unpin(&mut Context::from_waker(&waker)),
        Poll::Ready(None)
    ));
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

#[tokio::test]
async fn duplicate_result_ids_finish_after_each_distinct_result() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let layer = backend.middleware(&mut WorkerContext::new("duplicates"));
    let ids = [RandomId::generate(), RandomId::generate()];
    let service = layer.layer(service_fn(async |task: Task<u32>| {
        Ok::<_, BoxDynError>(task.args)
    }));
    for (value, id) in ids.iter().enumerate() {
        service
            .clone()
            .oneshot(TaskBuilder::new(value as u32).task_id(id.clone()).build())
            .await
            .unwrap();
    }
    let waker = Waker::from(Arc::new(WakeCounter::default()));
    for input in [
        vec![ids[0].clone(), ids[0].clone()],
        vec![ids[0].clone(), ids[1].clone(), ids[0].clone()],
    ] {
        let expected = if input.len() == 2 {
            &ids[..1]
        } else {
            &ids[..]
        };
        let mut results = WaitForCompletion::<u32>::wait_for(&mut backend, input);
        for (value, id) in expected.iter().enumerate() {
            assert!(matches!(
                results.poll_next_unpin(&mut Context::from_waker(&waker)),
                Poll::Ready(Some(Ok(result)))
                    if result.task_id == *id && result.result == Ok(value as u32)
            ));
        }
        assert!(
            matches!(
                results.poll_next_unpin(&mut Context::from_waker(&waker)),
                Poll::Ready(None)
            ),
            "observing every distinct requested task must finish the stream"
        );
    }
}

#[tokio::test]
async fn duplicate_result_ids_still_wait_for_missing_tasks_and_finish_after_errors() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let layer = backend.middleware(&mut WorkerContext::new("duplicate-errors"));
    let ids = [RandomId::generate(), RandomId::generate()];
    let service = layer.layer(service_fn(async |task: Task<u32>| {
        if task.args == 0 {
            Err::<u32, BoxDynError>("handler failed".into())
        } else {
            Ok(task.args)
        }
    }));
    let mut results = WaitForCompletion::<u32>::wait_for(
        &mut backend,
        [ids[0].clone(), ids[1].clone(), ids[0].clone()],
    );
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(counter.clone());
    assert!(
        results
            .poll_next_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );
    service
        .clone()
        .oneshot(TaskBuilder::new(7).task_id(ids[0].clone()).build())
        .await
        .unwrap();
    assert!(matches!(
        results.poll_next_unpin(&mut Context::from_waker(&waker)),
        Poll::Ready(Some(Ok(result))) if result.result == Ok(7)
    ));
    counter.0.store(0, Ordering::SeqCst);
    assert!(
        results
            .poll_next_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    service
        .oneshot(TaskBuilder::new(0).task_id(ids[1].clone()).build())
        .await
        .unwrap_err();
    assert!(counter.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        results.poll_next_unpin(&mut Context::from_waker(&waker)),
        Poll::Ready(Some(Err(error))) if error.to_string().contains("handler failed")
    ));
    assert!(
        matches!(
            results.poll_next_unpin(&mut Context::from_waker(&waker)),
            Poll::Ready(None)
        ),
        "an observed error also completes its requested task"
    );
}
