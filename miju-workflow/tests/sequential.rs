//! Exercise sequential workflows through existing backend and worker APIs.

use std::{
    sync::{Arc, Mutex},
    task::Context,
    time::Duration,
};

use futures_util::future::poll_fn;
use miju_core::{
    backend::{Backend, TaskSink},
    error::BoxDynError,
    task::{Task, data::Data, metadata::Metadata},
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt, service::IntoWorkerService,
    },
};
use miju_file_storage::JsonStorage;
use miju_workflow::{
    SteppedFlow,
    sequential::{GoTo, StepContext, WorkflowContext, service::handle_step_result},
};
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn worker_executes_typed_steps_in_order() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let first = seen.clone();
    let last = seen.clone();
    let flow = SteppedFlow::new("typed-steps")
        .and_then(
            move |value: u32, Data(ctx): Data<StepContext<JsonStorage<u32>>>| {
                first.lock().unwrap().push(ctx.current_step);
                assert!(ctx.has_next);
                async move { format!("item-{value}") }
            },
        )
        .and_then(|value: String| async move {
            assert_eq!(value, "item-7");
            value.len()
        })
        .and_then(
            move |len: usize,
                  Data(ctx): Data<StepContext<JsonStorage<u32>>>,
                  worker: WorkerContext| {
                last.lock().unwrap().push(ctx.current_step);
                assert!(!ctx.has_next);
                async move {
                    assert_eq!(len, 6);
                    worker.stop().unwrap();
                }
            },
        );
    let mut backend = JsonStorage::new_temp().unwrap();
    backend.push(7u32).await.unwrap();
    let worker = WorkerBuilder::new("typed-steps")
        .backend(backend)
        .on_event(|_, event| {
            if let Event::Error(error) = event {
                panic!("{error}");
            }
        })
        .build(flow);
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [0, 2]);
}

#[tokio::test]
async fn handler_and_decode_errors_are_returned() {
    async fn unexpected(_: u32) {
        panic!("next step must not run");
    }
    let calls = Arc::new(Mutex::new(0));
    let observed = calls.clone();
    let flow = SteppedFlow::new("errors")
        .and_then(move |_: u32| {
            *observed.lock().unwrap() += 1;
            async { Err::<u32, BoxDynError>("handler failed".into()) }
        })
        .and_then(unexpected);
    let backend = JsonStorage::<u32>::new_temp().unwrap();
    let mut reader = backend.clone();
    let service = flow.into_service(backend).service;
    let error = service
        .clone()
        .oneshot(Task::new(json!(7)))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "handler failed");
    assert_eq!(*calls.lock().unwrap(), 1);
    let error = service
        .oneshot(Task::new(json!("invalid")))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid type"));
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_empty(&mut reader);
}

#[tokio::test]
async fn next_transition_enqueues_encoded_output_and_step_metadata() {
    let backend = JsonStorage::<u32>::new_temp().unwrap();
    let mut reader = backend.clone();
    let mut context = StepContext::new(backend, 3, true);
    let response = handle_step_result(&mut context, GoTo::Next("next".to_owned()))
        .await
        .unwrap();
    let GoTo::Next(response) = response else {
        panic!("expected a next transition")
    };
    assert_eq!(response.result, json!("next"));
    let worker = WorkerContext::new("reader");
    let task = tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| Backend::poll_next(&mut reader, cx, &worker)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(task.args, json!("next"));
    assert_eq!(task.task_id(), response.next_task_id.as_ref());
    assert_eq!(
        WorkflowContext::extract(task.metadata())
            .unwrap()
            .step_index,
        4
    );
}

#[tokio::test]
async fn terminal_and_break_transitions_do_not_schedule_another_step() {
    let backend = JsonStorage::<u32>::new_temp().unwrap();
    let mut reader = backend.clone();
    let mut context = StepContext::new(backend, 0, false);
    assert!(matches!(
        handle_step_result(&mut context, GoTo::Next(7u32))
            .await
            .unwrap(),
        GoTo::Done
    ));
    assert!(matches!(
        handle_step_result(&mut context, GoTo::<Value>::Done)
            .await
            .unwrap(),
        GoTo::Done
    ));
    let GoTo::Break(response) = handle_step_result(&mut context, GoTo::Break(9u32))
        .await
        .unwrap()
    else {
        panic!("expected a break transition")
    };
    assert_eq!(response.result, json!(9));
    assert!(response.next_task_id.is_none());
    assert_empty(&mut reader);
}

fn assert_empty(backend: &mut JsonStorage<u32>) {
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Backend::poll_next(backend, &mut cx, &WorkerContext::new("empty")).is_pending());
}
