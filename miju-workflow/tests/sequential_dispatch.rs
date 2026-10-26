//! Sequential dispatch through registered routes and queued continuations.

use std::{collections::HashMap, panic::AssertUnwindSafe, task::Context, time::Duration};

use futures_util::{FutureExt, future::poll_fn};
use miju_core::{
    backend::{Backend, ext::shared::Shared},
    error::BoxDynError,
    task::{Task, metadata::Metadata},
    worker::context::WorkerContext,
};
use miju_workflow::{
    WorkflowSink,
    in_memory::InMemoryWorkflow,
    sequential::{
        GoTo, StepContext, StepResponse, WorkflowContext, and_then::AndThenService,
        service::WorkflowService,
    },
};
use serde_json::json;
use tower::{Service, ServiceExt, service_fn, util::BoxCloneSyncService};

type BackendHandle = Shared<InMemoryWorkflow<u32>>;
type StepService = BoxCloneSyncService<Task<Vec<u8>>, GoTo<StepResponse>, BoxDynError>;

async fn next_task(backend: &mut BackendHandle) -> Task<Vec<u8>> {
    tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| Backend::poll_next(backend, cx, &WorkerContext::new("reader"))),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

fn report_context() -> StepService {
    BoxCloneSyncService::new(service_fn(|task: Task<Vec<u8>>| async move {
        let ctx = task.data().get::<StepContext<BackendHandle>>().unwrap();
        Ok(GoTo::Break(StepResponse {
            result: json!({"index": ctx.current_step, "has_next": ctx.has_next}),
            next_task_id: None,
        }))
    }))
}

async fn dispatch_context(routes: &[usize], index: usize) -> serde_json::Value {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let services = routes.iter().map(|idx| (*idx, report_context())).collect();
    let service = WorkflowService::<_, u32, u32>::new(services, backend.clone());
    backend.push_step(7_u32, index).await.unwrap();
    let task = next_task(&mut backend).await;
    assert_eq!(
        WorkflowContext::extract(task.metadata())
            .unwrap()
            .step_index,
        index
    );
    let outcome = AssertUnwindSafe(service.oneshot(task)).catch_unwind().await;
    assert!(
        outcome.is_ok(),
        "a registered step must dispatch without arithmetic panic"
    );
    let GoTo::Break(response) = outcome.unwrap().unwrap() else {
        panic!("step did not run")
    };
    response.result
}

#[tokio::test]
async fn registered_steps_only_have_representable_immediate_successors() {
    for (routes, index, has_next) in [
        (vec![0], 0, false),
        (vec![0, 1], 0, true),
        (vec![0, 2], 0, false),
        (vec![usize::MAX - 1, usize::MAX], usize::MAX - 1, true),
        (vec![0, usize::MAX], usize::MAX, false),
        (vec![usize::MAX], usize::MAX, false),
    ] {
        assert_eq!(
            dispatch_context(&routes, index).await,
            json!({"index": index, "has_next": has_next})
        );
    }
}

#[tokio::test]
async fn continuation_into_maximum_step_finishes_without_wrapping() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let step: StepService =
        BoxCloneSyncService::new(AndThenService::<_, BackendHandle, u32>::new(service_fn(
            |task: Task<u32>| async move { Ok::<_, BoxDynError>(GoTo::Next(task.args + 1)) },
        )));
    let mut workflow = WorkflowService::<_, u32, u32>::new(
        HashMap::from([
            (0, step.clone()),
            (usize::MAX - 1, step.clone()),
            (usize::MAX, step),
        ]),
        backend.clone(),
    );
    backend.push_step(7_u32, usize::MAX - 1).await.unwrap();
    let first = workflow
        .ready()
        .await
        .unwrap()
        .call(next_task(&mut backend).await)
        .await
        .unwrap();
    let GoTo::Next(first) = first else {
        panic!("penultimate step must enqueue its successor")
    };
    assert!(first.next_task_id.is_some());
    assert_eq!(first.result, json!(8));
    let next = next_task(&mut backend).await;
    assert_eq!(
        WorkflowContext::extract(next.metadata())
            .unwrap()
            .step_index,
        usize::MAX
    );
    let outcome = AssertUnwindSafe(workflow.oneshot(next))
        .catch_unwind()
        .await;
    assert!(
        outcome.is_ok(),
        "a queued registered final step must execute without arithmetic panic"
    );
    assert!(
        matches!(outcome.unwrap().unwrap(), GoTo::Done),
        "maximum step must finish instead of enqueueing step zero"
    );
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Backend::poll_next(&mut backend, &mut cx, &WorkerContext::new("empty")).is_pending());
}
