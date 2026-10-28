//! Graph progress limits through restored metadata and the public service.

use futures_util::{FutureExt, task::noop_waker_ref};
use miju_core::{
    backend::{Backend, ext::shared::Shared},
    task::{Task, builder::TaskBuilder, metadata::Metadata},
    worker::{context::WorkerContext, service::IntoWorkerService},
};
use miju_workflow::{
    GraphFlow,
    graph::{GraphFlowContext, error::GraphFlowError, response::GraphNodeResponse},
    in_memory::InMemoryWorkflow,
};
use serde_json::json;
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tower::ServiceExt;

type Queue = Shared<InMemoryWorkflow<u32>>;

fn take(queue: &mut Queue) -> Task<Vec<u8>> {
    match queue.poll_next(
        &mut Context::from_waker(noop_waker_ref()),
        &WorkerContext::new("inspect"),
    ) {
        Poll::Ready(Some(Ok(task))) => task,
        other => panic!("missing continuation: {other:?}"),
    }
}

fn empty(queue: &mut Queue) {
    assert!(
        queue
            .poll_next(
                &mut Context::from_waker(noop_waker_ref()),
                &WorkerContext::new("inspect")
            )
            .is_pending()
    );
}

async fn overflow(outgoing: usize, position: usize) {
    let graph = GraphFlow::new("restored-position");
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let root = graph.add_task(move |x: u32| {
        c.fetch_add(1, Ordering::SeqCst);
        async move { x + 1 }
    });
    for _ in 0..outgoing {
        graph.add_task(async |x: u32| x).depends_on(&root);
    }
    let mut context = GraphFlowContext::new(None);
    context.current_position = position;
    let task = TaskBuilder::new(b"7".to_vec()).metadata(&context).build();
    assert_eq!(
        GraphFlowContext::extract(task.metadata())
            .unwrap()
            .current_position,
        position,
        "publicly restored metadata must actually reach execution"
    );
    let mut queue = InMemoryWorkflow::<u32>::create();
    let result = AssertUnwindSafe(graph.into_service(queue.clone()).service.oneshot(task))
        .catch_unwind()
        .await;
    let mut positions = Vec::new();
    while let Poll::Ready(Some(Ok(task))) = queue.poll_next(
        &mut Context::from_waker(noop_waker_ref()),
        &WorkerContext::new("observe"),
    ) {
        positions.push(
            GraphFlowContext::extract(task.metadata())
                .unwrap()
                .current_position,
        );
    }
    println!(
        "outgoing={outgoing} restored={position} caught_panic={} queued_positions={positions:?}",
        result.is_err()
    );
    assert!(
        result.is_ok(),
        "unrepresentable next position must return an error, not panic"
    );
    assert!(
        matches!(result.unwrap(), Err(GraphFlowError::Metadata(_))),
        "unrepresentable next position must return a metadata error"
    );
    assert!(
        positions.is_empty(),
        "overflow must not enqueue any successor"
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "this check does not demand changing the existing successful-handler boundary"
    );
    empty(&mut queue);
}

#[tokio::test]
async fn single_edge_max_position_returns_error_without_enqueue() {
    overflow(1, usize::MAX).await;
}

#[tokio::test]
async fn fanout_max_position_returns_error_without_enqueue() {
    overflow(2, usize::MAX).await;
}

#[tokio::test]
async fn fanout_max_minus_one_position_returns_error_without_enqueue() {
    overflow(2, usize::MAX - 1).await;
}

async fn valid(outgoing: usize, position: usize, expected: usize) {
    let graph = GraphFlow::new("position-control");
    let root = graph.add_task(async |x: u32| x + 1);
    for _ in 0..outgoing {
        graph.add_task(async |x: u32| x).depends_on(&root);
    }
    let mut context = GraphFlowContext::new(None);
    context.current_position = position;
    let mut queue = InMemoryWorkflow::<u32>::create();
    let result = graph
        .into_service(queue.clone())
        .service
        .oneshot(TaskBuilder::new(b"7".to_vec()).metadata(&context).build())
        .await
        .unwrap();
    if outgoing == 0 {
        assert_eq!(result, GraphNodeResponse::Complete { result: json!(8) });
    } else {
        for _ in 0..outgoing {
            let t = take(&mut queue);
            assert_eq!(
                GraphFlowContext::extract(t.metadata())
                    .unwrap()
                    .current_position,
                expected
            );
            assert_eq!(t.args, b"8");
        }
    }
    empty(&mut queue);
}

#[tokio::test]
async fn single_edge_max_minus_one_can_enqueue_max() {
    valid(1, usize::MAX - 1, usize::MAX).await;
}

#[tokio::test]
async fn fanout_max_minus_two_can_enqueue_max() {
    valid(2, usize::MAX - 2, usize::MAX).await;
}

#[tokio::test]
async fn terminal_node_at_max_completes_without_increment() {
    valid(0, usize::MAX, usize::MAX).await;
}

#[tokio::test]
async fn ordinary_fanout_keeps_existing_two_increment_behavior() {
    valid(2, 7, 9).await;
}
