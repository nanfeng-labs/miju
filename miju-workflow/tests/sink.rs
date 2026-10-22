//! Workflow enqueueing through byte and structured-value backends.

use std::{
    collections::{BTreeMap, HashSet},
    fmt::Debug,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    task::Context,
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, future::poll_fn};
use miju_core::{
    backend::{Backend, TaskSinkError},
    task::metadata::Metadata,
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt, service::IntoWorkerService,
    },
};
use miju_file_storage::JsonStorage;
use miju_workflow::{
    GraphFlow, SteppedFlow, WorkflowSink,
    graph::{
        GraphFlowContext,
        error::{GraphFlowError, GraphServiceError},
        response::GraphNodeResponse,
    },
    in_memory::InMemoryWorkflow,
    sequential::{GoTo, WorkflowContext},
};
use petgraph::graph::NodeIndex;
use serde_json::json;
use tower::ServiceExt;

const DEADLINE: Duration = Duration::from_secs(5);

async fn next_task<B: Backend>(backend: &mut B) -> B::Task
where
    B::Error: Debug,
{
    tokio::time::timeout(
        DEADLINE,
        poll_fn(|cx| Backend::poll_next(backend, cx, &WorkerContext::new("reader"))),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

fn assert_empty<B: Backend>(backend: &mut B) {
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Backend::poll_next(backend, &mut cx, &WorkerContext::new("empty")).is_pending());
}

#[tokio::test]
async fn enqueue_helpers_encode_inputs_and_allocate_distinct_task_ids() {
    let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
    let mut ids = HashSet::new();
    backend.push_start(7_u32).await.unwrap();
    let start = next_task(&mut backend).await;
    assert_eq!(start.args, b"7");
    assert_eq!(start.metadata().iter().count(), 0);
    assert!(ids.insert(start.task_id().cloned().unwrap()));

    backend
        .start_fan_out((7_u32, "blue".to_owned()))
        .await
        .unwrap();
    let fan_out = next_task(&mut backend).await;
    let parts: Vec<Vec<u8>> = serde_json::from_slice(&fan_out.args).unwrap();
    assert_eq!(parts, [b"7".to_vec(), b"\"blue\"".to_vec()]);
    assert_eq!(fan_out.metadata().iter().count(), 0);
    assert!(ids.insert(fan_out.task_id().cloned().unwrap()));

    backend.start_fan_out(vec![2_u32, 5]).await.unwrap();
    let vector = next_task(&mut backend).await;
    assert_eq!(vector.args, b"[[50],[53]]");
    assert!(ids.insert(vector.task_id().cloned().unwrap()));

    backend
        .push_step("later".to_owned(), usize::MAX)
        .await
        .unwrap();
    let step = next_task(&mut backend).await;
    assert_eq!(step.args, b"\"later\"");
    assert_eq!(
        WorkflowContext::extract(step.metadata())
            .unwrap()
            .step_index,
        usize::MAX
    );
    assert!(GraphFlowContext::extract(step.metadata()).is_err());
    assert!(ids.insert(step.task_id().cloned().unwrap()));

    backend.push_node(9_u32, NodeIndex::new(999)).await.unwrap();
    let node = next_task(&mut backend).await;
    assert_eq!(node.args, b"9");
    let context = GraphFlowContext::extract(node.metadata()).unwrap();
    assert_eq!(context.current_node, NodeIndex::new(999));
    assert_eq!(context.current_position, 999);
    assert_eq!(context.root_task_id.as_ref(), node.task_id());
    assert!(context.is_initial);
    assert!(context.prev_node.is_none());
    assert!(context.node_task_ids.is_empty());
    assert!(context.completed_nodes.is_empty());
    assert!(WorkflowContext::extract(node.metadata()).is_err());
    assert!(ids.insert(node.task_id().cloned().unwrap()));
    assert_empty(&mut backend);
}

#[tokio::test]
async fn encoding_failures_leave_the_queue_empty_and_reusable() {
    let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
    let invalid = BTreeMap::from([(vec![1_u8], 2_u32)]);
    let errors = [
        backend.push_start(invalid.clone()).await.unwrap_err(),
        backend.push_step(invalid.clone(), 1).await.unwrap_err(),
        backend
            .push_node(invalid.clone(), NodeIndex::new(1))
            .await
            .unwrap_err(),
        backend
            .start_fan_out(vec![BTreeMap::new(), invalid])
            .await
            .unwrap_err(),
    ];
    for error in errors {
        let TaskSinkError::CodecError(source) = error else {
            panic!("expected encoding error")
        };
        assert!(source.downcast_ref::<serde_json::Error>().is_some());
        assert!(source.to_string().contains("key must be a string"));
    }
    assert_empty(&mut backend);
    backend.push_start(11_u32).await.unwrap();
    assert_eq!(next_task(&mut backend).await.args, b"11");
    assert_empty(&mut backend);
}

#[tokio::test]
async fn closed_sink_errors_are_distinct_from_encoding_errors() {
    for method in 0..4 {
        let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
        SinkExt::close(&mut backend).await.unwrap();
        let error = match method {
            0 => backend.push_start(7_u32).await.unwrap_err(),
            1 => backend.start_fan_out((7_u32, 8_u32)).await.unwrap_err(),
            2 => backend.push_step(7_u32, 1).await.unwrap_err(),
            _ => backend
                .push_node(7_u32, NodeIndex::new(1))
                .await
                .unwrap_err(),
        };
        assert!(matches!(error, TaskSinkError::PushError(_)));
    }
    let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
    SinkExt::close(&mut backend).await.unwrap();
    let invalid = BTreeMap::from([(vec![1_u8], 2_u32)]);
    assert!(matches!(
        backend.push_start(invalid).await,
        Err(TaskSinkError::CodecError(_))
    ));
    assert!(matches!(
        backend.push_start(7_u32).await,
        Err(TaskSinkError::PushError(_))
    ));
    // The channel's error mapper is consumed by the first failed send.
    assert!(
        AssertUnwindSafe(backend.push_start(7_u32))
            .catch_unwind()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sequential_start_and_indexed_step_dispatch_accept_different_input_types() {
    let mut backend = InMemoryWorkflow::<u32>::create();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    let flow = SteppedFlow::new("resume-step")
        .and_then(|value: u32| async move { (value + 1).to_string() })
        .and_then(move |value: String| {
            observed.lock().unwrap().push(value);
            async {}
        });
    let service = flow.into_service(backend.clone()).service;

    backend.push_start(7_u32).await.unwrap();
    assert!(matches!(
        service
            .clone()
            .oneshot(next_task(&mut backend).await)
            .await
            .unwrap(),
        GoTo::Next(_)
    ));
    assert!(matches!(
        service
            .clone()
            .oneshot(next_task(&mut backend).await)
            .await
            .unwrap(),
        GoTo::Done
    ));
    backend.push_step("41".to_owned(), 1).await.unwrap();
    assert!(matches!(
        service
            .oneshot(next_task(&mut backend).await)
            .await
            .unwrap(),
        GoTo::Done
    ));
    assert_eq!(*seen.lock().unwrap(), ["8", "41"]);
    assert_empty(&mut backend);
}

#[tokio::test]
async fn fan_out_entry_request_runs_heterogeneous_branches_in_a_worker() {
    let backend = InMemoryWorkflow::<Vec<u8>>::create();
    let mut producer = backend.clone();
    producer
        .start_fan_out((7_u32, "blue".to_owned()))
        .await
        .unwrap();
    let graph = GraphFlow::new("entries");
    let number = graph.add_task(|value: u32| async move { value * 2 });
    let word = graph.add_task(|value: String| async move { value.len() });
    let seen = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&seen);
    let _join = graph
        .add_task(move |values: (u32, usize), worker: WorkerContext| {
            *observed.lock().unwrap() = Some(values);
            async move {
                worker.stop().unwrap();
            }
        })
        .depends_on((&number, &word));
    let worker = WorkerBuilder::new("fan-out")
        .backend(backend)
        .on_event(|_, event| {
            if let Event::Error(error) = event {
                panic!("{error}");
            }
        })
        .build(graph);
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), Some((14, 4)));
}

#[tokio::test]
async fn indexed_node_starts_fresh_and_cannot_resume_a_fan_in() {
    async fn unexpected_entry(_: u32) -> u32 {
        panic!("entry should be skipped");
    }
    let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
    let graph = GraphFlow::new("direct-node");
    let first = graph.add_task(unexpected_entry);
    let _last = graph
        .add_task(|value: u32| async move { value * 10 })
        .depends_on(&first);
    let service = graph.into_service(backend.clone()).service;
    backend.push_node(7_u32, NodeIndex::new(1)).await.unwrap();
    assert_eq!(
        service
            .clone()
            .oneshot(next_task(&mut backend).await)
            .await
            .unwrap(),
        GraphNodeResponse::Complete { result: json!(70) }
    );
    backend.push_node(7_u32, NodeIndex::new(99)).await.unwrap();
    assert!(
        matches!(service.oneshot(next_task(&mut backend).await).await,
        Err(GraphFlowError::MissingService(index)) if index == NodeIndex::new(99))
    );

    let graph = GraphFlow::new("join");
    let left = graph.add_task(|value: u32| async move { value });
    let right = graph.add_task(|value: u32| async move { value });
    let _join = graph
        .add_task(|(a, b): (u32, u32)| async move { a + b })
        .depends_on((&left, &right));
    let service = graph.into_service(backend.clone()).service;
    backend.push_node(7_u32, NodeIndex::new(2)).await.unwrap();
    assert!(matches!(
        service.oneshot(next_task(&mut backend).await).await,
        Err(GraphFlowError::Service(
            GraphServiceError::MissingPreviousNode
        ))
    ));
    assert_empty(&mut backend);
}

#[tokio::test]
async fn graph_input_layout_is_selected_by_the_helper_without_topology_validation() {
    let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
    let graph = GraphFlow::new("tuple-node");
    let _only = graph.add_task(|(a, b): (u32, u32)| async move { a + b });
    let service = graph.into_service(backend.clone()).service;
    backend.push_start((2_u32, 3_u32)).await.unwrap();
    assert!(matches!(
        service.clone().oneshot(next_task(&mut backend).await).await,
        Err(GraphFlowError::NodeExecutionError(_))
    ));
    backend.start_fan_out((2_u32, 3_u32)).await.unwrap();
    assert_eq!(
        service
            .oneshot(next_task(&mut backend).await)
            .await
            .unwrap(),
        GraphNodeResponse::Complete { result: json!(5) }
    );

    let graph = GraphFlow::new("vector-node");
    let _only = graph.add_task(|values: Vec<u32>| async move { values.len() });
    let service = graph.into_service(backend.clone()).service;
    backend.push_start(vec![2_u32, 3]).await.unwrap();
    assert_eq!(
        service
            .clone()
            .oneshot(next_task(&mut backend).await)
            .await
            .unwrap(),
        GraphNodeResponse::Complete { result: json!(2) }
    );
    backend.start_fan_out(vec![2_u32, 3]).await.unwrap();
    assert!(matches!(
        service.oneshot(next_task(&mut backend).await).await,
        Err(GraphFlowError::NodeExecutionError(_))
    ));

    let graph = GraphFlow::new("entries");
    let _left = graph.add_task(|value: u32| async move { value });
    let _right = graph.add_task(|value: u32| async move { value });
    let service = graph.into_service(backend.clone()).service;
    backend.start_fan_out(vec![2_u32]).await.unwrap();
    assert!(matches!(
        service.oneshot(next_task(&mut backend).await).await,
        Err(GraphFlowError::InputCountMismatch {
            expected: 2,
            actual: 1
        })
    ));
    assert_empty(&mut backend);
}

#[tokio::test]
async fn file_backend_uses_its_structured_codec_and_preserves_step_metadata() {
    let mut backend = JsonStorage::<u32>::new_temp().unwrap();
    backend
        .start_fan_out((7_u32, "blue".to_owned()))
        .await
        .unwrap();
    let entry = next_task(&mut backend).await;
    assert_eq!(entry.args, json!([7, "blue"]));
    assert!(entry.task_id().is_some());
    assert!(entry.metadata().contains_key("line_id"));
    assert!(WorkflowContext::extract(entry.metadata()).is_err());
    assert!(GraphFlowContext::extract(entry.metadata()).is_err());
    backend.push_step("later".to_owned(), 3).await.unwrap();
    let step = next_task(&mut backend).await;
    assert_eq!(step.args, json!("later"));
    assert_eq!(
        WorkflowContext::extract(step.metadata())
            .unwrap()
            .step_index,
        3
    );
    assert_ne!(step.task_id(), entry.task_id());
}
