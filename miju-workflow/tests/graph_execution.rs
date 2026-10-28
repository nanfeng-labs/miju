//! Graph dispatch and continuation scheduling through public services and workers.

use std::{
    collections::HashMap,
    future::{Future, Ready, ready},
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt, future::poll_fn, task::AtomicWaker};
use miju_codec::json::JsonCodec;
use miju_core::{
    backend::{Backend, BackendConfig, TaskResult, WaitForCompletion, ext::shared::Shared},
    task::{
        Task,
        builder::TaskBuilder,
        metadata::{Metadata, MetadataStore},
        status::Status,
        task_fn::task_fn,
        task_id::TaskId,
    },
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt, service::IntoWorkerService,
    },
};
use miju_workflow::{
    GraphFlow,
    graph::{
        GraphFlowContext, NodeInput, RootGraphService,
        decode::GraphCodec,
        error::{GraphFlowError, GraphServiceError},
        response::GraphNodeResponse,
    },
    in_memory::{InMemoryWorkflow, ResultStore},
};
use petgraph::graph::NodeIndex;
use serde_json::json;
use tower::{Service, ServiceExt};

type BackendType = Shared<InMemoryWorkflow<u32>>;
type RootService = RootGraphService<BackendType>;
const DEADLINE: Duration = Duration::from_secs(5);

async fn next_task(backend: &mut BackendType) -> Task<Vec<u8>> {
    tokio::time::timeout(
        DEADLINE,
        poll_fn(|cx| Backend::poll_next(backend, cx, &WorkerContext::new("read"))),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

fn assert_empty(backend: &mut BackendType) {
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(Backend::poll_next(backend, &mut cx, &WorkerContext::new("empty")).is_pending());
}

fn request(value: &[u8], context: &GraphFlowContext) -> Task<Vec<u8>> {
    TaskBuilder::new(value.to_vec())
        .metadata(context)
        .task_id(TaskId::from_string("request"))
        .build()
}

#[tokio::test]
async fn linear_dispatch_preserves_root_id_and_enqueues_fresh_task_context() {
    let graph = GraphFlow::new("linear");
    let first = graph.add_task(|value: u32| async move { value + 1 });
    let _last = graph
        .add_task(|value: u32| async move { value * 2 })
        .depends_on(&first);
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let root_id = TaskId::from_string("root");
    let response = service
        .clone()
        .oneshot(
            TaskBuilder::new(b"7".to_vec())
                .task_id(root_id.clone())
                .data("temporary".to_owned())
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(
        response,
        GraphNodeResponse::EnqueuedNext { result: json!(8) }
    );
    let continuation = next_task(&mut queue).await;
    assert_eq!(continuation.args, b"8");
    assert_ne!(continuation.task_id(), Some(&root_id));
    assert!(continuation.data().get::<String>().is_none());
    let context = GraphFlowContext::extract(continuation.metadata()).unwrap();
    assert_eq!(context.root_task_id, Some(root_id));
    assert_eq!(context.prev_node, Some(NodeIndex::new(0)));
    assert_eq!(context.current_node, NodeIndex::new(1));
    assert_eq!(context.current_position, 1);
    assert!(!context.is_initial);
    assert!(context.node_task_ids.is_empty());
    assert!(context.completed_nodes.is_empty());
    assert_eq!(
        service.oneshot(continuation).await.unwrap(),
        GraphNodeResponse::Complete { result: json!(16) }
    );
    assert_empty(&mut queue);
}

#[tokio::test]
async fn single_edge_into_join_records_current_predecessor_id() {
    let graph = GraphFlow::new("single-successor-join");
    let entry = graph.add_task(|value: u32| async move { value });
    let left = graph
        .add_task(|value: u32| async move { value * 2 })
        .depends_on(&entry);
    let right = graph
        .add_task(|value: u32| async move { value + 3 })
        .depends_on(&entry);
    let right_tail = graph
        .add_task(|value: u32| async move { value * 3 })
        .depends_on(&right);
    let join = graph
        .add_task(|(left, right): (u32, u32)| async move { left * 100 + right })
        .depends_on((&left, &right_tail));
    graph.validate().unwrap();
    let mut backend = InMemoryWorkflow::<u32>::create();
    let store = backend
        .middleware(&mut WorkerContext::new("results"))
        .store();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    service
        .clone()
        .oneshot(TaskBuilder::new(b"7".to_vec()).build())
        .await
        .unwrap();
    let mut joins = Vec::new();
    let mut predecessor_ids = Vec::new();
    // Store every actual branch result before dispatching either join task.
    for _ in 0..4 {
        let task = next_task(&mut queue).await;
        let context = GraphFlowContext::extract(task.metadata()).unwrap();
        if context.current_node == join.id() {
            joins.push(task);
        } else {
            let id = task.task_id().unwrap().to_string();
            if context.current_node == left.id() || context.current_node == right_tail.id() {
                predecessor_ids.push(TaskId::from_string(&id));
            }
            let response = service.clone().oneshot(task).await.unwrap();
            store_result(
                &store,
                &id,
                Status::Done,
                Ok(serde_json::to_value(response).unwrap()),
            );
        }
    }
    joins.push(next_task(&mut queue).await);
    assert_eq!(predecessor_ids.len(), 2);
    for id in predecessor_ids {
        assert_eq!(store.get(&id).unwrap().status, Status::Done);
    }
    let mut complete = Vec::new();
    for task in joins {
        let response = tokio::time::timeout(DEADLINE, service.clone().oneshot(task))
            .await
            .unwrap()
            .unwrap();
        if let GraphNodeResponse::Complete { result } = response {
            complete.push(result);
        }
    }
    assert_eq!(complete, vec![json!(1430)]);
    assert_empty(&mut queue);
}

async fn assert_fan_out_join_uses_current_predecessor(stale_predecessor: bool) {
    let graph = GraphFlow::new("fan-out-into-join");
    let entry = graph.add_task(|value: u32| async move { value });
    let left = graph
        .add_task(|value: u32| async move { value * 2 })
        .depends_on(&entry);
    let right = graph
        .add_task(|value: u32| async move { value + 3 })
        .depends_on(&entry);
    let tail = graph
        .add_task(|value: u32| async move { value * 3 })
        .depends_on(&right);
    let join = graph
        .add_task(|(left, right): (u32, u32)| async move { left * 100 + right })
        .depends_on((&left, &tail));
    let side = graph
        .add_task(|value: u32| async move { value + 1 })
        .depends_on(&tail);
    graph.validate().unwrap();
    let mut backend = InMemoryWorkflow::<u32>::create();
    let store = backend
        .middleware(&mut WorkerContext::new("results"))
        .store();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    service
        .clone()
        .oneshot(TaskBuilder::new(b"7".to_vec()).build())
        .await
        .unwrap();
    let mut joins = Vec::new();
    let mut predecessor_ids = Vec::new();
    let mut side_history = None;
    let mut side_results = Vec::new();
    for _ in 0..6 {
        let mut task = next_task(&mut queue).await;
        let mut context = GraphFlowContext::extract(task.metadata()).unwrap();
        if context.current_node == tail.id() && stale_predecessor {
            context
                .node_task_ids
                .insert(tail.id(), TaskId::from_string("stale-tail"));
            store_result(
                &store,
                "stale-tail",
                Status::Done,
                Ok(
                    serde_json::to_value(GraphNodeResponse::Complete { result: json!(900) })
                        .unwrap(),
                ),
            );
            let id = task.task_id().cloned().unwrap();
            task = TaskBuilder::new(task.args)
                .task_id(id)
                .metadata(&context)
                .build();
        }
        if context.current_node == join.id() {
            joins.push(task);
            continue;
        }
        let id = task.task_id().unwrap().to_string();
        if context.current_node == left.id() || context.current_node == tail.id() {
            predecessor_ids.push(TaskId::from_string(&id));
        }
        if context.current_node == side.id() {
            assert_eq!(Some(&context.node_task_ids), side_history.as_ref());
        }
        let response = service.clone().oneshot(task).await.unwrap();
        if context.current_node == tail.id() {
            let GraphNodeResponse::FanOut { node_task_ids, .. } = &response else {
                panic!("tail must enqueue both successors")
            };
            assert_eq!(node_task_ids.len(), 2);
            assert!(node_task_ids.contains_key(&join.id()));
            assert!(node_task_ids.contains_key(&side.id()));
            let mut expected = context.node_task_ids;
            expected.extend(node_task_ids.clone());
            side_history = Some(expected);
        }
        if context.current_node == side.id() {
            side_results.push(response.clone());
        }
        store_result(
            &store,
            &id,
            Status::Done,
            Ok(serde_json::to_value(response).unwrap()),
        );
    }
    assert_eq!(
        side_results,
        vec![GraphNodeResponse::Complete { result: json!(31) }]
    );
    assert_eq!(joins.len(), 2);
    assert_eq!(predecessor_ids.len(), 2);
    for id in predecessor_ids {
        assert_eq!(store.get(&id).unwrap().status, Status::Done);
    }
    let mut complete = Vec::new();
    for task in joins {
        let response = tokio::time::timeout(DEADLINE, service.clone().oneshot(task))
            .await
            .unwrap()
            .unwrap();
        if let GraphNodeResponse::Complete { result } = response {
            complete.push(result);
        }
    }
    assert_eq!(complete, vec![json!(1430)]);
    assert_empty(&mut queue);
}

#[tokio::test]
async fn fan_out_into_join_preserves_current_predecessor_without_changing_side_history() {
    assert_fan_out_join_uses_current_predecessor(false).await;
}

#[tokio::test]
async fn fan_out_into_join_replaces_stale_predecessor_without_changing_side_history() {
    assert_fan_out_join_uses_current_predecessor(true).await;
}

#[tokio::test]
async fn fan_out_side_accepts_current_id_with_comma() {
    let graph = GraphFlow::new("fan-out-side-custom-id");
    let entry = graph.add_task(|value: u32| async move { value });
    let left = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&entry);
    let tail = graph
        .add_task(|value: u32| async move { value + 1 })
        .depends_on(&entry);
    let side = graph
        .add_task(|value: u32| async move { value * 2 })
        .depends_on(&tail);
    graph
        .add_task(|(left, right): (u32, u32)| async move { left + right })
        .depends_on((&left, &tail));
    graph.validate().unwrap();
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let mut context = GraphFlowContext::new(None);
    context.current_node = tail.id();
    context.is_initial = false;
    let response = service
        .clone()
        .oneshot(
            TaskBuilder::new(b"7".to_vec())
                .task_id(TaskId::from_string("current,tail"))
                .metadata(&context)
                .build(),
        )
        .await
        .unwrap();
    let GraphNodeResponse::FanOut { node_task_ids, .. } = response else {
        panic!("tail must enqueue both successors")
    };
    let mut completed = Vec::new();
    for _ in 0..2 {
        let task = next_task(&mut queue).await;
        if task.task_id() == node_task_ids.get(&side.id()) {
            let context = GraphFlowContext::extract(task.metadata()).unwrap();
            assert!(!context.node_task_ids.contains_key(&tail.id()));
            completed.push(service.clone().oneshot(task).await.unwrap());
        }
    }
    assert_eq!(
        completed,
        vec![GraphNodeResponse::Complete { result: json!(16) }]
    );
    assert_empty(&mut queue);
}

#[tokio::test]
async fn worker_executes_fan_out_and_fan_in_with_stored_results() {
    let graph = GraphFlow::new("branches");
    let entry = graph.add_task(|value: u32| async move { value });
    let left = graph
        .add_task(|value: u32| async move { value * 2 })
        .depends_on(&entry);
    let right = graph
        .add_task(|value: u32| async move { value + 3 })
        .depends_on(&entry);
    let observed = Arc::new(AtomicUsize::new(0));
    let result = Arc::clone(&observed);
    let _join = graph
        .add_task(move |(left, right): (u32, u32), worker: WorkerContext| {
            result.store((left * 100 + right) as usize, Ordering::SeqCst);
            async move {
                worker.stop().unwrap();
                left + right
            }
        })
        .depends_on((&left, &right));
    let backend = InMemoryWorkflow::<Vec<u8>>::create();
    let mut producer = backend.clone();
    let mut observer = backend.clone();
    let root = TaskId::from_string("branch-root");
    producer
        .send(
            TaskBuilder::new(b"7".to_vec())
                .task_id(root.clone())
                .build(),
        )
        .await
        .unwrap();
    let worker = WorkerBuilder::new("graph-worker")
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
    assert_eq!(observed.load(Ordering::SeqCst), 1410);
    let mut results = WaitForCompletion::<GraphNodeResponse>::wait_for(&mut observer, [root]);
    let result = tokio::time::timeout(DEADLINE, results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let GraphNodeResponse::FanOut {
        response,
        node_task_ids,
    } = result.result.unwrap()
    else {
        panic!("expected fan-out result")
    };
    assert_eq!(response, json!(7));
    assert_eq!(node_task_ids.len(), 2);
}

#[tokio::test]
async fn multiple_entries_receive_ordered_compact_inputs_and_initial_metadata() {
    let graph = GraphFlow::<BackendType>::new("entries");
    let _first = graph.add_task(|value: u32| async move { value + 1 });
    let _second = graph.add_task(|value: String| async move { value.len() });
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let root = TaskId::from_string("entries-root");
    let compact = <(u32, String) as GraphCodec<BackendType>>::encode(
        (7, "blue".to_owned()),
        &JsonCodec::default(),
    )
    .unwrap();
    let response = service
        .clone()
        .oneshot(TaskBuilder::new(compact).task_id(root.clone()).build())
        .await
        .unwrap();
    let GraphNodeResponse::EntryFanOut { node_task_ids } = response else {
        panic!("expected entry fan-out")
    };
    assert_eq!(node_task_ids.len(), 2);
    for _ in 0..2 {
        let task = next_task(&mut queue).await;
        let context = GraphFlowContext::extract(task.metadata()).unwrap();
        assert_eq!(task.task_id(), node_task_ids.get(&context.current_node));
        assert_eq!(context.node_task_ids, node_task_ids);
        assert_eq!(context.root_task_id.as_ref(), Some(&root));
        assert!(context.prev_node.is_none());
        assert_eq!(context.current_position, 0);
        assert!(context.is_initial);
        let expected = if context.current_node == NodeIndex::new(0) {
            8
        } else {
            4
        };
        assert_eq!(
            service.clone().oneshot(task).await.unwrap(),
            GraphNodeResponse::Complete {
                result: json!(expected)
            }
        );
    }
    assert_empty(&mut queue);
    let error = service
        .clone()
        .oneshot(Task::new(b"[]".to_vec()))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphFlowError::InputCountMismatch {
            expected: 2,
            actual: 0
        }
    ));
    assert!(matches!(
        service
            .oneshot(Task::new(b"not json".to_vec()))
            .await
            .unwrap_err(),
        GraphFlowError::Codec(_)
    ));
    assert_empty(&mut queue);
}

#[tokio::test]
async fn sole_entry_at_nonzero_index_runs_before_its_successor() {
    let graph = GraphFlow::new("out-of-order");
    let last = graph.add_task(|value: u32| async move { value + 1 });
    let entry = graph.add_task(|value: u32| async move { value * 10 });
    let last = last.depends_on(&entry);
    graph.validate().unwrap();
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let root_id = TaskId::from_string("out-of-order-root");
    assert_eq!(
        service
            .clone()
            .oneshot(
                TaskBuilder::new(b"7".to_vec())
                    .task_id(root_id.clone())
                    .build()
            )
            .await
            .unwrap(),
        GraphNodeResponse::EnqueuedNext { result: json!(70) }
    );
    let continuation = next_task(&mut queue).await;
    assert_eq!(continuation.args, b"70");
    let context = GraphFlowContext::extract(continuation.metadata()).unwrap();
    assert_eq!(context.root_task_id, Some(root_id));
    assert_eq!(context.prev_node, Some(NodeIndex::new(1)));
    assert_eq!(context.current_node, last.id());
    assert_eq!(context.current_position, 1);
    assert!(!context.is_initial);
    assert_eq!(
        service.oneshot(continuation).await.unwrap(),
        GraphNodeResponse::Complete { result: json!(71) }
    );
    assert_empty(&mut queue);
}

#[tokio::test]
async fn explicit_context_preserves_target_with_a_nonzero_entry() {
    let graph = GraphFlow::new("resume-out-of-order");
    let last = graph.add_task(|value: u32| async move { value + 1 });
    let entry = graph.add_task(|value: u32| async move { value * 10 });
    let _last = last.depends_on(&entry);
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let mut context = GraphFlowContext::new(None);
    assert_eq!(
        service
            .clone()
            .oneshot(request(b"7", &context))
            .await
            .unwrap(),
        GraphNodeResponse::Complete { result: json!(8) }
    );
    assert_empty(&mut queue);
    context.current_node = NodeIndex::new(1);
    assert_eq!(
        service.oneshot(request(b"7", &context)).await.unwrap(),
        GraphNodeResponse::EnqueuedNext { result: json!(70) }
    );
    assert_eq!(next_task(&mut queue).await.args, b"70");
}

#[tokio::test]
async fn fan_out_uses_fresh_dependency_ids_and_preserves_history() {
    let graph = GraphFlow::new("metadata");
    let entry = graph.add_task(|value: u32| async move { value });
    let _left = graph
        .add_task(|value: u32| async move { value + 1 })
        .depends_on(&entry);
    let _right = graph
        .add_task(|value: u32| async move { value + 2 })
        .depends_on(&entry);
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let mut context = GraphFlowContext::new(Some(TaskId::from_string("root")));
    context.node_task_ids = HashMap::from([
        (NodeIndex::new(1), TaskId::from_string("old-left")),
        (NodeIndex::new(2), TaskId::from_string("old-right")),
        (NodeIndex::new(99), TaskId::from_string("unrelated")),
    ]);
    let GraphNodeResponse::FanOut { node_task_ids, .. } =
        service.oneshot(request(b"7", &context)).await.unwrap()
    else {
        panic!("expected fan-out")
    };
    for _ in 0..2 {
        let task = next_task(&mut queue).await;
        let next = GraphFlowContext::extract(task.metadata()).unwrap();
        assert_eq!(next.current_position, 2);
        assert_eq!(next.prev_node, Some(NodeIndex::new(0)));
        assert!(!next.is_initial);
        assert_eq!(next.node_task_ids.len(), 3);
        assert_eq!(
            next.node_task_ids.get(&NodeIndex::new(99)),
            context.node_task_ids.get(&NodeIndex::new(99))
        );
        for (node, id) in &node_task_ids {
            assert_eq!(next.node_task_ids.get(node), Some(id));
        }
        assert_eq!(task.task_id(), node_task_ids.get(&next.current_node));
        assert_eq!(task.task_id(), next.node_task_ids.get(&next.current_node));
    }
}

#[tokio::test]
async fn repeated_fan_out_joins_use_their_own_results() {
    let graph = GraphFlow::new("repeated-fan-out");
    let entry = graph.add_task(|value: u32| async move { value });
    let left = graph
        .add_task(|value: u32| async move { value * 2 })
        .depends_on(&entry);
    let right = graph
        .add_task(|value: u32| async move { value + 3 })
        .depends_on(&entry);
    let _join = graph
        .add_task(|(left, right): (u32, u32)| async move { left * 100 + right })
        .depends_on((&left, &right));
    let mut backend = InMemoryWorkflow::<u32>::create();
    let store = backend
        .middleware(&mut WorkerContext::new("results"))
        .store();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let mut context = GraphFlowContext::new(Some(TaskId::from_string("root")));
    let mut joins = Vec::new();
    for input in [b"7", b"9"] {
        let GraphNodeResponse::FanOut { node_task_ids, .. } = service
            .clone()
            .oneshot(request(input, &context))
            .await
            .unwrap()
        else {
            panic!("expected fan-out")
        };
        for _ in 0..2 {
            let task = next_task(&mut queue).await;
            let id = task.task_id().unwrap().to_string();
            let response = service.clone().oneshot(task).await.unwrap();
            store_result(
                &store,
                &id,
                Status::Done,
                Ok(serde_json::to_value(response).unwrap()),
            );
        }
        joins.push([next_task(&mut queue).await, next_task(&mut queue).await]);
        context.node_task_ids = node_task_ids;
    }
    // Both executions finish their branches before any join selects results.
    for (tasks, expected) in joins.into_iter().zip([1410, 1812]) {
        let mut complete = Vec::new();
        for task in tasks {
            let response = tokio::time::timeout(DEADLINE, service.clone().oneshot(task))
                .await
                .unwrap()
                .unwrap();
            if let GraphNodeResponse::Complete { result } = response {
                complete.push(result);
            }
        }
        assert_eq!(complete, vec![json!(expected)]);
    }
    assert_empty(&mut queue);
}

fn fan_in_fixture() -> (RootService, Arc<ResultStore>, GraphFlowContext) {
    let graph = GraphFlow::new("join");
    let left = graph.add_task(|value: u32| async move { value });
    let right = graph.add_task(|value: u32| async move { value });
    let _join = graph
        .add_task(|(right, left): (u32, u32)| async move { right * 100 + left })
        .depends_on((&right, &left));
    let mut backend = InMemoryWorkflow::<u32>::create();
    let store = backend
        .middleware(&mut WorkerContext::new("results"))
        .store();
    let mut context = GraphFlowContext::new(Some(TaskId::from_string("root")));
    context.current_node = NodeIndex::new(2);
    context.prev_node = Some(NodeIndex::new(1));
    context.node_task_ids = HashMap::from([
        (NodeIndex::new(0), TaskId::from_string("left")),
        (NodeIndex::new(1), TaskId::from_string("right")),
    ]);
    (graph.into_service(backend).service, store, context)
}

fn store_result(
    store: &ResultStore,
    id: &str,
    status: Status,
    result: Result<serde_json::Value, String>,
) {
    let task_id = TaskId::from_string(id);
    store.insert(
        task_id.clone(),
        TaskResult {
            task_id,
            attempt: 0,
            status,
            result,
        },
    );
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReadinessProbe {
    state: Arc<AtomicUsize>,
    waker: Arc<AtomicWaker>,
    calls: Arc<AtomicUsize>,
    reserved: bool,
}

impl Clone for ReadinessProbe {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            waker: Arc::clone(&self.waker),
            calls: Arc::clone(&self.calls),
            reserved: false,
        }
    }
}

impl Service<Task<u32>> for ReadinessProbe {
    type Response = u32;
    type Error = io::Error;
    type Future = Ready<Result<u32, io::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.waker.register(cx.waker());
        match self.state.load(Ordering::SeqCst) {
            0 => Poll::Pending,
            1 => {
                self.reserved = true;
                Poll::Ready(Ok(()))
            }
            _ => Poll::Ready(Err(io::Error::other("node readiness failed"))),
        }
    }

    fn call(&mut self, task: Task<u32>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ready(if std::mem::take(&mut self.reserved) {
            Ok(task.args + 1)
        } else {
            Err(io::Error::other("node called without readiness"))
        })
    }
}

#[tokio::test]
async fn graph_waits_for_the_dispatched_clone_to_be_ready() {
    let state = Arc::new(AtomicUsize::new(0));
    let waker = Arc::new(AtomicWaker::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let graph = GraphFlow::new("readiness");
    let _node = graph.add_node(
        "gated",
        ReadinessProbe {
            state: Arc::clone(&state),
            waker: Arc::clone(&waker),
            calls: Arc::clone(&calls),
            reserved: false,
        },
    );
    let service = graph
        .into_service(InMemoryWorkflow::<u32>::create())
        .service;
    let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
    let wake = Waker::from(Arc::clone(&wake_count));
    let mut cx = Context::from_waker(&wake);
    let mut response = Box::pin(service.clone().oneshot(Task::new(b"7".to_vec())));
    assert!(response.as_mut().poll(&mut cx).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    state.store(1, Ordering::SeqCst);
    waker.wake();
    assert!(wake_count.0.load(Ordering::SeqCst) > 0);
    assert_eq!(
        response.await.unwrap(),
        GraphNodeResponse::Complete { result: json!(8) }
    );
    assert_eq!(
        service.oneshot(Task::new(b"9".to_vec())).await.unwrap(),
        GraphNodeResponse::Complete { result: json!(10) }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn graph_returns_node_readiness_errors_without_calling_or_enqueuing() {
    let calls = Arc::new(AtomicUsize::new(0));
    let graph = GraphFlow::new("readiness-error");
    let first = graph.add_node(
        "failing",
        ReadinessProbe {
            state: Arc::new(AtomicUsize::new(2)),
            waker: Arc::new(AtomicWaker::new()),
            calls: Arc::clone(&calls),
            reserved: false,
        },
    );
    let _last = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&first);
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    let error = service.oneshot(Task::new(b"7".to_vec())).await.unwrap_err();
    assert!(matches!(
        error,
        GraphFlowError::Service(GraphServiceError::PollError(ref source))
            if source.to_string() == "node readiness failed"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_empty(&mut queue);
}

#[tokio::test]
async fn fan_in_waits_for_results_and_orders_values_by_edge_insertion() {
    for right in [
        GraphNodeResponse::Complete { result: json!(20) },
        GraphNodeResponse::EnqueuedNext { result: json!(20) },
        GraphNodeResponse::FanOut {
            response: json!(20),
            node_task_ids: HashMap::new(),
        },
    ] {
        let (service, store, context) = fan_in_fixture();
        store_result(
            &store,
            "right",
            Status::Done,
            Ok(serde_json::to_value(right).unwrap()),
        );
        let mut future = Box::pin(service.oneshot(request(b"ignored payload", &context)));
        let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_count));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        store_result(
            &store,
            "left",
            Status::Done,
            Ok(serde_json::to_value(GraphNodeResponse::Complete { result: json!(3) }).unwrap()),
        );
        assert!(wake_count.0.load(Ordering::SeqCst) > 0);
        assert_eq!(
            tokio::time::timeout(DEADLINE, future)
                .await
                .unwrap()
                .unwrap(),
            GraphNodeResponse::Complete {
                result: json!(2003)
            }
        );
    }
}

#[tokio::test]
async fn fan_in_requires_designated_predecessor_and_complete_id_mapping() {
    let (service, _store, mut context) = fan_in_fixture();
    context.prev_node = None;
    assert!(matches!(
        service.clone().oneshot(request(b"0", &context)).await,
        Err(GraphFlowError::Service(
            GraphServiceError::MissingPreviousNode
        ))
    ));
    context.prev_node = Some(NodeIndex::new(0));
    assert_eq!(
        tokio::time::timeout(DEADLINE, service.clone().oneshot(request(b"0", &context)))
            .await
            .unwrap()
            .unwrap(),
        GraphNodeResponse::WaitingForDependencies {
            pending_dependencies: context.node_task_ids.clone(),
        }
    );
    context.prev_node = Some(NodeIndex::new(1));
    context.node_task_ids.clear();
    assert_eq!(
        service.oneshot(request(b"0", &context)).await.unwrap(),
        GraphNodeResponse::WaitingForDependencies {
            pending_dependencies: HashMap::new()
        }
    );
}

#[tokio::test]
async fn fan_in_propagates_invalid_responses_failed_status_and_backend_errors() {
    let (service, store, context) = fan_in_fixture();
    store_result(
        &store,
        "left",
        Status::Done,
        Ok(serde_json::to_value(GraphNodeResponse::Complete { result: json!(3) }).unwrap()),
    );
    for response in [
        GraphNodeResponse::EntryFanOut {
            node_task_ids: HashMap::new(),
        },
        GraphNodeResponse::WaitingForDependencies {
            pending_dependencies: HashMap::new(),
        },
    ] {
        store_result(
            &store,
            "right",
            Status::Done,
            Ok(serde_json::to_value(response).unwrap()),
        );
        assert!(matches!(
            service.clone().oneshot(request(b"0", &context)).await,
            Err(GraphFlowError::Service(
                GraphServiceError::InvalidFanInDependencyResult
            ))
        ));
    }
    for (status, value) in [
        (
            Status::Failed,
            serde_json::to_value(GraphNodeResponse::Complete { result: json!(20) }).unwrap(),
        ),
        (Status::Done, json!("invalid response")),
    ] {
        store_result(&store, "right", status, Ok(value));
        assert!(matches!(
            service.clone().oneshot(request(b"0", &context)).await,
            Err(GraphFlowError::Service(
                GraphServiceError::DependencyTaskFailed(_)
            ))
        ));
    }
    store_result(
        &store,
        "right",
        Status::Failed,
        Err("handler failed".to_owned()),
    );
    assert!(matches!(
        service.oneshot(request(b"0", &context)).await,
        Err(GraphFlowError::Backend(_))
    ));
}

#[tokio::test]
async fn node_errors_and_invalid_metadata_do_not_enqueue_successors() {
    let graph = GraphFlow::new("failure");
    let first =
        graph.add_task(|_: u32| async { Err::<u32, _>(io::Error::other("handler failed")) });
    let _last = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&first);
    let backend = InMemoryWorkflow::<u32>::create();
    let mut queue = backend.clone();
    let service = graph.into_service(backend).service;
    for input in [b"7".as_slice(), b"invalid json"] {
        assert!(matches!(
            service.clone().oneshot(Task::new(input.to_vec())).await,
            Err(GraphFlowError::NodeExecutionError(_))
        ));
    }
    let mut context = GraphFlowContext::new(None);
    context.current_node = NodeIndex::new(99);
    assert!(
        matches!(service.clone().oneshot(request(b"7", &context)).await,
        Err(GraphFlowError::MissingService(index)) if index == NodeIndex::new(99))
    );
    let mut metadata = MetadataStore::new();
    metadata
        .insert("miju_workflow.graph.current_node", "invalid")
        .unwrap();
    assert!(matches!(
        service
            .oneshot(
                TaskBuilder::new(b"7".to_vec())
                    .with_metadata(metadata)
                    .build()
            )
            .await,
        Err(GraphFlowError::MetadataError(_))
    ));
    assert_empty(&mut queue);
}

#[derive(Clone)]
struct StatefulProbe {
    readiness_polls: Arc<AtomicUsize>,
    calls: u32,
}

impl Service<Task<u32>> for StatefulProbe {
    type Response = u32;
    type Error = io::Error;
    type Future = Ready<Result<u32, io::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.readiness_polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Task<u32>) -> Self::Future {
        self.calls += 1;
        ready(Ok(self.calls))
    }
}

#[tokio::test]
async fn root_checks_node_readiness_and_preserves_clone_local_service_state() {
    let graph = GraphFlow::new("state");
    let polls = Arc::new(AtomicUsize::new(0));
    let _node = graph.add_node(
        "counter",
        StatefulProbe {
            readiness_polls: Arc::clone(&polls),
            calls: 0,
        },
    );
    let mut service = graph
        .into_service(InMemoryWorkflow::<u32>::create())
        .service;
    for _ in 0..2 {
        let response = service
            .ready()
            .await
            .unwrap()
            .call(Task::new(b"0".to_vec()))
            .await
            .unwrap();
        assert_eq!(response, GraphNodeResponse::Complete { result: json!(1) });
    }
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert!(service.get_node_by_name_mut("missing").is_none());
    let node = service.get_node_by_name_mut("counter").unwrap();
    assert!(
        node.poll_ready(&mut Context::from_waker(
            futures_util::task::noop_waker_ref()
        ))
        .is_ready()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 3);
    node.call(
        TaskBuilder::new(NodeInput::Single(b"0".to_vec()))
            .data(JsonCodec::<Vec<u8>>::default())
            .build(),
    )
    .await
    .unwrap();
    assert_eq!(
        service.oneshot(Task::new(b"0".to_vec())).await.unwrap(),
        GraphNodeResponse::Complete { result: json!(2) }
    );
}

#[tokio::test]
async fn duplicate_names_resolve_to_latest_node_empty_graph_fans_out_and_cycles_panic() {
    let graph = GraphFlow::new("names");
    let _old = graph.add_node("same", task_fn(|_: u32| async { 1_u32 }));
    let _new = graph.add_node("same", task_fn(|_: u32| async { 2_u32 }));
    let mut service = graph
        .into_service(InMemoryWorkflow::<u32>::create())
        .service;
    let output = service
        .get_node_by_name_mut("same")
        .unwrap()
        .oneshot(
            TaskBuilder::new(NodeInput::Single(b"0".to_vec()))
                .data(JsonCodec::<Vec<u8>>::default())
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(output.1, json!(2));

    let empty = GraphFlow::new("empty")
        .into_service(InMemoryWorkflow::<u32>::create())
        .service;
    assert_eq!(
        empty.oneshot(Task::new(b"[]".to_vec())).await.unwrap(),
        GraphNodeResponse::EntryFanOut {
            node_task_ids: HashMap::new()
        }
    );

    let cyclic = GraphFlow::new("cycle");
    let node = cyclic.add_task(|value: u32| async move { value });
    let _self_edge = node.clone().depends_on(&node);
    assert!(
        catch_unwind(AssertUnwindSafe(
            || cyclic.into_service(InMemoryWorkflow::<u32>::create())
        ))
        .is_err()
    );
}
