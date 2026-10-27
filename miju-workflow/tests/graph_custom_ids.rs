//! Custom ID-generator compatibility through an otherwise unchanged public backend.

use std::{
    convert::Infallible,
    fmt,
    pin::Pin,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

use futures_util::{Sink, SinkExt, task::noop_waker_ref};
use miju_core::{
    backend::{
        Backend, BackendConfig, TaskResult, WaitForCompletion, WireFormatBackend,
        ext::shared::Shared,
    },
    task::{
        Task,
        builder::TaskBuilder,
        metadata::Metadata,
        task_id::{GenerateId, TaskId},
    },
    worker::{context::WorkerContext, service::IntoWorkerService},
};
use miju_workflow::{
    GraphFlow,
    graph::{GraphFlowContext, response::GraphNodeResponse},
    in_memory::InMemoryWorkflow,
};
use petgraph::graph::NodeIndex;
use serde_json::json;
use tower::ServiceExt;

type Inner = Shared<InMemoryWorkflow<u32>>;

#[derive(Clone, Debug, PartialEq)]
struct CommaGenerator;
impl fmt::Display for CommaGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "custom-generator")
    }
}
impl FromStr for CommaGenerator {
    type Err = Infallible;
    fn from_str(_: &str) -> Result<Self, Self::Err> {
        Ok(Self)
    }
}
impl GenerateId for CommaGenerator {
    fn generate() -> TaskId {
        static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
        TaskId::from_string(format!(
            "custom,{}",
            SEQUENCE.fetch_add(1, Ordering::SeqCst)
        ))
    }
}

#[derive(Clone, Debug)]
struct CommaIdBackend(Inner);
impl Backend for CommaIdBackend {
    type Task = <Inner as Backend>::Task;
    type Error = <Inner as Backend>::Error;
    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Backend::poll_ready(&mut self.0, cx, worker)
    }
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        Backend::poll_next(&mut self.0, cx, worker)
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Backend::poll_close(&mut self.0, cx, worker)
    }
}
impl BackendConfig for CommaIdBackend {
    type Args = <Inner as BackendConfig>::Args;
    type Id = CommaGenerator;
    type Kind = <Inner as BackendConfig>::Kind;
    type Config = <Inner as BackendConfig>::Config;
    type Layer = <Inner as BackendConfig>::Layer;
    fn config(&self) -> &Self::Config {
        self.0.config()
    }
    fn middleware(&mut self, worker: &mut WorkerContext) -> Self::Layer {
        self.0.middleware(worker)
    }
}
impl WireFormatBackend for CommaIdBackend {
    type Compact = <Inner as WireFormatBackend>::Compact;
    type Codec = <Inner as WireFormatBackend>::Codec;
    fn codec(&self) -> &Self::Codec {
        self.0.codec()
    }
}
impl Sink<Task<Vec<u8>>> for CommaIdBackend {
    type Error = <Inner as Backend>::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_ready_unpin(cx)
    }
    fn start_send(self: Pin<&mut Self>, task: Task<Vec<u8>>) -> Result<(), Self::Error> {
        self.get_mut().0.start_send_unpin(task)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_flush_unpin(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_close_unpin(cx)
    }
}
impl WaitForCompletion<GraphNodeResponse> for CommaIdBackend {
    type ResultStream = <Inner as WaitForCompletion<GraphNodeResponse>>::ResultStream;
    fn wait_for(&mut self, ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        self.0.wait_for(ids)
    }
    async fn check_status(
        &mut self,
        ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<GraphNodeResponse>>, Self::Error> {
        self.0.check_status(ids).await
    }
}

#[tokio::test]
async fn linear_graph_preserves_custom_ids_and_existing_dependency_history() {
    for include_history in [false, true] {
        let graph = GraphFlow::<CommaIdBackend>::new("linear-custom-ids");
        let entry = graph.add_task(|value: u32| async move { value + 1 });
        graph
            .add_task(|value: u32| async move { value * 2 })
            .depends_on(&entry);
        graph.validate().unwrap();
        let backend = CommaIdBackend(InMemoryWorkflow::<u32>::create());
        let mut queue = backend.clone();
        let service = graph.into_service(backend).service;
        let root = TaskId::from_string("root");
        let mut context = GraphFlowContext::new(Some(root.clone()));
        let mut request = TaskBuilder::new(b"7".to_vec()).task_id(root).build();
        if include_history {
            context
                .node_task_ids
                .insert(NodeIndex::new(99), TaskId::from_string("history"));
            request.inject_metadata(&context).unwrap();
        }
        assert_eq!(
            service.clone().oneshot(request).await.unwrap(),
            GraphNodeResponse::EnqueuedNext { result: json!(8) }
        );
        let mut cx = Context::from_waker(noop_waker_ref());
        let Poll::Ready(Some(Ok(next))) =
            Backend::poll_next(&mut queue, &mut cx, &WorkerContext::new("read"))
        else {
            panic!("one continuation must be queued")
        };
        assert!(next.task_id().unwrap().to_string().starts_with("custom,"));
        let actual = GraphFlowContext::extract(next.metadata()).unwrap();
        assert_eq!(actual.node_task_ids, context.node_task_ids);
        assert_eq!(actual.root_task_id, context.root_task_id);
        assert_eq!(
            service.oneshot(next).await.unwrap(),
            GraphNodeResponse::Complete { result: json!(16) }
        );
        assert!(Backend::poll_next(&mut queue, &mut cx, &WorkerContext::new("empty")).is_pending());
    }
}
