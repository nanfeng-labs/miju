//! Encode and enqueue workflow entry tasks or tasks targeting a step or node.
//!
//! [`WorkflowSink`] extends compatible backends without a wrapper. It uses the
//! backend codec, generates a task ID and sends one compact task per call.
//! [`push_start`](WorkflowSink::push_start) uses ordinary codec encoding;
//! [`start_fan_out`](WorkflowSink::start_fan_out) uses [`GraphCodec`] encoding.
//! The latter prepares one entry request; the graph executor creates the entry
//! node tasks when it processes that request.
//!
//! For a graph with two entry nodes, pass a tuple in entry-node index order:
//!
//! ```rust
//! use miju_core::worker::{builder::WorkerBuilder, context::WorkerContext};
//! use miju_workflow::{GraphFlow, WorkflowSink, in_memory::InMemoryWorkflow};
//!
//! # #[tokio::main]
//! # async fn main() {
//! let backend = InMemoryWorkflow::<Vec<u8>>::create();
//! let mut producer = backend.clone();
//! producer.start_fan_out((7_u32, "blue".to_owned())).await.unwrap();
//!
//! let graph = GraphFlow::new("entries");
//! let number = graph.add_task(|value: u32| async move { value * 2 });
//! let word = graph.add_task(|value: String| async move { value.len() });
//! let _join = graph
//!     .add_task(|values: (u32, usize), worker: WorkerContext| async move {
//!         assert_eq!(values, (14, 4));
//!         worker.stop().unwrap();
//!     })
//!     .depends_on((&number, &word));
//! let worker = WorkerBuilder::new("entries").backend(backend).build(graph);
//! # tokio::time::timeout(std::time::Duration::from_secs(5), async {
//! worker.run().await.unwrap();
//! # }).await.unwrap();
//! # }
//! ```
//!
//! These helpers do not inspect the workflow, validate indices or check input
//! counts. The chosen encoder must match the receiving service. In particular,
//! byte-based JSON gives tuples and vectors different layouts under ordinary
//! codec and graph encoding; see [`GraphCodec`].

use std::{fmt::Display, str::FromStr};

use futures_sink::Sink;
use miju_core::{
    backend::{Backend, BackendConfig, TaskSinkError, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, task_id::GenerateId},
};
use petgraph::graph::NodeIndex;

use crate::{
    graph::{GraphFlowContext, decode::GraphCodec},
    sequential::WorkflowContext,
};

/// Extension trait for pushing tasks into a workflow
///
/// Implemented for compatible sinks with a backend codec and ID generator. Input
/// types are not restricted to [`BackendConfig::Args`], allowing later steps to
/// accept different types. Each call creates a fresh task; it cannot preserve an
/// existing task's ID, application data, scheduling options or unrelated metadata.
/// The generated ID is not returned. Use a task builder and the backend sink when
/// those details must be supplied or retained by the caller.
///
/// Success means [`SinkExt::send`](futures_util::SinkExt::send) completed; it does
/// not mean the workflow executed. Encoding failures become
/// [`TaskSinkError::CodecError`] before sending. Sink failures become
/// [`TaskSinkError::PushError`]; whether a failed or cancelled send already
/// accepted a task depends on the backend. No rollback or retry is added.
pub trait WorkflowSink<Args>: WireFormatBackend + Backend + Sized
where
    Self::Codec: Codec<Args, Compact = Self::Compact>,
{
    /// Push a single task into the workflow sink at the start
    ///
    /// Uses ordinary backend codec encoding and adds no workflow context. A
    /// sequential executor therefore starts at step zero; graph entry selection
    /// follows [`RootGraphService`](crate::graph::RootGraphService)'s rules.
    fn push_start(
        &mut self,
        args: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Enqueue one entry request using [`GraphCodec`] encoding.
    ///
    /// For multiple graph entries, tuples with two to twelve elements or vectors
    /// encode a list of compact inputs. Each input must match the corresponding
    /// entry node in ascending index order. Scalars are encoded directly, so this
    /// method does not always create a fan-out envelope or require several nodes.
    /// No graph metadata is attached and no entry tasks are created here.
    /// Singleton tuple and vector decoding limits from [`GraphCodec`] still apply.
    fn start_fan_out(
        &mut self,
        args: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send
    where
        Args: GraphCodec<Self>,
        Args::Error: std::error::Error + Send + Sync + 'static;

    /// Push a step into the workflow sink at the specified index
    ///
    /// This is a helper method for pushing tasks into the workflow sink
    /// with the appropriate workflow context metadata.
    /// Ideally, this should be used internally by the workflow executor
    /// rather than being called directly.
    ///
    /// Uses ordinary codec encoding and sets only [`WorkflowContext::step_index`].
    /// Any index is accepted; existence and input compatibility are not checked.
    /// Earlier steps are skipped rather than restored.
    fn push_step(
        &mut self,
        args: Args,
        index: usize,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Push a node into the workflow sink at the specified index
    ///
    /// This is a helper method for pushing tasks into the workflow sink
    /// with the appropriate Graph flow context metadata.
    /// Ideally, this should be used internally by the Graph executor
    /// rather than being called directly.
    ///
    /// Uses ordinary codec encoding and creates fresh [`GraphFlowContext`] with
    /// the requested node and its index as the position. The generated task ID
    /// is also the root ID. The previous node is absent, dependency/completion
    /// sets are empty and `is_initial` is true. This cannot resume an existing
    /// execution or supply the dependency state needed by a fan-in node.
    /// Node existence and input compatibility are not checked.
    fn push_node(
        &mut self,
        node: Args,
        index: NodeIndex,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;
}

impl<S: Send, Args: Send, Compact, Err> WorkflowSink<Args> for S
where
    S: Sink<Task<Compact>, Error = Err>
        + Backend<Error = Err>
        + WireFormatBackend<Compact = Compact>
        + BackendConfig
        + Unpin,
    S::Id: GenerateId + Send + Sync + FromStr + Display,
    S::Codec: Codec<Args, Compact = Compact>,
    Err: std::error::Error + Send + Sync + 'static,
    <S::Codec as Codec<Args>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    Compact: Send + 'static,
    <S::Id as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    async fn push_start(&mut self, args: Args) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;

        let codec = self.codec();
        let task_id = S::Id::generate();
        let compact =
            S::Codec::encode(codec, &args).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact).task_id(task_id.clone()).build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn start_fan_out(&mut self, args: Args) -> Result<(), TaskSinkError<Self::Error>>
    where
        Args: GraphCodec<Self>,
        Args::Error: std::error::Error + Send + Sync + 'static,
    {
        use futures_util::SinkExt;
        let task_id = S::Id::generate();
        let codec = self.codec();
        let compact = Args::encode(args, codec).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact).task_id(task_id.clone()).build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let task_id = S::Id::generate();
        let codec = self.codec();
        let compact =
            S::Codec::encode(codec, &step).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .metadata(&WorkflowContext { step_index: index })
            .task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn push_node(
        &mut self,
        node: Args,
        index: NodeIndex,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let task_id = S::Id::generate();
        let codec = self.codec();
        let compact =
            S::Codec::encode(codec, &node).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .metadata(&GraphFlowContext {
                current_node: index,
                completed_nodes: Default::default(),
                current_position: index.index(),
                is_initial: true,
                node_task_ids: Default::default(),
                prev_node: None,
                root_task_id: Some(task_id.clone()),
            })
            .task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }
}
