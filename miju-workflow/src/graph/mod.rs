//! Build typed dependency graphs and execute their node services.
//!
//! [`GraphFlow`] registers services and returns [`NodeBuilder`] values.
//! [`NodeBuilder::depends_on`] connects dependency outputs to a node's input
//! through [`DepsCheck`], returning a [`NodeHandle`] with node and edge indices.
//! Call [`GraphFlow::validate`] to check for cycles and [`GraphFlow::to_dot`] to
//! inspect the topology. Construction does not invoke handlers or schedule tasks.
//! Passing a graph to a worker builds a [`RootGraphService`] through
//! [`IntoWorkerService`](miju_core::worker::service::IntoWorkerService).
//! That service executes the current node, enqueues successors and waits for
//! stored dependency results at fan-in nodes.
//!
//! [`GraphFlowContext`] records progress, while
//! [`GraphNodeResponse`](response::GraphNodeResponse) describes node outcomes.
//! [`GraphCodec`] and [`GraphNodeService`] adapt inputs and typed services.

use std::{
    collections::{HashMap, VecDeque},
    fmt::{self, Debug},
    marker::PhantomData,
    sync::Mutex,
};

use miju_core::{
    backend::{Backend, BackendConfig, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::Task,
    task::task_fn::{TaskFn, task_fn},
};
use petgraph::{
    Direction,
    algo::toposort,
    dot::Config,
    graph::{DiGraph, EdgeIndex, NodeIndex},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tower::{Service, util::BoxCloneSyncService};

use crate::{
    NodeService,
    graph::{decode::GraphCodec, error::GraphFlowError, node::GraphNodeService},
};

/// Task metadata for graph progress and dependency lookup.
pub mod context;
/// Encode and decode graph node inputs with a backend codec.
pub mod decode;
/// Errors for graph services, dependencies and metadata.
pub mod error;
/// Adapt typed services to compact inputs and JSON dependency results.
pub mod node;
/// Serializable graph node outcomes.
pub mod response;
/// Dispatch graph tasks and enqueue successors.
pub mod service;

pub use context::GraphFlowContext;
pub use service::RootGraphService;

/// Build a graph of typed task services and their dependencies.
///
/// Adding nodes and edges does not execute services. The graph may contain cycles
/// until [`Self::validate`] is called; that check does not validate names,
/// dependency ownership, wire formats or entry/exit counts.
///
/// Node names are labels rather than unique identifiers. Reusing a name retains
/// both nodes but associates the label with the most recently added one.
///
/// Convert through [`IntoWorkerService`](miju_core::worker::service::IntoWorkerService)
/// to execute the graph. Conversion panics on cycles or poisoned locks. The
/// backend must support compact task delivery and result waiting; see
/// [`RootGraphService`] for execution contracts and limits.
#[derive(Debug)]
pub struct GraphFlow<B>
where
    B: WireFormatBackend,
{
    pub(crate) name: String,
    graph: Mutex<DiGraph<NodeService<B::Compact>, ()>>,
    node_mapping: Mutex<HashMap<String, NodeIndex>>,
}

impl<B> fmt::Display for GraphFlow<B>
where
    B: Backend + WireFormatBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Graph name: {}", self.name)?;
        writeln!(f, "Dot format:")?;
        f.write_str(&self.to_dot())
    }
}

impl<B> GraphFlow<B>
where
    B: Backend + WireFormatBackend,
{
    /// Create a new Graph workflow builder
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            graph: Mutex::new(DiGraph::new()),
            node_mapping: Mutex::new(HashMap::new()),
        }
    }
    /// Create a graph with the same behavior and type constraints as [`Self::new`].
    ///
    /// This constructor does not add graph-wide input or output type parameters.
    #[must_use]
    pub fn new_typed(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            graph: Mutex::new(DiGraph::new()),
            node_mapping: Mutex::new(HashMap::new()),
        }
    }

    /// Export the current graph topology as DOT text.
    ///
    /// Quotes and backslashes in node names are escaped so labels retain their text.
    ///
    /// # Panics
    /// Panics if an internal graph or name-map lock is poisoned.
    pub fn to_dot(&self) -> String {
        let names = self
            .node_mapping
            .lock()
            .expect("could not lock nodes")
            .iter()
            .map(|(name, &idx)| (idx, name.clone()))
            .collect::<HashMap<_, _>>();
        let get_node_attributes = |_, (index, _)| {
            format!(
                "label=\"{}\"",
                names
                    .get(&index)
                    .cloned()
                    .unwrap_or_default()
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
            )
        };
        let graph = self.graph.lock().expect("could not lock graph");
        let dot = petgraph::dot::Dot::with_attr_getters(
            &*graph,
            &[Config::NodeNoLabel, Config::EdgeNoLabel],
            &|_, _| String::new(),
            &get_node_attributes,
        );
        format!("{dot:?}")
    }
}

impl<B> GraphFlow<B>
where
    B: Backend + WireFormatBackend,
{
    /// Register a named service and return its typed node builder.
    ///
    /// The service is wrapped in [`GraphNodeService`] but is not called. Repeated
    /// names retain all nodes and replace the name's mapping with the newest node.
    ///
    /// # Panics
    /// Panics if an internal lock is poisoned.
    #[must_use]
    pub fn add_node<S, Input, CodecError>(
        &self,
        name: &str,
        service: S,
    ) -> NodeBuilder<'_, Input, S::Response, B>
    where
        S: Service<Task<Input>> + Send + 'static + Sync + Clone,
        S::Future: Send + 'static,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError>
            + Codec<S::Response, Compact = B::Compact, Error = CodecError>
            + Send
            + Sync
            + Clone
            + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        S::Error: Into<BoxDynError>,
        B: BackendConfig + Send + Sync + 'static,
        Input: GraphCodec<B, Error = CodecError> + Send + Sync + 'static + DeserializeOwned,
        S::Response: Serialize,
    {
        let svc: GraphNodeService<S, B, Input> = GraphNodeService::new(service);
        let node = self
            .graph
            .lock()
            .expect("Failed to lock graph mutex")
            .add_node(BoxCloneSyncService::new(svc));
        self.node_mapping
            .lock()
            .expect("Failed to lock node_mapping mutex")
            .insert(name.to_owned(), node);
        NodeBuilder {
            id: node,
            graph: self,
            _phantom: PhantomData,
        }
    }

    /// Register a task function using its Rust type name as the node label.
    ///
    /// Argument extraction and response conversion follow [`task_fn`]. Reusing
    /// the same function type reuses its label; it still creates another node.
    pub fn add_task<F, Input, O, FnArgs, Err, CodecError>(
        &self,
        task: F,
    ) -> NodeBuilder<'_, Input, O, B>
    where
        TaskFn<F, Input, FnArgs>: Service<Task<Input>, Response = O, Error = Err> + Clone,
        F: Send + 'static + Sync,
        Input: Send + 'static + Sync,
        FnArgs: Send + 'static + Sync,
        <TaskFn<F, Input, FnArgs> as Service<Task<Input>>>::Future: Send + 'static,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError> + 'static,
        B::Codec:
            Codec<O, Compact = B::Compact, Error = CodecError> + Send + Sync + Clone + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        Err: Into<BoxDynError>,
        B: BackendConfig + Send + Sync + 'static,
        Input: GraphCodec<B, Error = CodecError> + Send + Sync + DeserializeOwned + 'static,
        O: Serialize,
    {
        self.add_node(std::any::type_name::<F>(), task_fn(task))
    }

    /// Register a task function whose output can convert to a node index.
    ///
    /// Registration uses the same node adapter as [`Self::add_task`]. This method
    /// does not inspect the output, select an edge or invoke the conversion.
    /// Execution follows all outgoing edges as for an ordinary node; the
    /// output-to-index conversion is not used to choose a successor.
    pub fn route<F, Input, O, FnArgs, Err, CodecError>(
        &self,
        router: F,
    ) -> NodeBuilder<'_, Input, O, B>
    where
        TaskFn<F, Input, FnArgs>: Service<Task<Input>, Response = O, Error = Err> + Clone,
        F: Send + 'static + Sync,
        Input: Send + 'static + Sync,
        FnArgs: Send + 'static + Sync,
        <TaskFn<F, Input, FnArgs> as Service<Task<Input>>>::Future: Send + 'static,
        O: Into<NodeIndex> + Serialize,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError> + 'static,
        B::Codec:
            Codec<O, Compact = B::Compact, Error = CodecError> + Send + Sync + Clone + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        Err: Into<BoxDynError>,
        B: BackendConfig + Send + Sync + 'static,
        Input: GraphCodec<B, Error = CodecError> + Send + Sync + 'static + DeserializeOwned,
    {
        self.add_node::<TaskFn<F, Input, FnArgs>, Input, CodecError>(
            std::any::type_name::<F>(),
            task_fn(router),
        )
    }

    /// Check the current graph for directed cycles.
    ///
    /// Empty graphs and disconnected acyclic components are accepted. Node names,
    /// dependency provenance and codec compatibility are not checked.
    ///
    /// # Errors
    /// Returns [`GraphFlowError::CyclicDAG`] for a cycle, including a self-loop.
    ///
    /// # Panics
    /// Panics if the graph lock is poisoned.
    pub fn validate(&self) -> Result<(), GraphFlowError> {
        // Validate Graph (check for cycles)
        toposort(
            &*self.graph.lock().expect("Failed to lock graph mutex"),
            None,
        )
        .map_err(GraphFlowError::CyclicDAG)?;
        Ok(())
    }
    /// Build the Graph executor
    pub(crate) fn build(self, backend: B) -> Result<RootGraphService<B>, GraphFlowError> {
        // Validate Graph (check for cycles)
        let sorted = toposort(
            &*self.graph.lock().expect("Failed to lock graph mutex"),
            None,
        )
        .map_err(GraphFlowError::CyclicDAG)?;

        fn find_edge_nodes<N, E>(graph: &DiGraph<N, E>, direction: Direction) -> Vec<NodeIndex> {
            graph
                .node_indices()
                .filter(|&n| graph.neighbors_directed(n, direction).count() == 0)
                .collect()
        }

        let graph = self
            .graph
            .into_inner()
            .expect("Failed to unlock graph mutex");

        Ok(RootGraphService {
            start_nodes: find_edge_nodes(&graph, Direction::Incoming),
            end_nodes: find_edge_nodes(&graph, Direction::Outgoing),
            graph,
            node_mapping: self
                .node_mapping
                .into_inner()
                .expect("Failed to unlock node_mapping mutex"),
            topological_order: sorted,
            not_ready: VecDeque::new(),
            backend,
        })
    }
}

/// A typed reference to a registered node that can add incoming dependency edges.
///
/// Cloning this builder refers to the same node. Calling `depends_on` on separate
/// clones appends edges to that node; it does not replace existing dependencies.
pub struct NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    pub(crate) id: NodeIndex,
    pub(crate) graph: &'a GraphFlow<B>,
    _phantom: PhantomData<(Input, Output)>,
}

impl<'a, Input, Output, B> std::fmt::Debug for NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBuilder")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<'a, Input, Output, B> Clone for NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            graph: self.graph,
            _phantom: PhantomData,
        }
    }
}

impl<Input, Output, B> NodeBuilder<'_, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    /// Add edges from the supplied dependencies to this node.
    ///
    /// Each dependency's output must match this node's input through [`DepsCheck`].
    /// Returned edge IDs follow the order supplied by `to_node_indices`; duplicate
    /// dependencies create parallel edges. The returned handle lists only the
    /// edges added by this call. Insertion is not atomic and does not check cycles
    /// or whether the supplied indices came from this graph.
    ///
    /// # Panics
    /// Panics if a dependency index is outside the graph or a graph lock is
    /// poisoned. The built-in vector dependency conversion also panics for fewer
    /// than two entries. A panic while adding an edge can poison the graph lock.
    ///
    /// A dependency producing text cannot feed an integer input:
    ///
    /// ```compile_fail,E0277
    /// use miju_workflow::{GraphFlow, in_memory::InMemoryWorkflow};
    ///
    /// let graph = GraphFlow::<InMemoryWorkflow<u32>>::new("typed-edges");
    /// let text = graph.add_task(|value: u32| async move { value.to_string() });
    /// let _next = graph.add_task(|value: u32| async move { value + 1 })
    ///     .depends_on(&text);
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    pub fn depends_on<D>(self, deps: D) -> NodeHandle<Input, Output>
    where
        D: DepsCheck<Input>,
    {
        let mut edges = Vec::new();
        for dep in deps.to_node_indices() {
            edges.push(self.graph.graph.lock().unwrap().add_edge(dep, self.id, ()));
        }
        NodeHandle {
            id: self.id,
            edges,
            _phantom: PhantomData,
        }
    }
}

/// Node and edge indices returned by a dependency connection.
///
/// Handles retain input/output type markers but no graph identity or lifetime.
/// Use a handle only with the graph that created it.
#[derive(Clone, Debug)]
pub struct NodeHandle<Input, Output> {
    pub(crate) id: NodeIndex,
    pub(crate) edges: Vec<EdgeIndex>,
    pub(crate) _phantom: PhantomData<(Input, Output)>,
}

impl<Input, Output> NodeHandle<Input, Output> {
    /// Get the node ID
    #[must_use]
    pub fn id(&self) -> NodeIndex {
        self.id
    }

    /// Get the edge IDs added by the `depends_on` call that created this handle.
    #[must_use]
    pub fn edges(&self) -> &[EdgeIndex] {
        &self.edges
    }
}

/// Convert typed dependency references to ordered node indices.
///
/// A node builder or handle supplies its output type. Borrowed tuples of up to
/// eight builders or handles supply a tuple of output types. Vectors supply
/// `Vec<Output>` and require at least two entries. One-element tuple forms also
/// support scalar output for compatibility with a single dependency.
///
/// Implementations return indices directly; graph identity and validity are not
/// checked. Custom implementations must supply indices from the target graph.
pub trait DepsCheck<Input> {
    /// Return dependency indices in the order used to insert incoming edges.
    fn to_node_indices(&self) -> Vec<NodeIndex>;
}

impl<'a, Input, Output, B> DepsCheck<Output> for &NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for &NodeHandle<Input, Output> {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for NodeHandle<Input, Output> {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for (&NodeHandle<Input, Output>,) {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<Input, Output> DepsCheck<Output> for (NodeHandle<Input, Output>,) {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for (NodeBuilder<'a, Input, Output, B>,)
where
    B: Backend + WireFormatBackend,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for (&NodeBuilder<'a, Input, Output, B>,)
where
    B: Backend + WireFormatBackend,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<Output, T: DepsCheck<Output>> DepsCheck<Vec<Output>> for Vec<T> {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        assert!(self.len() > 1, "dependencies should be more than one");
        self.iter()
            .flat_map(|item| item.to_node_indices())
            .collect()
    }
}

macro_rules! impl_deps_check {
    ($( $len:literal => ( $( $in:ident $out:ident $idx:tt ),+ ) ),+ $(,)?) => {
        $(
            impl<'a, $( $in, )+ $( $out, )+ B> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeBuilder<'a, $in, $out, B>, )+ ) where B: Backend + WireFormatBackend
            {
                fn to_node_indices(&self) -> Vec<NodeIndex> {
                    vec![ $( self.$idx.id ),+ ]
                }
            }

            impl<$( $in, )+ $( $out, )+> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeHandle<$in, $out>, )+ )
            {
                fn to_node_indices(&self) -> Vec<NodeIndex> {
                    vec![ $( self.$idx.id ),+ ]
                }
            }
        )+
    };
}

impl_deps_check! {
    1 => (Input1 Output1 0),
    2 => (Input1 Output1 0, Input2 Output2 1),
    3 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2),
    4 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3),
    5 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4),
    6 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5),
    7 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5, Input7 Output7 6),
    8 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5, Input7 Output7 6, Input8 Output8 7),
}

/// Input for graph nodes.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum NodeInput<Compact> {
    /// A compact payload decoded through the node input's graph codec.
    Single(Compact),
    /// Dependency results deserialized as a JSON array in the supplied order.
    FanIn(Vec<Value>),
}
