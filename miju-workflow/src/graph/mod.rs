//! Graph progress metadata, input codecs and typed node adapters.
//!
//! [`GraphFlowContext`] records node positions and task IDs. [`GraphNodeResponse`](response::GraphNodeResponse)
//! describes branching, pending dependencies and completion. [`GraphCodec`](decode::GraphCodec)
//! converts node inputs, and [`GraphNodeService`](node::GraphNodeService) adapts a
//! typed service to compact or fan-in input. Graph construction and scheduling
//! are not yet available.

use serde_json::Value;

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

pub use context::GraphFlowContext;

/// Input for graph nodes.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum NodeInput<Compact> {
    /// A compact payload decoded through the node input's graph codec.
    Single(Compact),
    /// Dependency results deserialized as a JSON array in the supplied order.
    FanIn(Vec<Value>),
}
