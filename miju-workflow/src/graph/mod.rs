//! Graph progress metadata, response records and error types.
//!
//! [`GraphFlowContext`] records node positions and task IDs. [`GraphNodeResponse`](response::GraphNodeResponse)
//! describes branching, pending dependencies and completion. This module currently
//! provides these data types; graph construction and execution are not yet available.

/// Task metadata for graph progress and dependency lookup.
pub mod context;
/// Errors for graph services, dependencies and metadata.
pub mod error;
/// Serializable graph node outcomes.
pub mod response;

pub use context::GraphFlowContext;
