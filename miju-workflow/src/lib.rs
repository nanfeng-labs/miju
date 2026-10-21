#![doc = include_str!("../README.md")]

use graph::NodeInput;
use miju_core::{error::BoxDynError, task::Task};
use sequential::router::{GoTo, StepResponse};

type BoxedService<Input, Output> = tower::util::BoxCloneSyncService<Input, Output, BoxDynError>;
type SteppedService<Compact> = BoxedService<Task<Compact>, GoTo<StepResponse>>;
type NodeService<Compact> = BoxedService<Task<NodeInput<Compact>>, (Compact, serde_json::Value)>;

/// Sequential workflow composition and execution.
pub mod sequential;

pub use {graph::GraphFlow, sequential::SteppedFlow};

/// In-memory workflow tasks and result waiting.
pub mod in_memory;

/// Typed graph construction, progress metadata and node adapters.
pub mod graph;
