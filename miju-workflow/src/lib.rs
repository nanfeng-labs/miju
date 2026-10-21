#![doc = include_str!("../README.md")]

use miju_core::{error::BoxDynError, task::Task};
use sequential::router::{GoTo, StepResponse};

type BoxedService<Input, Output> = tower::util::BoxCloneSyncService<Input, Output, BoxDynError>;
type SteppedService<Compact> = BoxedService<Task<Compact>, GoTo<StepResponse>>;

/// Sequential workflow composition and execution.
pub mod sequential;

pub use sequential::SteppedFlow;
