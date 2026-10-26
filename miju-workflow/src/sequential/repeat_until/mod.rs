//! Repeat a step with its original input until the handler produces an output.
//!
//! `None` enqueues another task at the same step, preserving the compact payload.
//! `Some(output)` advances or completes the workflow. Read [`RepeaterState`] with
//! [`Meta`](miju_core::task::metadata::Meta) to inspect the iteration count and
//! previous task ID. Repetition has no automatic delay or iteration limit.

use std::fmt::Display;
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::str::FromStr;
use std::task::Context;

use futures_util::future::BoxFuture;
use futures_util::{FutureExt, Sink, SinkExt};
use miju_core::backend::codec::Codec;
use miju_core::backend::{BackendConfig, TaskSinkError, WireFormatBackend};
use miju_core::error::BoxDynError;
use miju_core::task::builder::TaskBuilder;
use miju_core::task::metadata::{Metadata, MetadataError, MetadataStore};
use miju_core::task::task_fn::{TaskFn, task_fn};
use miju_core::task::task_id::{GenerateId, TaskId};
use miju_core::{backend::Backend, task::Task};
use serde::{Deserialize, Serialize};
use serde_json::to_value;
use tower::Service;

use crate::sequential::router::WorkflowRouter;
use crate::sequential::{GoTo, Layer, Stack, Step, StepContext, StepResponse, WorkflowContext};
use crate::{SteppedFlow, SteppedService};

/// A layer that represents a `repeat_until` step in the workflow.
#[derive(Clone, Debug)]
pub struct RepeatUntil<F, Input, Output> {
    repeater: F,
    _marker: PhantomData<(Input, Output)>,
}

impl<F, Input, Output, S> Layer<S> for RepeatUntil<F, Input, Output>
where
    F: Clone,
{
    type Step = RepeatUntilStep<S, F, Input, Output>;

    fn layer(&self, step: S) -> Self::Step {
        RepeatUntilStep {
            inner: step,
            repeater: self.repeater.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}
impl<Start, L, Input, B: Backend> SteppedFlow<Start, Input, B, L> {
    /// Repeat with the same input until the handler returns `Some(output)`.
    ///
    /// Returning `None` enqueues the original compact input at the same step with
    /// a new task ID, an incremented iteration count and the current task ID as
    /// its predecessor. The result is [`GoTo::Break`] with a null value and a
    /// `next_task_id`; that task still needs to run.
    ///
    /// Returning `Some(output)` enqueues the next step and returns [`GoTo::Next`],
    /// or returns [`GoTo::Break`] with the output when this is the terminal step.
    /// The current registration bounds require the output type to match the input
    /// type when chaining a typed next step. A terminal repeater can change types.
    ///
    /// Use `Meta<RepeaterState>` as an extra handler argument to read progress.
    /// The initial iteration count is zero. Repetition has no built-in limit or
    /// delay, so the handler must eventually return `Some` or an error. Handler
    /// and decoding errors propagate without scheduling another task.
    /// If another iteration would overflow the count, the step returns an error
    /// without scheduling another task.
    ///
    /// ```rust
    /// use miju_core::{backend::ext::shared::Shared, task::metadata::Meta};
    /// use miju_workflow::{
    ///     SteppedFlow, in_memory::InMemoryWorkflow,
    ///     sequential::repeat_until::RepeaterState,
    /// };
    ///
    /// let flow = SteppedFlow::<u32, u32, Shared<InMemoryWorkflow<u32>>>::new("repeat")
    ///     .repeat_until(async |value: u32, Meta(state): Meta<RepeaterState>| {
    ///         (state.iterations() >= 2).then_some(value)
    ///     })
    ///     .and_then(async |value: u32| value.to_string());
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn repeat_until<F, Output, FnArgs>(
        self,
        repeater: F,
    ) -> SteppedFlow<Start, Output, B, Stack<RepeatUntil<TaskFn<F, Input, FnArgs>, Input, Output>, L>>
    where
        TaskFn<F, Input, FnArgs>: Service<Task<Input>, Response = Option<Output>>,
    {
        self.add_step(RepeatUntil {
            repeater: task_fn(repeater),
            _marker: PhantomData::<(Input, Output)>,
        })
    }
}

/// The step implementation for the `repeat_until` layer.
#[derive(Clone, Debug)]
pub struct RepeatUntilStep<S, R, Input, Output> {
    inner: S,
    repeater: R,
    _marker: PhantomData<(Input, Output)>,
}

/// The service that handles the `repeat_until` logic
#[derive(Debug)]
pub struct RepeatUntilService<F, B, Input, Output> {
    repeater: F,
    _marker: std::marker::PhantomData<(B, Input, Output)>,
}

impl<F, B, Input, Output> Clone for RepeatUntilService<F, B, Input, Output>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            repeater: self.repeater.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F, Res, B, Input, CodecError, Err> Service<Task<B::Compact>>
    for RepeatUntilService<F, B, Input, Res>
where
    F: Service<Task<Input>, Response = Option<Res>> + Send + 'static + Clone,
    B: Backend<Error = Err>
        + WireFormatBackend
        + BackendConfig
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact>, Error = Err>
        + Unpin
        + 'static,
    B::Codec: Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Res, Error = CodecError, Compact = B::Compact>
        + Codec<Option<Res>, Error = CodecError, Compact = B::Compact>
        + Send
        + Sync
        + Clone
        + 'static,
    B::Id: GenerateId + Send + Sync + Display + FromStr + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static, // We don't need Clone because decoding just needs a reference
    Res: Send + Serialize + 'static,
{
    type Response = GoTo<StepResponse>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.repeater.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, task: Task<B::Compact>) -> Self::Future {
        let state: RepeaterState = Metadata::extract(task.metadata()).unwrap_or_default();
        let mut ctx =
            task.data().get::<StepContext<B>>().cloned().expect(
                "StepContext missing, Did you call the repeater outside of a workflow step?",
            );
        let mut repeater = self.repeater.clone();
        let codec = ctx.backend.codec().clone();

        (async move {
            let mut compact = None;
            let decoded: Input = B::Codec::decode(&codec, &task.args)?;
            let prev_task_id = task.task_id().cloned();
            let repeat_task = task.map_args(|c| {
                compact = Some(c);
                decoded
            });
            let response = repeater.call(repeat_task).await.map_err(|e| e.into())?;
            Ok(match response {
                Some(res) if ctx.has_next => {
                    let task_id = B::Id::generate();
                    let next_step = TaskBuilder::new(B::Codec::encode(&codec, &res)?)
                        .task_id(task_id.clone())
                        .metadata(&WorkflowContext {
                            step_index: ctx.current_step + 1,
                        })
                        .build();
                    ctx.backend
                        .send(next_step)
                        .await
                        .map_err(TaskSinkError::PushError)?;
                    GoTo::Next(StepResponse {
                        result: to_value(&res)?,
                        next_task_id: Some(task_id),
                    })
                }
                Some(res) => GoTo::Break(StepResponse {
                    result: to_value(&res)?,
                    next_task_id: None,
                }),
                None => {
                    let iterations = state
                        .iterations
                        .checked_add(1)
                        .ok_or("Repeat iteration count overflow")?;
                    let task_id = B::Id::generate();
                    let next_step =
                        TaskBuilder::new(compact.take().expect("Compact args should be set"))
                            .task_id(task_id.clone())
                            .metadata(&WorkflowContext {
                                step_index: ctx.current_step,
                            })
                            .metadata(&RepeaterState {
                                iterations,
                                prev_task_id,
                            })
                            .build();
                    ctx.backend
                        .send(next_step)
                        .await
                        .map_err(TaskSinkError::PushError)?;
                    GoTo::Break(StepResponse {
                        result: to_value(&None::<Res>)?,
                        next_task_id: Some(task_id),
                    })
                }
            })
        }
        .boxed()) as _
    }
}

/// Metadata describing progress through one repeat step.
///
/// Missing entries represent zero iterations and no predecessor. Extracting
/// malformed entries returns an error; the repeat service itself falls back to
/// the default state on that error. A `Meta<RepeaterState>` handler argument still
/// reports the malformed metadata as an extraction error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepeaterState {
    iterations: usize,
    prev_task_id: Option<TaskId>,
}

impl RepeaterState {
    /// Get the number of preceding `None` results in this repetition chain.
    #[must_use]
    pub fn iterations(&self) -> usize {
        self.iterations
    }

    /// Get the ID of the task that enqueued this iteration, when it had an ID.
    #[must_use]
    pub fn previous_task_id(&self) -> Option<&TaskId> {
        self.prev_task_id.as_ref()
    }
}

/// An error representing an invalid [`RepeaterState`]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RepeaterStateError {
    /// Missing iterations key (currently extraction defaults missing values to zero)
    #[error("the data for key {REPEATER_ITERATIONS_KEY} is missing")]
    MissingIterations,

    /// Could not parse iterations
    #[error("could not parse key {REPEATER_ITERATIONS_KEY}")]
    ParseIterations(#[from] ParseIntError),

    /// Could not parse a task id
    #[error("could not parse key {REPEATER_PREV_TASK_ID_KEY}")]
    ParseTaskId,

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

const REPEATER_ITERATIONS_KEY: &str = "miju_workflow.repeater.iterations";
const REPEATER_PREV_TASK_ID_KEY: &str = "miju_workflow.repeater.prev_task_id";

impl Metadata for RepeaterState {
    type Error = RepeaterStateError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let iterations = map
            .get(REPEATER_ITERATIONS_KEY)
            .map(|s| s.parse::<usize>())
            .transpose()?
            .unwrap_or(0);

        let prev_task_id = map
            .get(REPEATER_PREV_TASK_ID_KEY)
            .map(|value| TaskId::from_str(value).map_err(|_| RepeaterStateError::ParseTaskId))
            .transpose()?;

        Ok(Self {
            iterations,
            prev_task_id,
        })
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), RepeaterStateError> {
        map.insert(REPEATER_ITERATIONS_KEY, self.iterations.to_string())?;

        if let Some(task_id) = &self.prev_task_id {
            map.insert(REPEATER_PREV_TASK_ID_KEY, task_id.to_string())?;
        }
        Ok(())
    }
}

impl<B, F, Input, Res, S, Err, CodecError> Step<Input, B> for RepeatUntilStep<S, F, Input, Res>
where
    F: Service<Task<Input>, Response = Option<Res>> + Send + Sync + 'static + Clone,
    B: Backend<Error = Err>
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact>, Error = Err>
        + Unpin
        + WireFormatBackend
        + BackendConfig
        + 'static,
    B::Codec: Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Res, Error = CodecError, Compact = B::Compact>
        + Codec<Option<Res>, Error = CodecError, Compact = B::Compact>
        + Send
        + Sync
        + Clone
        + 'static,
    B::Id: GenerateId + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + Sync + 'static, // We don't need Clone because decoding just needs a reference
    Res: Serialize + Send + Sync + 'static,
    S: Step<Input, B> + Send + 'static,
    B::Id: FromStr + Display + Sync,
{
    type Response = Res;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(RepeatUntilService {
            repeater: self.repeater.clone(),
            _marker: PhantomData::<(B, Input, Res)>,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}
