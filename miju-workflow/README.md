# miju-workflow

Build typed sequences of tasks and execute them with miju workers. Each successful
nonterminal step encodes its output and enqueues a new task for the next step.

## Context

`WorkflowContext` stores a step index
in task metadata; `StepContext<B>` carries a backend, the current step index, and
whether another step follows.

## Store a step index

Use the core `Metadata` trait to write and read workflow progress:

```rust
use miju_core::task::metadata::{Metadata, MetadataStore};
use miju_workflow::sequential::WorkflowContext;

let mut metadata = MetadataStore::new();
WorkflowContext { step_index: 2 }.inject(&mut metadata)?;
let restored = WorkflowContext::extract(&metadata)?;
assert_eq!(restored.step_index, 2);
# Ok::<(), miju_workflow::sequential::context::WorkflowContextError>(())
```

The metadata key is `miju_workflow.context.step_index`. Missing keys, invalid
unsigned integer values, and duplicate writes return `WorkflowContextError`.
A default context starts at index zero, but extracting from empty metadata returns
an error. Neither context checks whether its index names an existing step.

Cloning a `StepContext` clones its backend; sharing behavior depends on that
backend's `Clone` implementation. The context types carry state only; `SteppedFlow` constructs the pipeline and
its workflow service dispatches tasks to their steps.


## Sequential execution

This example uses a temporary JSON file through `miju-file-storage`:

```rust
use miju_core::{
    backend::TaskSink,
    error::BoxDynError,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_file_storage::JsonStorage;
use miju_workflow::SteppedFlow;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxDynError> {
    let mut backend = JsonStorage::new_temp()?;
    backend.push(12u32).await?;
    let flow = SteppedFlow::new("format-count")
        .and_then(async |count: u32| count + 1)
        .and_then(async |count: u32| format!("count={count}"))
        .and_then(async |message: String, worker: WorkerContext| {
            assert_eq!(message, "count=13");
            worker.stop().unwrap();
        });
    let worker = WorkerBuilder::new("format-worker").backend(backend).build(flow);
    tokio::time::timeout(std::time::Duration::from_secs(5), worker.run()).await??;
    Ok(())
}
```

Handlers use the same argument extraction as ordinary core tasks. A step can
read `Data<StepContext<B>>` for its position and backend. Each continuation has
a fresh task ID and step metadata; arbitrary metadata and application data from
the preceding task are not automatically copied.

## Execution limits

Backends must provide compact task delivery, a codec, a sink, configuration, and
cloneable handles. Backend persistence, scheduling, and wakeup limits still apply;
the temporary-file example does not establish crash recovery or exactly-once execution.

Handler and decoding failures are returned by the service without scheduling the
next step. The last ordinary step returns `GoTo::Done`; its output is not retained
in that response. Missing or malformed workflow metadata currently dispatches to
step zero. An unknown index or an empty workflow can panic. The service also has
inherited readiness limitations, so custom steps must not assume every inner
service is polled for readiness before its first call.

The low-level `GoTo::DelayFor` transition attaches scheduling metadata; actual
delay enforcement depends on the backend. Delay builders, filtering, folding,
repetition, graph execution, and workflow-specific in-memory storage are not yet
available in this package.
