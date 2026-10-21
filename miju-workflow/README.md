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
backend's `Clone` implementation. The context types carry state only;
`SteppedFlow` constructs the pipeline and its workflow service dispatches tasks
to their steps.

## Sequential execution

This example runs in memory and waits for the entry task's stored result:

```rust
use miju_core::{
    backend::{TaskSink, WaitForCompletion},
    task::{builder::TaskBuilder, task_id::{GenerateId, RandomId}},
    error::BoxDynError,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use futures_util::StreamExt;
use miju_workflow::{SteppedFlow, in_memory::InMemoryWorkflow, sequential::{GoTo, StepResponse}};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxDynError> {
    let mut backend = InMemoryWorkflow::create();
    let mut observer = backend.clone();
    let entry_id = RandomId::generate();
    backend.push_task(TaskBuilder::new(12u32).task_id(entry_id.clone()).build()).await?;
    let flow = SteppedFlow::new("format-count")
        .and_then(async |count: u32| count + 1)
        .and_then(async |count: u32| format!("count={count}"))
        .and_then(async |message: String, worker: WorkerContext| {
            assert_eq!(message, "count=13");
            worker.stop().unwrap();
        });
    let worker = WorkerBuilder::new("format-worker").backend(backend).build(flow);
    tokio::time::timeout(std::time::Duration::from_secs(5), worker.run()).await??;
    let mut results = WaitForCompletion::<GoTo<StepResponse>>::wait_for(&mut observer, [entry_id]);
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), results.next())
        .await?.expect("entry result")?;
    assert!(matches!(result.result, Ok(GoTo::Next(_))));
    Ok(())
}
```

Handlers use the same argument extraction as ordinary core tasks. A step can
read `Data<StepContext<B>>` for its position and backend. Each continuation has
a fresh task ID and step metadata; arbitrary metadata and application data from
the preceding task are not automatically copied.

## Delay steps

Use `delay_for` for a fixed duration or `delay_with` to compute it from the
decoded task. Both add a separate step that re-enqueues the original payload
with a fresh task ID, the next step index, and a `run_at` timestamp:

```rust
use std::time::Duration;
use miju_core::{backend::ext::shared::Shared, task::Task};
use miju_workflow::{SteppedFlow, in_memory::InMemoryWorkflow};

let flow = SteppedFlow::<u32, u32, Shared<InMemoryWorkflow<u32>>>::new("scheduled")
    .delay_for(Duration::from_secs(30))
    .delay_with(|task: Task<u32>| Duration::from_secs(u64::from(task.args)))
    .and_then(async |value: u32| value.to_string());
```

This constructs the workflow; executing it requires a worker and a backend.
Delay steps do not sleep. The backend must enforce scheduling timestamps, which
use whole Unix seconds. The in-memory backend currently delivers these tasks
immediately. A delay result contains the duration and the next task ID.

Always follow a delay with a processing step. A terminal delay currently enqueues
an index that has no registered step, causing dispatch to panic. This limitation
applies to both fixed and computed delays.

## In-memory results

`InMemoryWorkflow::create()` returns a cloneable `Shared` handle. Arguments are
encoded as JSON bytes; a result-storage layer records each executed task. The
public `ResultStore`, `StoreResultsLayer`, and `StoreResultsService` types also
allow inspecting or composing that middleware directly. A task passed directly
to the layer must already have an ID.

`wait_for` yields results for the requested IDs and finishes after every unique
requested ID has been observed. Supply distinct IDs: duplicate IDs currently
prevent termination. Waiting for an entry task does not wait for all subsequent
steps; a `GoTo::Next` result provides the next task ID. `check_status` returns
only results currently stored and omits missing IDs.

Handler errors are returned as stream errors; `check_status` fails if a requested
stored task failed. A successful value that cannot be decoded into the requested
output type instead produces an error inside `TaskResult.result`.

Results are process-local, unbounded, and retained after being read. There is no
persistence or automatic eviction. The current waiter implementation has a
check/register race, shares registration IDs when streams are cloned, and does
not remove registrations when pending streams are dropped. Avoid assuming that
result waiting is reliable under all concurrent interleavings. If serializing a
successful handler output fails, the service returns an error without recording
a result, so a waiter for that task may remain pending.

The backend delivers queued tasks without enforcing scheduling timestamps.
For temporary-file integration, `miju-file-storage::JsonStorage` remains available
and is exercised by the sequential integration tests.

## Features

The default `tracing` feature enables result-storage diagnostics and core worker
logs. Disable default features to omit those events. Shared backend handles and
JSON encoding remain available in both configurations.

## Execution limits

Backends must provide compact task delivery, a codec, a sink, configuration, and
cloneable handles. Backend persistence, scheduling, and wakeup limits still apply;
neither the in-memory backend nor the file-backed integration establishes
crash recovery or exactly-once execution.

Handler and decoding failures are returned by the service without scheduling the
next step. The last ordinary step returns `GoTo::Done`; its output is not retained
in that response. Missing or malformed workflow metadata currently dispatches to
step zero. An unknown index or an empty workflow can panic. The service also has
inherited readiness limitations, so custom steps must not assume every inner
service is polled for readiness before its first call.

The low-level `GoTo::DelayFor` transition also attaches scheduling metadata;
actual delay enforcement depends on the backend. Filtering, folding, repetition,
and graph execution are not yet available in this package.
