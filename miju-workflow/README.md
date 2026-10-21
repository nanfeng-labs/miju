# miju-workflow

Build typed sequences of tasks and execute them with miju workers. Ordinary
nonterminal steps encode their outputs and enqueue tasks for the next step;
collection steps filter and map elements or reduce them into an accumulator.

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

## Filter and map collections

Use `filter_map` on an iterable to run a handler for each element. Returning
`Some(value)` keeps the mapped value; `None` discards it. The workflow enqueues
element tasks and a collector, which waits for their results before forwarding
a nonempty `Vec<Output>` to the next step. This requires a backend that records
results and implements `WaitForCompletion`, such as `InMemoryWorkflow`:

```rust
use miju_core::{
    backend::TaskSink,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_workflow::{SteppedFlow, in_memory::InMemoryWorkflow};

# #[tokio::main]
# async fn main() {
let backend = InMemoryWorkflow::create();
let mut producer = backend.clone();
producer.push(vec![1u32, 2, 3, 4]).await.unwrap();

let flow = SteppedFlow::new("even-labels")
    .filter_map(async |n: u32| (n % 2 == 0).then(|| format!("item-{n}")))
    .and_then(async |mut labels: Vec<String>, worker: WorkerContext| {
        labels.sort();
        assert_eq!(labels, ["item-2", "item-4"]);
        worker.stop().unwrap();
    });
let worker = WorkerBuilder::new("even-labels").backend(backend).build(flow);
# tokio::time::timeout(std::time::Duration::from_secs(5), async {
worker.run().await.unwrap();
# }).await.unwrap();
# }
```

Output order follows the backend's result stream and need not match input order.
Empty inputs and all-filtered collections currently stop at the collector with
`GoTo::Break([])`; subsequent steps do not run. The entry task returns `GoTo::Done`
after enqueueing, so waiting for its ID alone does not observe collection
completion. Fan-out is not transactional: an enqueue failure can leave already
created element tasks without a collector. Invalid element payloads can panic
during decoding. Result-stream errors propagate, while malformed collected
values or unexpected transitions are skipped.

## Fold collections

Use `fold` to reduce a nonempty iterable into a single value. The handler receives
`(accumulator, item)` and returns an accumulator of the same type. Its initial
value comes from `Default`; each item is processed in iterator order by a
separate queued task. When `fold` is the first step, worker construction currently
needs an explicit collection type in `WorkflowService`. The accumulator may have
a different type from the items:

```rust
use miju_core::{
    backend::TaskSink,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_workflow::{
    SteppedFlow, in_memory::InMemoryWorkflow, sequential::service::WorkflowService,
};

# #[tokio::main]
# async fn main() {
let backend = InMemoryWorkflow::create();
let mut producer = backend.clone();
producer.push(vec![2u32, 4, 6]).await.unwrap();

let flow = SteppedFlow::new("sum")
    .fold(async |(sum, value): (u64, u32)| sum + u64::from(value))
    .and_then(async |total: u64, worker: WorkerContext| {
        assert_eq!(total, 12);
        worker.stop().unwrap();
    });
let worker = WorkerBuilder::new("sum")
    .backend(backend)
    .build::<_, WorkflowService<_, Vec<u32>, Vec<u32>>, _>(flow);
# tokio::time::timeout(std::time::Duration::from_secs(5), async {
worker.run().await.unwrap();
# }).await.unwrap();
# }
```

The initializer and intermediate tasks return `GoTo::Next` with a continuation
ID. The last item returns `GoTo::Break` containing the final value. If a later
workflow step exists, it is already enqueued and its ID is included in that
`Break` response; the variant alone does not mean that execution has stopped.
A terminal fold retains its final value and has no next task ID.

Fold requires a codec and task sink, without requiring `WaitForCompletion`.
Every continuation serializes the accumulator and all remaining items, so large
collections incur repeated copying and encoding. Empty input currently panics
when processing its first collection task. Validate that input is nonempty;
when composed after `filter_map`, an all-filtered result stops before reaching
the fold. Missing or unrecognized fold-state metadata starts initialization.

## Repeat until a result is ready

Use `repeat_until` when a step should run again with the same input until its
handler returns `Some(output)`. Returning `None` creates a new task at the same
step with the original encoded arguments. Read `Meta<RepeaterState>` to inspect
the number of prior repetitions and the previous task ID:

```rust
use miju_core::{
    backend::TaskSink,
    task::metadata::Meta,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_workflow::{
    SteppedFlow, in_memory::InMemoryWorkflow, sequential::repeat_until::RepeaterState,
};

# #[tokio::main]
# async fn main() {
let backend = InMemoryWorkflow::create();
let mut producer = backend.clone();
producer.push(7u32).await.unwrap();

let flow = SteppedFlow::new("repeat")
    .repeat_until(async |value: u32, Meta(state): Meta<RepeaterState>| {
        (state.iterations() >= 2).then_some(value)
    })
    .and_then(async |value: u32, worker: WorkerContext| {
        assert_eq!(value, 7);
        worker.stop().unwrap();
    });
let worker = WorkerBuilder::new("repeat").backend(backend).build(flow);
# tokio::time::timeout(std::time::Duration::from_secs(5), async {
worker.run().await.unwrap();
# }).await.unwrap();
# }
```

The example calls the handler three times, with iteration counts zero, one and
two. There is no built-in delay or maximum count; the handler controls when to
exit. `None` preserves the original payload, without carrying changes to the
decoded input into the next task. Arbitrary task metadata and application data
are not automatically copied into that new task.

A repeated task returns `GoTo::Break` with a null result and the next iteration's
ID. `Some(output)` returns `GoTo::Next` when another step follows, or a terminal
`GoTo::Break` retaining the output. Inspect `next_task_id` to distinguish another
iteration from completion. Current registration bounds require matching input
and output types when chaining a typed next step; a terminal repeater may change
types. Missing repetition metadata defaults to the initial state. Malformed
metadata also resets the service's state, although a handler requesting
`Meta<RepeaterState>` receives an extraction error instead.

## In-memory results

`InMemoryWorkflow::create()` returns a cloneable `Shared` handle. Arguments are
encoded as JSON bytes; a result-storage layer records each executed task. The
public `ResultStore`, `StoreResultsLayer`, and `StoreResultsService` types also
allow inspecting or composing that middleware directly. A task passed directly
to the layer must already have an ID.

`wait_for` yields results for the requested IDs and finishes after every unique
requested ID has been observed. Supply distinct IDs: duplicate IDs currently
prevent termination. Waiting for an entry task does not wait for all subsequent
steps; inspect `StepResponse.next_task_id` for continuations, including a fold's
final `GoTo::Break` and a repeater's `GoTo::Break` between iterations.
`check_status` returns only results currently stored and omits missing IDs.

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

## Graph data types

The `graph` module provides progress metadata, response records and error types.
`GraphFlowContext` tracks the current and previous nodes, completed nodes,
dependency task IDs, topological position and root task ID. It can be stored in
task metadata or serialized with Serde:

```rust
use miju_core::task::{metadata::{Metadata, MetadataStore}, task_id::TaskId};
use miju_workflow::graph::GraphFlowContext;
use petgraph::graph::NodeIndex;

let mut context = GraphFlowContext::new(Some(TaskId::from_string("root")));
context.current_node = NodeIndex::new(2);
context.current_position = 1;
context.prev_node = Some(NodeIndex::new(0));
context.completed_nodes.insert(NodeIndex::new(0));
context.node_task_ids.insert(NodeIndex::new(0), TaskId::from_string("entry"));
context.is_initial = false;

let mut metadata = MetadataStore::new();
context.inject(&mut metadata)?;
let restored = GraphFlowContext::extract(&metadata)?;
let dependencies = restored.get_dependency_task_ids(&[NodeIndex::new(0), NodeIndex::new(1)]);
assert_eq!(restored.current_node, NodeIndex::new(2));
assert_eq!(dependencies.len(), 1);
# Ok::<(), miju_workflow::graph::context::GraphFlowContextError>(())
```

Metadata keys use the `miju_workflow.graph.` prefix. Extraction requires
`current_node` and `current_position`; missing optional fields use empty/default
values. Dependency lookup omits nodes without a recorded task ID and does not
check completion. `new` starts at node zero with `is_initial = true`, while
derived `Default` sets that flag to false. Neither validates node indices or
selects a graph's actual entry node.

Missing or malformed `is_initial` metadata becomes true. Injection can partially
write before returning an error, and a duplicate `is_initial` key currently
panics. Inject into a fresh metadata store. Node/task mappings are comma-separated
without escaping, so task IDs containing commas cannot round-trip reliably.

`graph::response::GraphNodeResponse` represents entry fan-out, node fan-out,
next-step enqueueing, waiting dependencies and completion. Its values carry JSON
results and node/task mappings. `graph::error` supplies `GraphFlowError` and
`GraphServiceError`, including wrapped source errors. These data types do not
construct a graph, schedule tasks or execute nodes; those APIs are not yet
available.

## Features

The default `tracing` feature enables result-storage diagnostics and core worker
logs. Disable default features to omit those events. Shared backend handles and
JSON encoding remain available in both configurations.

## Execution limits

Backends must provide compact task delivery, a codec, a sink, configuration, and
cloneable handles. Backend persistence, scheduling, and wakeup limits still apply;
neither the in-memory backend nor the file-backed integration establishes
crash recovery or exactly-once execution.

Ordinary handler and decoding failures are returned by the service without
scheduling the next step. The last ordinary step returns `GoTo::Done`; its output is not retained
in that response. Missing or malformed workflow metadata currently dispatches to
step zero. An unknown index or an empty workflow can panic. The service also has
inherited readiness limitations, so custom steps must not assume every inner
service is polled for readiness before its first call.

The low-level `GoTo::DelayFor` transition also attaches scheduling metadata;
actual delay enforcement depends on the backend. Graph execution is not yet
available in this package.
