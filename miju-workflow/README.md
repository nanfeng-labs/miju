<p align="center">
  <img src="../assets/miju-workflow-header.svg" alt="miju-workflow" width="480" />
</p>

<h1 align="center">miju-workflow</h1>

Build typed sequences and dependency graphs of tasks for miju workers. Ordinary
nonterminal steps encode their outputs and enqueue tasks for the next step;
collection steps filter and map elements or reduce them into an accumulator.
Graph services execute nodes, enqueue outgoing tasks and collect stored results
when branches converge.

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
Reusing the in-memory sink after a failed send to a closed channel can also panic;
the underlying error mapper is consumed by its first failure.

The backend delivers queued tasks without enforcing scheduling timestamps.
For temporary-file integration, `miju-file-storage::JsonStorage` remains available
and is exercised by the sequential integration tests.

## Enqueue workflow tasks

Import `WorkflowSink` to encode and send entry, step or node inputs through a
compatible backend. Each call generates an ID and enqueues one task:

| Method | Encoding | Workflow metadata |
| --- | --- | --- |
| `push_start(args)` | Ordinary backend codec | None; execution starts through the service's entry path. |
| `start_fan_out(args)` | `GraphCodec` | None; the executor distributes multiple entry inputs. |
| `push_step(args, index)` | Ordinary backend codec | The requested sequential step index. |
| `push_node(args, index)` | Ordinary backend codec | Fresh graph context rooted at the generated task ID. |

Inputs may differ from the backend's configured argument type, which supports
later steps accepting a different type. These helpers do not inspect a workflow
or validate indices, input types or entry counts. `push_node` leaves the previous
node and dependency IDs empty; it cannot resume a fan-in or restore existing
execution progress. `start_fan_out` encodes according to the input type rather
than graph topology. See the [sink module](sink) for a runnable multi-entry example.

For byte-based JSON, a single tuple node with two or more fields needs the graph
tuple layout from `start_fan_out`. A single vector node instead expects ordinary
codec encoding from `push_start`; graph vector encoding is for separately encoded
entry inputs. The existing `GraphCodec` singleton and collection limits apply.

Encoding failures return `TaskSinkError::CodecError` before sending; backend send
failures return `TaskSinkError::PushError`. Success means the sink's send operation
completed, not that handlers ran. Cancellation and partial-send behavior depend
on the backend. The helpers return no task ID and cannot carry caller-supplied
metadata, scheduling options or application data; use a task builder and a compact
backend sink when those details are needed.

## Graph construction

`GraphFlow` registers task functions or Tower services and connects their outputs
to dependent inputs. `add_task` uses a function's Rust type name as its label;
`add_node` accepts an explicit name. `depends_on` adds incoming edges and returns
a handle exposing the node and edge indices:

```rust
use miju_core::task::task_fn::task_fn;
use miju_workflow::{GraphFlow, in_memory::InMemoryWorkflow};

let graph = GraphFlow::<InMemoryWorkflow<u32>>::new("counts");
let first = graph.add_node("first", task_fn(|value: u32| async move { value + 1 }));
let second = graph.add_node("second", task_fn(|value: u32| async move { value + 2 }));
let total = graph.add_task(|(left, right): (u32, u32)| async move { left + right })
    .depends_on((&first, &second));
let values = graph.add_task(|values: Vec<u32>| async move { values.len() })
    .depends_on(vec![&first, &second]);

graph.validate()?;
assert_eq!(total.edges().len(), 2);
assert_eq!(values.edges().len(), 2);
assert!(graph.to_dot().contains("first"));
# Ok::<(), miju_workflow::graph::error::GraphFlowError>(())
```

Dependency output types must match the receiving node's input. Single builders
and handles supply one output, borrowed tuples support up to eight outputs, and
vectors supply a homogeneous collection. Vector dependencies currently require
at least two entries; shorter vectors panic. Custom `DepsCheck` implementations
can supply indices directly.

Connecting nodes does not execute handlers or check for cycles. `validate`
detects directed cycles, including self-loops, while accepting empty graphs and
disconnected acyclic components. It does not check graph ownership, names or wire
compatibility. `new_typed` currently behaves the same as `new`. `route` registers
a function with an additional output-to-node-index bound, but execution follows
ordinary outgoing edges and does not use that conversion to select a route.

Builders and handles must refer to the same graph. Their indices do not retain
graph identity: a foreign in-range index can connect an unintended node, while
an out-of-range index panics and can poison the graph lock. Repeated dependencies
create parallel edges. Cloned builders append to the same node, and each returned
handle lists only the edges added by its own `depends_on` call.

DOT export inserts labels without escaping. Repeated names retain all nodes but
only label the newest one. Use distinct names without quotes, backslashes or
newlines when relying on DOT output. `RootGraphService::get_node_by_name_mut`
also selects the most recently registered node when names repeat.

## Graph execution

Passing a `GraphFlow` to `WorkerBuilder::build` converts it through
`IntoWorkerService` into a `graph::RootGraphService`. Conversion validates cycles
and panics if validation fails; call `graph.validate()` first to handle that error.
The graph service receives compact tasks directly. For an in-memory worker, use
`InMemoryWorkflow<Vec<u8>>`; `WorkflowSink::push_start` encodes the entry input:

```rust
use miju_core::worker::{builder::WorkerBuilder, context::WorkerContext};
use miju_workflow::{GraphFlow, WorkflowSink, in_memory::InMemoryWorkflow};

# #[tokio::main]
# async fn main() {
let backend = InMemoryWorkflow::<Vec<u8>>::create();
let mut producer = backend.clone();
let graph = GraphFlow::new("counts");
let first = graph.add_task(|value: u32| async move { value + 1 });
let _last = graph
    .add_task(|value: u32, worker: WorkerContext| async move {
        assert_eq!(value, 8);
        worker.stop().unwrap();
        value * 2
    })
    .depends_on(&first);
graph.validate().unwrap();

producer.push_start(7_u32).await.unwrap();
let worker = WorkerBuilder::new("counts").backend(backend).build(graph);
# tokio::time::timeout(std::time::Duration::from_secs(5), async {
worker.run().await.unwrap();
# }).await.unwrap();
# }
```

The worker backend's configured argument type must match its compact task type.
Using `TaskSink::push` with an already encoded byte vector would encode the bytes
again. Direct service calls are also available through
`IntoWorkerService::into_service(backend).service`.

With several entry nodes, the initial argument is a codec-encoded
`Vec<B::Compact>`, with one separately encoded input per entry in ascending node
index order. For JSON, this is a list of byte arrays, not an ordinary JSON tuple
of the handler values. `WorkflowSink::start_fan_out` accepts tuples with two to
twelve elements or vectors and prepares that layout. The input count must equal
the entry count. An empty graph accepts an encoded empty list and returns
`EntryFanOut` with no tasks.

Each node call reports one scheduling outcome:

| Response | Meaning |
| --- | --- |
| `EntryFanOut` | Initial inputs have been enqueued for the entry nodes. |
| `FanOut` | Several successors were enqueued; includes this node's result and their IDs. |
| `EnqueuedNext` | One successor was enqueued; includes this node's result. |
| `WaitingForDependencies` | This call did not execute the fan-in node or requeue itself. |
| `Complete` | A terminal node finished; includes its result. |

These responses do not certify completion of an entire graph. In particular,
waiting for the root task's result does not wait for all branches. At a fan-in,
the predecessor with the greatest node index waits for all recorded dependency
IDs. Other predecessors return `WaitingForDependencies`. Successful dependency
results must have `Done` status and contain `FanOut`, `EnqueuedNext` or `Complete`;
their values are supplied in incoming edge insertion order. The backend must
store these responses and implement `WaitForCompletion<GraphNodeResponse>`.

Execution currently assumes node zero for an initial task with one entry node,
even if the actual entry has another index. Missing or malformed graph metadata
also takes the initial-task path; reinjecting context into partial metadata can
fail or panic on duplicate keys. Use fresh initial metadata and register the
single entry first.

The root service clones node services for each call and skips their readiness
polls. Clone-local mutable state does not persist between calls. New tasks copy
graph metadata but omit arbitrary application data and unrelated metadata.
Fan-out records fresh IDs for newly enqueued nodes and retains other mappings.
Single-edge scheduling does not update dependency IDs, and independently extended
branches do not merge their mappings. Fan-out increments the position twice, and
completed-node sets are not updated. Do not rely on those fields as a complete
execution history or assume joins across longer branches are fully supported.

Dependency waits have no timeout. Missing mappings can return a waiting response
without scheduling another attempt, and backend errors are collected only after
the result stream ends. Enqueueing several successors has no rollback on partial
failure, and retries can repeat handler effects or enqueue duplicate work.
Nested workflow composition is not available yet.

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
`GraphServiceError`, including wrapped source errors. `RootGraphService` uses
these records to report node execution and continuation scheduling.

## Graph node adapters

`graph::node::GraphNodeService` wraps a typed Tower service. It accepts
`graph::NodeInput::Single` containing a backend compact value, or
`NodeInput::FanIn` containing ordered JSON dependency results. The task must carry
the backend codec in runtime data. For example, a standalone adapter can add two
dependency results and return both encoded bytes and JSON:

```rust
use miju_codec::json::JsonCodec;
use miju_core::task::{builder::TaskBuilder, task_fn::task_fn};
use miju_workflow::{
    graph::{NodeInput, node::GraphNodeService},
    in_memory::InMemoryWorkflow,
};
use serde_json::json;
use tower::ServiceExt;

# #[tokio::main]
# async fn main() -> Result<(), miju_core::error::BoxDynError> {
let handler = task_fn(|(left, right): (u32, u32)| async move { left + right });
let node = GraphNodeService::<_, InMemoryWorkflow<u32>, (u32, u32)>::new(handler);
let task = TaskBuilder::new(NodeInput::FanIn(vec![json!(4), json!(5)]))
    .data(JsonCodec::<Vec<u8>>::default())
    .build();
let (compact, value) = node.oneshot(task).await?;
assert_eq!(compact, b"9");
assert_eq!(value, json!(9));
# Ok(())
# }
```

The adapter delegates readiness and preserves the task's execution context.
Input decoding errors skip the handler. Handler errors, output encoding errors
and JSON serialization errors are returned; handler effects are not rolled back
if serialization fails. Missing codec data panics before a future is returned,
including for fan-in inputs.

Single inputs use `graph::decode::GraphCodec`. Scalars, options and results pass
through the backend codec. Tuples with two to twelve elements use a list of
individually encoded elements and panic if its decoded length is wrong. Vectors
and one-element tuples have asymmetric encoding/decoding; for example, byte JSON
cannot round-trip a nonempty `Vec<u32>` or `(u32,)` through this trait. Node output
uses ordinary codec encoding, which can differ from the graph tuple layout.

Fan-in inputs bypass `GraphCodec::decode`: their JSON values are deserialized as
an array into the handler input. This preserves the supplied order but does not
fetch results, check readiness or infer which dependency belongs in each position.
`RootGraphService` performs that dependency lookup before calling the adapter.

## Features

The default `tracing` feature enables graph execution, result-storage and core
worker diagnostics. Disable default features to omit those events. Shared backend handles and
JSON encoding remain available in both configurations.

## Execution limits

Backends must provide compact task delivery, a codec, a sink, configuration, and
cloneable handles. Backend persistence, scheduling, and wakeup limits still apply;
neither the in-memory backend nor the file-backed integration establishes
crash recovery or exactly-once execution.

In sequential workflows, ordinary handler and decoding failures are returned by the service without
scheduling the next step. The last ordinary step returns `GoTo::Done`; its output is not retained
in that response. Missing or malformed workflow metadata currently dispatches to
step zero. An unknown index or an empty workflow can panic. The service also has
inherited readiness limitations, so custom steps must not assume every inner
service is polled for readiness before its first call.

The low-level `GoTo::DelayFor` transition also attaches scheduling metadata;
actual delay enforcement depends on the backend. Graph-specific execution limits
are described above.
