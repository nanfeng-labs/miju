# miju-workflow

Context types for sequential tasks in miju. `WorkflowContext` stores a step index
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
backend's `Clone` implementation. These types carry state only. This batch does
not yet provide workflow construction, execution, or storage.
