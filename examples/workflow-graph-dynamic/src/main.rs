//! Build a graph at runtime and collect a dynamically sized set of dependencies.

use miju::prelude::*;
use miju_workflow::{GraphFlow, WorkflowSink, in_memory::InMemoryWorkflow};
use tracing::info;
use tracing_subscriber::EnvFilter;

const BRANCHES: usize = 1_000;

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let graph = GraphFlow::new("generated-branches");
    let source = graph.add_node("source", task_fn(|(): ()| async { 1_i32 }));
    let mut branches = Vec::with_capacity(BRANCHES);
    for index in 0..BRANCHES {
        let branch = graph
            .add_node(
                &format!("increment-{index}"),
                task_fn(|value: i32| async move { value + 1 }),
            )
            .depends_on(&source);
        branches.push(branch);
    }

    // A Vec of node handles becomes a Vec of results, after every dependency finishes.
    graph
        .add_node(
            "sum",
            task_fn(|values: Vec<i32>, worker: WorkerContext| async move {
                let total = values.iter().sum::<i32>();
                info!(branches = values.len(), total, "Collected dynamic graph");
                worker.stop()?;
                Ok::<_, BoxDynError>(total)
            }),
        )
        .depends_on(branches);
    graph.validate()?;

    let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
    // A single unit input starts the source, which fans its value out to all branches.
    backend.push_start(()).await?;
    WorkerBuilder::new("workflow-graph-dynamic")
        .backend(backend)
        .enable_tracing()
        .build(graph)
        .run()
        .await?;
    Ok(())
}
