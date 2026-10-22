//! Feed three graph entries and collect their typed outputs in dependency order.

use miju::prelude::*;
use miju_workflow::{GraphFlow, WorkflowSink, in_memory::InMemoryWorkflow};
use tracing::info;
use tracing_subscriber::EnvFilter;

async fn name(user_id: u32) -> Result<String, BoxDynError> {
    Ok(user_id.to_string())
}

async fn age(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 20)
}

async fn address_code(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 100)
}

// Graph nodes are task services: injected arguments work alongside dependency input.
async fn collect(
    (name, age, address): (String, usize, usize),
    worker: WorkerContext,
) -> Result<usize, BoxDynError> {
    let total = name.parse::<usize>()? + age + address;
    info!(%name, age, address, total, "Collected graph results");
    worker.stop()?;
    Ok(total)
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let mut backend = InMemoryWorkflow::<Vec<u8>>::create();
    // One entry request carries one input per root node, in registration order.
    backend.start_fan_out(vec![42_u32, 43, 44]).await?;

    let graph = GraphFlow::new("profile-summary");
    let name_node = graph.add_task(name);
    let age_node = graph.add_task(age);
    let address_node = graph.add_task(address_code);
    // Tuple positions and output types must agree with collect's first argument.
    graph
        .add_task(collect)
        .depends_on((&name_node, &age_node, &address_node));
    graph.validate()?;

    // DOT output shows three entry nodes pointing to one collector; view it in Graphviz.
    info!("Workflow graph (DOT):\n{graph}");
    WorkerBuilder::new("workflow-graph")
        .backend(backend)
        .parallelize(tokio::spawn)
        .enable_tracing()
        .on_event(|_, event| info!(%event, "Workflow event"))
        .build(graph)
        .run()
        .await?;
    Ok(())
}
