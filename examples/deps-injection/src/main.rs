//! Extract task metadata, shared state, and an application-defined argument.

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use miju::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

const TASK_COUNT: u32 = 10;
type Counter = Arc<AtomicUsize>;

#[derive(Debug)]
struct ApiClient;

// The client does not depend on a payload type, so it can be extracted from any Sync request.
impl<Request: Sync> FromRequest<Request> for ApiClient {
    type Error = Infallible;

    async fn from_request(_request: &Request) -> Result<Self, Self::Error> {
        // This stand-in demonstrates custom extraction without making network requests.
        Ok(Self)
    }
}

// A handler accepts its payload followed by up to 16 FromRequest arguments.
async fn process(
    value: u32,            // Required first argument; its type must match the backend payload.
    task: TaskContext,     // Context for this task's execution, including elapsed time.
    worker: WorkerContext, // The executing worker supplies this shared handle.
    task_id: TaskId,       // Identifier carried by the task received from the backend.
    attempt: Attempt,      // Execution attempt counter associated with the task.
    count: Data<Counter>,  // Shared application state supplied by WorkerBuilder::data.
    client: ApiClient,     // Constructed through the custom FromRequest implementation.
) -> Result<(), BoxDynError> {
    assert_eq!(task.task_id(), task_id.to_string());
    assert_eq!(attempt.current(), 1);
    let processed = count.fetch_add(1, Ordering::Relaxed) + 1;
    info!(
        value,
        worker = worker.name(),
        %task_id,
        attempt = attempt.current(),
        elapsed = ?task.elapsed(),
        processed,
        ?client,
        "Task arguments extracted"
    );

    if value == TASK_COUNT - 1 {
        worker.stop()?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let count = Counter::default();
    let mut backend = MemoryStorage::new();
    for value in 0..TASK_COUNT {
        backend.push(value).await?;
    }

    WorkerBuilder::new("deps-injection")
        .backend(backend)
        .data(count.clone())
        .enable_tracing()
        .build(process)
        .run()
        .await?;

    assert_eq!(count.load(Ordering::Relaxed), TASK_COUNT as usize);
    info!("All ten tasks used the same counter");
    Ok(())
}
