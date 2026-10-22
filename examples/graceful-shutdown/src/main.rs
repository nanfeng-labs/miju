//! Finish the current batch and simulate a checkpoint before shutting down.

use std::time::Duration;

use miju::prelude::*;
use tracing::{Instrument, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct CheckpointTask;

async fn process(_task: CheckpointTask, worker: WorkerContext) {
    let mut completed_batches = 0;
    info!("Task started; press Ctrl+C to request a checkpoint and shutdown");

    loop {
        // Finish one unit of work before checking whether shutdown was requested.
        tokio::time::sleep(Duration::from_secs(5)).await;
        completed_batches += 1;
        info!(completed_batches, "Batch completed");

        if worker.is_shutting_down() {
            info!(completed_batches, "Saving task state");
            // Simulate a checkpoint write; this example does not persist data.
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("Task cleanup finished");
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let mut backend = MemoryStorage::new();
    backend.push(CheckpointTask).await?;

    WorkerBuilder::new("graceful-shutdown")
        .backend(backend)
        .concurrency(2)
        .enable_tracing()
        .build(process)
        // The signal requests shutdown; the worker still waits for task cleanup.
        .run_until(tokio::signal::ctrl_c())
        .instrument(tracing::info_span!("graceful-shutdown"))
        .await?;

    info!("Worker shutdown complete");
    Ok(())
}
