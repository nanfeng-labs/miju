//! Stop a standalone worker after a finite batch; press Ctrl+C to exit the process.

use std::time::Duration;

use miju::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

const TASK_COUNT: u32 = 100;

#[derive(Debug)]
struct BatchTask {
    id: u32,
}

async fn process(task: BatchTask, worker: WorkerContext) {
    info!(id = task.id, worker = worker.name(), "Task started");

    if task.id == TASK_COUNT - 1 {
        // All tasks were queued in order before startup, so this is the last one admitted.
        // has_pending_tasks counts active tasks, not messages still in the backend.
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if !worker.has_pending_tasks() {
                    info!("Batch drained; requesting worker shutdown");
                    worker.stop().expect("the worker has already started");
                    break;
                }
            }
        });
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    info!(id = task.id, "Task finished");
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
    for id in 0..TASK_COUNT {
        backend.push(BatchTask { id }).await?;
    }

    WorkerBuilder::new("unmonitored-worker")
        .backend(backend)
        .concurrency(20)
        .enable_tracing()
        .on_event(|worker, event| info!(worker = worker.name(), %event, "Worker event"))
        .build(process)
        // run_until also waits for its signal after the worker stops itself.
        // Ctrl+C exits, or requests an earlier shutdown that drains active tasks.
        .run_until(tokio::signal::ctrl_c())
        .await?;

    info!("Worker shutdown complete");
    Ok(())
}
