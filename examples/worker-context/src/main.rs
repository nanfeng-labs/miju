//! Inspect task execution from both its handler and an external worker handle.

use std::time::Duration;

use miju::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

async fn process(value: u32, task_id: TaskId, worker: WorkerContext) -> Result<(), BoxDynError> {
    assert_eq!(value, 42);

    // Task lookup requires the task-context feature and a currently tracked task ID.
    let task = worker.get_task(&task_id.to_string())?;
    let execution = task
        .execution_context()
        .expect("the running handler keeps its execution context alive");
    assert_eq!(execution.attempt(), 1);
    assert_eq!(execution.status(), Status::Running);
    assert_eq!(execution.task_id(), Some(&task_id));
    info!(%task_id, status = ?execution.status(), "Inspecting the running task");

    // Keep the task active so the external observer can inspect the same context.
    tokio::time::sleep(Duration::from_secs(5)).await;
    worker.stop()?;
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

    let mut backend = MemoryStorage::new();
    backend.push(42u32).await?;

    let context = WorkerContext::new("worker-context");
    let worker = WorkerBuilder::new(&context)
        .backend(backend)
        .enable_tracing()
        .build(process);

    // Clones share worker state; the observer does not need ownership of the worker.
    let observed = context.clone();
    let observer = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if observed.is_stopped() {
                break;
            }
            info!(
                worker = observed.name(),
                count = observed.task_count(),
                tasks = ?observed.tasks()?,
                "Worker task snapshot"
            );
        }
        Ok::<(), BoxDynError>(())
    });

    worker.run().await?;
    observer.await??;
    assert!(!context.has_pending_tasks());
    info!("Worker stopped with no tracked tasks");
    Ok(())
}
