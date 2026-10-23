//! Cancel a running task, observe its terminal state, and then stop the worker.

use std::time::Duration;

use miju::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

const CANCEL_AFTER: Duration = Duration::from_secs(3);
const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(10);

async fn process(value: u32, task: TaskContext, worker: WorkerContext) -> Result<(), BoxDynError> {
    assert_eq!(value, 42);
    let execution = task
        .execution_context()
        .expect("the running task has an execution context");
    assert_eq!(execution.attempt(), 1);
    assert_eq!(execution.status(), Status::Running);
    info!(task_id = task.task_id(), "Task is running");

    // A cloned TaskContext controls the same task from another async operation.
    let cancellation = task.clone();
    tokio::spawn(async move {
        tokio::time::sleep(CANCEL_AFTER).await;
        cancellation.cancel().expect("the task is still running");
        info!("Cancellation requested");
    });

    // This observer must outlive the handler that cancellation will drop.
    tokio::spawn(async move {
        tokio::select! {
            () = task.executed() => {
                // executed() also resolves on normal completion, so check which occurred.
                assert!(task.is_cancelled());
                let elapsed = task.elapsed();
                assert!(elapsed >= CANCEL_AFTER);
                assert!(elapsed < OBSERVATION_TIMEOUT);
                info!(?elapsed, "Cancellation observed through executed()");
            }
            () = tokio::time::sleep(OBSERVATION_TIMEOUT) => {
                panic!("the task did not report cancellation in time");
            }
        }

        // Cancellation wakes the observer; the worker records Killed on its next poll.
        // Observe that transition instead of assuming it happens within a fixed delay.
        tokio::time::timeout(OBSERVATION_TIMEOUT, async {
            while execution.status() != Status::Killed {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the worker should record the task as killed");
        assert_eq!(execution.status(), Status::Killed);
        info!(status = ?execution.status(), "Task cancelled before handler completion");
        worker.stop().expect("the worker has already started");
    });

    // Each simulated work step yields so the worker can observe cancellation.
    // Normal completion would take ten seconds, longer than the cancellation delay.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    unreachable!("cancellation must prevent the rest of this handler from running");
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

    let context = WorkerContext::new("task-context");
    let worker = WorkerBuilder::new(&context)
        .backend(backend)
        .enable_tracing()
        .on_event(|_, event| info!(%event, "Worker event"))
        .build(process);

    // Fail visibly if a background assertion fails before it can stop the worker.
    tokio::time::timeout(Duration::from_secs(15), worker.run()).await??;
    assert!(!context.has_pending_tasks());
    info!("Worker stopped after task cancellation");
    Ok(())
}
