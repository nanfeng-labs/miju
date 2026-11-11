//! Reopening file storage resumes only tasks without a stored result.

use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use futures_util::task::noop_waker_ref;
use miju_core::{
    backend::{Backend, TaskSink},
    error::{BoxDynError, WorkerError},
    task::{builder::TaskBuilder, data::Data, task_id::TaskId},
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_file_storage::JsonStorage;
use serde_json::json;

fn delivered(store: &mut JsonStorage<u32>) -> Vec<TaskId> {
    let worker = WorkerContext::new("reader");
    let mut ids = Vec::new();
    while let Poll::Ready(Some(Ok(task))) =
        Backend::poll_next(store, &mut Context::from_waker(noop_waker_ref()), &worker)
    {
        ids.push(task.task_id().unwrap().clone());
    }
    ids
}

#[tokio::test]
async fn reopened_storage_delivers_only_tasks_without_results() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.json");
    let mut store = JsonStorage::<u32>::new(&path).unwrap();
    for id in ["pending", "succeeded", "failed", "later"] {
        store
            .push_task(TaskBuilder::new(1).task_id(TaskId::from_string(id)).build())
            .await
            .unwrap();
    }
    store.set_result(1, json!({"Ok": 2})).unwrap();
    store.set_result(2, json!({"Err": "boom"})).unwrap();
    drop(store);

    let mut reopened = JsonStorage::<u32>::new(&path).unwrap();
    assert_eq!(
        delivered(&mut reopened),
        vec![TaskId::from_string("pending"), TaskId::from_string("later")]
    );
}

#[tokio::test]
async fn tasks_finished_by_a_worker_are_not_run_again_after_reopening() {
    async fn job(
        value: u32,
        seen: Data<Arc<Mutex<Vec<u32>>>>,
        worker: WorkerContext,
    ) -> Result<u32, BoxDynError> {
        let count = {
            let mut seen = seen.lock().unwrap();
            seen.push(value);
            seen.len()
        };
        if count == 3 && worker.name() == "first" {
            worker.stop()?;
        }
        Ok(value)
    }
    async fn run(store: JsonStorage<u32>, name: &str) -> Vec<u32> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        WorkerBuilder::new(name)
            .backend(store)
            .data(seen.clone())
            .build(job)
            .run_until(async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok::<(), WorkerError>(())
            })
            .await
            .unwrap();
        seen.lock().unwrap().clone()
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.json");
    let mut store = JsonStorage::<u32>::new(&path).unwrap();
    for value in 1..=3 {
        store.push(value).await.unwrap();
    }
    assert_eq!(run(store, "first").await, vec![1, 2, 3]);
    assert!(
        run(JsonStorage::new(&path).unwrap(), "second")
            .await
            .is_empty()
    );
}
