//! Application line metadata remains separate from file acknowledgement addresses.

use futures_util::task::noop_waker_ref;
use miju_core::{
    backend::{Backend, TaskSink, WaitForCompletion},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataStore, status::Status, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use miju_file_storage::{CsvStorage, JsonStorage};
use serde_json::{Value, json};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    task::{Context, Poll},
};

fn metadata(value: &str) -> MetadataStore {
    let mut values = MetadataStore::new();
    values.insert("line_id", value).unwrap();
    values
}

fn take<B: Backend<Task = Task<Value>>>(backend: &mut B) -> Task<Value> {
    assert!(
        matches!(
            Backend::poll_ready(
                backend,
                &mut Context::from_waker(noop_waker_ref()),
                &WorkerContext::new("metadata-line-id")
            ),
            Poll::Ready(Ok(()))
        ),
        "complete task source must be ready before dispatch"
    );
    let output = catch_unwind(AssertUnwindSafe(|| {
        Backend::poll_next(
            backend,
            &mut Context::from_waker(noop_waker_ref()),
            &WorkerContext::new("metadata-line-id"),
        )
    }));
    assert!(
        output.is_ok(),
        "accepted user line_id metadata must not panic during dispatch"
    );
    let Poll::Ready(Some(Ok(task))) = output.unwrap() else {
        panic!("complete task must dispatch");
    };
    task
}

#[tokio::test]
async fn json_collision_dispatch_and_ack_preserve_user_metadata_and_correct_row() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.json");
    let mut store = JsonStorage::<u32>::new(&path).unwrap();
    let first = TaskId::from_string("first");
    let second = TaskId::from_string("second");
    store
        .push_task(TaskBuilder::new(3).task_id(first.clone()).build())
        .await
        .unwrap();
    store
        .push_task(
            TaskBuilder::new(7)
                .task_id(second.clone())
                .with_metadata(metadata("0"))
                .build(),
        )
        .await
        .unwrap();
    let first_task = take(&mut store);
    assert_eq!(first_task.metadata().get("line_id"), Some("0"));
    let task = take(&mut store);
    assert_eq!(task.metadata().get("line_id"), Some("0"));
    assert_eq!(task.args, json!(7));
    let task = task.try_map_args(serde_json::from_value::<u32>).unwrap();
    let cloned = task.clone().into_builder().build();
    let mut acknowledger = store.clone();
    acknowledger
        .ack(&Ok::<u32, BoxDynError>(70), cloned.ctx())
        .await
        .unwrap();
    let results = <JsonStorage<u32> as WaitForCompletion<u32>>::check_status(
        &mut store,
        [first.clone(), second.clone()],
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 2);
    let first_result = results.iter().find(|row| row.task_id == first).unwrap();
    assert_eq!(
        first_result.status,
        Status::Pending,
        "ack must not complete a different row selected by user metadata"
    );
    let second_result = results.iter().find(|row| row.task_id == second).unwrap();
    assert_eq!(second_result.status, Status::Done);
    assert_eq!(second_result.result, Ok(70));
    let mut reopened = JsonStorage::<u32>::new(&path).unwrap();
    let _ = take(&mut reopened);
    let task = take(&mut reopened);
    assert_eq!(task.metadata().get("line_id"), Some("0"));
    assert_eq!(task.args, json!(7));
}

#[tokio::test]
async fn json_reopened_text_collision_remains_acknowledgeable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.json");
    let id = TaskId::from_string("text");
    let mut store = JsonStorage::<u32>::new(&path).unwrap();
    store
        .push_task(
            TaskBuilder::new(7)
                .task_id(id.clone())
                .with_metadata(metadata("external-reference"))
                .build(),
        )
        .await
        .unwrap();
    drop(store);
    let mut store = JsonStorage::<u32>::new(&path).unwrap();
    let task = take(&mut store);
    assert_eq!(task.metadata().get("line_id"), Some("external-reference"));
    store
        .ack(
            &Err::<u32, BoxDynError>("handler failed".into()),
            task.ctx(),
        )
        .await
        .unwrap();
    let rows = <JsonStorage<u32> as WaitForCompletion<u32>>::check_status(&mut store, [id])
        .await
        .unwrap();
    assert_eq!(rows[0].result, Err("handler failed".into()));
}

#[tokio::test]
async fn csv_live_collision_can_be_dispatched_and_acknowledged() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.csv");
    let id = TaskId::from_string("csv-task");
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    store
        .push_task(
            TaskBuilder::new(7)
                .task_id(id.clone())
                .with_metadata(metadata("external-reference"))
                .build(),
        )
        .await
        .unwrap();
    let task = take(&mut store);
    assert_eq!(task.metadata().get("line_id"), Some("external-reference"));
    store
        .ack(&Ok::<u32, BoxDynError>(14), task.ctx())
        .await
        .unwrap();
    let rows = <CsvStorage<u32> as WaitForCompletion<u32>>::check_status(&mut store, [id])
        .await
        .unwrap();
    assert_eq!(rows[0].result, Ok(14));
}

#[tokio::test]
async fn existing_noncollision_metadata_and_manual_ack_are_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.json");
    let mut store = JsonStorage::<u32>::new(&path).unwrap();
    let id = TaskId::from_string("plain");
    store
        .push_task(TaskBuilder::new(7).task_id(id.clone()).build())
        .await
        .unwrap();
    let task = take(&mut store);
    assert_eq!(task.metadata().get("line_id"), Some("0"));
    let manual = TaskBuilder::new(()).with_metadata(metadata("0")).build();
    store
        .ack(&Ok::<u32, BoxDynError>(15), manual.ctx())
        .await
        .unwrap();
    let rows = <JsonStorage<u32> as WaitForCompletion<u32>>::check_status(&mut store, [id])
        .await
        .unwrap();
    assert_eq!(rows[0].result, Ok(15));
}

#[tokio::test]
async fn csv_reopened_collision_preserves_application_metadata_and_acknowledges() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.csv");
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    let id = TaskId::from_string("csv-reopened");
    store
        .push_task(
            TaskBuilder::new(7)
                .task_id(id.clone())
                .with_metadata(metadata("external-reference"))
                .build(),
        )
        .await
        .unwrap();
    drop(store);
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    let task = take(&mut store);
    assert_eq!(task.args, json!(7));
    assert_eq!(task.metadata().get("line_id"), Some("external-reference"));
    assert_eq!(task.metadata().get("ctx.line_id"), None);
    store
        .ack(&Ok::<u32, BoxDynError>(14), task.ctx())
        .await
        .unwrap();
    let rows = <CsvStorage<u32> as WaitForCompletion<u32>>::check_status(&mut store, [id])
        .await
        .unwrap();
    assert_eq!(rows[0].result, Ok(14));
}

#[tokio::test]
async fn csv_metadata_keys_survive_reopens_and_result_rewrites() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.csv");
    let mut metadata = MetadataStore::new();
    metadata.insert("queue", "primary").unwrap();
    metadata.insert("ctx.queue", "literal-prefix").unwrap();
    metadata.insert("", "empty-key").unwrap();
    metadata.insert("ctx.", "literal-empty-prefix").unwrap();
    metadata.insert("标签", "001").unwrap();
    let mut expected = metadata.clone();
    expected.insert("line_id", "0").unwrap();
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    store
        .push_task(TaskBuilder::new(7).with_metadata(metadata).build())
        .await
        .unwrap();

    for round in 0..3 {
        drop(store);
        store = CsvStorage::<u32>::new(&path).unwrap();
        let task = take(&mut store);
        assert_eq!(task.args, json!(7));
        assert_eq!(task.metadata().as_map(), expected.as_map());
        if round < 2 {
            store.set_result(0, json!(14 + round)).unwrap();
        }
    }
}

#[tokio::test]
async fn csv_result_rewrite_preserves_metadata_columns() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.csv");
    let mut metadata = MetadataStore::new();
    metadata.insert("queue", "primary").unwrap();
    metadata.insert("ctx.queue", "literal-prefix").unwrap();
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    store
        .push_task(TaskBuilder::new(7).with_metadata(metadata).build())
        .await
        .unwrap();
    drop(store);
    let before = std::fs::read_to_string(&path).unwrap();
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    store.set_result(0, json!(14)).unwrap();
    let after = std::fs::read_to_string(&path).unwrap();
    assert_eq!(after.lines().next(), before.lines().next());
}

#[tokio::test]
async fn csv_reopened_numeric_collision_acknowledges_only_dispatched_row() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.csv");
    let first = TaskId::from_string("first");
    let second = TaskId::from_string("second");
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    for (id, value) in [(first.clone(), "external-first"), (second.clone(), "0")] {
        store
            .push_task(
                TaskBuilder::new(7)
                    .task_id(id)
                    .with_metadata(metadata(value))
                    .build(),
            )
            .await
            .unwrap();
    }
    drop(store);
    let original = std::fs::read_to_string(&path).unwrap();
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    let _ = take(&mut store);
    let original_task = take(&mut store);
    let task = original_task.clone().into_builder().build();
    assert_eq!(task.metadata().get("line_id"), Some("0"));
    store
        .ack(&Ok::<u32, BoxDynError>(14), task.ctx())
        .await
        .unwrap();
    assert_eq!(task.metadata().as_map(), original_task.metadata().as_map());
    let rows = <CsvStorage<u32> as WaitForCompletion<u32>>::check_status(
        &mut store,
        [first.clone(), second.clone()],
    )
    .await
    .unwrap();
    let updated = std::fs::read_to_string(&path).unwrap();
    assert_eq!(updated.lines().next(), original.lines().next());
    assert_eq!(updated.lines().nth(1), original.lines().nth(1));
    let completed = rows.iter().find(|row| row.task_id == second).unwrap();
    assert_eq!(completed.status, Status::Done);
    assert_eq!(completed.result, Ok(14));
}
