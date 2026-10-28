//! CSV completion results survive closing and reopening the storage.

use futures_util::{StreamExt, future::poll_fn};
use miju_core::{
    backend::{Backend, TaskSink, WaitForCompletion},
    error::BoxDynError,
    task::{builder::TaskBuilder, status::Status, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use miju_file_storage::CsvStorage;
use serde_json::{Value, json};

async fn close(store: &mut CsvStorage<u32>) {
    poll_fn(|cx| Backend::poll_close(store, cx, &WorkerContext::new("csv-close")))
        .await
        .unwrap();
}

#[tokio::test]
async fn result_histories_keep_their_meaning_after_close_and_reopen() {
    for (count, first, second) in [
        (1, Ok(json!(14)), None),
        (1, Err("failure".to_owned()), None),
        (2, Ok(Value::Null), None),
        (2, Ok(json!("done")), Some(Err("failure".to_owned()))),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.csv");
        let mut store = CsvStorage::<u32>::new(&path).unwrap();
        for index in 1..=count {
            store
                .push_task(
                    TaskBuilder::new(index)
                        .task_id(TaskId::from(u64::from(index)))
                        .build(),
                )
                .await
                .unwrap();
        }
        let context = WorkerContext::new("csv-history");
        for expected in [Some(first.clone()), second.clone()].into_iter().flatten() {
            poll_fn(|cx| Backend::poll_ready(&mut store, cx, &context))
                .await
                .unwrap();
            let task = poll_fn(|cx| Backend::poll_next(&mut store, cx, &context))
                .await
                .unwrap()
                .unwrap();
            let acknowledgement: Result<Value, BoxDynError> = expected.map_err(Into::into);
            store.ack(&acknowledgement, task.ctx()).await.unwrap();
        }
        close(&mut store).await;
        drop(store);
        let mut restored = CsvStorage::<u32>::new(&path).unwrap();
        let mut results = <CsvStorage<u32> as WaitForCompletion<Value>>::check_status(
            &mut restored,
            (1..=u64::from(count)).map(TaskId::from),
        )
        .await
        .unwrap();
        results.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        assert_eq!(results.len(), count as usize);
        assert_eq!(results[0].result, first);
        if count == 2 {
            if let Some(expected) = second {
                assert_eq!(results[1].result, expected);
                assert_eq!(results[1].status, Status::Failed);
            } else {
                assert_eq!(results[1].status, Status::Pending);
                assert!(results[1].result.is_err());
            }
        }
    }
}

#[tokio::test]
async fn acknowledged_result_survives_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("results.csv");
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    let id = TaskId::from_string("completed");
    store
        .push_task(TaskBuilder::new(7).task_id(id.clone()).build())
        .await
        .unwrap();
    let context = WorkerContext::new("csv-results");
    poll_fn(|cx| Backend::poll_ready(&mut store, cx, &context))
        .await
        .unwrap();
    let task = poll_fn(|cx| Backend::poll_next(&mut store, cx, &context))
        .await
        .unwrap()
        .unwrap();
    store
        .ack(&Ok::<u32, BoxDynError>(14), task.ctx())
        .await
        .unwrap();
    poll_fn(|cx| Backend::poll_close(&mut store, cx, &context))
        .await
        .unwrap();
    drop(store);

    let mut restored = CsvStorage::<u32>::new(&path).unwrap();
    let results =
        <CsvStorage<u32> as WaitForCompletion<u32>>::check_status(&mut restored, [id.clone()])
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].task_id, id);
    assert_eq!(results[0].result, Ok(14));
    assert_eq!(results[0].status, Status::Done);
}

#[tokio::test]
async fn mixed_results_remain_distinct_across_rewrites_and_appends() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed.csv");
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    for index in 1..=3 {
        store
            .push_task(
                TaskBuilder::new(index)
                    .task_id(TaskId::from(u64::from(index)))
                    .build(),
            )
            .await
            .unwrap();
    }
    let results = [
        Ok(json!(null)),
        Ok(json!({"a.b": [true, null, "001", " a,b\n雪 ", u64::MAX], "empty": {}})),
        Err("failed,\nwith details".to_owned()),
    ];
    for expected in results {
        let mut clone = store.clone();
        clone
            .set_result(0, serde_json::to_value(&expected).unwrap())
            .unwrap();
        clone
            .set_result(1, serde_json::to_value(Err::<Value, _>("failure")).unwrap())
            .unwrap();
        close(&mut clone).await;
        drop(clone);
        close(&mut store).await;
        drop(store);
        store = CsvStorage::new(&path).unwrap();
        let mut observed = <CsvStorage<u32> as WaitForCompletion<Value>>::check_status(
            &mut store,
            [1, 2, 3].map(TaskId::from),
        )
        .await
        .unwrap();
        observed.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        assert_eq!(observed.len(), 3);
        assert_eq!(observed[0].result, expected);
        assert_eq!(observed[1].result, Err("failure".to_owned()));
        assert_eq!(observed[2].status, Status::Pending);
        assert_eq!(observed[2].result, Err("Task not completed yet".to_owned()));
    }
    store
        .push_task(TaskBuilder::new(4).task_id(TaskId::from(4)).build())
        .await
        .unwrap();
    store.set_result(3, json!({"Ok": 42})).unwrap();
    close(&mut store).await;
    drop(store);
    let mut restored = CsvStorage::<u32>::new(&path).unwrap();
    let mut output =
        <CsvStorage<u32> as WaitForCompletion<u32>>::wait_for(&mut restored, [TaskId::from(4)]);
    assert_eq!(output.next().await.unwrap().unwrap().result, Ok(42));
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn legacy_result_files_keep_their_layout_and_scalar_reader() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy.csv");
    std::fs::write(&path, "args,result,task_id\n7,14,first\n").unwrap();
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    let (_, raw) = store.next().await.unwrap().unwrap();
    assert_eq!(serde_json::to_value(raw).unwrap()["result"], json!(14));
    store.set_result(0, json!(16)).unwrap();
    store
        .push_task(
            TaskBuilder::new(8)
                .task_id(TaskId::from_string("second"))
                .build(),
        )
        .await
        .unwrap();
    close(&mut store).await;
    drop(store);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "args,result,task_id\n7,16,first\n8,null,second\n"
    );
    let mut restored = CsvStorage::<u32>::new(&path).unwrap();
    let (_, raw) = restored.next().await.unwrap().unwrap();
    assert_eq!(serde_json::to_value(raw).unwrap()["result"], json!(16));
}

#[test]
fn malformed_versioned_results_are_rejected_without_modifying_files() {
    for cell in ["", "0", "gg", "é0", "6e756c6c", "5b312c325d"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("invalid.csv");
        let bytes = format!("args,result_v1,task_id\n7,{cell},first\n");
        std::fs::write(&path, &bytes).unwrap();
        assert!(CsvStorage::<u32>::new(&path).is_err(), "cell {cell:?}");
        assert_eq!(std::fs::read_to_string(path).unwrap(), bytes);
    }
}
