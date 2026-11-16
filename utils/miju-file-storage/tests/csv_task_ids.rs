//! CSV task IDs keep their variant across close and reopen, while legacy files
//! keep their own layout.

use futures_util::{StreamExt, future::poll_fn};
use miju_core::{
    backend::{Backend, TaskSink, WaitForCompletion},
    task::{builder::TaskBuilder, status::Status, task_id::TaskId},
    worker::context::WorkerContext,
};
use miju_file_storage::{CsvStorage, FileStorageError};
use serde_json::{Value, json};
use std::path::Path;

fn hex(value: &Value) -> String {
    serde_json::to_vec(value)
        .unwrap()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A new-format file with one pending row per encoded ID.
fn versioned_csv(ids: &[Value]) -> String {
    let mut text = "args,result_v1,task_id_v1\n".to_owned();
    for (index, id) in ids.iter().enumerate() {
        text.push_str(&format!("{},5b5d,{}\n", index + 1, hex(id)));
    }
    text
}

async fn close(store: &mut CsvStorage<u32>) {
    poll_fn(|cx| Backend::poll_close(store, cx, &WorkerContext::new("close")))
        .await
        .unwrap();
}

async fn status(
    store: &mut CsvStorage<u32>,
    ids: Vec<TaskId>,
) -> Vec<miju_core::backend::TaskResult<u32>> {
    <CsvStorage<u32> as WaitForCompletion<u32>>::check_status(store, ids)
        .await
        .unwrap()
}

/// Task IDs in row order, as a worker would receive them.
async fn row_ids(store: &CsvStorage<u32>, count: usize) -> Vec<TaskId> {
    let mut reader = store.clone();
    let worker = WorkerContext::new("read-ids");
    let mut ids = Vec::new();
    for _ in 0..count {
        poll_fn(|cx| Backend::poll_ready(&mut reader, cx, &worker))
            .await
            .unwrap();
        let task = poll_fn(|cx| Backend::poll_next(&mut reader, cx, &worker))
            .await
            .unwrap()
            .unwrap();
        ids.push(task.task_id().unwrap().clone());
    }
    ids
}

fn assert_rejected_unchanged(path: &Path, text: &str) {
    std::fs::write(path, text).unwrap();
    let result = CsvStorage::<u32>::new(path);
    assert!(
        matches!(&result, Err(FileStorageError::AdapterError(error)) if error.kind() == std::io::ErrorKind::InvalidData),
        "expected invalid CSV data for {text:?}: {result:?}"
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), text);
}

fn header(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn integer_and_string_ids_with_the_same_text_stay_distinct_after_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.csv");
    let ids = [TaskId::from_int(42), TaskId::from_string("42")];
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    for (index, id) in ids.iter().enumerate() {
        store
            .push_task(
                TaskBuilder::new(index as u32 + 1)
                    .task_id(id.clone())
                    .build(),
            )
            .await
            .unwrap();
    }
    // Acknowledgement writes results by line, as the worker's ack layer does.
    store.set_result(0, json!({"Ok": 10})).unwrap();
    store.set_result(1, json!({"Ok": 20})).unwrap();
    close(&mut store).await;
    drop(store);

    // Completed rows are not delivered to workers again, so look them up by ID.
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    let results = status(&mut store, ids.to_vec()).await;
    assert_eq!(results.len(), 2, "both IDs must be found: {results:?}");
    for (id, expected) in ids.iter().zip([10, 20]) {
        let result = results.iter().find(|result| &result.task_id == id).unwrap();
        assert_eq!(result.result, Ok(expected), "result for {id:?}");
    }
    let mut waited =
        <CsvStorage<u32> as WaitForCompletion<u32>>::wait_for(&mut store, [ids[0].clone()]);
    assert_eq!(waited.next().await.unwrap().unwrap().result, Ok(10));
}

#[tokio::test]
async fn versioned_ids_preserve_strings_and_results_through_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new.csv");
    let ids = vec![
        TaskId::from_int(42),
        TaskId::from_string("42"),
        TaskId::from_int(u64::MAX),
        TaskId::from_string(u64::MAX.to_string()),
        TaskId::from_string(""),
        TaskId::from_string("a,b\n雪\r\0"),
        TaskId::from_string("7b22496e74223a34327d"),
    ];
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    for (index, id) in ids.iter().enumerate() {
        store
            .push_task(
                TaskBuilder::new(index as u32 + 1)
                    .task_id(id.clone())
                    .build(),
            )
            .await
            .unwrap();
    }
    close(&mut store).await;
    drop(store);
    assert_eq!(header(&path), "args,result_v1,task_id_v1");

    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    assert_eq!(row_ids(&store, ids.len()).await, ids);
    for index in 0..ids.len() {
        store
            .set_result(index, json!({"Ok": (index + 1) * 10}))
            .unwrap();
    }
    store
        .push_task(
            TaskBuilder::new(99)
                .task_id(TaskId::from_string("append"))
                .build(),
        )
        .await
        .unwrap();
    close(&mut store).await;
    drop(store);

    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    let results = status(&mut store, ids.clone()).await;
    assert_eq!(results.len(), ids.len());
    for (index, id) in ids.iter().enumerate() {
        assert_eq!(
            results
                .iter()
                .find(|result| &result.task_id == id)
                .unwrap()
                .result,
            Ok((index as u32 + 1) * 10)
        );
    }
    assert_eq!(
        status(&mut store, vec![TaskId::from_string("append")]).await[0].status,
        Status::Pending
    );
}

#[tokio::test]
async fn legacy_id_files_keep_their_layout_on_append_and_result_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.csv");
    let text = format!(
        "args,result_v1,task_id\n1,{},42\n2,{},42\n",
        hex(&json!([{"Ok": 10}])),
        hex(&json!([{"Ok": 20}]))
    );
    std::fs::write(&path, &text).unwrap();
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
    // The existing ambiguity remains for legacy files.
    assert_eq!(
        status(
            &mut store,
            vec![TaskId::from_int(42), TaskId::from_string("42")]
        )
        .await
        .len(),
        1
    );
    store
        .push_task(
            TaskBuilder::new(3)
                .task_id(TaskId::from_string("third"))
                .build(),
        )
        .await
        .unwrap();
    store.set_result(0, json!({"Ok": 11})).unwrap();
    store.set_result(2, json!({"Ok": 30})).unwrap();
    close(&mut store).await;
    drop(store);

    let written = std::fs::read_to_string(&path).unwrap();
    assert_eq!(written.lines().next(), Some("args,result_v1,task_id"));
    assert!(!written.contains("task_id_v1"));
    let mut store = CsvStorage::<u32>::new(&path).unwrap();
    assert_eq!(
        status(&mut store, vec![TaskId::from_int(42)]).await[0].result,
        Ok(11)
    );
    assert_eq!(
        status(&mut store, vec![TaskId::from_string("third")]).await[0].result,
        Ok(30)
    );
}

#[tokio::test]
async fn legacy_text_is_not_read_as_tagged_and_empty_ids_keep_the_old_policy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.csv");
    let encoded = hex(&json!({"Int": 42}));
    let text = format!("args,result_v1,task_id\n1,5b5d,{encoded}\n2,5b5d,\n3,5b5d,0\n");
    std::fs::write(&path, &text).unwrap();
    let store = CsvStorage::<u32>::new(&path).unwrap();
    let ids = row_ids(&store, 3).await;
    assert_eq!(ids[0], TaskId::from_string(encoded));
    assert!(matches!(&ids[1], TaskId::String(generated) if !generated.is_empty()));
    assert_eq!(ids[2], TaskId::from_int(0));
    assert_eq!(std::fs::read_to_string(path).unwrap(), text);
}

#[tokio::test]
async fn legacy_result_and_id_formats_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    for versioned in [false, true] {
        let path = dir.path().join(format!("{versioned}.csv"));
        let (column, id) = if versioned {
            ("task_id_v1", hex(&json!({"String": "first"})))
        } else {
            ("task_id", "first".to_owned())
        };
        std::fs::write(&path, format!("args,result,{column}\n7,14,{id}\n")).unwrap();
        let mut store = CsvStorage::<u32>::new(&path).unwrap();
        let (_, raw) = store.next().await.unwrap().unwrap();
        assert_eq!(serde_json::to_value(raw).unwrap()["result"], 14);
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

        let expected = if versioned {
            format!(
                "args,result,task_id_v1\n7,16,{id}\n8,null,{}\n",
                hex(&json!({"String": "second"}))
            )
        } else {
            "args,result,task_id\n7,16,first\n8,null,second\n".to_owned()
        };
        assert_eq!(std::fs::read_to_string(&path).unwrap(), expected);
    }
}

#[test]
fn malformed_versioned_id_cells_are_rejected_without_fallback_or_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let cells = [
        String::new(),
        "0".into(),
        "gg".into(),
        "é0".into(),
        hex(&json!(42)),
        hex(&json!("42")),
        hex(&json!({"Int": -1})),
        hex(&json!({"String": 42})),
        hex(&json!({"Unknown": "x"})),
        hex(&json!({"Typed": "42"})),
        hex(&json!({"Typed": "plain"})),
    ];
    for (index, cell) in cells.iter().enumerate() {
        assert_rejected_unchanged(
            &dir.path().join(format!("{index}.csv")),
            &format!("args,result_v1,task_id_v1\n1,5b5d,{cell}\n"),
        );
    }
}

#[test]
fn unsupported_mixed_and_duplicate_id_columns_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let id = hex(&json!({"Int": 42}));
    let cases = [
        ("args,result_v1,task_id_v2", format!("1,5b5d,{id}")),
        (
            "args,result_v1,task_id,task_id_v1",
            format!("1,5b5d,42,{id}"),
        ),
        (
            "args,result_v1,task_id_v1,task_id_v1",
            format!("1,5b5d,{id},{id}"),
        ),
        (
            "args,result_v1,task_id_v1,task_id_v2",
            format!("1,5b5d,{id},{id}"),
        ),
    ];
    for (index, (header, row)) in cases.iter().enumerate() {
        assert_rejected_unchanged(
            &dir.path().join(format!("{index}-header-only.csv")),
            &format!("{header}\n"),
        );
        assert_rejected_unchanged(
            &dir.path().join(format!("{index}-row.csv")),
            &format!("{header}\n{row}\n"),
        );
    }
}

#[test]
fn a_versioned_header_requires_a_versioned_id_on_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let rows = [
        "1,5b5d".to_owned(),
        "1,5b5d,42".to_owned(),
        format!("1,5b5d,{}\n2,5b5d,42", hex(&json!({"Int": 1}))),
    ];
    for (index, row) in rows.iter().enumerate() {
        assert_rejected_unchanged(
            &dir.path().join(format!("{index}.csv")),
            &format!("args,result_v1,task_id_v1\n{row}\n"),
        );
    }
}

#[tokio::test]
async fn empty_and_header_only_files_keep_the_selected_id_layout() {
    let dir = tempfile::tempdir().unwrap();
    for (name, initial, expected_column) in [
        ("empty", "", "task_id_v1"),
        ("legacy", "args,result_v1,task_id\n", "task_id"),
        ("new", "args,result_v1,task_id_v1\n", "task_id_v1"),
    ] {
        let path = dir.path().join(format!("{name}.csv"));
        std::fs::write(&path, initial).unwrap();
        let mut store = CsvStorage::<u32>::new(&path).unwrap();
        store
            .push_task(
                TaskBuilder::new(1)
                    .task_id(TaskId::from_string("42"))
                    .build(),
            )
            .await
            .unwrap();
        close(&mut store).await;
        drop(store);
        assert_eq!(header(&path), format!("args,result_v1,{expected_column}"));
        let store = CsvStorage::<u32>::new(&path).unwrap();
        let expected = if name == "legacy" {
            TaskId::from_int(42)
        } else {
            TaskId::from_string("42")
        };
        assert_eq!(row_ids(&store, 1).await[0], expected);
    }
}

#[tokio::test]
async fn typed_ids_need_their_feature_and_same_text_strings_stay_strings() {
    let dir = tempfile::tempdir().unwrap();
    let texts = [
        "550e8400-e29b-41d4-a716-446655440000",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    ];
    for (index, text) in texts.iter().enumerate() {
        // Feature unification decides whether miju-core parses typed IDs here.
        let parsed: Option<TaskId> = text
            .parse()
            .ok()
            .filter(|id| !matches!(id, TaskId::Int(_) | TaskId::String(_)));
        let path = dir.path().join(format!("typed-{index}.csv"));
        let raw = versioned_csv(&[json!({"Typed": text})]);
        if let Some(id) = parsed {
            std::fs::write(&path, raw).unwrap();
            let store = CsvStorage::<u32>::new(&path).unwrap();
            assert_eq!(row_ids(&store, 1).await[0], id);
            drop(store);
            let pair = dir.path().join(format!("pair-{index}.csv"));
            let mut store = CsvStorage::<u32>::new(&pair).unwrap();
            store
                .push_task(TaskBuilder::new(1).task_id(id.clone()).build())
                .await
                .unwrap();
            store
                .push_task(
                    TaskBuilder::new(2)
                        .task_id(TaskId::from_string(*text))
                        .build(),
                )
                .await
                .unwrap();
            close(&mut store).await;
            drop(store);
            let store = CsvStorage::<u32>::new(&pair).unwrap();
            assert_eq!(row_ids(&store, 2).await, [id, TaskId::from_string(*text)]);
        } else {
            assert_rejected_unchanged(&path, &raw);
        }
        let path = dir.path().join(format!("string-{index}.csv"));
        std::fs::write(&path, versioned_csv(&[json!({"String": text})])).unwrap();
        let store = CsvStorage::<u32>::new(&path).unwrap();
        assert_eq!(row_ids(&store, 1).await[0], TaskId::from_string(*text));
    }
}
