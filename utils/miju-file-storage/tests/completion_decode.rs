//! Completion decoding errors remain observable without aborting other results.

use futures_util::{FutureExt, StreamExt};
use miju_core::{
    backend::{TaskSink, WaitForCompletion},
    task::{builder::TaskBuilder, status::Status, task_id::TaskId},
    worker::{builder::WorkerBuilder, event::Event},
};
use miju_file_storage::{FileStorageError, JsonStorage};
use std::{panic::AssertUnwindSafe, pin::pin, time::Duration};

async fn completed_results(values: Vec<f64>) -> (JsonStorage<u32>, Vec<TaskId>) {
    let mut store = JsonStorage::new_temp().unwrap();
    let mut ids = Vec::new();
    for index in 0..values.len() {
        let id = TaskId::from(index as u64 + 1);
        store
            .push_task(TaskBuilder::new(index as u32).task_id(id.clone()).build())
            .await
            .unwrap();
        ids.push(id);
    }
    let worker = WorkerBuilder::new("completion-decode")
        .backend(store.clone())
        .build(move |index: u32| std::future::ready(values[index as usize]));
    let mut events = pin!(worker.stream());
    assert!(matches!(events.next().await, Some(Ok(Event::Start))));
    for _ in &ids {
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), events.next())
                .await
                .unwrap(),
            Some(Ok(Event::Success(_)))
        ));
    }
    (store, ids)
}

#[tokio::test]
async fn non_finite_worker_output_yields_json_error_once() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let (mut store, ids) = completed_results(vec![value]).await;
        let mut results =
            <JsonStorage<u32> as WaitForCompletion<f64>>::wait_for(&mut store, ids.clone());
        let observed = AssertUnwindSafe(results.next()).catch_unwind().await;
        assert!(
            matches!(observed, Ok(Some(Err(FileStorageError::Json(ref error)))) if error.is_data()),
            "a completed record must yield a JSON error without unwinding: {observed:?}"
        );
        assert!(results.next().await.is_none());
        // Consuming a query item must not rewrite the stored result.
        let mut again = <JsonStorage<u32> as WaitForCompletion<f64>>::wait_for(&mut store, ids);
        assert!(matches!(
            again.next().await,
            Some(Err(FileStorageError::Json(_)))
        ));
    }
}

#[tokio::test]
async fn decode_errors_preserve_other_requested_results() {
    let (mut store, ids) = completed_results(vec![f64::NAN, 1.5, f64::INFINITY, -2.5]).await;
    let requested = ids.iter().rev().cloned().chain(ids.iter().cloned());
    let mut results = <JsonStorage<u32> as WaitForCompletion<f64>>::wait_for(&mut store, requested);
    let mut errors = 0;
    let mut decoded = Vec::new();
    loop {
        let observed =
            AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(1), results.next()))
                .catch_unwind()
                .await;
        assert!(
            observed.is_ok(),
            "result polling must not unwind: {observed:?}"
        );
        let Some(item) = observed
            .unwrap()
            .expect("the next result must be available")
        else {
            break;
        };
        match item {
            Err(FileStorageError::Json(error)) => {
                assert!(error.is_data());
                errors += 1;
            }
            Ok(result) => {
                assert_eq!(result.status, Status::Done);
                decoded.push((result.task_id, result.result));
            }
            Err(error) => panic!("unexpected backend error: {error:?}"),
        }
    }
    assert_eq!(errors, 2);
    assert_eq!(
        decoded,
        vec![(ids[1].clone(), Ok(1.5)), (ids[3].clone(), Ok(-2.5))]
    );
}

#[tokio::test]
async fn finite_worker_output_and_empty_query_keep_existing_behavior() {
    let (mut store, ids) = completed_results(vec![1.5, -2.5]).await;
    let mut results =
        <JsonStorage<u32> as WaitForCompletion<f64>>::wait_for(&mut store, ids.clone());
    for (id, value) in ids.into_iter().zip([1.5, -2.5]) {
        let result = results.next().await.unwrap().unwrap();
        assert_eq!(result.task_id, id);
        assert_eq!(result.status, Status::Done);
        assert_eq!(result.result, Ok(value));
    }
    assert!(results.next().await.is_none());
    let mut empty = <JsonStorage<u32> as WaitForCompletion<f64>>::wait_for(&mut store, []);
    assert!(empty.next().await.is_none());
}
