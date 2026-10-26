//! Completion queries preserve their error and retry behavior during clone writes.

use futures_util::{FutureExt, StreamExt};
use miju_core::{
    backend::{TaskSink, WaitForCompletion},
    task::{builder::TaskBuilder, task_id::TaskId},
};
use miju_file_storage::{Adapter, FileStorage, FileStorageError, JsonAdapter, RawTask};
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    panic::AssertUnwindSafe,
    sync::mpsc::{self, Receiver, Sender},
    thread::{self, JoinHandle},
    time::Duration,
};

thread_local! {
    static WRITE_GATE: RefCell<Option<(Sender<()>, Receiver<()>)>> = const { RefCell::new(None) };
}

#[derive(Clone, Default)]
struct PausingAdapter;

impl Adapter for PausingAdapter {
    type Line = Value;
    type Error = serde_json::Error;

    fn serialize(&self, line: &Value) -> Result<Vec<u8>, Self::Error> {
        if let Some((entered, release)) = WRITE_GATE.with(|gate| gate.borrow_mut().take()) {
            entered.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        JsonAdapter.serialize(line)
    }

    fn deserialize(&self, raw: &[u8]) -> Result<Value, Self::Error> {
        JsonAdapter.deserialize(raw)
    }

    fn to_entry(&self, line: Value) -> Result<RawTask, Self::Error> {
        JsonAdapter.to_entry(line)
    }

    fn from_entry(entry: &RawTask) -> Result<Value, Self::Error> {
        JsonAdapter::from_entry(entry)
    }
}

type Storage = FileStorage<u32, PausingAdapter>;

async fn pending_store() -> (Storage, TaskId) {
    let mut store = Storage::new_temp().unwrap();
    let id = TaskId::from(7_u64);
    store
        .push_task(TaskBuilder::new(7_u32).task_id(id.clone()).build())
        .await
        .unwrap();
    (store, id)
}

fn pause_result_write(mut writer: Storage) -> (Sender<()>, JoinHandle<()>) {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        WRITE_GATE.with(|gate| *gate.borrow_mut() = Some((entered_tx, release_rx)));
        writer.set_result(0, json!({"Ok": 14})).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    (release_tx, handle)
}

#[tokio::test]
async fn check_status_reports_contention_and_can_be_retried() {
    let (mut store, id) = pending_store().await;
    let (release, writer) = pause_result_write(store.clone());
    let result = AssertUnwindSafe(<Storage as WaitForCompletion<u32>>::check_status(
        &mut store,
        [id.clone()],
    ))
    .catch_unwind()
    .await;
    release.send(()).unwrap();
    writer.join().unwrap();
    assert!(
        matches!(result, Ok(Err(FileStorageError::WouldBlockLock))),
        "a concurrent result write must return WouldBlockLock rather than panic: {result:?}"
    );
    let results = <Storage as WaitForCompletion<u32>>::check_status(&mut store, [id.clone()])
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].task_id, id);
    assert_eq!(results[0].result, Ok(14));
}

#[tokio::test]
async fn result_stream_reports_contention_without_losing_requested_task() {
    let (mut store, id) = pending_store().await;
    let mut results = <Storage as WaitForCompletion<u32>>::wait_for(&mut store, [id.clone()]);
    let (release, writer) = pause_result_write(store.clone());
    let first = results.next().now_or_never();
    release.send(()).unwrap();
    writer.join().unwrap();
    assert!(
        matches!(first, Some(Some(Err(FileStorageError::WouldBlockLock)))),
        "a concurrent result write must yield WouldBlockLock rather than end the stream: {first:?}"
    );
    let result = tokio::time::timeout(Duration::from_secs(1), results.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.task_id, id);
    assert_eq!(result.result, Ok(14));
    assert!(results.next().await.is_none());
}

#[tokio::test]
async fn uncontended_queries_keep_pending_completed_and_empty_behavior() {
    let (mut store, id) = pending_store().await;
    let pending = <Storage as WaitForCompletion<u32>>::check_status(&mut store, [id.clone()])
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, miju_core::task::status::Status::Pending);
    assert!(pending[0].result.is_err());
    let missing =
        <Storage as WaitForCompletion<u32>>::check_status(&mut store, [TaskId::from(8_u64)])
            .await
            .unwrap();
    assert!(missing.is_empty());
    let mut empty = <Storage as WaitForCompletion<u32>>::wait_for(&mut store, []);
    assert!(empty.next().await.is_none());
    store.set_result(0, json!({"Ok":14})).unwrap();
    let mut results =
        <Storage as WaitForCompletion<u32>>::wait_for(&mut store, [id.clone(), id.clone()]);
    let first = results.next().await.unwrap().unwrap();
    assert_eq!(first.task_id, id);
    assert_eq!(first.result, Ok(14));
    assert!(results.next().await.is_none());
}
