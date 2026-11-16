//! Completion queries during clone writes: `check_status` reports contention,
//! `wait_for` waits through it, and a poisoned lock ends a `wait_for` query.

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
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, JoinHandle},
    time::Duration,
};

enum WriteAction {
    /// Pause inside the write while the storage guards are held.
    Pause(Sender<()>, Receiver<()>),
    /// Panic inside the write while the storage guards are held.
    Panic,
}

thread_local! {
    static WRITE_ACTION: RefCell<Option<WriteAction>> = const { RefCell::new(None) };
}

#[derive(Clone, Default)]
struct PausingAdapter;

impl Adapter for PausingAdapter {
    type Line = Value;
    type Error = serde_json::Error;

    fn serialize(&self, line: &Value) -> Result<Vec<u8>, Self::Error> {
        match WRITE_ACTION.with(|action| action.borrow_mut().take()) {
            Some(WriteAction::Pause(entered, release)) => {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            Some(WriteAction::Panic) => panic!("adapter panic while storage guards are held"),
            None => {}
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
        WRITE_ACTION
            .with(|action| *action.borrow_mut() = Some(WriteAction::Pause(entered_tx, release_rx)));
        writer.set_result(0, json!({"Ok": 14})).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    (release_tx, handle)
}

/// Poison the records' lock and wait until the panicking writer has unwound,
/// so no writer still holds the lock when the query runs.
fn poison_records(mut writer: Storage, line: usize) {
    let handle = thread::spawn(move || {
        WRITE_ACTION.with(|action| *action.borrow_mut() = Some(WriteAction::Panic));
        writer.set_result(line, json!({"Ok": 99})).unwrap();
    });
    assert!(handle.join().is_err(), "the poisoning writer must panic");
}

/// Counts wakes so a test can require the stream to wake itself.
#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn expect_wake(wakes: &WakeCount) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while wakes.0.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a waiting query must arrange its own wake");
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
async fn result_stream_waits_through_contention_without_losing_requested_task() {
    let (mut store, id) = pending_store().await;
    let mut results = <Storage as WaitForCompletion<u32>>::wait_for(&mut store, [id.clone()]);
    let (release, writer) = pause_result_write(store.clone());
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let first = results.poll_next_unpin(&mut Context::from_waker(&waker));
    assert!(
        matches!(first, Poll::Pending),
        "a concurrent result write must suspend the query, not yield an error: {first:?}"
    );
    release.send(()).unwrap();
    writer.join().unwrap();
    // No manual poll here: the stream has to wake the task itself.
    expect_wake(&wakes).await;
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
async fn collecting_results_during_contention_yields_only_results() {
    let (mut store, first_id) = pending_store().await;
    let second_id = TaskId::from(8_u64);
    store
        .push_task(TaskBuilder::new(8_u32).task_id(second_id.clone()).build())
        .await
        .unwrap();
    store.set_result(1, json!({"Ok": 28})).unwrap();
    let (release, writer) = pause_result_write(store.clone());
    let stream = <Storage as WaitForCompletion<u32>>::wait_for(
        &mut store,
        [first_id.clone(), second_id.clone()],
    );
    // Graph and filter collect every item before converting to a Result; the cap
    // only keeps the original implementation from looping forever in this test.
    let mut collect = Box::pin(stream.take(16).collect::<Vec<_>>());
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    assert!(
        collect
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending(),
        "the collector must wait for results instead of filling with lock errors"
    );
    release.send(()).unwrap();
    writer.join().unwrap();
    expect_wake(&wakes).await;
    let mut results = tokio::time::timeout(Duration::from_secs(2), collect)
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    results.sort_by_key(|result| result.result.clone().unwrap());
    assert_eq!(
        results
            .iter()
            .map(|result| (result.task_id.clone(), result.result.clone()))
            .collect::<Vec<_>>(),
        vec![(first_id, Ok(14)), (second_id, Ok(28))]
    );
}

#[tokio::test]
async fn poisoned_records_end_the_result_stream_after_one_error() {
    for after_partial_result in [false, true] {
        let (mut store, unfinished) = pending_store().await;
        let finished = TaskId::from(8_u64);
        store
            .push_task(TaskBuilder::new(8_u32).task_id(finished.clone()).build())
            .await
            .unwrap();
        store.set_result(1, json!({"Ok": 28})).unwrap();
        let ids = if after_partial_result {
            vec![unfinished, finished]
        } else {
            vec![unfinished]
        };
        let mut results = <Storage as WaitForCompletion<u32>>::wait_for(&mut store, ids);
        if after_partial_result {
            assert_eq!(results.next().await.unwrap().unwrap().result, Ok(28));
        }
        poison_records(store.clone(), 0);
        let first = results.next().now_or_never();
        assert!(
            matches!(first, Some(Some(Err(FileStorageError::PoisonedLock)))),
            "a poisoned lock must be reported at once (after_partial_result={after_partial_result}): {first:?}"
        );
        assert!(
            matches!(results.next().now_or_never(), Some(None)),
            "the query must end after reporting the poisoned lock (after_partial_result={after_partial_result})"
        );
    }
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
