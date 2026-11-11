//! Idempotency keys of failed sends are released; flushed keys stay recorded.

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use futures_util::{Sink, SinkExt, lock::Mutex as AsyncMutex};
use miju_core::{
    backend::memory::{MemorySink, MemoryStorageError},
    task::{Task, builder::TaskBuilder},
};

/// Buffers sends and writes them on flush, like a sink that defers storage.
struct Deferred {
    buffered: Vec<u32>,
    stored: Arc<Mutex<Vec<u32>>>,
    fail_flush: Arc<AtomicBool>,
}

impl Sink<Task<u32>> for Deferred {
    type Error = MemoryStorageError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, task: Task<u32>) -> Result<(), Self::Error> {
        self.get_mut().buffered.push(task.args);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.fail_flush.swap(false, Ordering::SeqCst) {
            this.buffered.clear();
            return Poll::Ready(Err(MemoryStorageError::Other("write failed".into())));
        }
        this.stored.lock().unwrap().append(&mut this.buffered);
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

fn setup() -> (MemorySink<u32>, Arc<Mutex<Vec<u32>>>, Arc<AtomicBool>) {
    let stored = Arc::new(Mutex::new(Vec::new()));
    let fail_flush = Arc::new(AtomicBool::new(false));
    let inner: Box<dyn Sink<Task<u32>, Error = MemoryStorageError> + Send + Sync + Unpin> =
        Box::new(Deferred {
            buffered: Vec::new(),
            stored: stored.clone(),
            fail_flush: fail_flush.clone(),
        });
    let sink = MemorySink::new(Arc::new(AsyncMutex::new(inner)));
    (sink, stored, fail_flush)
}

fn keyed(value: u32, key: &str) -> Task<u32> {
    TaskBuilder::new(value).idempotency_key(key).build()
}

#[tokio::test]
async fn keyed_retry_after_a_failed_deferred_write_is_stored() {
    let (mut sink, stored, fail_flush) = setup();
    fail_flush.store(true, Ordering::SeqCst);

    assert!(sink.send(keyed(1, "k")).await.is_err());
    // The failed write must not leave the key behind as a duplicate marker.
    sink.send(keyed(1, "k")).await.unwrap();
    sink.send(keyed(1, "k")).await.unwrap();

    assert_eq!(*stored.lock().unwrap(), vec![1]);
}

#[tokio::test]
async fn flushed_keys_stay_recorded_after_a_later_failure() {
    let (mut sink, stored, fail_flush) = setup();
    sink.send(keyed(1, "a")).await.unwrap();

    fail_flush.store(true, Ordering::SeqCst);
    assert!(sink.send(keyed(2, "b")).await.is_err());

    sink.send(keyed(3, "a")).await.unwrap();
    sink.send(keyed(2, "b")).await.unwrap();
    assert_eq!(*stored.lock().unwrap(), vec![1, 2]);
}
