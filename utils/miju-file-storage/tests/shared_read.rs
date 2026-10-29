#![allow(missing_docs)]

use futures_util::{FutureExt, Stream, StreamExt, future::poll_fn, task::noop_waker};
use miju_core::{
    backend::{TaskSink, factory::BackendFactory, memory::MemoryStorage},
    task::Task,
};
use miju_file_storage::SharedJsonStore;
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    cell::RefCell,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::time::timeout;

async fn next_after_pending(
    mut receiver: MemoryStorage<String>,
    pending: Arc<AtomicBool>,
) -> Task<String> {
    poll_fn(move |cx| {
        let result = Pin::new(&mut receiver).poll_next(cx);
        if result.is_pending() {
            pending.store(true, Ordering::SeqCst);
        }
        result
    })
    .await
    .expect("shared store receiver must remain open")
}

#[test]
fn concurrent_reader_receives_successful_large_write_without_panicking() {
    const ATTEMPTS: usize = 8;
    const READERS: usize = 4;
    const PAYLOAD_LEN: usize = 8 * 1024 * 1024;
    let mut successful_writes = 0;

    for trial in 0..ATTEMPTS {
        let mut factory = SharedJsonStore::new();
        let mut producer: MemoryStorage<String> = factory.create().unwrap();
        let expected = "\\".repeat(PAYLOAD_LEN);
        let payload = expected.clone();
        let barrier = Arc::new(Barrier::new(READERS + 2));
        let writer_done = Arc::new(AtomicBool::new(false));

        let writer_barrier = Arc::clone(&barrier);
        let writer_done_for_thread = Arc::clone(&writer_done);
        let writer = std::thread::spawn(move || {
            writer_barrier.wait();
            let result = producer.push(payload).now_or_never();
            writer_done_for_thread.store(true, Ordering::SeqCst);
            result
        });

        let mut readers = Vec::with_capacity(READERS);
        for index in 0..READERS {
            let consumer: MemoryStorage<String> = factory.create().unwrap();
            let reader_barrier = Arc::clone(&barrier);
            let writer_done = Arc::clone(&writer_done);
            readers.push(std::thread::spawn(move || {
                let mut consumer = consumer;
                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                reader_barrier.wait();
                // Give the writer time to acquire the shared entry lock before
                // repeated polls observe the public contention path.
                std::thread::sleep(Duration::from_millis(5));
                let started = Instant::now();
                let mut polls = 0;
                let result = loop {
                    polls += 1;
                    let polled =
                        catch_unwind(AssertUnwindSafe(|| consumer.poll_next_unpin(&mut cx)));
                    match polled {
                        Ok(Poll::Ready(Some(task))) => break Ok(task.args),
                        Ok(Poll::Ready(None)) => break Err("store ended".to_owned()),
                        Ok(Poll::Pending) => {
                            if writer_done.load(Ordering::SeqCst) && polls > 100 {
                                break Err(
                                    "reader stayed pending after writer completion".to_owned()
                                );
                            }
                            if started.elapsed() > Duration::from_secs(5) {
                                break Err("reader poll deadline exceeded".to_owned());
                            }
                            std::thread::sleep(Duration::from_micros(100));
                        }
                        Err(error) => {
                            let panic = error
                                .downcast_ref::<String>()
                                .cloned()
                                .or_else(|| error.downcast_ref::<&str>().map(|s| s.to_string()))
                                .unwrap_or_else(|| "non-string panic".to_owned());
                            break Err(format!("panic: {panic}"));
                        }
                    }
                };
                (index, polls, result)
            }));
        }

        barrier.wait();
        let send_result = writer.join().expect("producer thread must not panic");
        let Some(send_result) = send_result else {
            panic!("public memory push unexpectedly returned Pending");
        };
        let reader_results: Vec<_> = readers
            .into_iter()
            .map(|reader| {
                reader
                    .join()
                    .expect("reader thread must not panic outside poll")
            })
            .collect();
        let reader_summary: Vec<_> = reader_results
            .iter()
            .map(|(index, polls, result)| {
                (
                    *index,
                    *polls,
                    result
                        .as_ref()
                        .map(|value| value.len())
                        .map_err(String::as_str),
                )
            })
            .collect();
        println!("trial={trial} send={send_result:?} readers={reader_summary:?}");

        if let Err(error) = send_result {
            assert!(
                reader_results.iter().all(|(_, _, result)| {
                    !result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.starts_with("panic:"))
                }),
                "reader panicked during a rejected write: {reader_results:?}"
            );
            eprintln!("skipping attempt after public send error: {error}");
            continue;
        }

        successful_writes += 1;
        for (_, _, result) in reader_results {
            let received = result.unwrap_or_else(|error| panic!("reader failed: {error}"));
            assert_eq!(received, expected);
        }
    }

    assert!(
        successful_writes > 0,
        "probe must include a successful write"
    );
}

#[tokio::test]
async fn cloned_factory_readers_each_wake_and_keep_an_independent_cursor() {
    let mut factory = SharedJsonStore::new();
    let mut cloned_factory = factory.clone();
    let mut producer: MemoryStorage<String> = factory.create().unwrap();
    let reader_one: MemoryStorage<String> = factory.create().unwrap();
    let reader_two: MemoryStorage<String> = cloned_factory.create().unwrap();
    let pending_one = Arc::new(AtomicBool::new(false));
    let pending_two = Arc::new(AtomicBool::new(false));
    let reader_one = tokio::spawn(next_after_pending(reader_one, Arc::clone(&pending_one)));
    let reader_two = tokio::spawn(next_after_pending(reader_two, Arc::clone(&pending_two)));
    for _ in 0..64 {
        if pending_one.load(Ordering::SeqCst) && pending_two.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(pending_one.load(Ordering::SeqCst));
    assert!(pending_two.load(Ordering::SeqCst));

    producer.push("shared entry".to_owned()).await.unwrap();
    let one = timeout(Duration::from_secs(2), reader_one)
        .await
        .expect("first registered reader must be woken")
        .unwrap();
    let two = timeout(Duration::from_secs(2), reader_two)
        .await
        .expect("second registered reader must be woken")
        .unwrap();
    assert_eq!(one.args, "shared entry");
    assert_eq!(two.args, "shared entry");
}

#[tokio::test]
async fn cancelling_an_empty_read_does_not_prevent_a_later_read() {
    let mut factory = SharedJsonStore::new();
    let mut producer: MemoryStorage<String> = factory.create().unwrap();
    let mut consumer: MemoryStorage<String> = factory.create().unwrap();
    {
        let mut next = Box::pin(consumer.next());
        let pending_observed = Arc::new(AtomicBool::new(false));
        let pending_observed_in_future = Arc::clone(&pending_observed);
        let pending = poll_fn(|cx| {
            let result = next.as_mut().poll(cx);
            if result.is_pending() {
                pending_observed_in_future.store(true, Ordering::SeqCst);
            }
            result
        });
        assert!(timeout(Duration::from_millis(10), pending).await.is_err());
        assert!(pending_observed.load(Ordering::SeqCst));
    }

    producer
        .push("after cancellation".to_owned())
        .await
        .unwrap();
    let task = timeout(Duration::from_secs(2), consumer.next())
        .await
        .expect("later read must complete")
        .expect("successful write must be delivered");
    assert_eq!(task.args, "after cancellation");
}

thread_local! {
    static ON_REENTRANT_DESERIALIZE: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None);
}

#[derive(Clone, Serialize)]
#[serde(transparent)]
struct ReentrantArgs(u32);

impl<'de> Deserialize<'de> for ReentrantArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        ON_REENTRANT_DESERIALIZE.with(|callback| {
            if let Some(callback) = callback.borrow_mut().as_mut() {
                callback();
            }
        });
        Ok(Self(value))
    }
}

#[test]
fn typed_deserialization_can_send_to_an_independent_shared_handle() {
    let mut factory = SharedJsonStore::new();
    let mut producer: MemoryStorage<ReentrantArgs> = factory.create().unwrap();
    let mut consumer: MemoryStorage<ReentrantArgs> = factory.create().unwrap();
    let nested_sender: MemoryStorage<String> = factory.create().unwrap();
    producer
        .push(ReentrantArgs(42))
        .now_or_never()
        .expect("shared push must complete without executor suspension")
        .unwrap();

    let nested_sender = Arc::new(Mutex::new(Some(nested_sender)));
    let nested_sender_for_callback = Arc::clone(&nested_sender);
    let nested_send_succeeded = Arc::new(AtomicBool::new(false));
    let nested_send_succeeded_for_callback = Arc::clone(&nested_send_succeeded);
    ON_REENTRANT_DESERIALIZE.with(|callback| {
        *callback.borrow_mut() = Some(Box::new(move || {
            let mut nested_sender = nested_sender_for_callback.lock().unwrap();
            let nested_sender = nested_sender.as_mut().unwrap();
            let result = nested_sender
                .push("from deserializer".to_owned())
                .now_or_never()
                .expect("nested push must not suspend");
            nested_send_succeeded_for_callback.store(result.is_ok(), Ordering::SeqCst);
        }));
    });

    let decoded = catch_unwind(AssertUnwindSafe(|| consumer.next().now_or_never()));
    ON_REENTRANT_DESERIALIZE.with(|callback| {
        callback.borrow_mut().take();
    });
    let task = decoded
        .expect("typed reader must not panic")
        .expect("queued task must be immediately available")
        .expect("successful write must be delivered");
    assert_eq!(task.args.0, 42);
    assert!(nested_send_succeeded.load(Ordering::SeqCst));

    let mut nested_sender = nested_sender.lock().unwrap().take().unwrap();
    let nested_task = nested_sender
        .next()
        .now_or_never()
        .expect("nested write must be immediately readable")
        .expect("nested write must be delivered");
    assert_eq!(nested_task.args, "from deserializer");
}

#[test]
fn shared_store_and_memory_backend_keep_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<SharedJsonStore>();
    assert_send_sync::<MemoryStorage<String>>();
}
