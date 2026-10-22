//! Verify optional execution controls through real worker pipelines.

#![cfg(any(feature = "limit", feature = "timeout", feature = "filter"))]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use miju::prelude::*;

#[cfg(feature = "limit")]
#[tokio::test(start_paused = true)]
async fn concurrency_bounds_in_flight_tasks() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let mut storage = MemoryStorage::new();
    for value in 0..4usize {
        storage.push(value).await.unwrap();
    }
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(Mutex::new(0));
    let running = active.clone();
    let maximum = peak.clone();
    let done = completed.clone();

    let worker = WorkerBuilder::new("bounded")
        .backend(storage)
        .concurrency(2)
        .on_event(move |worker, event| match event {
            Event::Success(_) => {
                let mut count = done.lock().unwrap();
                *count += 1;
                if *count == 4 {
                    worker.stop().unwrap();
                }
            }
            Event::Error(error) => panic!("{error}"),
            _ => {}
        })
        .build(move |_: usize| {
            let active = running.clone();
            let peak = maximum.clone();
            async move {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                active.fetch_sub(1, Ordering::SeqCst);
            }
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(*completed.lock().unwrap(), 4);
}

#[cfg(feature = "limit")]
#[tokio::test(start_paused = true)]
async fn rate_limit_delays_calls_until_the_next_period() {
    let mut storage = MemoryStorage::new();
    for value in 0..3usize {
        storage.push(value).await.unwrap();
    }
    let observed = Arc::new(Mutex::new(Vec::new()));
    let starts = observed.clone();
    let period = Duration::from_secs(2);
    let worker = WorkerBuilder::new("paced")
        .backend(storage)
        .rate_limit(1, period)
        .on_event(move |worker, event| match event {
            Event::Success(value) => {
                let mut starts = starts.lock().unwrap();
                starts.push(*value.downcast_ref::<tokio::time::Instant>().unwrap());
                if starts.len() == 3 {
                    worker.stop().unwrap();
                }
            }
            Event::Error(error) => panic!("{error}"),
            _ => {}
        })
        .build(|_: usize| async { Ok::<_, BoxDynError>(tokio::time::Instant::now()) });

    tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap()
        .unwrap();
    let starts = observed.lock().unwrap();
    assert_eq!(starts.len(), 3);
    assert!(starts.windows(2).all(|pair| pair[1] - pair[0] >= period));
}

#[cfg(feature = "timeout")]
#[tokio::test(start_paused = true)]
async fn timeout_reports_elapsed_and_drops_pending_execution() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let mut storage = MemoryStorage::new();
    storage.push(false).await.unwrap();
    storage.push(true).await.unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = dropped.clone();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let results = observed.clone();
    let worker = WorkerBuilder::new("deadline")
        .backend(storage)
        .timeout(Duration::from_secs(1))
        .on_event(move |worker, event| {
            let outcome = match event {
                Event::Success(_) => "completed",
                Event::Error(error) => {
                    assert!(
                        error
                            .downcast_ref::<tower::timeout::error::Elapsed>()
                            .is_some()
                    );
                    "elapsed"
                }
                _ => return,
            };
            let mut results = results.lock().unwrap();
            results.push(outcome);
            if results.len() == 2 {
                worker.stop().unwrap();
            }
        })
        .build(move |slow: bool| {
            let flag = flag.clone();
            async move {
                if slow {
                    let _guard = DropFlag(flag);
                    std::future::pending::<()>().await;
                }
            }
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*observed.lock().unwrap(), ["completed", "elapsed"]);
    assert!(dropped.load(Ordering::SeqCst));
}

#[cfg(feature = "filter")]
#[tokio::test]
async fn filter_rejects_or_transforms_before_calling_the_handler() {
    let mut storage = MemoryStorage::new();
    storage.push(0usize).await.unwrap();
    storage.push(1usize).await.unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let results = observed.clone();
    let worker = WorkerBuilder::new("filtered")
        .backend(storage)
        .filter(|mut task: Task<usize>| {
            if task.args == 0 {
                Err::<_, BoxDynError>("rejected".into())
            } else {
                task.args += 1;
                Ok(task)
            }
        })
        .on_event(move |worker, event| {
            let outcome = match event {
                Event::Success(value) => Ok(*value.downcast_ref::<usize>().unwrap()),
                Event::Error(error) => Err(error.to_string()),
                _ => return,
            };
            let mut results = results.lock().unwrap();
            results.push(outcome);
            if results.len() == 2 {
                worker.stop().unwrap();
            }
        })
        .build(|value: usize| async move {
            assert_eq!(value, 2);
            value
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        *observed.lock().unwrap(),
        [Err("rejected".to_owned()), Ok(2)]
    );
}

#[cfg(feature = "filter")]
#[tokio::test]
async fn async_filter_waits_for_the_predicate_before_calling_the_handler() {
    let mut storage = MemoryStorage::new();
    storage.push(0usize).await.unwrap();
    storage.push(1usize).await.unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let results = observed.clone();
    let worker = WorkerBuilder::new("async-filtered")
        .backend(storage)
        .filter_async(|mut task: Task<usize>| async move {
            tokio::task::yield_now().await;
            if task.args == 0 {
                Err::<_, BoxDynError>("rejected".into())
            } else {
                task.args += 1;
                Ok(task)
            }
        })
        .on_event(move |worker, event| {
            let outcome = match event {
                Event::Success(value) => Ok(*value.downcast_ref::<usize>().unwrap()),
                Event::Error(error) => Err(error.to_string()),
                _ => return,
            };
            let mut results = results.lock().unwrap();
            results.push(outcome);
            if results.len() == 2 {
                worker.stop().unwrap();
            }
        })
        .build(|value: usize| async move {
            assert_eq!(value, 2);
            value
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let results = observed.lock().unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.contains(&Err("rejected".to_owned())));
    assert!(results.contains(&Ok(2)));
}
