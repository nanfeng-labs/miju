//! Exercise retry policies through worker execution and task metadata.

#![cfg(feature = "retry")]

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use miju::{
    layers::retry::{
        HasherRng, RetryConfig, RetryConfigError, RetryMetadataExt,
        backoff::{ExponentialBackoffMaker, MakeBackoff},
    },
    prelude::*,
};

#[tokio::test]
async fn fixed_retries_share_attempts_and_stop_on_success_or_exhaustion() {
    for (policy, succeeds_at, expected) in [
        (RetryPolicy::retries(0), usize::MAX, 1),
        (RetryPolicy::retries(2), usize::MAX, 3),
        (RetryPolicy::retries(3), 2, 2),
        (RetryPolicy::default(), usize::MAX, 6),
    ] {
        let mut storage = MemoryStorage::new();
        let task = TaskBuilder::new(succeeds_at)
            .task_id("fixed".to_owned().into())
            .build();
        let observed = task.clone();
        storage.push_task(task).await.unwrap();
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let calls = attempts.clone();
        let worker = WorkerBuilder::new("fixed-retries")
            .backend(storage)
            .retry(policy)
            .on_event(move |worker, event| match event {
                Event::Success(value) => {
                    assert_eq!(value.downcast_ref::<usize>(), Some(&succeeds_at));
                    worker.stop().unwrap();
                }
                Event::Error(error) => {
                    assert_eq!(succeeds_at, usize::MAX);
                    assert!(error.downcast_ref::<io::Error>().is_some());
                    worker.stop().unwrap();
                }
                _ => {}
            })
            .build(move |succeeds_at: usize, attempt: Attempt| {
                calls.lock().unwrap().push(attempt.current());
                async move {
                    if attempt.current() == succeeds_at {
                        Ok(attempt.current())
                    } else {
                        Err(io::Error::other("retry me"))
                    }
                }
            });

        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *attempts.lock().unwrap(),
            (1..=expected).collect::<Vec<_>>()
        );
        assert_eq!(observed.attempt(), expected);
        assert_eq!(
            observed.status(),
            if succeeds_at == usize::MAX {
                Status::Failed
            } else {
                Status::Done
            }
        );
    }
}

#[tokio::test]
async fn predicate_retries_transient_errors_and_preserves_the_final_error() {
    let mut storage = MemoryStorage::new();
    storage.push(()).await.unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let calls = attempts.clone();
    let worker = WorkerBuilder::new("conditional-retries")
        .backend(storage)
        .retry(
            RetryPolicy::retries(5)
                .retry_if(|error: &BoxDynError| {
                    error
                        .downcast_ref::<io::Error>()
                        .is_some_and(|error| error.kind() == io::ErrorKind::ConnectionReset)
                })
                .from_task_config(),
        )
        .on_event(|worker, event| {
            if let Event::Error(error) = event {
                assert_eq!(
                    error.downcast_ref::<io::Error>().unwrap().kind(),
                    io::ErrorKind::PermissionDenied
                );
                worker.stop().unwrap();
            }
        })
        .build(move |(): (), attempt: Attempt| {
            calls.lock().unwrap().push(attempt.current());
            async move {
                let kind = if attempt.current() == 1 {
                    io::ErrorKind::ConnectionReset
                } else {
                    io::ErrorKind::PermissionDenied
                };
                Err::<(), _>(io::Error::new(kind, "failed"))
            }
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*attempts.lock().unwrap(), [1, 2]);
}

#[tokio::test]
async fn abort_and_worker_shutdown_stop_retries() {
    for abort in [false, true] {
        let mut storage = MemoryStorage::new();
        let task = TaskBuilder::new(abort)
            .task_id("stop".to_owned().into())
            .build();
        let observed = task.clone();
        storage.push_task(task).await.unwrap();
        let worker = WorkerBuilder::new("stop-retries")
            .backend(storage)
            .retry(RetryPolicy::retries(5))
            .on_event(move |worker, event| {
                if let Event::Error(error) = event {
                    assert_eq!(error.downcast_ref::<AbortError>().is_some(), abort);
                    if abort {
                        worker.stop().unwrap();
                    }
                }
            })
            .build(|abort: bool, worker: WorkerContext| async move {
                if abort {
                    Err::<(), BoxDynError>(AbortError::new("stop retrying").into())
                } else {
                    worker.stop().unwrap();
                    Err("shutting down".into())
                }
            });

        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.attempt(), 1);
        assert_eq!(
            observed.status(),
            if abort {
                Status::Killed
            } else {
                Status::Failed
            }
        );
    }
}

#[tokio::test(start_paused = true)]
async fn backoff_waits_between_attempts() {
    let mut storage = MemoryStorage::new();
    storage.push(()).await.unwrap();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let calls = starts.clone();
    let backoff = ExponentialBackoffMaker::new(
        Duration::from_millis(10),
        Duration::from_millis(100),
        0.0,
        HasherRng::default(),
    )
    .unwrap()
    .make_backoff();
    let worker = WorkerBuilder::new("backoff-retries")
        .backend(storage)
        .retry(
            BackoffRetryPolicy::new(3, backoff)
                .from_task_config()
                .retry_if(|_: &BoxDynError| true),
        )
        .on_event(|worker, event| match event {
            Event::Success(_) => worker.stop().unwrap(),
            Event::Error(error) => panic!("{error}"),
            _ => {}
        })
        .build(move |(): (), attempt: Attempt| {
            calls.lock().unwrap().push(tokio::time::Instant::now());
            async move {
                if attempt.current() == 4 {
                    Ok(())
                } else {
                    Err(io::Error::other("retry me"))
                }
            }
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let starts = starts.lock().unwrap();
    assert_eq!(starts.len(), 4);
    let waits: Vec<_> = starts.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert_eq!(waits, [10, 20, 40].map(Duration::from_millis));
}

#[test]
fn retry_metadata_round_trips_and_reports_invalid_values() {
    let task = TaskBuilder::new(()).retries(3).build();
    assert_eq!(RetryConfig::extract(task.metadata()).unwrap().retries, 3);
    assert_eq!(task.metadata().get("miju.retries.config"), Some("3"));
    let mut metadata = MetadataStore::new();
    assert!(matches!(
        RetryConfig::extract(&metadata),
        Err(RetryConfigError::MissingKey)
    ));
    RetryConfig { retries: 2 }.inject(&mut metadata).unwrap();
    assert!(matches!(
        RetryConfig { retries: 4 }.inject(&mut metadata),
        Err(RetryConfigError::DuplicateEntry(_))
    ));
    assert_eq!(RetryConfig::extract(&metadata).unwrap().retries, 2);
    let mut invalid = MetadataStore::new();
    invalid
        .insert("miju.retries.config", "not a number")
        .unwrap();
    assert!(matches!(
        RetryConfig::extract(&invalid),
        Err(RetryConfigError::Parse(_))
    ));
}

#[tokio::test]
async fn task_metadata_wrapper_preserves_the_inner_retry_limit() {
    for configured in [false, true] {
        let mut storage = MemoryStorage::new();
        let builder = TaskBuilder::new(()).task_id("metadata".to_owned().into());
        let task = if configured {
            builder.retries(10)
        } else {
            builder
        }
        .build();
        let observed = task.clone();
        storage.push_task(task).await.unwrap();
        let worker = WorkerBuilder::new("metadata-retries")
            .backend(storage)
            .retry(RetryPolicy::retries(2).from_task_config())
            .on_event(|worker, event| {
                if matches!(event, Event::Error(_)) {
                    worker.stop().unwrap();
                }
            })
            .build(|(): ()| async { Err::<(), _>(io::Error::other("failed")) });
        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.attempt(), 3);
        assert_eq!(observed.status(), Status::Failed);
    }
}

#[cfg(feature = "catch-panic")]
#[tokio::test]
async fn custom_panic_predicate_prevents_reexecution() {
    use miju::layers::catch_panic::{CatchPanicLayer, PanicError};

    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(true)
        .task_id("panic".to_owned().into())
        .build();
    let observed = task.clone();
    storage.push_task(task).await.unwrap();
    let worker = WorkerBuilder::new("panic-retries")
        .backend(storage)
        .retry(
            RetryPolicy::retries(3)
                .retry_if(|error: &BoxDynError| error.downcast_ref::<PanicError>().is_none()),
        )
        .layer(CatchPanicLayer::with_panic_handler(|_| {
            PanicError("caught".to_owned())
        }))
        .on_event(|worker, event| {
            if let Event::Error(error) = event {
                assert_eq!(error.downcast_ref::<PanicError>().unwrap().0, "caught");
                worker.stop().unwrap();
            }
        })
        .build(|fail: bool| async move {
            if fail {
                panic!("handler panic");
            }
        });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.attempt(), 1);
}
