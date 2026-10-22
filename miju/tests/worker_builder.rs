//! Exercise middleware composition through the application prelude.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use miju::prelude::*;

#[tokio::test]
async fn optional_layers_preserve_request_and_response_order() {
    for enabled in [false, true] {
        let mut storage = MemoryStorage::new();
        storage.push(3usize).await.unwrap();
        let observed = Arc::new(Mutex::new(None));
        let output = observed.clone();
        let optional = enabled.then(|| {
            tower::util::MapRequestLayer::new(|mut task: Task<usize>| {
                task.args *= 2;
                task
            })
        });

        let worker = WorkerBuilder::new("layer-order")
            .backend(storage)
            .option_layer(optional)
            .map_request(|mut task: Task<usize>| {
                task.args += 1;
                task
            })
            .data(4usize)
            .layer_fn(|service| tower::util::MapResponse::new(service, |value: usize| value + 1))
            .map_response(|value: usize| value * 2)
            .on_event(move |_, event| match event {
                Event::Success(value) => {
                    *output.lock().unwrap() = Some(*value.downcast_ref::<usize>().unwrap());
                }
                Event::Error(error) => panic!("{error}"),
                _ => {}
            })
            .build(
                |value: usize, Data(bonus): Data<usize>, worker: WorkerContext| async move {
                    worker.stop().unwrap();
                    value + bonus
                },
            );

        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            Some(if enabled { 23 } else { 17 })
        );
    }
}

#[tokio::test]
async fn result_layers_handle_success_and_failure() {
    for fail in [false, true] {
        let mut storage = MemoryStorage::new();
        storage.push(fail).await.unwrap();
        let observed = Arc::new(Mutex::new(None));
        let output = observed.clone();

        let worker = WorkerBuilder::new("result-mapping")
            .backend(storage)
            .map_result(|result: Result<String, BoxDynError>| {
                result.map(|value| format!("done:{value}"))
            })
            .then(|result: Result<usize, std::io::Error>| async move {
                Ok::<_, BoxDynError>(match result {
                    Ok(value) => format!("ok:{value}"),
                    Err(error) => error.to_string(),
                })
            })
            .map_err(|error: BoxDynError| std::io::Error::other(format!("converted:{error}")))
            .and_then(|value: usize| async move { Ok::<_, BoxDynError>(value * 2) })
            .map_future(add_one)
            .on_event(move |_, event| match event {
                Event::Success(value) => {
                    *output.lock().unwrap() = Some(value.downcast_ref::<String>().unwrap().clone());
                }
                Event::Error(error) => panic!("{error}"),
                _ => {}
            })
            .build(|fail: bool, worker: WorkerContext| async move {
                worker.stop().unwrap();
                if fail {
                    Err::<usize, BoxDynError>("failed".into())
                } else {
                    Ok(3)
                }
            });

        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(if fail {
                "done:converted:failed"
            } else {
                "done:ok:8"
            })
        );
    }
}

#[cfg(all(
    feature = "limit",
    feature = "tracing",
    feature = "retry",
    feature = "timeout",
    feature = "catch-panic"
))]
#[tokio::test(start_paused = true)]
async fn composed_middleware_retries_a_timed_out_attempt_and_completes_the_task() {
    let mut storage = MemoryStorage::new();
    let task = TaskBuilder::new(7usize).task_id(1u64.into()).build();
    storage.push_task(task.clone()).await.unwrap();
    let observed = Arc::new(Mutex::new(None));
    let output = observed.clone();

    let worker = WorkerBuilder::new("composed-retry")
        .backend(storage)
        .concurrency(1)
        .enable_tracing()
        .retry(RetryPolicy::retries(1))
        .timeout(Duration::from_secs(1))
        .catch_panic()
        .on_event(move |worker, event| match event {
            Event::Success(value) => {
                *output.lock().unwrap() = Some(*value.downcast_ref::<usize>().unwrap());
                worker.stop().unwrap();
            }
            Event::Error(error) => panic!("{error}"),
            _ => {}
        })
        .build(|value: usize, attempt: Attempt| async move {
            if attempt.current() == 1 {
                std::future::pending::<()>().await;
            }
            value * 2
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*observed.lock().unwrap(), Some(14));
    assert_eq!(task.attempt(), 2);
    assert_eq!(task.status(), Status::Done);
}

async fn add_one<F>(future: F) -> Result<usize, BoxDynError>
where
    F: Future<Output = Result<usize, BoxDynError>>,
{
    future.await.map(|value| value + 1)
}
