//! Verify panic conversion and worker recovery through the public layer API.

#![cfg(feature = "catch-panic")]

use std::{
    error::Error,
    future::Ready,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use miju::{
    layers::catch_panic::{CatchPanicLayer, PanicError},
    prelude::*,
};
use tower::{Layer, Service, ServiceExt, service_fn};

fn assert_aborted_panic(error: &BoxDynError, message: &str) {
    let abort = error.downcast_ref::<AbortError>().unwrap();
    let panic = abort
        .source()
        .unwrap()
        .downcast_ref::<PanicError>()
        .unwrap();
    assert_eq!(panic.0, message);
    assert_eq!(panic.to_string(), format!("PanicError: {message}"));
}

#[tokio::test]
async fn success_and_service_errors_pass_through() {
    let service = service_fn(|task: Task<bool>| async move {
        if task.args {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"))
        } else {
            Ok(42)
        }
    });
    let mut service = CatchPanicLayer::default().layer(service);

    assert_eq!(
        service
            .ready()
            .await
            .unwrap()
            .call(Task::new(false))
            .await
            .unwrap(),
        42
    );
    let error = service
        .ready()
        .await
        .unwrap()
        .call(Task::new(true))
        .await
        .unwrap_err();
    let original = error.downcast_ref::<io::Error>().unwrap();
    assert_eq!(original.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(original.to_string(), "denied");
}

#[tokio::test]
async fn synchronous_call_panic_becomes_an_abort_error() {
    let service = service_fn(|_: Task<()>| -> Ready<Result<(), io::Error>> {
        panic!("call failed");
    });
    let mut service = CatchPanicLayer::new().layer(service);

    let future = service.call(Task::new(()));
    assert_aborted_panic(&future.await.unwrap_err(), "call failed");
}

#[tokio::test]
async fn polling_panics_preserve_strings_and_handle_other_payloads() {
    for (payload, message) in [(0, "borrowed"), (1, "owned"), (2, "Unknown panic")] {
        let service = service_fn(|task: Task<usize>| async move {
            tokio::task::yield_now().await;
            match task.args {
                0 => panic!("borrowed"),
                1 => std::panic::panic_any(String::from("owned")),
                2 => std::panic::panic_any(123usize),
                _ => Ok::<(), io::Error>(()),
            }
        });
        let error = CatchPanicLayer::new()
            .layer(service)
            .oneshot(Task::new(payload))
            .await
            .unwrap_err();
        assert_aborted_panic(&error, message);
    }
}

#[tokio::test]
async fn custom_handler_receives_original_payload_without_an_abort_wrapper() {
    for synchronous in [false, true] {
        let service = service_fn(move |_: Task<()>| {
            if synchronous {
                std::panic::panic_any(123usize);
            }
            async move {
                tokio::task::yield_now().await;
                if !synchronous {
                    std::panic::panic_any(123usize);
                }
                Ok::<(), io::Error>(())
            }
        });
        let layer = CatchPanicLayer::with_panic_handler(|payload| {
            assert_eq!(payload.downcast_ref::<usize>(), Some(&123));
            io::Error::other("custom panic error")
        });
        let error = layer
            .layer(service)
            .oneshot(Task::new(()))
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().to_string(),
            "custom panic error"
        );
        assert!(error.downcast_ref::<AbortError>().is_none());
    }
}

#[tokio::test]
async fn worker_reports_panic_and_processes_another_task() {
    let mut storage = MemoryStorage::new();
    storage.push(true).await.unwrap();
    storage.push(false).await.unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let results = observed.clone();
    let worker = WorkerBuilder::new("panic-recovery")
        .backend(storage)
        .catch_panic()
        .on_event(move |worker, event| {
            let outcome = match event {
                Event::Error(error) => {
                    assert_aborted_panic(error, "task failed");
                    "panicked"
                }
                Event::Success(value) => {
                    assert_eq!(value.downcast_ref::<usize>(), Some(&42));
                    "completed"
                }
                _ => return,
            };
            let mut results = results.lock().unwrap();
            results.push(outcome);
            if results.len() == 2 {
                worker.stop().unwrap();
            }
        })
        .build(|fail: bool| async move {
            if fail {
                panic!("task failed");
            }
            42usize
        });

    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let results = observed.lock().unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.contains(&"panicked"));
    assert!(results.contains(&"completed"));
}
