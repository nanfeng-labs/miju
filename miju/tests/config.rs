//! Exercise worker configuration, backend construction, and middleware behavior.

#![cfg(feature = "config")]

use miju::{
    backend::{BackendConfig, TryNewBackend, finalize::Ephemeral},
    config::{Middleware, MiddlewareStack, WorkerConfig, WorkerFromConfig},
    prelude::*,
};
use std::{
    collections::{HashSet, VecDeque},
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tower::{Service, ServiceExt, service_fn};

// A fallible backend whose queue is supplied entirely through configuration.
struct SeededBackend {
    config: Vec<usize>,
    tasks: VecDeque<Task<usize>>,
}

impl Backend for SeededBackend {
    type Task = Task<usize>;
    type Error = io::Error;

    fn poll_ready(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<io::Result<Self::Task>>> {
        Poll::Ready(self.tasks.pop_front().map(Ok))
    }

    fn poll_close(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl BackendConfig for SeededBackend {
    type Args = usize;
    type Id = RandomId;
    type Kind = Ephemeral;
    type Config = Vec<usize>;
    type Layer = Identity;

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}

impl TryNewBackend for SeededBackend {
    type Backend = Self;

    fn try_new(config: Self::Config) -> io::Result<Self> {
        if config.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty queue"));
        }
        let tasks = config
            .iter()
            .map(|value| {
                TaskBuilder::new(*value)
                    .task_id(value.to_string().into())
                    .build()
            })
            .collect();
        Ok(Self { config, tasks })
    }
}

#[test]
fn worker_config_round_trips_the_backend_configuration() {
    let value = serde_json::json!({"name": "configured", "backend": null, "middleware": []});
    let config: WorkerConfig<MemoryStorage<usize>> = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(config).unwrap(), value);
}

#[test]
fn configuration_requires_fields_and_only_accepts_enabled_variants() {
    let value = serde_json::json!({"name": "configured", "backend": [], "middleware": []});
    for field in ["name", "backend", "middleware"] {
        let mut incomplete = value.clone();
        incomplete.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<WorkerConfig<SeededBackend>>(incomplete).is_err());
    }
    for (variant, enabled) in [
        (
            serde_json::json!({"Timeout": {"duration": {"secs": 1, "nanos": 0}}}),
            cfg!(feature = "timeout"),
        ),
        (
            serde_json::json!({"RateLimit": {"num": 2, "per": {"secs": 1, "nanos": 0}}}),
            cfg!(feature = "limit"),
        ),
        (
            serde_json::json!({"Concurrency": {"max": 2}}),
            cfg!(feature = "limit"),
        ),
        (
            serde_json::json!("CatchPanic"),
            cfg!(feature = "catch-panic"),
        ),
        (serde_json::json!("Tracing"), cfg!(feature = "tracing")),
        (serde_json::json!({"Retries": {"max": 3}}), false),
        (serde_json::json!("Filter"), false),
    ] {
        let result = serde_json::from_value::<Middleware>(variant.clone());
        assert_eq!(result.is_ok(), enabled, "{variant}");
        if let Ok(middleware) = result {
            assert_eq!(serde_json::to_value(middleware).unwrap(), variant);
        }
    }
}

#[test]
fn backend_constructor_errors_are_returned_unchanged() {
    let config = WorkerConfig::<SeededBackend> {
        name: "invalid-backend".into(),
        backend: Vec::new(),
        middleware: HashSet::new(),
    };
    let Err(error) = WorkerBuilder::try_config(config) else {
        panic!("backend construction must fail");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(error.to_string(), "empty queue");
}

#[tokio::test(start_paused = true)]
async fn parsed_configuration_builds_and_runs_a_named_worker() {
    let middleware: Vec<serde_json::Value> = vec![
        #[cfg(feature = "timeout")]
        serde_json::json!({"Timeout": {"duration": {"secs": 1, "nanos": 0}}}),
        #[cfg(feature = "limit")]
        serde_json::json!({"RateLimit": {"num": 1, "per": {"secs": 0, "nanos": 100_000_000}}}),
        #[cfg(feature = "limit")]
        serde_json::json!({"Concurrency": {"max": 1}}),
        #[cfg(feature = "catch-panic")]
        serde_json::json!("CatchPanic"),
        #[cfg(feature = "tracing")]
        serde_json::json!("Tracing"),
    ];
    let config: WorkerConfig<SeededBackend> = serde_json::from_value(serde_json::json!({
        "name": "seeded-worker", "backend": [3, 7], "middleware": middleware
    }))
    .unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let results = observed.clone();
    let worker = WorkerBuilder::try_config(config)
        .unwrap()
        .on_event(move |worker, event| match event {
            Event::Success(value) => {
                assert_eq!(worker.name(), "seeded-worker");
                let mut results = results.lock().unwrap();
                results.push(*value.downcast_ref::<usize>().unwrap());
                if results.len() == 2 {
                    worker.stop().unwrap();
                }
            }
            Event::Error(error) => panic!("{error}"),
            _ => {}
        })
        .build(|value: usize| async move { value * 2 });
    tokio::time::timeout(Duration::from_secs(5), worker.run())
        .await
        .unwrap()
        .unwrap();
    let mut results = observed.lock().unwrap();
    results.sort_unstable();
    assert_eq!(*results, [6, 14]);
}

#[tokio::test]
async fn empty_stack_preserves_responses_and_typed_errors() {
    let mut service = MiddlewareStack::<MemoryStorage<bool>>::new(HashSet::new()).layer(
        service_fn(|task: Task<bool>| async move {
            if task.args {
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"))
            } else {
                Ok(42)
            }
        }),
    );
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
    let error = service.oneshot(Task::new(true)).await.unwrap_err();
    let original = error.downcast_ref::<io::Error>().unwrap();
    assert_eq!(original.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(original.to_string(), "denied");
}

#[cfg(feature = "limit")]
#[test]
fn identical_descriptors_collapse_but_different_limits_remain() {
    let config: WorkerConfig<MemoryStorage<usize>> = serde_json::from_value(serde_json::json!({
        "name": "limits", "backend": null, "middleware": [
            {"Concurrency": {"max": 1}},
            {"Concurrency": {"max": 2}},
            {"Concurrency": {"max": 1}}
        ]
    }))
    .unwrap();
    assert_eq!(
        config.middleware,
        HashSet::from([
            Middleware::Concurrency { max: 1 },
            Middleware::Concurrency { max: 2 }
        ])
    );
}

#[cfg(feature = "limit")]
#[tokio::test(start_paused = true)]
async fn configured_limits_delay_readiness_and_release_permits() {
    let mut service = MiddlewareStack::<MemoryStorage<usize>>::new(HashSet::from([
        Middleware::RateLimit {
            num: 1,
            per: Duration::from_secs(2),
        },
        Middleware::Concurrency { max: 1 },
    ]))
    .layer(service_fn(|task: Task<usize>| async move {
        Ok::<_, io::Error>(task.args)
    }));
    let start = tokio::time::Instant::now();
    let first = service.ready().await.unwrap().call(Task::new(1));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), service.ready())
            .await
            .is_err()
    );
    assert_eq!(first.await.unwrap(), 1);
    assert_eq!(
        service
            .ready()
            .await
            .unwrap()
            .call(Task::new(2))
            .await
            .unwrap(),
        2
    );
    assert!(tokio::time::Instant::now() - start >= Duration::from_secs(3));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), service.ready())
            .await
            .is_err()
    );
    assert_eq!(service.oneshot(Task::new(3)).await.unwrap(), 3);
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(4));
}

#[cfg(feature = "timeout")]
#[tokio::test(start_paused = true)]
async fn configured_timeout_returns_elapsed() {
    let service = MiddlewareStack::<MemoryStorage<()>>::new(HashSet::from([Middleware::Timeout {
        duration: Duration::from_secs(1),
    }]))
    .layer(service_fn(|_: Task<()>| {
        std::future::pending::<io::Result<()>>()
    }));
    let error = tokio::time::timeout(Duration::from_secs(5), service.oneshot(Task::new(())))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .downcast_ref::<tower::timeout::error::Elapsed>()
            .is_some()
    );
}

#[cfg(feature = "catch-panic")]
#[tokio::test]
async fn configured_panic_capture_preserves_the_payload() {
    use std::error::Error;

    let service =
        MiddlewareStack::<MemoryStorage<()>>::new(HashSet::from([Middleware::CatchPanic])).layer(
            service_fn(|_: Task<()>| async {
                panic!("configured panic");
                #[allow(unreachable_code)]
                Ok::<_, io::Error>(())
            }),
        );
    let error = service.oneshot(Task::new(())).await.unwrap_err();
    let abort = error.downcast_ref::<AbortError>().unwrap();
    let payload = abort
        .source()
        .unwrap()
        .downcast_ref::<miju::layers::catch_panic::PanicError>()
        .unwrap();
    assert_eq!(payload.0, "configured panic");
}
