//! Checks that initialization and ready collection work on the same fold service.

use std::{
    future::{Ready, ready},
    io,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
    time::Duration,
};

use futures_util::future::poll_fn as stream_poll_fn;
use miju_core::{
    backend::{Backend, ext::shared::Shared},
    task::{Task, builder::TaskBuilder, metadata::Metadata},
    worker::context::WorkerContext,
};
use miju_workflow::{
    in_memory::InMemoryWorkflow,
    sequential::{
        GoTo, StepContext,
        fold::{FoldService, FoldState},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower::{Service, ServiceExt};

type Queue = Shared<InMemoryWorkflow<Vec<u32>>>;
static DEFAULTS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Deserialize, Serialize)]
struct Seed(usize);

impl Default for Seed {
    fn default() -> Self {
        Self(DEFAULTS.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

struct ReadyHandler {
    ready: bool,
}

impl Clone for ReadyHandler {
    fn clone(&self) -> Self {
        Self { ready: false }
    }
}

impl Service<Task<(Seed, u32)>> for ReadyHandler {
    type Response = Seed;
    type Error = io::Error;
    type Future = Ready<Result<Seed, io::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Task<(Seed, u32)>) -> Self::Future {
        let (seed, item) = task.args;
        if std::mem::take(&mut self.ready) {
            ready(Ok(Seed(seed.0 + item as usize)))
        } else {
            ready(Err(io::Error::other("handler instance was not ready")))
        }
    }
}

async fn next_task(queue: &mut Queue) -> Task<Vec<u8>> {
    tokio::time::timeout(
        Duration::from_secs(2),
        stream_poll_fn(|cx| Backend::poll_next(queue, cx, &WorkerContext::new("fold-reader"))),
    )
    .await
    .expect("the initialized fold should enqueue its continuation")
    .expect("the in-memory queue should remain open")
    .expect("the queued continuation should be readable")
}

#[tokio::test]
async fn fold_initial_accumulator_survives_ready_collection_on_the_same_service() {
    DEFAULTS.store(0, Ordering::SeqCst);
    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let mut reader = backend.clone();
    let mut service: FoldService<ReadyHandler, Seed, Vec<u32>, Queue> =
        FoldService::new(ReadyHandler { ready: false });

    let initialization = TaskBuilder::new(b"[7]".to_vec())
        .metadata(&FoldState::Init)
        .data(StepContext::new(backend.clone(), 4, false))
        .build();
    let GoTo::Next(initial_response) = service
        .ready()
        .await
        .unwrap()
        .call(initialization)
        .await
        .unwrap()
    else {
        panic!("fold initialization should enqueue a collection task");
    };
    assert_eq!(initial_response.result, json!(1));

    let mut continuation = next_task(&mut reader).await;
    let (queued_seed, queued_items): (Seed, Vec<u32>) =
        serde_json::from_slice(&continuation.args).unwrap();
    assert_eq!(queued_seed.0, 1);
    assert_eq!(queued_items, [7]);
    assert_eq!(DEFAULTS.load(Ordering::SeqCst), 1);
    assert_eq!(
        FoldState::extract(continuation.metadata()).unwrap(),
        FoldState::Collection
    );
    continuation.inject_data(StepContext::new(backend, 4, false));

    let GoTo::Break(result) = service
        .ready()
        .await
        .unwrap()
        .call(continuation)
        .await
        .unwrap()
    else {
        panic!("the one-item fold should finish after calling the ready handler");
    };
    assert_eq!(result.result, json!(8));
    assert!(result.next_task_id.is_none());
    assert_eq!(DEFAULTS.load(Ordering::SeqCst), 1);
}
