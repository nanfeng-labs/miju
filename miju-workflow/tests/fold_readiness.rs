//! Fold calls retain readiness granted to the handler instance.

use std::{
    future::{Ready, ready},
    io,
    task::{Context, Poll},
};

use miju_core::{
    backend::ext::shared::Shared,
    task::{Task, builder::TaskBuilder},
};
use miju_workflow::{
    in_memory::InMemoryWorkflow,
    sequential::{
        GoTo,
        context::StepContext,
        fold::{FoldService, FoldState},
    },
};
use serde_json::json;
use tower::{Service, ServiceExt};

struct ReadyHandler {
    ready: bool,
}

impl Clone for ReadyHandler {
    fn clone(&self) -> Self {
        Self { ready: false }
    }
}

impl Service<Task<(u32, u32)>> for ReadyHandler {
    type Response = u32;
    type Error = io::Error;
    type Future = Ready<Result<u32, io::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Task<(u32, u32)>) -> Self::Future {
        if !std::mem::take(&mut self.ready) {
            return ready(Err(io::Error::other("handler instance was not ready")));
        }
        ready(Ok(task.args.0 + task.args.1))
    }
}

#[tokio::test]
async fn fold_calls_the_handler_instance_that_reported_ready() {
    type Backend = Shared<InMemoryWorkflow<Vec<u32>>>;
    let backend = InMemoryWorkflow::<Vec<u32>>::create();
    let service = FoldService::<_, u32, Vec<u32>, Backend>::new(ReadyHandler { ready: false });
    let task = TaskBuilder::new(b"[10,[7]]".to_vec())
        .metadata(&FoldState::Collection)
        .data(StepContext::new(backend, 0, false))
        .build();

    let result = service.oneshot(task).await;
    assert!(
        result.is_ok(),
        "a ready fold must call its ready handler: {result:?}"
    );
    let GoTo::Break(response) = result.unwrap() else {
        panic!("the last item must return its accumulated value");
    };
    assert_eq!(response.result, json!(17));
    assert!(response.next_task_id.is_none());
}
