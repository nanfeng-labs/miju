//! Sequential dispatch must obtain readiness from each actual step instance.

use std::{
    collections::HashMap,
    future::{Ready, ready},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use futures_util::task::{AtomicWaker, noop_waker_ref};
use miju_core::{
    backend::ext::shared::Shared,
    error::BoxDynError,
    task::{Task, builder::TaskBuilder},
};
use miju_workflow::{
    in_memory::InMemoryWorkflow,
    sequential::{GoTo, WorkflowContext, router::StepResponse, service::WorkflowService},
};
use tower::{Service, ServiceExt, util::BoxCloneSyncService};

#[derive(Default)]
struct Readiness {
    open: AtomicBool,
    failed: AtomicBool,
    polls: AtomicUsize,
    waker: AtomicWaker,
}

struct Step {
    state: Arc<Readiness>,
    ready: bool,
}

impl Clone for Step {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            ready: false,
        }
    }
}

impl Service<Task<Vec<u8>>> for Step {
    type Response = GoTo<StepResponse>;
    type Error = BoxDynError;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.state.polls.fetch_add(1, Ordering::SeqCst);
        self.state.waker.register(cx.waker());
        if self.state.failed.load(Ordering::SeqCst) {
            return Poll::Ready(Err("step readiness failed".into()));
        }
        if !self.ready && !self.state.open.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        self.ready = true;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Task<Vec<u8>>) -> Self::Future {
        assert!(self.ready, "the called step instance must be ready");
        self.ready = false;
        ready(Ok(GoTo::Done))
    }
}

type Workflow = WorkflowService<Shared<InMemoryWorkflow<u32>>, u32, u32>;

fn workflow(states: &[(usize, Arc<Readiness>)]) -> Workflow {
    let services: HashMap<_, _> = states
        .iter()
        .map(|(index, state)| {
            let service = Step {
                state: state.clone(),
                ready: false,
            };
            (*index, BoxCloneSyncService::new(service))
        })
        .collect();
    WorkflowService::new(services, InMemoryWorkflow::create())
}

fn request(index: usize) -> Task<Vec<u8>> {
    TaskBuilder::new(Vec::new())
        .metadata(&WorkflowContext { step_index: index })
        .build()
}

#[tokio::test]
async fn first_dispatch_waits_for_every_registered_step() {
    let first = Arc::new(Readiness::default());
    let last = Arc::new(Readiness::default());
    first.open.store(true, Ordering::SeqCst);
    let mut service = workflow(&[(0, first.clone()), (7, last.clone())]);
    assert!(
        service
            .poll_ready(&mut Context::from_waker(noop_waker_ref()))
            .is_pending()
    );
    last.open.store(true, Ordering::SeqCst);
    last.waker.wake();
    service
        .ready()
        .await
        .unwrap()
        .call(request(7))
        .await
        .unwrap();
    service
        .ready()
        .await
        .unwrap()
        .call(request(0))
        .await
        .unwrap();
    assert_eq!(first.polls.load(Ordering::SeqCst), 1);
}

#[test]
fn first_readiness_error_is_returned_without_dispatch() {
    let state = Arc::new(Readiness::default());
    state.failed.store(true, Ordering::SeqCst);
    let mut service = workflow(&[(0, state)]);
    let Poll::Ready(Err(error)) = service.poll_ready(&mut Context::from_waker(noop_waker_ref()))
    else {
        panic!("the step readiness error must be returned");
    };
    assert_eq!(error.to_string(), "step readiness failed");
}

#[tokio::test]
async fn cloned_workflow_obtains_readiness_for_its_own_steps() {
    let state = Arc::new(Readiness::default());
    state.open.store(true, Ordering::SeqCst);
    let mut original = workflow(&[(0, state.clone())]);
    original.ready().await.unwrap();
    let mut cloned = original.clone();
    cloned
        .ready()
        .await
        .unwrap()
        .call(request(0))
        .await
        .unwrap();
    original.call(request(0)).await.unwrap();
    assert_eq!(state.polls.load(Ordering::SeqCst), 2);
}
