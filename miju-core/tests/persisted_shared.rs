//! Persistence middleware initialized by multiple workers over one driver.

#![cfg(all(feature = "shared", feature = "sleep"))]

use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{StreamExt, task::noop_waker_ref};
use miju_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        codec::Codec,
        ext::shared::Shared,
        finalize::Durable,
        persistence::{Persisted, Persistence, TaskEvent, TaskPersistLayer},
    },
    task::{Task, builder::TaskBuilder, status::Status},
    worker::{builder::WorkerBuilder, context::WorkerContext, event::Event},
};

#[derive(Debug, Default)]
struct Records {
    tasks: VecDeque<Task<u32>>,
    events: Vec<TaskEvent<u32>>,
}

#[derive(Clone, Debug, Default)]
struct Provider(Arc<Mutex<Records>>);

impl Persistence for Provider {
    type Compact = u32;
    type Response = u32;
    type Error = io::Error;

    async fn register(&mut self, _: &WorkerContext) -> io::Result<()> {
        Ok(())
    }

    async fn heartbeat(&mut self, _: &WorkerContext) -> io::Result<()> {
        Ok(())
    }

    async fn fetch_next(&mut self, _: &WorkerContext) -> io::Result<Vec<Task<u32>>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .tasks
            .pop_front()
            .into_iter()
            .collect())
    }

    async fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<Task<u32>>,
        _: &WorkerContext,
    ) -> io::Result<u64> {
        let len = tasks.len() as u64;
        self.0.lock().unwrap().tasks.extend(tasks);
        Ok(len)
    }

    async fn handle_events(
        &mut self,
        events: Vec<TaskEvent<u32>>,
        _: &WorkerContext,
    ) -> io::Result<()> {
        self.0.lock().unwrap().events.extend(events);
        Ok(())
    }

    async fn push_tasks(&mut self, tasks: Vec<Task<u32>>) -> io::Result<()> {
        self.0.lock().unwrap().tasks.extend(tasks);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NumberCodec;

impl Codec<u32> for NumberCodec {
    type Compact = u32;
    type Error = io::Error;

    fn encode(&self, value: &u32) -> io::Result<u32> {
        Ok(*value)
    }

    fn decode(&self, value: &u32) -> io::Result<u32> {
        Ok(*value)
    }
}

#[derive(Clone)]
struct Adapter(Persisted<Provider>);

impl Backend for Adapter {
    type Task = Task<u32>;
    type Error = io::Error;

    fn poll_ready(&mut self, cx: &mut Context<'_>, worker: &WorkerContext) -> Poll<io::Result<()>> {
        self.0.poll_ready(cx, worker, Duration::from_secs(3600))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<io::Result<Self::Task>>> {
        self.0.poll_next(cx, worker)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>, worker: &WorkerContext) -> Poll<io::Result<()>> {
        self.0.poll_close(cx, worker)
    }
}

impl WireFormatBackend for Adapter {
    type Codec = NumberCodec;
    type Compact = u32;

    fn codec(&self) -> &NumberCodec {
        &NumberCodec
    }
}

impl BackendConfig for Adapter {
    type Args = u32;
    type Id = String;
    type Kind = Durable;
    type Config = ();
    type Layer = TaskPersistLayer<NumberCodec, u32>;

    fn config(&self) -> &() {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        self.0
            .shared_layer(NumberCodec, 2)
            .lock_tasks(true)
            .persist_results(true)
    }
}

async fn complete_and_stop(value: u32, worker: WorkerContext) -> io::Result<u32> {
    worker.stop().unwrap();
    Ok(value + 1)
}

fn run_shared_workers(complete_before_second_stream: bool) {
    let provider = Provider::default();
    provider
        .0
        .lock()
        .unwrap()
        .tasks
        .extend([11, 22].map(|value| {
            TaskBuilder::new(value)
                .task_id(u64::from(value).into())
                .build()
        }));
    let shared = Shared::new(Adapter(Persisted::new(provider.clone())));
    let first = WorkerBuilder::new("first")
        .backend(shared.clone())
        .build(complete_and_stop);
    let second = WorkerBuilder::new("second")
        .backend(shared)
        .build(complete_and_stop);
    let mut first = Box::pin(first.stream());
    let mut cx = Context::from_waker(noop_waker_ref());
    if complete_before_second_stream {
        assert!(matches!(
            first.poll_next_unpin(&mut cx),
            Poll::Ready(Some(Ok(Event::Start)))
        ));
        let event = first.poll_next_unpin(&mut cx);
        assert!(
            matches!(event, Poll::Ready(Some(Ok(Event::Success(_))))),
            "{event:?}"
        );
    }
    let mut second = Box::pin(second.stream());
    if !complete_before_second_stream {
        assert!(matches!(
            first.poll_next_unpin(&mut cx),
            Poll::Ready(Some(Ok(Event::Start)))
        ));
        let event = first.poll_next_unpin(&mut cx);
        assert!(
            matches!(event, Poll::Ready(Some(Ok(Event::Success(_))))),
            "{event:?}"
        );
    }
    assert!(matches!(
        first.poll_next_unpin(&mut cx),
        Poll::Ready(Some(Ok(Event::Exit)))
    ));
    assert!(matches!(first.poll_next_unpin(&mut cx), Poll::Ready(None)));
    assert_eq!(
        provider
            .0
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|event| matches!(event, TaskEvent::Complete(_)))
            .count(),
        1,
        "first completion must survive second stream initialization"
    );
    assert!(matches!(
        second.poll_next_unpin(&mut cx),
        Poll::Ready(Some(Ok(Event::Start)))
    ));
    let event = second.poll_next_unpin(&mut cx);
    assert!(
        matches!(event, Poll::Ready(Some(Ok(Event::Success(_))))),
        "{event:?}"
    );
    assert!(matches!(
        second.poll_next_unpin(&mut cx),
        Poll::Ready(Some(Ok(Event::Exit)))
    ));
    assert!(matches!(second.poll_next_unpin(&mut cx), Poll::Ready(None)));
    let records = provider.0.lock().unwrap();
    assert!(records.tasks.is_empty());
    let completed: Vec<_> = records
        .events
        .iter()
        .filter_map(|event| match event {
            TaskEvent::Complete(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(
        completed.len(),
        2,
        "queued completion must survive second stream construction"
    );
    for (result, value) in completed.into_iter().zip([11_u32, 22]) {
        assert_eq!(result.task_id, u64::from(value).into());
        assert_eq!(result.result, Ok(value + 1));
        assert_eq!(result.attempt, 1);
        assert_eq!(result.status, Status::Done);
    }
}

#[test]
fn initializing_shared_worker_preserves_existing_service() {
    run_shared_workers(false);
}

#[test]
fn initializing_shared_worker_preserves_pending_completion() {
    run_shared_workers(true);
}
