//! An idle Persisted driver fetches again on a polling strategy's schedule,
//! independently of its heartbeat interval.

#![cfg(feature = "sleep")]

use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use miju_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        codec::Codec,
        ext::BackendExt,
        finalize::Durable,
        persistence::{Persisted, Persistence, TaskEvent, TaskPersistLayer},
    },
    error::WorkerError,
    task::{Task, builder::TaskBuilder, data::Data},
    worker::{builder::WorkerBuilder, context::WorkerContext},
};

#[derive(Clone, Debug, Default)]
struct Provider(Arc<Mutex<VecDeque<Task<u32>>>>);

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
        Ok(self.0.lock().unwrap().pop_front().into_iter().collect())
    }

    async fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<Task<u32>>,
        _: &WorkerContext,
    ) -> io::Result<u64> {
        let len = tasks.len() as u64;
        self.0.lock().unwrap().extend(tasks);
        Ok(len)
    }

    async fn handle_events(&mut self, _: Vec<TaskEvent<u32>>, _: &WorkerContext) -> io::Result<()> {
        Ok(())
    }

    async fn push_tasks(&mut self, tasks: Vec<Task<u32>>) -> io::Result<()> {
        self.0.lock().unwrap().extend(tasks);
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
        // Far longer than the test, so only the polling strategy can wake the worker.
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
        self.0.layer(NumberCodec, 4)
    }
}

type Ran = Arc<Mutex<Vec<u32>>>;

async fn record(value: u32, ran: Data<Ran>) -> io::Result<u32> {
    ran.lock().unwrap().push(value);
    Ok(value)
}

#[tokio::test]
async fn idle_driver_fetches_on_the_polling_interval() {
    let provider = Provider::default();
    let tasks = provider.0.clone();
    let ran = Ran::default();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    // A separate task, so only the worker's own timers can wake it.
    let worker = tokio::spawn(
        WorkerBuilder::new("idle")
            .backend(
                Adapter(Persisted::new(provider)).poll_with_interval(Duration::from_millis(20)),
            )
            .data(ran.clone())
            .build(record)
            .run_until(async move {
                let _ = stopped.await;
                Ok::<(), WorkerError>(())
            }),
    );
    // Let the worker fetch an empty batch, then add a task without waking it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    tasks
        .lock()
        .unwrap()
        .push_back(TaskBuilder::new(7).task_id(1u64.into()).build());
    for _ in 0..100 {
        if !ran.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let observed = ran.lock().unwrap().clone();
    stop.send(()).unwrap();
    worker.await.unwrap().unwrap();
    assert_eq!(observed, [7]);
}
