//! A task ID delivered again while its first execution runs is rejected without
//! recording a result, so the running execution's result is the only one stored.

#![cfg(feature = "task-context")]

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{FutureExt, future::BoxFuture};
use miju_core::{
    backend::{TaskSink, memory::MemoryStorage},
    error::{BoxDynError, WorkerError},
    task::{ExecutionContext, Task, builder::TaskBuilder, task_id::TaskId},
    worker::{
        builder::WorkerBuilder,
        event::Event,
        ext::{
            ack::{Acknowledge, AcknowledgementExt},
            event_listener::EventListenerExt,
        },
    },
};

type Log = Arc<Mutex<Vec<String>>>;

/// Two deliveries of task 7; the first runs long enough for the second to arrive.
fn deliveries() -> [Task<u32>; 2] {
    [1, 2].map(|args| TaskBuilder::new(args).task_id(TaskId::from_int(7)).build())
}

async fn slow_first(value: u32) -> Result<u32, io::Error> {
    if value == 1 {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(value * 10)
}

async fn stop_after_both_deliveries() -> Result<(), WorkerError> {
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}

fn record_errors(log: &Log) -> impl Fn(&miju_core::worker::context::WorkerContext, &Event) + use<> {
    let log = log.clone();
    move |_, event| {
        if let Event::Error(error) = event {
            log.lock().unwrap().push(error.to_string());
        }
    }
}

#[derive(Clone)]
struct Recorder(Log);

impl Acknowledge<u32> for Recorder {
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<(), io::Error>>;

    fn ack(&mut self, res: &Result<u32, BoxDynError>, ctx: &ExecutionContext) -> Self::Future {
        let result = res.as_ref().map_err(ToString::to_string);
        self.0
            .lock()
            .unwrap()
            .push(format!("{} {result:?}", ctx.task_id().unwrap()));
        futures_util::future::ready(Ok(())).boxed()
    }
}

#[tokio::test]
async fn rejected_duplicate_is_not_acknowledged() {
    let mut backend = MemoryStorage::new();
    for task in deliveries() {
        backend.push_task(task).await.unwrap();
    }
    let (acks, errors) = (Log::default(), Log::default());
    WorkerBuilder::new("duplicate-ack")
        .backend(backend)
        .ack_with(Recorder(acks.clone()))
        .on_event(record_errors(&errors))
        .build(slow_first)
        .run_until(stop_after_both_deliveries())
        .await
        .unwrap();

    assert_eq!(*errors.lock().unwrap(), ["task already has a token"]);
    assert_eq!(*acks.lock().unwrap(), ["7 Ok(10)"]);
}

#[cfg(feature = "sleep")]
mod persisted {
    use std::{
        collections::VecDeque,
        task::{Context, Poll},
    };

    use miju_core::{
        backend::{
            Backend, BackendConfig, WireFormatBackend,
            codec::Codec,
            finalize::Durable,
            persistence::{Persisted, Persistence, TaskEvent, TaskPersistLayer},
        },
        worker::context::WorkerContext,
    };

    use super::*;

    #[derive(Debug, Default)]
    struct Records {
        tasks: VecDeque<Task<u32>>,
        completions: Vec<String>,
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
            Ok(tasks.len() as u64)
        }

        async fn handle_events(
            &mut self,
            events: Vec<TaskEvent<u32>>,
            _: &WorkerContext,
        ) -> io::Result<()> {
            let mut records = self.0.lock().unwrap();
            for event in events {
                if let TaskEvent::Complete(result) = event {
                    records
                        .completions
                        .push(format!("{} {:?}", result.task_id, result.result));
                }
            }
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

        fn poll_ready(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<io::Result<()>> {
            self.0.poll_ready(cx, worker, Duration::from_millis(20))
        }

        fn poll_next(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Option<io::Result<Self::Task>>> {
            self.0.poll_next(cx, worker)
        }

        fn poll_close(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<io::Result<()>> {
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
            self.0.layer(NumberCodec, 8).persist_results(true)
        }
    }

    #[tokio::test]
    async fn rejected_duplicate_is_not_persisted() {
        let provider = Provider::default();
        provider.0.lock().unwrap().tasks.extend(deliveries());
        let errors = Log::default();
        WorkerBuilder::new("duplicate-persist")
            .backend(Adapter(Persisted::new(provider.clone())))
            .on_event(record_errors(&errors))
            .build(slow_first)
            .run_until(stop_after_both_deliveries())
            .await
            .unwrap();

        assert_eq!(*errors.lock().unwrap(), ["task already has a token"]);
        assert_eq!(provider.0.lock().unwrap().completions, ["7 Ok(10)"]);
    }
}
