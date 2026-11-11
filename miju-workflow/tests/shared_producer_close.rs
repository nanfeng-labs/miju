//! Closing one producer handle leaves a shared in-memory queue usable by the others.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::SinkExt;
use miju_core::{
    backend::TaskSink,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use miju_workflow::in_memory::InMemoryWorkflow;

const DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn closing_one_producer_keeps_the_shared_queue_open() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut closing = backend.clone();
    let mut producer = backend.clone();
    closing.close().await.unwrap();

    for value in 0..3 {
        producer.push(value).await.unwrap();
    }
    let processed = Arc::new(AtomicUsize::new(0));
    let count = processed.clone();
    let worker = WorkerBuilder::new("consumer").backend(backend).build(
        move |_: u32, worker: WorkerContext| {
            let seen = count.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if seen == 3 {
                    worker.stop().unwrap();
                }
            }
        },
    );
    tokio::time::timeout(DEADLINE, worker.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
