//! Stopping one worker leaves a shared in-memory queue usable by its peers.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use miju_core::{
    backend::TaskSink,
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt,
    },
};
use miju_workflow::in_memory::InMemoryWorkflow;

const DEADLINE: Duration = Duration::from_secs(5);

// The default current-thread runtime keeps the two workers' startup sequential.
#[tokio::test]
async fn stopping_one_worker_keeps_the_shared_queue_open_for_peers() {
    let backend = InMemoryWorkflow::<u32>::create();
    let mut producer = backend.clone();
    let started = Arc::new(AtomicUsize::new(0));
    let stopped_count = Arc::new(AtomicUsize::new(0));
    let peer_count = Arc::new(AtomicUsize::new(0));
    let stopped_context = Arc::new(Mutex::new(None::<WorkerContext>));

    let (count, on_start) = (stopped_count.clone(), started.clone());
    let context = stopped_context.clone();
    let stopped = WorkerBuilder::new("stopped")
        .backend(backend.clone())
        .on_event(move |worker, event| {
            if matches!(event, Event::Start) {
                *context.lock().unwrap() = Some(worker.clone());
                on_start.fetch_add(1, Ordering::SeqCst);
            }
        })
        .build(move |_: u32| {
            count.fetch_add(1, Ordering::SeqCst);
            async {}
        });

    let (count, on_start) = (peer_count.clone(), started.clone());
    let peer = WorkerBuilder::new("peer")
        .backend(backend)
        .on_event(move |_, event| {
            if matches!(event, Event::Start) {
                on_start.fetch_add(1, Ordering::SeqCst);
            }
        })
        .build(move |_: u32, worker: WorkerContext| {
            let seen = count.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if seen == 3 {
                    worker.stop().unwrap();
                }
            }
        });

    let stopped = tokio::spawn(stopped.run());
    let peer = tokio::spawn(peer.run());
    tokio::time::timeout(DEADLINE, async {
        while started.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let context = stopped_context.lock().unwrap().take().unwrap();
    context.stop().unwrap();
    tokio::time::timeout(DEADLINE, stopped)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    for value in 0..3 {
        producer.push(value).await.unwrap();
    }
    tokio::time::timeout(DEADLINE, peer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stopped_count.load(Ordering::SeqCst), 0);
    assert_eq!(peer_count.load(Ordering::SeqCst), 3);
}
