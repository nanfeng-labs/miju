//! Closing a memory backend for one worker leaves its queue open to senders.

use futures_util::future::poll_fn;
use miju_core::{
    backend::{Backend, TaskSink, memory::MemoryStorage},
    worker::context::WorkerContext,
};

#[tokio::test]
async fn backend_close_keeps_the_queue_open_for_later_tasks() {
    let mut storage = MemoryStorage::<u32>::new();
    let worker = WorkerContext::new("memory-close");
    poll_fn(|cx| Backend::poll_close(&mut storage, cx, &worker))
        .await
        .unwrap();

    // Closing the shared sender made the first push fail and the second panic.
    storage.push(1).await.unwrap();
    storage.push(2).await.unwrap();

    for expected in [1, 2] {
        poll_fn(|cx| Backend::poll_ready(&mut storage, cx, &worker))
            .await
            .unwrap();
        let task = poll_fn(|cx| Backend::poll_next(&mut storage, cx, &worker))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.args, expected);
    }
}
