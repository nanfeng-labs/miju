//! A ready handle owns the next send until it sends, flushes, closes, or drops.

use futures_util::{
    FutureExt, Sink, SinkExt, StreamExt, lock::Mutex as AsyncMutex, stream, task::noop_waker_ref,
};
use miju_core::{
    backend::{
        Backend,
        memory::{MemorySink, MemoryStorage, MemoryStorageError},
    },
    task::{Task, builder::TaskBuilder},
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

#[derive(Default)]
struct State {
    slot: Option<u32>,
    waiter: Option<Waker>,
    fail_ready: bool,
    fail_send: bool,
}
struct OneSlot(Arc<Mutex<State>>);
impl Sink<Task<u32>> for OneSlot {
    type Error = MemoryStorageError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self.0.lock().unwrap();
        if std::mem::take(&mut state.fail_ready) {
            return Poll::Ready(Err(MemoryStorageError::Other(
                io::Error::other("readiness failed").into(),
            )));
        }
        if state.slot.is_some() {
            state.waiter = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn start_send(self: Pin<&mut Self>, task: Task<u32>) -> Result<(), Self::Error> {
        let mut state = self.0.lock().unwrap();
        assert!(
            state.slot.is_none(),
            "start_send requires the slot granted by poll_ready"
        );
        if std::mem::take(&mut state.fail_send) {
            return Err(MemoryStorageError::Other(
                io::Error::other("send failed").into(),
            ));
        }
        state.slot = Some(task.args);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
fn setup() -> (MemorySink<u32>, Arc<Mutex<State>>) {
    let state = Arc::new(Mutex::new(State::default()));
    (
        MemorySink::new(Arc::new(AsyncMutex::new(Box::new(OneSlot(state.clone()))))),
        state,
    )
}
fn take(state: &Mutex<State>) -> u32 {
    let (item, waker) = {
        let mut state = state.lock().unwrap();
        (state.slot.take().unwrap(), state.waiter.take())
    };
    if let Some(waker) = waker {
        waker.wake();
    }
    item
}
fn ready(sink: &mut MemorySink<u32>) {
    assert!(matches!(
        sink.poll_ready_unpin(&mut Context::from_waker(noop_waker_ref())),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn clones_cannot_both_reserve_one_free_slot() {
    let (mut first, state) = setup();
    let mut second = first.clone();
    ready(&mut first);
    ready(&mut first);
    assert!(
        second
            .poll_ready_unpin(&mut Context::from_waker(noop_waker_ref()))
            .is_pending()
    );
    first.start_send_unpin(Task::new(1)).unwrap();
    assert_eq!(take(&state), 1);
    ready(&mut second);
    second.start_send_unpin(Task::new(2)).unwrap();
    assert_eq!(take(&state), 2);
}

#[derive(Default)]
struct Notify(tokio::sync::Notify);
impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn releasing_unused_readiness_notifies_a_waiting_clone() {
    for release in ["drop", "flush", "close", "duplicate", "send-error"] {
        let (mut first, state) = setup();
        ready(&mut first);
        first
            .start_send_unpin(TaskBuilder::new(1).idempotency_key("known").build())
            .unwrap();
        assert_eq!(take(&state), 1);
        ready(&mut first);
        let mut second = first.clone();
        let notified = Arc::new(Notify::default());
        let waker = Waker::from(notified.clone());
        assert!(
            second
                .poll_ready_unpin(&mut Context::from_waker(&waker))
                .is_pending()
        );
        match release {
            "drop" => drop(first),
            "flush" => assert!(
                first
                    .poll_flush_unpin(&mut Context::from_waker(noop_waker_ref()))
                    .is_ready()
            ),
            "close" => assert!(
                first
                    .poll_close_unpin(&mut Context::from_waker(noop_waker_ref()))
                    .is_ready()
            ),
            "duplicate" => first
                .start_send_unpin(TaskBuilder::new(2).idempotency_key("known").build())
                .unwrap(),
            "send-error" => {
                state.lock().unwrap().fail_send = true;
                assert!(first.start_send_unpin(Task::new(2)).is_err());
            }
            _ => unreachable!(),
        }
        tokio::time::timeout(Duration::from_secs(2), notified.0.notified())
            .await
            .unwrap();
        ready(&mut second);
        second.start_send_unpin(Task::new(3)).unwrap();
        assert_eq!(take(&state), 3);
    }
}

#[test]
fn pending_or_failed_inner_readiness_does_not_keep_the_reservation() {
    let (mut first, state) = setup();
    let mut second = first.clone();
    state.lock().unwrap().fail_ready = true;
    assert!(matches!(
        first.poll_ready_unpin(&mut Context::from_waker(noop_waker_ref())),
        Poll::Ready(Err(_))
    ));
    ready(&mut second);
    second.start_send_unpin(Task::new(1)).unwrap();
    assert!(
        first
            .poll_ready_unpin(&mut Context::from_waker(noop_waker_ref()))
            .is_pending()
    );
    assert_eq!(take(&state), 1);
    ready(&mut second);
    second.start_send_unpin(Task::new(2)).unwrap();
    assert_eq!(take(&state), 2);
}

#[test]
fn backend_readiness_does_not_reserve_a_producer_slot() {
    let (mut sender, _) = setup();
    let mut backend = MemoryStorage::new_with(sender.clone(), Box::pin(stream::pending()));
    assert!(matches!(
        Backend::poll_ready(
            &mut backend,
            &mut Context::from_waker(noop_waker_ref()),
            &WorkerContext::new("reader")
        ),
        Poll::Ready(Ok(()))
    ));
    ready(&mut sender);
    sender.start_send_unpin(Task::new(1)).unwrap();
    fn traits<T: Send + Sync + Unpin>() {}
    traits::<MemorySink<std::rc::Rc<()>>>();
}

#[test]
fn producer_close_flushes_accepted_tasks_without_waiting_for_unused_peer_readiness() {
    let (sender, mut receiver) = futures_channel::mpsc::unbounded::<Task<u32>>();
    let mut first = MemorySink::new(Arc::new(AsyncMutex::new(Box::new(
        sender.sink_map_err(MemoryStorageError::SendError),
    ))));
    let mut peer = first.clone();
    first.feed(Task::new(7)).now_or_never().unwrap().unwrap();
    ready(&mut first);
    peer.close()
        .now_or_never()
        .expect("close must not wait for an unsent peer task")
        .unwrap();
    assert_eq!(receiver.next().now_or_never().unwrap().unwrap().args, 7);
    assert!(receiver.next().now_or_never().unwrap().is_none());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        first.start_send_unpin(Task::new(8))
    }));
    assert!(matches!(result, Ok(Err(MemoryStorageError::SendError(_)))));
}

#[test]
fn context_clone_panic_releases_the_send_reservation() {
    struct PanicsOnClone;
    impl Clone for PanicsOnClone {
        fn clone(&self) -> Self {
            panic!("context clone");
        }
    }
    let (mut first, state) = setup();
    let mut second = first.clone();
    ready(&mut first);
    let task = TaskBuilder::new(1).data(PanicsOnClone).build();
    let _observer = task.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| first.start_send_unpin(task)))
            .is_err()
    );
    ready(&mut second);
    second.start_send_unpin(Task::new(2)).unwrap();
    assert_eq!(take(&state), 2);
}

#[test]
fn flush_of_delivered_work_does_not_wait_for_unused_peer_readiness() {
    let (sender, mut receiver) = futures_channel::mpsc::unbounded::<Task<u32>>();
    let mut first = MemorySink::new(Arc::new(AsyncMutex::new(Box::new(
        sender.sink_map_err(MemoryStorageError::SendError),
    ))));
    let mut peer = first.clone();
    first.feed(Task::new(7)).now_or_never().unwrap().unwrap();
    assert_eq!(receiver.next().now_or_never().unwrap().unwrap().args, 7);
    ready(&mut peer);
    first
        .flush()
        .now_or_never()
        .expect("flush must not wait for an unsent peer task")
        .unwrap();
    peer.start_send_unpin(Task::new(8)).unwrap();
    assert_eq!(receiver.next().now_or_never().unwrap().unwrap().args, 8);
}

#[test]
fn idle_worker_stops_while_a_producer_holds_unused_readiness() {
    let (mut producer, state) = setup();
    let backend = MemoryStorage::new_with(producer.clone(), Box::pin(stream::pending()));
    ready(&mut producer);
    let context = WorkerContext::new("unused-producer-permit");
    let worker = WorkerBuilder::new(&context)
        .backend(backend)
        .build(|_: u32| async {});
    let mut run = Box::pin(worker.run());
    assert!(run.as_mut().now_or_never().is_none());
    context.stop().unwrap();
    run.as_mut()
        .now_or_never()
        .expect("worker stop must not wait for an unsent producer task")
        .unwrap();
    assert!(context.is_terminated());
    producer.start_send_unpin(Task::new(9)).unwrap();
    assert_eq!(take(&state), 9);
}
