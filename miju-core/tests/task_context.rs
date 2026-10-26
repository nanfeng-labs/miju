//! Regression tests for task execution notifications.
#![cfg(feature = "task-context")]

use futures_util::{FutureExt, SinkExt, StreamExt, stream::BoxStream};
use miju_core::{
    backend::dequeue::VecDequeBackend,
    error::WorkerError,
    task::{builder::TaskBuilder, context::TaskContext, task_id::TaskId},
    worker::{builder::WorkerBuilder, context::WorkerContext, event::Event},
};
use std::{
    future::{Future, pending, poll_fn, ready},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::sync::Notify;

#[derive(Default)]
struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn notification() -> (Arc<Counter>, Waker) {
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(counter.clone());
    (counter, waker)
}
async fn running() -> (
    TaskContext,
    Arc<Notify>,
    BoxStream<'static, Result<Event, WorkerError>>,
) {
    let worker = WorkerContext::new("completion-waiters");
    let mut backend = VecDequeBackend::new();
    backend
        .send(
            TaskBuilder::new(1_u32)
                .task_id(TaskId::from_string("task"))
                .build(),
        )
        .await
        .unwrap();
    let gate = Arc::new(Notify::new());
    let handler_gate = gate.clone();
    let mut events = WorkerBuilder::new(&worker)
        .backend(backend)
        .build(move |_: u32| {
            let gate = handler_gate.clone();
            async move {
                gate.notified().await;
            }
        })
        .stream()
        .boxed();
    assert!(matches!(events.next().await, Some(Ok(Event::Start))));
    assert!(events.next().now_or_never().is_none());
    (worker.get_task("task").unwrap(), gate, events)
}

async fn executed_waiters_case(cancelled: bool) {
    let (parent, gate, mut events) = running().await;
    let mut first = parent.executed();
    let mut second = parent.clone().executed();
    let (a, wa) = notification();
    let (b, wb) = notification();
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&wa))
            .is_pending()
    );
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(&wb))
            .is_pending()
    );
    if cancelled {
        parent.cancel().unwrap();
    } else {
        gate.notify_one();
        assert!(matches!(events.next().await, Some(Ok(Event::Success(_)))));
    }
    assert!(
        a.0.load(Ordering::SeqCst) > 0,
        "the first pending completion waiter must receive a notification"
    );
    assert!(
        b.0.load(Ordering::SeqCst) > 0,
        "the second pending completion waiter must receive a notification"
    );
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&wa))
            .is_ready()
    );
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(&wb))
            .is_ready()
    );
}

#[tokio::test]
async fn single_waiter_and_already_completed_wait_keep_working() {
    let (parent, gate, mut events) = running().await;
    let mut wait = parent.executed();
    let (count, waker) = notification();
    assert!(
        Pin::new(&mut wait)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    gate.notify_one();
    assert!(matches!(events.next().await, Some(Ok(Event::Success(_)))));
    assert!(count.0.load(Ordering::SeqCst) > 0);
    assert!(
        Pin::new(&mut wait)
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    assert!(parent.executed().now_or_never().is_some());
}

#[tokio::test]
async fn completion_notifies_every_executed_waiter() {
    executed_waiters_case(false).await;
}
#[tokio::test]
async fn cancellation_notifies_every_executed_waiter() {
    executed_waiters_case(true).await;
}

#[tokio::test]
async fn repolling_one_waiter_preserves_other_waiter_registration() {
    let (parent, _, _events) = running().await;
    let mut first = parent.executed();
    let mut second = parent.executed();
    let (old, old_waker) = notification();
    let (other, other_waker) = notification();
    let (current, current_waker) = notification();
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(&other_waker))
            .is_pending()
    );
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&current_waker))
            .is_pending()
    );
    parent.cancel().unwrap();
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert!(other.0.load(Ordering::SeqCst) > 0);
    assert!(current.0.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn dropping_waiter_releases_registration_without_removing_its_sibling() {
    let (parent, _, _events) = running().await;
    let mut first = parent.executed();
    let mut second = parent.executed();
    let (remaining, remaining_waker) = notification();
    let (removed, removed_waker) = notification();
    let weak = Arc::downgrade(&removed);
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&remaining_waker))
            .is_pending()
    );
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(&removed_waker))
            .is_pending()
    );
    drop(removed_waker);
    drop(removed);
    drop(second);
    assert!(
        weak.upgrade().is_none(),
        "dropped waiter must release its registered waker"
    );
    parent.cancel().unwrap();
    assert!(remaining.0.load(Ordering::SeqCst) > 0);
}

async fn subtasks_notified_case(cancelled: bool) {
    let (parent, gate, mut events) = running().await;
    let mut first = parent.run_until_executed(pending::<u32>());
    let mut second = parent.run_until_executed(pending::<u32>());
    let mut wait = parent.executed();
    let (a, wa) = notification();
    let (b, wb) = notification();
    let (observer, observer_waker) = notification();
    assert_eq!(parent.len(), 2);
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&wa))
            .is_pending()
    );
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(&wb))
            .is_pending()
    );
    assert!(
        Pin::new(&mut wait)
            .poll(&mut Context::from_waker(&observer_waker))
            .is_pending()
    );
    if cancelled {
        parent.cancel().unwrap();
    } else {
        gate.notify_one();
        assert!(matches!(events.next().await, Some(Ok(Event::Success(_)))));
    }
    assert!(
        a.0.load(Ordering::SeqCst) > 0,
        "the first pending subtask must receive its parent's notification"
    );
    assert!(
        b.0.load(Ordering::SeqCst) > 0,
        "the second pending subtask must receive its parent's notification"
    );
    assert!(observer.0.load(Ordering::SeqCst) > 0);
    assert_eq!(
        Pin::new(&mut first).poll(&mut Context::from_waker(&wa)),
        Poll::Ready(None)
    );
    assert_eq!(
        Pin::new(&mut second).poll(&mut Context::from_waker(&wb)),
        Poll::Ready(None)
    );
    assert!(
        Pin::new(&mut wait)
            .poll(&mut Context::from_waker(&observer_waker))
            .is_ready()
    );
    drop(first);
    assert_eq!(parent.len(), 1);
    drop(second);
    assert!(parent.is_empty());
}

#[tokio::test]
async fn completion_notifies_subtasks_and_executed_waiter() {
    subtasks_notified_case(false).await;
}

#[tokio::test]
async fn cancellation_notifies_subtasks_and_executed_waiter() {
    subtasks_notified_case(true).await;
}

#[tokio::test]
async fn dropping_subtask_releases_registration_and_preserves_sibling() {
    let (parent, _, _events) = running().await;
    let mut survivor = parent.run_until_executed(pending::<u32>());
    let mut dropped = parent.run_until_executed(pending::<u32>());
    let (remaining, remaining_waker) = notification();
    let (removed, removed_waker) = notification();
    let weak = Arc::downgrade(&removed);
    assert!(
        Pin::new(&mut survivor)
            .poll(&mut Context::from_waker(&remaining_waker))
            .is_pending()
    );
    assert!(
        Pin::new(&mut dropped)
            .poll(&mut Context::from_waker(&removed_waker))
            .is_pending()
    );
    drop(removed_waker);
    drop(removed);
    drop(dropped);
    assert_eq!(parent.len(), 1);
    assert!(weak.upgrade().is_none());
    parent.cancel().unwrap();
    assert!(
        remaining.0.load(Ordering::SeqCst) > 0,
        "dropping a subtask must preserve its sibling's notification"
    );
    assert_eq!(
        Pin::new(&mut survivor).poll(&mut Context::from_waker(&remaining_waker)),
        Poll::Ready(None)
    );
    drop(survivor);
    assert!(parent.is_empty());
}

#[tokio::test]
async fn ready_subtask_result_keeps_priority_over_parent_cancellation() {
    let (parent, _, _events) = running().await;
    let mut child = parent.run_until_executed(ready(42_u32));
    assert_eq!(parent.len(), 1);
    parent.cancel().unwrap();
    let (_, waker) = notification();
    assert_eq!(
        Pin::new(&mut child).poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Some(42))
    );
    drop(child);
    assert!(parent.is_empty());
}

async fn parent_finishes_during_subtask_poll_case(cancelled: bool) {
    let (parent, gate, mut events) = running().await;
    let mut child = parent.run_until_executed(poll_fn(|cx| {
        if cancelled {
            parent.cancel().unwrap();
        } else {
            gate.notify_one();
            assert!(matches!(
                events.poll_next_unpin(cx),
                Poll::Ready(Some(Ok(Event::Success(_))))
            ));
        }
        Poll::<u32>::Pending
    }));
    let (_, waker) = notification();
    assert_eq!(
        Pin::new(&mut child).poll(&mut Context::from_waker(&waker)),
        Poll::Ready(None),
        "a parent transition during the child poll must not be missed"
    );
    drop(child);
    assert!(parent.is_empty());
}

#[tokio::test]
async fn completion_during_subtask_first_poll_is_observed() {
    parent_finishes_during_subtask_poll_case(false).await;
}

#[tokio::test]
async fn cancellation_during_subtask_first_poll_is_observed() {
    parent_finishes_during_subtask_poll_case(true).await;
}
