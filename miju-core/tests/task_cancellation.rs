//! Regression tests for cancellation notifications.
#![cfg(feature = "task-context")]

use futures_util::Stream;
use miju_core::{
    backend::{TaskSink, memory::MemoryStorage},
    task::context::{TaskContext, WaitForExecutionFuture},
    worker::{
        builder::WorkerBuilder,
        context::WorkerContext,
        event::Event,
        ext::long_running::{LongRunningExt, TaskRunner, future::LongRunningError},
    },
};
use std::{
    future::{Future, pending},
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Observation {
    polls: AtomicUsize,
    drops: AtomicUsize,
    cancel_wakes: AtomicUsize,
    waker: Mutex<Option<Waker>>,
}
struct Dormant<T> {
    seen: Arc<Observation>,
    wait: Option<WaitForExecutionFuture>,
    cancel: Option<TaskContext>,
    notify: Option<Arc<CountWake>>,
    output: PhantomData<fn() -> T>,
}
impl<T> Dormant<T> {
    fn new(seen: Arc<Observation>, wait: Option<WaitForExecutionFuture>) -> Self {
        Self {
            seen,
            wait,
            cancel: None,
            notify: None,
            output: PhantomData,
        }
    }
}
impl<T> Future for Dormant<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        this.seen.polls.fetch_add(1, Ordering::SeqCst);
        *this.seen.waker.lock().unwrap() = Some(cx.waker().clone());
        if let Some(wait) = &mut this.wait {
            let _ = Pin::new(wait).poll(cx);
        }
        if let Some(cancel) = this.cancel.take() {
            let before = this.notify.as_ref().unwrap().0.load(Ordering::SeqCst);
            cancel.cancel().unwrap();
            this.seen.cancel_wakes.store(
                this.notify.as_ref().unwrap().0.load(Ordering::SeqCst) - before,
                Ordering::SeqCst,
            );
        }
        Poll::Pending
    }
}
impl<T> Drop for Dormant<T> {
    fn drop(&mut self) {
        self.seen.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Default)]
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

enum MainCancellation {
    Direct,
    Stop,
    Cooperative,
    DuringPoll,
}

async fn main_case(case: MainCancellation) -> bool {
    let cooperative = matches!(case, MainCancellation::Cooperative);
    let stop = matches!(case, MainCancellation::Stop);
    let cancel_inside = matches!(case, MainCancellation::DuringPoll);
    let direct = matches!(
        case,
        MainCancellation::Direct | MainCancellation::DuringPoll
    );
    let mut backend = MemoryStorage::new();
    backend.push(7_u32).await.unwrap();
    let worker_ctx = WorkerContext::new("main-cancel");
    let wakes = Arc::new(CountWake::default());
    let handler_wakes = wakes.clone();
    let seen = Arc::new(Observation::default());
    let handler_seen = seen.clone();
    let worker =
        WorkerBuilder::new(&worker_ctx)
            .backend(backend)
            .build(move |_: u32, task: TaskContext| {
                let mut future =
                    Dormant::<()>::new(handler_seen.clone(), cooperative.then(|| task.executed()));
                future.cancel = cancel_inside.then_some(task);
                future.notify = Some(handler_wakes.clone());
                future
            });
    let mut stream = Box::pin(worker.stream());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        Poll::Ready(Some(Ok(Event::Start)))
    ));
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    assert_eq!(seen.polls.load(Ordering::SeqCst), 1);
    let task = worker_ctx.tasks().unwrap().pop().unwrap();
    let before = wakes.0.load(Ordering::SeqCst);
    if !cancel_inside {
        if direct {
            task.cancel().unwrap();
        } else {
            worker_ctx.cancel_task(&task).unwrap();
        }
    }
    let notified = if cancel_inside {
        seen.cancel_wakes.load(Ordering::SeqCst) > 0
    } else {
        wakes.0.load(Ordering::SeqCst) > before
    };
    assert!(task.is_cancelled());
    if stop {
        worker_ctx.stop().unwrap();
    }
    let first = stream.as_mut().poll_next(&mut cx);
    let ready_without_rescue = first.is_ready();
    if first.is_pending() {
        assert_eq!(worker_ctx.task_count(), 1);
        seen.waker.lock().unwrap().as_ref().unwrap().wake_by_ref();
        assert!(matches!(
            stream.as_mut().poll_next(&mut cx),
            Poll::Ready(Some(Ok(Event::Error(_))))
        ));
    } else {
        assert!(matches!(first, Poll::Ready(Some(Ok(Event::Error(_))))));
    }
    assert_eq!(worker_ctx.task_count(), 0);
    assert_eq!(
        seen.polls.load(Ordering::SeqCst),
        1,
        "cancellation exits in wrapper before polling dormant handler again"
    );
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    println!(
        "main cooperative={cooperative} stop={stop} cancel_inside={cancel_inside} direct={direct}: notified={notified}, ready_without_rescue={ready_without_rescue}, handler_polls=1, removed=true"
    );
    notified && ready_without_rescue
}

#[tokio::test]
async fn main_cancel_supplies_its_own_notification() {
    assert!(
        main_case(MainCancellation::Direct).await,
        "main cancellation needs its own wake, not a later inner I/O notification"
    );
}
#[tokio::test]
async fn stop_does_not_replace_the_cancelled_child_notification() {
    assert!(
        main_case(MainCancellation::Stop).await,
        "outer stop wake must not be the only cancellation notification"
    );
}
#[tokio::test]
async fn cooperative_main_listener_is_a_working_control() {
    assert!(main_case(MainCancellation::Cooperative).await);
}
#[tokio::test]
async fn cancellation_during_inner_poll_is_not_lost() {
    assert!(
        main_case(MainCancellation::DuringPoll).await,
        "cancel during inner poll must leave the wrapper scheduled"
    );
}

async fn runner_case(cooperative: bool, cancel_before_first_poll: bool, count: usize) -> bool {
    let captured = Arc::new(Mutex::new(None::<(TaskRunner<u32>, TaskContext)>));
    let saved = captured.clone();
    let mut backend = MemoryStorage::new();
    backend.push(7_u32).await.unwrap();
    let worker_ctx = WorkerContext::new("runner-cancel");
    let worker = WorkerBuilder::new(&worker_ctx)
        .backend(backend)
        .long_running()
        .build(
            move |_: u32, runner: TaskRunner<u32>, context: TaskContext| {
                *saved.lock().unwrap() = Some((runner, context));
                pending::<()>()
            },
        );
    let mut worker_stream = Box::pin(worker.stream());
    let mut worker_cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        worker_stream.as_mut().poll_next(&mut worker_cx),
        Poll::Ready(Some(Ok(Event::Start)))
    ));
    assert!(
        worker_stream
            .as_mut()
            .poll_next(&mut worker_cx)
            .is_pending()
    );
    let (mut runner, context) = captured.lock().unwrap().take().unwrap();
    let mut observations = Vec::new();
    for _ in 0..count {
        let seen = Arc::new(Observation::default());
        runner.execute(Dormant::<Result<u32, std::io::Error>>::new(
            seen.clone(),
            cooperative.then(|| context.executed()),
        ));
        observations.push(seen);
    }
    let wakes = Arc::new(CountWake::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut runner = Box::pin(runner);
    if cancel_before_first_poll {
        context.cancel().unwrap();
    } else {
        assert!(runner.as_mut().poll_next(&mut cx).is_pending());
        for seen in &observations {
            assert_eq!(seen.polls.load(Ordering::SeqCst), 1);
        }
    }
    let before = wakes.0.load(Ordering::SeqCst);
    if !cancel_before_first_poll {
        context.cancel().unwrap();
    }
    let notified = cancel_before_first_poll || wakes.0.load(Ordering::SeqCst) > before;
    let mut immediate = 0;
    let mut total = 0;
    for _ in 0..count {
        match runner.as_mut().poll_next(&mut cx) {
            Poll::Ready(Some(Err(LongRunningError::Cancelled))) => {
                immediate += 1;
                total += 1;
            }
            Poll::Pending => break,
            other @ Poll::Ready(_) => panic!("unexpected runner result: {other:?}"),
        }
    }
    for seen in &observations {
        if let Some(w) = seen.waker.lock().unwrap().as_ref() {
            w.wake_by_ref();
        }
    }
    while total < count {
        assert!(matches!(
            runner.as_mut().poll_next(&mut cx),
            Poll::Ready(Some(Err(LongRunningError::Cancelled)))
        ));
        total += 1;
    }
    assert!(matches!(
        runner.as_mut().poll_next(&mut cx),
        Poll::Ready(None)
    ));
    for seen in &observations {
        assert_eq!(
            seen.polls.load(Ordering::SeqCst),
            usize::from(!cancel_before_first_poll)
        );
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
    println!(
        "runner cooperative={cooperative} before_first={cancel_before_first_poll} count={count}: notified={notified}, immediate_cancelled={immediate}, total_after_wake={total}"
    );
    notified && immediate == count
}
#[tokio::test]
async fn long_running_cancel_supplies_its_own_notification() {
    assert!(
        runner_case(false, false, 1).await,
        "LongRunningFuture must arrange notification for its own Cancelled result"
    );
}
#[tokio::test]
async fn multiple_long_running_operations_need_independent_notifications() {
    assert!(runner_case(false, false, 2).await);
}
#[tokio::test]
async fn cooperative_long_running_listener_is_a_working_control() {
    assert!(runner_case(true, false, 1).await);
}
#[tokio::test]
async fn cancellation_before_first_long_running_poll_already_works() {
    assert!(runner_case(false, true, 2).await);
}
