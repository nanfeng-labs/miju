//! Repeated stop/kill preserve terminal state and cancel every tracked task.

use futures_util::{FutureExt, StreamExt};
#[cfg(feature = "task-context")]
use miju_core::task::{builder::TaskBuilder, context::TaskContext};
use miju_core::{
    backend::{TaskSink, memory::MemoryStorage},
    error::{WorkerError, WorkerStateError},
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt,
    },
};
use std::{
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[test]
fn kill_preserves_not_shutting_down_guard() {
    let mut context = WorkerContext::new("guard");
    context.start().unwrap();
    assert!(matches!(
        context.kill(),
        Err(WorkerError::StateError(WorkerStateError::InvalidState(_)))
    ));
    assert!(context.is_running());
    context.stop().unwrap();
    context.kill().unwrap();
    assert!(context.is_terminated());
    println!("guard: kill before stop rejected, empty stopped worker kill succeeds");
}

#[cfg(feature = "task-context")]
#[test]
fn kill_continues_after_an_already_cancelled_task() {
    let mut storage = MemoryStorage::new();
    for id in ["one", "two", "three"] {
        storage
            .push_task(
                TaskBuilder::new(1_u32)
                    .task_id(id.to_owned().into())
                    .build(),
            )
            .now_or_never()
            .unwrap()
            .unwrap();
    }
    let mut context = WorkerContext::new("kill-after-cancel");
    let mut stream = WorkerBuilder::new(&context)
        .backend(storage)
        .build(|_: u32, _: TaskContext| pending::<()>())
        .stream();
    assert!(matches!(
        stream.next().now_or_never(),
        Some(Some(Ok(Event::Start)))
    ));
    assert!(stream.next().now_or_never().is_none());
    assert_eq!(context.task_count(), 3);
    context.stop().unwrap();
    let tokens = context.tasks().unwrap();
    context.cancel_task(&tokens[0]).unwrap();
    assert_eq!(tokens.iter().filter(|t| t.is_cancelled()).count(), 1);
    let result = context.kill();
    let after = tokens.iter().filter(|t| t.is_cancelled()).count();
    println!(
        "kill-after-cancel: result={result:?}, cancelled={after}/3, terminated={}",
        context.is_terminated()
    );
    assert!(
        result.is_ok(),
        "kill must finish cancellation even if one token was already cancelled"
    );
    assert_eq!(after, 3, "all task tokens must receive cancellation");
    assert!(context.is_terminated());
    context.kill().unwrap();
    drop(stream);
    assert_eq!(context.task_count(), 0);
}

#[cfg(feature = "task-context")]
#[test]
fn kill_twice_before_repoll_is_idempotent() {
    let mut storage = MemoryStorage::new();
    storage.push(1_u32).now_or_never().unwrap().unwrap();
    let mut context = WorkerContext::new("kill-twice");
    let mut stream = WorkerBuilder::new(&context)
        .backend(storage)
        .build(|_: u32| pending::<()>())
        .stream();
    assert!(matches!(
        stream.next().now_or_never(),
        Some(Some(Ok(Event::Start)))
    ));
    assert!(stream.next().now_or_never().is_none());
    context.stop().unwrap();
    context.kill().unwrap();
    assert!(context.is_terminated());
    let second = context.kill();
    println!(
        "kill-twice: second={second:?}, tracked={}",
        context.task_count()
    );
    assert!(
        second.is_ok(),
        "second kill should accept an already-cancelled tracked task"
    );
    assert!(context.is_terminated());
    drop(stream);
    assert_eq!(context.task_count(), 0);
}

#[test]
fn stop_after_finished_run_preserves_termination_and_emits_once() {
    let mut storage = MemoryStorage::new();
    storage.push(1_u32).now_or_never().unwrap().unwrap();
    let context = WorkerContext::new("stop-after-return");
    let stops = Arc::new(AtomicUsize::new(0));
    let counted = stops.clone();
    let actual = Arc::new(Mutex::new(None::<WorkerContext>));
    let capture = actual.clone();
    let worker = WorkerBuilder::new(&context)
        .backend(storage)
        .on_event(move |worker, event| match event {
            Event::Start => {
                *capture.lock().unwrap() = Some(worker.clone());
            }
            Event::Success(_) => worker.stop().unwrap(),
            Event::Stop => {
                counted.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        })
        .build(|_: u32| async {});
    worker
        .run()
        .now_or_never()
        .expect("immediate task and requested shutdown should finish")
        .unwrap();
    assert!(context.is_terminated());
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    let actual_context = actual.lock().unwrap().take().unwrap();
    actual_context.stop().unwrap();
    println!(
        "late-stop: terminated={}, state={}, Stop events={}",
        context.is_terminated(),
        context.state(),
        stops.load(Ordering::SeqCst)
    );
    assert!(
        context.is_terminated(),
        "late stop must preserve a terminated state"
    );
    assert_eq!(
        stops.load(Ordering::SeqCst),
        1,
        "late stop must not re-emit Stop"
    );
}

#[test]
fn repeated_stop_during_drain_emits_once() {
    let mut storage = MemoryStorage::new();
    storage.push(1_u32).now_or_never().unwrap().unwrap();
    let context = WorkerContext::new("stop-twice");
    let stops = Arc::new(AtomicUsize::new(0));
    let counted = stops.clone();
    let actual = Arc::new(Mutex::new(None::<WorkerContext>));
    let capture = actual.clone();
    let mut stream = WorkerBuilder::new(&context)
        .backend(storage)
        .on_event(move |worker, event| match event {
            Event::Start => {
                *capture.lock().unwrap() = Some(worker.clone());
            }
            Event::Stop => {
                counted.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        })
        .build(|_: u32| pending::<()>())
        .stream();
    assert!(matches!(
        stream.next().now_or_never(),
        Some(Some(Ok(Event::Start)))
    ));
    assert!(stream.next().now_or_never().is_none());
    let actual_context = actual.lock().unwrap().take().unwrap();
    actual_context.stop().unwrap();
    actual_context.stop().unwrap();
    assert!(context.is_stopped());
    println!(
        "repeated-stop: Stop events={}",
        stops.load(Ordering::SeqCst)
    );
    assert_eq!(
        stops.load(Ordering::SeqCst),
        1,
        "stop must be idempotent while draining"
    );
}

#[test]
fn concurrent_stop_requests_emit_one_stop_event() {
    let context = WorkerContext::new("concurrent-stop");
    let stops = Arc::new(AtomicUsize::new(0));
    let counted = stops.clone();
    let actual = Arc::new(Mutex::new(None::<WorkerContext>));
    let capture = actual.clone();
    let mut stream = WorkerBuilder::new(&context)
        .backend(MemoryStorage::<u32>::new())
        .on_event(move |worker, event| match event {
            Event::Start => *capture.lock().unwrap() = Some(worker.clone()),
            Event::Stop => {
                counted.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        })
        .build(|_: u32| pending::<()>())
        .stream();
    assert!(matches!(
        stream.next().now_or_never(),
        Some(Some(Ok(Event::Start)))
    ));
    let actual = actual.lock().unwrap().take().unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let worker = actual.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                worker.stop().unwrap();
            });
        }
    });
    assert!(context.is_stopped());
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[test]
fn pause_resume_stop_guards_and_valid_transitions() {
    let mut context = WorkerContext::new("state-controls");
    assert!(matches!(
        context.stop(),
        Err(WorkerError::StateError(WorkerStateError::NotStarted))
    ));
    context.start().unwrap();
    context.pause().unwrap();
    assert!(context.is_paused());
    context.resume().unwrap();
    assert!(context.is_running());
    context.stop().unwrap();
    assert!(context.pause().is_err());
    assert!(context.resume().is_err());
    context.kill().unwrap();
    assert!(context.is_terminated());
    println!("sequential valid transitions and invalid-state controls pass");
}
