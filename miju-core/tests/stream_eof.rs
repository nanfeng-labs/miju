//! Exhausted inputs must not prevent later destination or backend work.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use futures_util::{SinkExt, stream};
use miju_core::{
    backend::{
        Backend,
        dequeue::VecDequeBackend,
        ext::{
            BackendExt,
            pipe::{Pipe, PipeExt},
            poll_strategy::{PollSnapshot, PollStrategy, Strategy, StreamStrategy},
        },
        memory::MemoryStorage,
    },
    task::Task,
    worker::context::WorkerContext,
};

#[derive(Default)]
struct Notifications(AtomicUsize);

impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn pipe_does_not_repoll_exhausted_stream_when_destination_is_pending() {
    let polls = Arc::new(AtomicUsize::new(0));
    let source_polls = polls.clone();
    let mut value = Some(42u32);
    let mut ended = false;
    let source = stream::poll_fn(move |_| {
        assert!(!ended, "source stream polled after EOF");
        source_polls.fetch_add(1, Ordering::SeqCst);
        if let Some(value) = value.take() {
            Poll::Ready(Some(Ok::<_, io::Error>(value)))
        } else {
            ended = true;
            Poll::Ready(None)
        }
    });
    let mut producer = VecDequeBackend::new();
    let mut pipe = source.pipe_to(producer.clone());
    let worker = WorkerContext::new("pipe-eof");
    let notifications = Arc::new(Notifications::default());
    let waker = Waker::from(notifications.clone());
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(
        Backend::poll_ready(&mut pipe, &mut cx, &worker),
        Poll::Ready(Ok(()))
    ));
    let Poll::Ready(Some(Ok(task))) = pipe.poll_next(&mut cx, &worker) else {
        panic!("missing piped task");
    };
    assert_eq!(task.args, 42);
    for _ in 0..2 {
        assert!(matches!(
            Backend::poll_ready(&mut pipe, &mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        assert!(pipe.poll_next(&mut cx, &worker).is_pending());
    }
    assert_eq!(polls.load(Ordering::SeqCst), 2);

    notifications.0.store(0, Ordering::SeqCst);
    assert!(matches!(
        producer.poll_ready_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    producer.start_send_unpin(Task::new(99)).unwrap();
    assert!(matches!(
        producer.poll_flush_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(notifications.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        Backend::poll_ready(&mut pipe, &mut cx, &worker),
        Poll::Ready(Ok(()))
    ));
    let Poll::Ready(Some(Ok(task))) = pipe.poll_next(&mut cx, &worker) else {
        panic!("source EOF stopped destination consumption");
    };
    assert_eq!(task.args, 99);
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[test]
fn poll_with_keeps_polling_backend_after_notification_stream_exhaustion() {
    let polls = Arc::new(AtomicUsize::new(0));
    let stream_polls = polls.clone();
    let mut ended = false;
    let notifications_stream = stream::poll_fn(move |_| {
        assert!(!ended, "notification stream polled after EOF");
        stream_polls.fetch_add(1, Ordering::SeqCst);
        ended = true;
        Poll::Ready(None::<()>)
    });
    let mut backend = MemoryStorage::<u32>::new().poll_with_stream(notifications_stream);
    let worker = WorkerContext::new("poll-with-eof");
    let notifications = Arc::new(Notifications::default());
    let waker = Waker::from(notifications.clone());
    let mut cx = Context::from_waker(&waker);

    for _ in 0..2 {
        assert!(matches!(
            Backend::poll_ready(&mut backend, &mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        assert!(backend.poll_next(&mut cx, &worker).is_pending());
    }
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    notifications.0.store(0, Ordering::SeqCst);
    assert!(matches!(
        backend.poll_ready_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    backend.start_send_unpin(Task::new(42)).unwrap();
    assert!(matches!(
        backend.poll_flush_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(notifications.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        Backend::poll_ready(&mut backend, &mut cx, &worker),
        Poll::Ready(Ok(()))
    ));
    let Poll::Ready(Some(Ok(task))) = backend.poll_next(&mut cx, &worker) else {
        panic!("notification EOF stopped backend consumption");
    };
    assert_eq!(task.args, 42);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Default)]
struct ExhaustsOnce {
    ended: bool,
}

impl Backend for ExhaustsOnce {
    type Task = Task<u32>;
    type Error = io::Error;

    fn poll_ready(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        assert!(!self.ended, "source readiness polled after EOF");
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<io::Result<Self::Task>>> {
        assert!(!self.ended, "source backend polled after EOF");
        self.ended = true;
        Poll::Ready(None)
    }

    fn poll_close(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn pipe_clone_preserves_source_exhaustion() {
    let mut pipe = Pipe::new(ExhaustsOnce::default(), VecDequeBackend::new());
    let worker = WorkerContext::new("pipe-clone-eof");
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Backend::poll_ready(&mut pipe, &mut cx, &worker),
        Poll::Ready(Ok(()))
    ));
    assert!(pipe.poll_next(&mut cx, &worker).is_pending());
    let mut cloned = pipe.clone();
    for backend in [&mut pipe, &mut cloned] {
        assert!(matches!(
            Backend::poll_ready(backend, &mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        assert!(backend.poll_next(&mut cx, &worker).is_pending());
    }
}

#[test]
fn stream_strategy_clones_preserve_their_exhaustion_state() {
    let mut ended = false;
    let source = stream::iter(std::iter::from_fn(move || {
        assert!(!ended, "cloned stream polled after EOF");
        ended = true;
        None::<()>
    }));
    let mut strategy = StreamStrategy::new(source);
    let mut before_eof = strategy.clone();
    let snapshot = PollSnapshot::new();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(strategy.poll_drive(&mut cx, &snapshot), Poll::Ready(None));
    let mut after_eof = strategy.clone();
    for strategy in [&mut strategy, &mut before_eof, &mut after_eof] {
        for _ in 0..2 {
            assert_eq!(strategy.poll_drive(&mut cx, &snapshot), Poll::Ready(None));
        }
    }
}

#[test]
fn exhausted_stream_strategy_keeps_first_child_priority() {
    let mut ended = false;
    let source = stream::poll_fn(move |_| {
        assert!(!ended, "combined stream polled after EOF");
        ended = true;
        Poll::Ready(None::<()>)
    });
    let later_polls = Arc::new(AtomicUsize::new(0));
    let calls = later_polls.clone();
    let later = stream::poll_fn(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Some(()))
    });
    let mut strategy = Strategy::new().stream(source).stream(later);
    let snapshot = PollSnapshot::new();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..2 {
        assert_eq!(
            strategy.poll_drive(&mut cx, &snapshot),
            Poll::Ready(Some(()))
        );
    }
    assert_eq!(later_polls.load(Ordering::SeqCst), 0);
}
