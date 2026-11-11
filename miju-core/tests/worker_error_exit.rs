//! Fail-fast worker teardown and execution ownership regressions.

use std::{
    fmt,
    future::{Future, pending},
    io,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use futures_channel::oneshot;
use futures_util::Stream;
use miju_core::{
    backend::{Backend, BackendConfig, finalize::Ephemeral},
    error::{WorkerError, WorkerStateError},
    task::{Task, builder::TaskBuilder, status::Status, task_id::RandomId},
    worker::{
        builder::WorkerBuilder,
        context::WorkerContext,
        event::Event,
        ext::event_listener::{EventListenerExt, EventListenerLayer},
    },
};
use tower_layer::{Identity, Stack};

#[derive(Clone, Copy)]
enum AfterTask {
    Idle,
    ErrorOnReady,
    ErrorOnNext,
    Eof,
}

#[derive(Default)]
struct Counts {
    polls: AtomicUsize,
    drops: AtomicUsize,
    closes: AtomicUsize,
}

struct ProbeBackend {
    task: Option<Task<u32>>,
    after: AfterTask,
    failure: oneshot::Receiver<io::Error>,
    counts: Arc<Counts>,
}

impl Backend for ProbeBackend {
    type Task = Task<u32>;
    type Error = io::Error;

    fn poll_ready(&mut self, cx: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        if self.task.is_none() && matches!(self.after, AfterTask::ErrorOnReady) {
            return Pin::new(&mut self.failure)
                .poll(cx)
                .map(|error| Err(error.unwrap()));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<io::Result<Self::Task>>> {
        if let Some(task) = self.task.take() {
            return Poll::Ready(Some(Ok(task)));
        }
        match self.after {
            AfterTask::ErrorOnNext => Pin::new(&mut self.failure)
                .poll(cx)
                .map(|error| Some(Err(error.unwrap()))),
            AfterTask::Eof => Poll::Ready(None),
            AfterTask::Idle | AfterTask::ErrorOnReady => Poll::Pending,
        }
    }

    fn poll_close(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
        self.counts.closes.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

impl BackendConfig for ProbeBackend {
    type Id = RandomId;
    type Args = u32;
    type Kind = Ephemeral;
    type Config = ();
    type Layer = Identity;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}

struct Held {
    done: oneshot::Receiver<()>,
    counts: Arc<Counts>,
}

impl Future for Held {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.counts.polls.fetch_add(1, Ordering::SeqCst);
        Pin::new(&mut self.done)
            .poll(cx)
            .map(|result| result.unwrap())
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct Fixture {
    worker: WorkerContext,
    task: Task<u32>,
    backend: Option<ProbeBackend>,
    counts: Arc<Counts>,
    done_tx: Option<oneshot::Sender<()>>,
    done_rx: Option<oneshot::Receiver<()>>,
    fail_tx: Option<oneshot::Sender<io::Error>>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Fixture {
    fn new(after: AfterTask) -> Self {
        let task = TaskBuilder::new(7_u32).task_id(1_u64.into()).build();
        let counts = Arc::new(Counts::default());
        let (done_tx, done_rx) = oneshot::channel();
        let (fail_tx, fail_rx) = oneshot::channel();
        Self {
            worker: WorkerContext::new("worker-exit"),
            backend: Some(ProbeBackend {
                task: Some(task.clone()),
                after,
                failure: fail_rx,
                counts: counts.clone(),
            }),
            task,
            counts,
            done_tx: Some(done_tx),
            done_rx: Some(done_rx),
            fail_tx: Some(fail_tx),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn builder(&mut self) -> WorkerBuilder<u32, ProbeBackend, Stack<EventListenerLayer, Identity>> {
        let events = self.events.clone();
        WorkerBuilder::new(&self.worker)
            .backend(self.backend.take().unwrap())
            .on_event(move |_, event| {
                events.lock().unwrap().push(match event {
                    Event::Start => "start",
                    Event::Success(_) => "success",
                    Event::Stop => "stop",
                    Event::Exit => "exit",
                    Event::Error(_) => "error",
                    _ => "other",
                });
            })
    }

    fn handler(&mut self) -> impl Fn(u32) -> Held + Clone + Send + Sync + use<> {
        let receiver = Arc::new(Mutex::new(Some(self.done_rx.take().unwrap())));
        let counts = self.counts.clone();
        move |_: u32| Held {
            done: receiver.lock().unwrap().take().unwrap(),
            counts: counts.clone(),
        }
    }

    fn assert_held(&self) {
        assert!(self.counts.polls.load(Ordering::SeqCst) > 0);
        assert_eq!(self.counts.drops.load(Ordering::SeqCst), 0);
        assert_eq!(self.worker.task_count(), 1);
        assert_eq!(self.task.attempt(), 1);
        assert_eq!(self.task.status(), Status::Running);
    }

    fn assert_aborted(&self) {
        assert_eq!(self.counts.drops.load(Ordering::SeqCst), 1);
        assert_eq!(self.counts.closes.load(Ordering::SeqCst), 0);
        assert_eq!(self.worker.task_count(), 0);
        assert_eq!(self.task.attempt(), 1);
        assert_eq!(self.task.status(), Status::Killed);
        assert!(self.worker.is_terminated());
        assert!(!self.worker.is_ready());
        assert_eq!(*self.events.lock().unwrap(), ["start"]);
    }
}

struct Observations {
    worker: WorkerContext,
    counts: Arc<Counts>,
    snapshots: Mutex<Vec<(bool, usize, usize)>>,
}

struct ObserveWake(Weak<Observations>);

impl Wake for ObserveWake {
    fn wake(self: Arc<Self>) {
        if let Some(observation) = self.0.upgrade() {
            observation.snapshots.lock().unwrap().push((
                observation.worker.is_terminated(),
                observation.worker.task_count(),
                observation.counts.drops.load(Ordering::SeqCst),
            ));
        }
    }
}

fn observe(fixture: &Fixture) -> (Arc<Observations>, Waker) {
    let observations = Arc::new(Observations {
        worker: fixture.worker.clone(),
        counts: fixture.counts.clone(),
        snapshots: Mutex::new(Vec::new()),
    });
    let waker = Waker::from(Arc::new(ObserveWake(Arc::downgrade(&observations))));
    (observations, waker)
}

fn assert_final_wake(observations: &Observations) {
    let snapshots = observations.snapshots.lock().unwrap();
    let terminal: Vec<_> = snapshots.iter().filter(|snapshot| snapshot.0).collect();
    assert!(!terminal.is_empty());
    assert!(terminal.iter().all(|snapshot| **snapshot == (true, 0, 1)));
}

#[derive(Debug)]
struct IdentityError(Arc<()>);

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original failure")
    }
}

impl std::error::Error for IdentityError {}

fn error(identity: &Arc<()>) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        IdentityError(identity.clone()),
    )
}

fn assert_error(error: &io::Error, identity: &Arc<()>) {
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    let original = error
        .get_ref()
        .unwrap()
        .downcast_ref::<IdentityError>()
        .unwrap();
    assert!(Arc::ptr_eq(&original.0, identity));
}

#[test]
fn backend_errors_drop_handlers_before_finalizing_the_owner() {
    for after in [AfterTask::ErrorOnReady, AfterTask::ErrorOnNext] {
        let mut fixture = Fixture::new(after);
        let handler = fixture.handler();
        let mut run = Box::pin(fixture.builder().build(handler).run());
        let (observations, waker) = observe(&fixture);
        let mut cx = Context::from_waker(&waker);
        assert!(run.as_mut().poll(&mut cx).is_pending());
        fixture.assert_held();
        let identity = Arc::new(());
        fixture
            .fail_tx
            .take()
            .unwrap()
            .send(error(&identity))
            .unwrap();
        let Poll::Ready(Err(WorkerError::BackendError(failure))) = run.as_mut().poll(&mut cx)
        else {
            panic!("expected the original backend failure");
        };
        assert_error(failure.downcast_ref::<io::Error>().unwrap(), &identity);
        fixture.assert_aborted();
        assert_final_wake(&observations);
    }
}

#[test]
fn signal_and_context_errors_keep_their_identity_on_teardown() {
    for with_context in [false, true] {
        let mut fixture = Fixture::new(AfterTask::Idle);
        let handler = fixture.handler();
        let worker = fixture.builder().build(handler);
        let (signal_tx, signal_rx) = oneshot::channel::<io::Result<()>>();
        let mut run: Pin<Box<dyn Future<Output = Result<(), WorkerError>>>> = if with_context {
            let mut signal = Some(signal_rx);
            Box::pin(worker.run_with_ctx(move |_| {
                let signal = signal.take().unwrap();
                async move { signal.await.unwrap().map_err(WorkerError::from) }
            }))
        } else {
            Box::pin(worker.run_until(async move { signal_rx.await.unwrap() }))
        };
        let (observations, waker) = observe(&fixture);
        let mut cx = Context::from_waker(&waker);
        assert!(run.as_mut().poll(&mut cx).is_pending());
        fixture.assert_held();
        let identity = Arc::new(());
        signal_tx.send(Err(error(&identity))).unwrap();
        let Poll::Ready(Err(WorkerError::IoError(failure))) = run.as_mut().poll(&mut cx) else {
            panic!("expected the original signal/context failure");
        };
        assert_error(&failure, &identity);
        fixture.assert_aborted();
        assert_final_wake(&observations);
    }
}

#[test]
fn cancelling_each_runner_drops_owned_work_without_backend_close() {
    for runner in 0..3 {
        let mut fixture = Fixture::new(AfterTask::Idle);
        let handler = fixture.handler();
        let worker = fixture.builder().build(handler);
        let mut run: Pin<Box<dyn Future<Output = Result<(), WorkerError>>>> = match runner {
            0 => Box::pin(worker.run()),
            1 => Box::pin(worker.run_until(pending::<io::Result<()>>())),
            _ => Box::pin(worker.run_with_ctx(|_| pending())),
        };
        let (observations, waker) = observe(&fixture);
        assert!(
            run.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        fixture.assert_held();
        drop(run);
        fixture.assert_aborted();
        assert_final_wake(&observations);
    }
}

#[test]
fn unpolled_and_failed_start_streams_do_not_terminate_an_active_owner() {
    let mut owner = Fixture::new(AfterTask::Idle);
    let handler = owner.handler();
    let mut stream = Box::pin(owner.builder().build(handler).stream());
    let (observations, waker) = observe(&owner);
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        Poll::Ready(Some(Ok(Event::Start)))
    ));
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    owner.assert_held();

    for poll_start in [false, true] {
        let mut other = Fixture::new(AfterTask::Idle);
        other.worker = owner.worker.clone();
        let handler = other.handler();
        let mut rejected = Box::pin(other.builder().build(handler).stream());
        if poll_start {
            assert!(matches!(
                rejected.as_mut().poll_next(&mut cx),
                Poll::Ready(Some(Err(WorkerError::StateError(
                    WorkerStateError::AlreadyStarted
                ))))
            ));
        }
        drop(rejected);
        assert!(owner.worker.is_running());
        owner.assert_held();
        assert_eq!(other.counts.polls.load(Ordering::SeqCst), 0);
        assert_eq!(other.counts.closes.load(Ordering::SeqCst), 0);
    }
    drop(stream);
    owner.assert_aborted();
    assert_final_wake(&observations);
}

#[test]
fn dropping_an_unpolled_worker_keeps_its_context_pending() {
    let mut fixture = Fixture::new(AfterTask::Idle);
    let handler = fixture.handler();
    drop(fixture.builder().build(handler).stream());
    assert!(fixture.worker.is_pending());
    assert_eq!(fixture.worker.task_count(), 0);
    assert_eq!(fixture.counts.polls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.counts.drops.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.counts.closes.load(Ordering::SeqCst), 0);
    assert!(fixture.events.lock().unwrap().is_empty());
}

#[test]
fn a_yielded_backend_error_does_not_end_a_retained_stream() {
    let mut fixture = Fixture::new(AfterTask::ErrorOnNext);
    let handler = fixture.handler();
    let mut stream = Box::pin(fixture.builder().build(handler).stream());
    let (observations, waker) = observe(&fixture);
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        Poll::Ready(Some(Ok(Event::Start)))
    ));
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    fixture.assert_held();
    let identity = Arc::new(());
    fixture
        .fail_tx
        .take()
        .unwrap()
        .send(error(&identity))
        .unwrap();
    let Poll::Ready(Some(Err(WorkerError::BackendError(failure)))) =
        stream.as_mut().poll_next(&mut cx)
    else {
        panic!("expected a yielded backend error");
    };
    assert_error(failure.downcast_ref::<io::Error>().unwrap(), &identity);
    assert!(fixture.worker.is_running());
    fixture.assert_held();
    drop(stream);
    fixture.assert_aborted();
    assert_final_wake(&observations);
}

#[test]
fn successful_shutdown_closes_once_and_drop_adds_no_events_or_wakes() {
    let mut fixture = Fixture::new(AfterTask::Eof);
    let handler = fixture.handler();
    let mut stream = Box::pin(fixture.builder().build(handler).stream());
    let (observations, waker) = observe(&fixture);
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        Poll::Ready(Some(Ok(Event::Start)))
    ));
    assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    fixture.assert_held();
    fixture.done_tx.take().unwrap().send(()).unwrap();
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        Poll::Ready(Some(Ok(Event::Success(_))))
    ));
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        Poll::Ready(Some(Ok(Event::Exit)))
    ));
    assert!(fixture.worker.is_terminated());
    assert_eq!(fixture.worker.task_count(), 0);
    assert_eq!(fixture.task.status(), Status::Done);
    assert_eq!(fixture.task.attempt(), 1);
    assert_eq!(fixture.counts.closes.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.counts.drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        *fixture.events.lock().unwrap(),
        ["start", "success", "stop", "exit"]
    );
    let wakes = observations.snapshots.lock().unwrap().len();
    drop(stream);
    assert_eq!(observations.snapshots.lock().unwrap().len(), wakes);
    assert_eq!(
        *fixture.events.lock().unwrap(),
        ["start", "success", "stop", "exit"]
    );
}
