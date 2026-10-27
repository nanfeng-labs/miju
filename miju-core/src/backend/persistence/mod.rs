//! Persistence providers, polling state, and task lifecycle events.
//!
//! [`Persistence`](crate::backend::persistence::Persistence) supplies storage operations.
//! [`Persisted`](crate::backend::persistence::Persisted) drives registration,
//! heartbeats, event processing, and task batches, and implements a submission
//! sink. A storage integration connects its polling methods to
//! [`Backend`](crate::backend::Backend) and uses
//! [`Persisted::shared_layer`](crate::backend::persistence::Persisted::shared_layer) for
//! backend middleware that can be initialized by multiple workers. The driver
//! itself does not implement `Backend`.
//!
//! Providers are responsible for atomic task claims and storage guarantees.
//! Cloning a driver clones the provider and resets all local polling, event,
//! and submission state; whether providers share storage depends on their
//! own `Clone` implementation.

use std::{
    collections::VecDeque,
    fmt::Debug,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_channel::mpsc::{self, Receiver, Sender};
use futures_timer::Delay;
use futures_util::FutureExt;

pub use crate::backend::persistence::{event::TaskEvent, service::TaskPersistLayer, state::State};
use crate::{backend::future::BoxSyncFuture, task::Task, worker::context::WorkerContext};

mod event;
mod service;
mod sink;
mod state;

/// Persistence layer for registering workers, maintaining their liveness,
/// fetching work, recovering abandoned tasks, and handling task events.
pub trait Persistence: Clone + Send + 'static {
    /// The compact type the upstream backend will use
    type Compact;

    /// The response type to be stored in the db
    type Response;

    /// Error returned by persistence operations.
    type Error: std::error::Error;

    /// Registers the worker with the persistence backend.
    fn register(
        &mut self,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Records a heartbeat for the worker.
    fn heartbeat(
        &mut self,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Fetches the next batch of tasks available for processing.
    fn fetch_next(
        &mut self,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<Vec<Task<Self::Compact>>, Self::Error>> + Send;

    /// Re-enqueues tasks that were abandoned by a worker.
    fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<Task<Self::Compact>>,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Handles task lifecycle events produced by the worker.
    fn handle_events(
        &mut self,
        events: Vec<TaskEvent<Self::Response>>,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Handle tasks being pushed
    fn push_tasks(
        &mut self,
        tasks: Vec<Task<Self::Compact>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A driver that advances a [`Persistence`] instance and emits its tasks.
///
/// Storage integrations delegate backend polling to this driver and use its
/// event layer to connect task execution to persistence.
#[derive(Debug)]
pub struct Persisted<P>
where
    P: Persistence,
{
    inner: P,
    state: State<Task<P::Compact>, P::Error>,
    heartbeat_timer: Option<Delay>,
    receiver: Option<Receiver<TaskEvent<P::Response>>>,
    shared_sender: Option<Sender<TaskEvent<P::Response>>>,
    sink_buffer: Vec<Task<P::Compact>>,
    sink_future: Option<BoxSyncFuture<Result<(), P::Error>>>,
}

impl<P: Clone> Clone for Persisted<P>
where
    P: Persistence,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            state: State::NotBootstrapped,
            heartbeat_timer: None,
            receiver: None,
            shared_sender: None,
            sink_buffer: Vec::new(),
            sink_future: None,
        }
    }
}

impl<P> Deref for Persisted<P>
where
    P: Persistence,
{
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<P> DerefMut for Persisted<P>
where
    P: Persistence,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

macro_rules! poll_fut_or_return {
    ($fut:expr, $cx:expr, $state:expr) => {
        match $fut.poll_unpin($cx) {
            Poll::Ready(Ok(v)) => v,
            Poll::Ready(Err(e)) => {
                $state = State::Dead;
                return Poll::Ready(Err(e));
            }
            Poll::Pending => return Poll::Pending,
        }
    };
}

impl<P> Persisted<P>
where
    P: Persistence + Clone,
    P::Compact: Send + 'static,
    P::Response: Send,
{
    /// Creates a new `Persisted` in the `NotBootstrapped` state.
    ///
    /// The worker will register itself with the backend on the first call
    /// to [`poll_ready`](Self::poll_ready).
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            heartbeat_timer: None,
            receiver: None,
            shared_sender: None,
            state: State::NotBootstrapped,
            sink_buffer: Vec::new(),
            sink_future: None,
        }
    }

    /// Get the current state
    pub fn state(&self) -> &State<Task<P::Compact>, P::Error> {
        &self.state
    }

    /// Gets mutable access to the polling state.
    ///
    /// The caller must preserve the state machine invariants, including an
    /// initialized heartbeat timer before driving the `Ready` state.
    pub fn state_mut(&mut self) -> &mut State<Task<P::Compact>, P::Error> {
        &mut self.state
    }

    /// Drains any pending events from the receiver.
    ///
    /// Returns `None` if there's nothing to process.
    fn try_drain_events(&mut self, _: &mut Context<'_>) -> Option<Vec<TaskEvent<P::Response>>> {
        let receiver = self.receiver.as_mut()?;
        let mut events = Vec::new();
        let mut _polls = 0u32;

        while let Ok(event) = receiver.try_recv() {
            _polls += 1;
            events.push(event);
        }

        trace!(
            drained = events.len(),
            polls = _polls,
            "try_drain_events: receiver drain complete"
        );

        if events.is_empty() {
            None
        } else {
            debug!(
                count = events.len(),
                "try_drain_events: buffered events ready"
            );
            Some(events)
        }
    }

    /// Builds the task event layer and installs its receiver.
    ///
    /// The channel capacity is `batch_size * 4`. Calling this again replaces
    /// the receiver and disconnects services built from the previous layer.
    /// This also replaces a channel installed by [`shared_layer`](Self::shared_layer).
    /// Use `shared_layer` for repeated backend middleware initialization.
    /// Result persistence and task locking are disabled until explicitly enabled.
    pub fn layer<C>(&mut self, codec: C, batch_size: usize) -> TaskPersistLayer<C, P::Response> {
        // The service currently emits only Lock and Complete.
        let buffer = batch_size * 4;
        let (tx, rx) = mpsc::channel(buffer);
        self.receiver = Some(rx);
        self.shared_sender = None;
        TaskPersistLayer::new(tx, codec)
    }

    /// Builds a task event layer that shares this driver's event channel.
    ///
    /// Use this in backend middleware that may be initialized by multiple workers.
    /// The first call installs a channel with buffer size `batch_size * 4`,
    /// replacing any previous [`layer`](Self::layer). Later calls reuse the
    /// channel and its buffer size, even after all returned layers are dropped.
    /// Each call uses its own codec, with result persistence and locking disabled.
    ///
    /// Calling `layer` explicitly replaces the shared channel and disconnects
    /// its services. Cloning the driver starts with no event channel.
    pub fn shared_layer<C>(
        &mut self,
        codec: C,
        batch_size: usize,
    ) -> TaskPersistLayer<C, P::Response> {
        let sender = self.shared_sender.get_or_insert_with(|| {
            let (sender, receiver) = mpsc::channel(batch_size * 4);
            self.receiver = Some(receiver);
            sender
        });
        TaskPersistLayer::new(sender.clone(), codec)
    }

    /// Drives the provider's internal state machine (bootstrap, heartbeat,
    /// event processing) until it is genuinely ready for [`poll_next`](Self::poll_next)
    /// to be called, or until it must return `Pending`.
    pub fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
        heartbeat_interval: Duration,
    ) -> Poll<Result<(), P::Error>> {
        trace!("poll_ready: {:?}", self.state);
        loop {
            match &mut self.state {
                State::NotBootstrapped => {
                    debug!("poll_ready: NotBootstrapped -> registering worker");
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();
                    let fut = async move {
                        provider.register(&worker).await?;
                        Ok(())
                    };
                    self.state = State::RegisterWorker(fut.boxed().into());
                }

                State::RegisterWorker(fut) | State::HeartBeat(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => {
                        warn!(error = ?e, "poll_ready: HeartBeat failed -> Dead");
                        self.state = State::Dead;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(())) => {
                        debug!(
                            ?heartbeat_interval,
                            "poll_ready: HeartBeat complete, arming heartbeat timer"
                        );
                        worker.heartbeat(cx);
                        self.heartbeat_timer = Some(Delay::new(heartbeat_interval));
                        self.state = State::Ready;
                    }
                },

                State::ProcessEvents(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => {
                        warn!(error = ?e, "poll_ready: ProcessEvents failed -> Dead");
                        self.state = State::Dead;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(())) => {
                        trace!("poll_ready: ProcessEvents complete -> Ready");
                        self.state = State::Ready;
                    }
                },

                State::Ready => {
                    let heartbeat_due = Pin::new(self.heartbeat_timer.as_mut().unwrap())
                        .poll(cx)
                        .is_ready();

                    trace!(
                        heartbeat_due,
                        ?heartbeat_interval,
                        "poll_ready: State::Ready evaluating heartbeat timer"
                    );

                    if heartbeat_due {
                        debug!("poll_ready: heartbeat due -> dispatching HeartBeat future");
                        let mut provider = self.inner.clone();
                        let worker = worker.clone();
                        let fut = async move {
                            provider.heartbeat(&worker).await?;
                            Ok(())
                        };
                        self.state = State::HeartBeat(fut.boxed().into());
                        continue;
                    }

                    if let Some(events) = self.try_drain_events(cx) {
                        debug!(
                            count = events.len(),
                            "poll_ready: draining events -> dispatching ProcessEvents future"
                        );
                        let mut provider = self.inner.clone();
                        let worker = worker.clone();
                        let fut = async move { provider.handle_events(events, &worker).await };
                        self.state = State::ProcessEvents(fut.boxed().into());
                        continue;
                    }

                    trace!("poll_ready: no heartbeat/events pending -> Ready(Ok)");
                    return Poll::Ready(Ok(()));
                }

                _ => {
                    trace!(state = ?self.state, "poll_ready: state handled elsewhere -> Ready(Ok)");
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    /// Pulls the next task from the backend, fetching a fresh batch when the
    /// internal buffer is exhausted.
    ///
    /// Assumes [`poll_ready`](Self::poll_ready) has already driven the state
    /// machine into `State::Ready`. An in-progress fetch returns `Pending`;
    /// `Dead` returns `None`. States owned by `poll_ready` or `poll_close`
    /// cause a panic when this method is called out of order.
    #[allow(clippy::type_complexity)]
    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<P::Compact>, P::Error>>> {
        trace!("poll_next: poll: {state:?}", state = self.state);
        loop {
            match &mut self.state {
                State::Ready => {
                    trace!("poll_next: State::Ready -> dispatching Fetch future");
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();
                    let fut = async move { provider.fetch_next(&worker).await };
                    self.state = State::Fetch(fut.boxed().into());
                }

                State::Fetch(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(tasks)) if tasks.is_empty() => {
                        trace!(
                            "poll_next: fetch_next returned empty batch -> Ready, parking waker"
                        );
                        self.state = State::Ready;
                        worker.register_waker(cx);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(tasks)) => {
                        debug!(count = tasks.len(), "poll_next: fetch_next returned batch");
                        self.state = State::Buffered(VecDeque::from(tasks));
                    }
                    Poll::Ready(Err(e)) => {
                        warn!(error = ?e, "poll_next: fetch_next failed -> Dead");
                        self.state = State::Dead;
                        return Poll::Ready(Some(Err(e)));
                    }
                },

                State::Buffered(buffer) => {
                    if let Some(task) = buffer.pop_front() {
                        if buffer.is_empty() {
                            trace!("poll_next: buffer exhausted after pop -> Ready");
                            self.state = State::Ready;
                        }
                        trace!(task_id = ?task.task_id(), "poll_next: yielding task from buffer");
                        return Poll::Ready(Some(Ok(task)));
                    }
                    trace!("poll_next: buffer already empty -> Ready");
                    self.state = State::Ready;
                }

                State::Dead => {
                    debug!("poll_next: State::Dead -> Ready(None)");
                    return Poll::Ready(None);
                }

                // RegisterWorker/HeartBeat/ProcessEvents/NotBootstrapped are
                // poll_ready's responsibility — if we land here, the caller
                // skipped poll_ready or called poll_next while it was still Pending.
                _ => {
                    error!(state = ?self.state, "poll_next: state not owned by poll_next -> Panic");
                    unreachable!("poll_ready should have been called first");
                }
            }
        }
    }

    /// Gracefully winds down the provider: finishes in-flight bootstrap/heartbeat
    /// event futures, re-enqueues any abandoned buffered tasks, then flushes
    /// remaining events before signalling completion.
    ///
    /// An in-progress fetch is dropped without awaiting its result or returning
    /// tasks it may already have claimed. Submission flushing is separate: use
    /// the `Sink` interface to flush tasks queued by producers.
    /// On a close-path error, normal task polling stops and the error is returned.
    /// Later close polls can finish without retrying the failed operation.
    pub fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), P::Error>> {
        trace!("poll_close: Start: {:?}", self.state);
        loop {
            match &mut self.state {
                // Let any in-flight future finish before we start forcing drains
                State::RegisterWorker(fut) | State::HeartBeat(fut) => {
                    poll_fut_or_return!(fut, cx, self.state);
                    debug!("poll_close: in-flight RegisterWorker/HeartBeat completed");
                    worker.heartbeat(cx);
                    self.state = State::Ready;
                }

                State::ProcessEvents(fut) => {
                    poll_fut_or_return!(fut, cx, self.state);
                    trace!("poll_close: ProcessEvents complete -> Ready");
                    self.state = State::Ready;
                }

                State::ReenqueueOrphaned(fut) => {
                    poll_fut_or_return!(fut, cx, self.state);
                    debug!("poll_close: ReenqueueOrphaned complete -> Ready");
                    self.state = State::Ready;
                }

                State::Buffered(remaining) => {
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();

                    if !remaining.is_empty() {
                        debug!(
                            count = remaining.len(),
                            "poll_close: re-enqueuing abandoned buffered tasks"
                        );
                        let tasks = Vec::from(std::mem::take(remaining));
                        let fut = async move { provider.reenqueue_abandoned(tasks, &worker).await };
                        self.state = State::ReenqueueOrphaned(fut.boxed().into());
                    } else {
                        trace!("poll_close: buffer already empty -> Ready");
                        self.state = State::Ready;
                    }
                }

                State::Ready => {
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();

                    if let Some(events) = self.try_drain_events(cx) {
                        debug!(
                            count = events.len(),
                            "poll_close: flushing remaining events before shutdown"
                        );
                        let fut = async move { provider.handle_events(events, &worker).await };
                        self.state = State::ProcessEvents(fut.boxed().into());
                        continue;
                    }
                    debug!("poll_close: no remaining work -> Ready(Ok)");
                    return Poll::Ready(Ok(()));
                }

                _ => {
                    trace!("poll_close: unexpected state -> forcing Ready");
                    self.state = State::Ready;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::codec::Codec,
        task::{builder::TaskBuilder, status::Status},
    };
    use futures_channel::oneshot;
    use futures_util::{SinkExt, future::poll_fn, task::noop_waker_ref};
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Mutex},
    };
    use tower_layer::Layer;
    use tower_service::Service;

    #[derive(Debug, Default)]
    struct Records {
        calls: Vec<&'static str>,
        batches: VecDeque<Vec<Task<u32>>>,
        reenqueued: Vec<u32>,
        pushes: Vec<Vec<u32>>,
        events: Vec<TaskEvent<u32>>,
        failure: Option<&'static str>,
        gates: HashMap<&'static str, oneshot::Receiver<()>>,
    }

    #[derive(Debug, Clone, Default)]
    struct Provider(Arc<Mutex<Records>>);

    impl Provider {
        fn gate(&self, name: &'static str) -> oneshot::Sender<()> {
            let (tx, rx) = oneshot::channel();
            self.0.lock().unwrap().gates.insert(name, rx);
            tx
        }
        async fn operation(&self, name: &'static str) -> io::Result<()> {
            let (gate, fail) = {
                let mut records = self.0.lock().unwrap();
                records.calls.push(name);
                (records.gates.remove(name), records.failure == Some(name))
            };
            if let Some(gate) = gate {
                gate.await.unwrap();
            }
            if fail {
                Err(io::Error::other(name))
            } else {
                Ok(())
            }
        }
    }

    impl Persistence for Provider {
        type Compact = u32;
        type Response = u32;
        type Error = io::Error;
        async fn register(&mut self, _: &WorkerContext) -> io::Result<()> {
            self.operation("register").await
        }
        async fn heartbeat(&mut self, _: &WorkerContext) -> io::Result<()> {
            self.operation("heartbeat").await
        }
        async fn fetch_next(&mut self, _: &WorkerContext) -> io::Result<Vec<Task<u32>>> {
            let tasks = self
                .0
                .lock()
                .unwrap()
                .batches
                .pop_front()
                .unwrap_or_default();
            self.operation("fetch").await?;
            Ok(tasks)
        }
        async fn reenqueue_abandoned(
            &mut self,
            tasks: Vec<Task<u32>>,
            _: &WorkerContext,
        ) -> io::Result<u64> {
            self.operation("reenqueue").await?;
            let count = tasks.len() as u64;
            self.0
                .lock()
                .unwrap()
                .reenqueued
                .extend(tasks.into_iter().map(|task| task.args));
            Ok(count)
        }
        async fn handle_events(
            &mut self,
            events: Vec<TaskEvent<u32>>,
            _: &WorkerContext,
        ) -> io::Result<()> {
            self.operation("events").await?;
            self.0.lock().unwrap().events.extend(events);
            Ok(())
        }
        async fn push_tasks(&mut self, tasks: Vec<Task<u32>>) -> io::Result<()> {
            self.0
                .lock()
                .unwrap()
                .pushes
                .push(tasks.into_iter().map(|task| task.args).collect());
            self.operation("push").await
        }
    }

    #[derive(Clone, Debug)]
    struct NumberCodec {
        fail: bool,
    }
    impl Codec<u32> for NumberCodec {
        type Compact = u32;
        type Error = io::Error;
        fn encode(&self, value: &u32) -> io::Result<u32> {
            if self.fail {
                Err(io::Error::other("encode"))
            } else {
                Ok(*value)
            }
        }
        fn decode(&self, value: &u32) -> io::Result<u32> {
            Ok(*value)
        }
    }

    const HEARTBEAT: Duration = Duration::from_secs(3600);
    fn task(value: u32) -> Task<u32> {
        TaskBuilder::new(value)
            .task_id(u64::from(value).into())
            .build()
    }
    fn fixture() -> (Provider, Persisted<Provider>, WorkerContext) {
        let provider = Provider::default();
        let driver = Persisted::new(provider.clone());
        (provider, driver, WorkerContext::new("persistence"))
    }
    fn ready(driver: &mut Persisted<Provider>, worker: &WorkerContext) {
        assert!(matches!(
            driver.poll_ready(
                &mut Context::from_waker(noop_waker_ref()),
                worker,
                HEARTBEAT
            ),
            Poll::Ready(Ok(()))
        ));
    }
    fn heartbeat(driver: &mut Persisted<Provider>, worker: &WorkerContext) {
        let mut provider = driver.inner.clone();
        let worker = worker.clone();
        driver.state = State::HeartBeat(
            async move { provider.heartbeat(&worker).await }
                .boxed()
                .into(),
        );
    }
    fn queue_event(driver: &mut Persisted<Provider>) {
        let (mut tx, rx) = mpsc::channel(4);
        tx.try_send(TaskEvent::Cancel {
            task_id: 1u64.into(),
        })
        .unwrap();
        driver.receiver = Some(rx);
    }

    #[test]
    fn registration_heartbeat_events_fetch_and_reenqueue_follow_driver_order() {
        let (provider, mut driver, worker) = fixture();
        let registration = provider.gate("register");
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(driver.poll_ready(&mut cx, &worker, HEARTBEAT).is_pending());
        assert!(matches!(driver.state(), State::RegisterWorker(_)));
        registration.send(()).unwrap();
        ready(&mut driver, &worker);
        queue_event(&mut driver);
        let pulse = provider.gate("heartbeat");
        heartbeat(&mut driver, &worker);
        assert!(driver.poll_ready(&mut cx, &worker, HEARTBEAT).is_pending());
        assert_eq!(provider.0.lock().unwrap().calls, ["register", "heartbeat"]);
        pulse.send(()).unwrap();
        ready(&mut driver, &worker);
        provider
            .0
            .lock()
            .unwrap()
            .batches
            .push_back(vec![task(1), task(2)]);
        let Poll::Ready(Some(Ok(first))) = driver.poll_next(&mut cx, &worker) else {
            panic!("missing task")
        };
        assert_eq!(first.args, 1);
        assert!(matches!(driver.state(), State::Buffered(_)));
        assert!(matches!(
            driver.poll_close(&mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        let records = provider.0.lock().unwrap();
        assert_eq!(
            records.calls,
            ["register", "heartbeat", "events", "fetch", "reenqueue"]
        );
        assert_eq!(records.reenqueued, [2]);
        assert!(matches!(&records.events[..], [TaskEvent::Cancel { .. }]));
    }

    #[test]
    fn empty_fetch_parks_and_registers_the_worker_waker() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Count(AtomicUsize);
        impl std::task::Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let (provider, mut driver, worker) = fixture();
        ready(&mut driver, &worker);
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = count.clone().into();
        let mut cx = Context::from_waker(&waker);
        assert!(driver.poll_next(&mut cx, &worker).is_pending());
        assert!(matches!(driver.state(), State::Ready));
        assert_eq!(provider.0.lock().unwrap().calls, ["register", "fetch"]);
        worker.wake();
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn normal_polling_errors_enter_dead_and_next_returns_none() {
        for operation in ["register", "heartbeat", "events", "fetch"] {
            let (provider, mut driver, worker) = fixture();
            if operation != "register" {
                ready(&mut driver, &worker);
            }
            provider.0.lock().unwrap().failure = Some(operation);
            if operation == "heartbeat" {
                heartbeat(&mut driver, &worker);
            }
            if operation == "events" {
                queue_event(&mut driver);
            }
            let mut cx = Context::from_waker(noop_waker_ref());
            let result = if operation == "fetch" {
                driver
                    .poll_next(&mut cx, &worker)
                    .map(|next| next.unwrap().map(|_| ()))
            } else {
                driver.poll_ready(&mut cx, &worker, HEARTBEAT)
            };
            let Poll::Ready(Err(error)) = result else {
                panic!("expected {operation} error")
            };
            assert_eq!(error.to_string(), operation);
            assert!(matches!(driver.state(), State::Dead));
            assert!(matches!(
                driver.poll_next(&mut cx, &worker),
                Poll::Ready(None)
            ));
        }
    }

    #[test]
    fn shutdown_waits_for_registration_heartbeat_and_events() {
        for operation in ["register", "heartbeat", "events"] {
            let (provider, mut driver, worker) = fixture();
            if operation != "register" {
                ready(&mut driver, &worker);
            }
            let gate = provider.gate(operation);
            if operation == "heartbeat" {
                heartbeat(&mut driver, &worker);
            }
            if operation == "events" {
                queue_event(&mut driver);
            }
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(driver.poll_ready(&mut cx, &worker, HEARTBEAT).is_pending());
            assert!(driver.poll_close(&mut cx, &worker).is_pending());
            gate.send(()).unwrap();
            assert!(matches!(
                driver.poll_close(&mut cx, &worker),
                Poll::Ready(Ok(()))
            ));
            assert!(matches!(driver.state(), State::Ready));
            assert_eq!(
                provider
                    .0
                    .lock()
                    .unwrap()
                    .calls
                    .iter()
                    .filter(|name| **name == operation)
                    .count(),
                1
            );
        }
    }

    #[test]
    fn shutdown_reenqueues_before_draining_events_and_stops_on_error() {
        for failure in [None, Some("reenqueue"), Some("events")] {
            let (provider, mut driver, worker) = fixture();
            ready(&mut driver, &worker);
            driver.state = State::Buffered(VecDeque::from([task(3)]));
            provider.0.lock().unwrap().failure = failure;
            queue_event(&mut driver);
            let mut cx = Context::from_waker(noop_waker_ref());
            let result = driver.poll_close(&mut cx, &worker);
            match failure {
                None => {
                    assert!(matches!(result, Poll::Ready(Ok(()))));
                    assert_eq!(
                        provider.0.lock().unwrap().calls,
                        ["register", "reenqueue", "events"]
                    );
                }
                Some(operation) => {
                    let Poll::Ready(Err(error)) = result else {
                        panic!("expected close error")
                    };
                    assert_eq!(error.to_string(), operation);
                    assert!(matches!(
                        driver.poll_close(&mut cx, &worker),
                        Poll::Ready(Ok(()))
                    ));
                    assert_eq!(
                        provider
                            .0
                            .lock()
                            .unwrap()
                            .calls
                            .iter()
                            .filter(|name| **name == operation)
                            .count(),
                        1
                    );
                }
            }
        }
    }

    #[test]
    fn shutdown_drops_fetch_after_provider_has_claimed_tasks() {
        let (provider, mut driver, worker) = fixture();
        ready(&mut driver, &worker);
        provider.0.lock().unwrap().batches.push_back(vec![task(9)]);
        let gate = provider.gate("fetch");
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(driver.poll_next(&mut cx, &worker).is_pending());
        assert!(matches!(driver.state(), State::Fetch(_)));
        assert!(provider.0.lock().unwrap().batches.is_empty());
        assert!(matches!(
            driver.poll_close(&mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        assert!(
            gate.send(()).is_err(),
            "fetch future should have been dropped"
        );
        assert!(provider.0.lock().unwrap().reenqueued.is_empty());
        assert_eq!(provider.0.lock().unwrap().calls, ["register", "fetch"]);
    }

    #[test]
    #[should_panic(expected = "poll_ready should have been called first")]
    fn polling_before_registration_panics() {
        let (_, mut driver, worker) = fixture();
        let _ = driver.poll_next(&mut Context::from_waker(noop_waker_ref()), &worker);
    }

    #[test]
    fn clone_resets_driver_state_channels_and_submission_buffers() {
        let (provider, mut driver, worker) = fixture();
        ready(&mut driver, &worker);
        let _layer = driver.shared_layer(NumberCodec { fail: false }, 2);
        driver.start_send_unpin(task(1)).unwrap();
        driver.state = State::Buffered(VecDeque::from([task(2)]));
        let clone = driver.clone();
        assert!(Arc::ptr_eq(&clone.inner.0, &provider.0));
        assert!(matches!(clone.state(), State::NotBootstrapped));
        assert!(clone.heartbeat_timer.is_none());
        assert!(clone.receiver.is_none());
        assert!(clone.shared_sender.is_none());
        assert!(clone.sink_buffer.is_empty());
        assert!(clone.sink_future.is_none());
        assert_eq!(driver.sink_buffer.len(), 1);
    }

    #[test]
    fn shared_layers_keep_their_codecs_and_options_and_close_without_sender_eof() {
        let (provider, mut driver, worker) = fixture();
        let mut first = driver
            .shared_layer(NumberCodec { fail: false }, 2)
            .persist_results(true)
            .layer(tower::service_fn(|req: Task<u32>| async move {
                Ok::<_, io::Error>(req.args)
            }));
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(first.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(
            first.call(task(7)).poll_unpin(&mut cx),
            Poll::Ready(Ok(7))
        ));
        let mut second = driver
            .shared_layer(NumberCodec { fail: true }, 1)
            .lock_tasks(true)
            .persist_results(true)
            .layer(tower::service_fn(|req: Task<u32>| async move {
                Ok::<_, io::Error>(req.args)
            }));
        assert!(matches!(second.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(
            second.call(task(8)).poll_unpin(&mut cx),
            Poll::Ready(Ok(8))
        ));
        let mut default =
            driver
                .shared_layer(NumberCodec { fail: true }, 1)
                .layer(tower::service_fn(|req: Task<u32>| async move {
                    Ok::<_, io::Error>(req.args)
                }));
        assert!(matches!(default.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(
            default.call(task(9)).poll_unpin(&mut cx),
            Poll::Ready(Ok(9))
        ));
        drop((first, second, default));

        let release = provider.gate("events");
        assert!(driver.poll_ready(&mut cx, &worker, HEARTBEAT).is_pending());
        assert!(driver.poll_close(&mut cx, &worker).is_pending());
        release.send(()).unwrap();
        assert!(matches!(
            driver.poll_close(&mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        let records = provider.0.lock().unwrap();
        assert!(
            matches!(&records.events[..], [TaskEvent::Complete(first), TaskEvent::Lock { task_id }, TaskEvent::Complete(second)]
            if first.result == Ok(7) && *task_id == 8u64.into() && second.result == Err("encode".to_owned()))
        );
    }

    #[test]
    fn shared_channel_retains_the_same_backpressure_as_direct_layers() {
        for shared in [false, true] {
            for batch in [0, 1, 2] {
                let (provider, mut driver, worker) = fixture();
                let layer = if shared {
                    driver.shared_layer(NumberCodec { fail: false }, batch)
                } else {
                    driver.layer(NumberCodec { fail: false }, batch)
                };
                let mut service = layer.persist_results(true).layer(tower::service_fn(
                    |req: Task<u32>| async move { Ok::<_, io::Error>(req.args) },
                ));
                let mut cx = Context::from_waker(noop_waker_ref());
                for _ in 0..batch * 4 {
                    assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
                    assert!(matches!(
                        service.call(task(1)).poll_unpin(&mut cx),
                        Poll::Ready(Ok(1))
                    ));
                }
                assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
                let mut waiting = service.call(task(2));
                assert!(waiting.poll_unpin(&mut cx).is_pending());
                if shared {
                    let _later = driver.shared_layer(NumberCodec { fail: false }, usize::MAX);
                    assert!(waiting.poll_unpin(&mut cx).is_pending());
                }
                ready(&mut driver, &worker);
                assert!(matches!(waiting.poll_unpin(&mut cx), Poll::Ready(Ok(2))));
                assert_eq!(provider.0.lock().unwrap().events.len(), batch * 4 + 1);
            }
        }
    }

    #[test]
    fn shared_capacity_survives_layer_drop_and_driver_close() {
        let (provider, mut driver, worker) = fixture();
        drop(driver.shared_layer(NumberCodec { fail: false }, 0));
        ready(&mut driver, &worker);
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            driver.poll_close(&mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        let mut service = driver
            .shared_layer(NumberCodec { fail: false }, usize::MAX)
            .persist_results(true)
            .layer(tower::service_fn(|req: Task<u32>| async move {
                Ok::<_, io::Error>(req.args)
            }));
        assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        let mut pending = service.call(task(1));
        assert!(pending.poll_unpin(&mut cx).is_pending());
        assert!(matches!(
            driver.poll_close(&mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        assert!(matches!(pending.poll_unpin(&mut cx), Poll::Ready(Ok(1))));
        assert_eq!(provider.0.lock().unwrap().events.len(), 1);
    }

    #[test]
    fn explicit_layer_replaces_shared_channel_and_next_shared_call_starts_fresh() {
        let (_, mut driver, _) = fixture();
        let mut direct = driver
            .layer(NumberCodec { fail: false }, 0)
            .layer(tower::service_fn(|_: Task<u32>| async {
                Ok::<_, io::Error>(1)
            }));
        let mut shared =
            driver
                .shared_layer(NumberCodec { fail: false }, 1)
                .layer(tower::service_fn(|_: Task<u32>| async {
                    Ok::<_, io::Error>(1)
                }));
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(direct.poll_ready(&mut cx), Poll::Ready(Err(_))));
        assert!(matches!(shared.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        let mut replacement =
            driver
                .layer(NumberCodec { fail: false }, 0)
                .layer(tower::service_fn(|_: Task<u32>| async {
                    Ok::<_, io::Error>(1)
                }));
        assert!(matches!(shared.poll_ready(&mut cx), Poll::Ready(Err(_))));
        assert!(matches!(
            replacement.poll_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        let mut restarted = driver
            .shared_layer(NumberCodec { fail: false }, 1)
            .persist_results(true)
            .layer(tower::service_fn(|_: Task<u32>| async {
                Ok::<_, io::Error>(1)
            }));
        assert!(matches!(
            replacement.poll_ready(&mut cx),
            Poll::Ready(Err(_))
        ));
        assert!(matches!(restarted.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(
            restarted.call(task(1)).poll_unpin(&mut cx),
            Poll::Ready(Ok(1))
        ));
    }

    #[test]
    fn cloned_driver_shared_channels_disconnect_independently_on_drop() {
        let (_, mut driver, _) = fixture();
        let mut original =
            driver
                .shared_layer(NumberCodec { fail: false }, 1)
                .layer(tower::service_fn(|_: Task<u32>| async {
                    Ok::<_, io::Error>(1)
                }));
        let mut cloned = driver.clone();
        let mut independent =
            cloned
                .shared_layer(NumberCodec { fail: false }, 1)
                .layer(tower::service_fn(|_: Task<u32>| async {
                    Ok::<_, io::Error>(1)
                }));
        let mut cx = Context::from_waker(noop_waker_ref());
        drop(driver);
        assert!(matches!(original.poll_ready(&mut cx), Poll::Ready(Err(_))));
        assert!(matches!(
            independent.poll_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        drop(cloned);
        assert!(matches!(
            independent.poll_ready(&mut cx),
            Poll::Ready(Err(_))
        ));
    }

    #[test]
    fn sink_flush_includes_tasks_accepted_while_an_existing_batch_is_pending() {
        let (provider, mut driver, _) = fixture();
        let gate = provider.gate("push");
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            driver.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        driver.start_send_unpin(task(1)).unwrap();
        assert!(driver.poll_flush_unpin(&mut cx).is_pending());
        assert!(matches!(
            driver.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        driver.start_send_unpin(task(2)).unwrap();
        gate.send(()).unwrap();
        assert!(matches!(
            driver.poll_flush_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(provider.0.lock().unwrap().pushes, [vec![1], vec![2]]);
        assert!(matches!(
            driver.poll_close_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(provider.0.lock().unwrap().pushes, [vec![1], vec![2]]);
        assert!(matches!(driver.state(), State::NotBootstrapped));
    }

    #[test]
    fn sink_close_waits_for_tasks_accepted_during_an_existing_flush() {
        let (provider, mut driver, _) = fixture();
        let first = provider.gate("push");
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            driver.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        driver.start_send_unpin(task(1)).unwrap();
        assert!(driver.poll_flush_unpin(&mut cx).is_pending());
        assert!(matches!(
            driver.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        driver.start_send_unpin(task(2)).unwrap();
        let second = provider.gate("push");
        first.send(()).unwrap();
        assert!(driver.poll_close_unpin(&mut cx).is_pending());
        assert_eq!(provider.0.lock().unwrap().pushes, [vec![1], vec![2]]);
        second.send(()).unwrap();
        assert!(matches!(
            driver.poll_close_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(provider.0.lock().unwrap().pushes, [vec![1], vec![2]]);
        assert!(matches!(driver.state(), State::NotBootstrapped));
    }

    #[test]
    fn failed_push_is_not_rebuffered_or_retried() {
        let (provider, mut driver, _) = fixture();
        provider.0.lock().unwrap().failure = Some("push");
        driver.start_send_unpin(task(1)).unwrap();
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            driver.poll_flush_unpin(&mut cx),
            Poll::Ready(Err(_))
        ));
        assert!(driver.sink_buffer.is_empty());
        assert!(driver.sink_future.is_none());
        assert!(matches!(
            driver.poll_flush_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(provider.0.lock().unwrap().pushes, [vec![1]]);
    }

    #[tokio::test]
    async fn event_layer_orders_call_lock_execution_and_completion() {
        let (provider, mut driver, worker) = fixture();
        ready(&mut driver, &worker);
        let layer = driver
            .layer(NumberCodec { fail: false }, 2)
            .lock_tasks(true)
            .persist_results(true);
        let inner_gate = provider.gate("execute");
        let recorded = provider.clone();
        let mut service = layer.layer(tower::service_fn(move |req: Task<u32>| {
            recorded.0.lock().unwrap().calls.push("call");
            let recorded = recorded.clone();
            async move {
                recorded.operation("execute").await?;
                assert_eq!(req.raw_attempt().increment(), 0);
                req.raw_status().store(Status::Done);
                Ok::<_, io::Error>(req.args + 1)
            }
        }));
        poll_fn(|cx| service.poll_ready(cx)).await.unwrap();
        let mut future = service.call(task(7));
        assert_eq!(provider.0.lock().unwrap().calls, ["register", "call"]);
        assert!(driver.receiver.as_mut().unwrap().try_recv().is_err());
        assert!(
            future
                .poll_unpin(&mut Context::from_waker(noop_waker_ref()))
                .is_pending()
        );
        ready(&mut driver, &worker);
        assert!(
            matches!(&provider.0.lock().unwrap().events[..], [TaskEvent::Lock { task_id }] if *task_id == 7u64.into())
        );
        inner_gate.send(()).unwrap();
        assert_eq!(future.await.unwrap(), 8);
        ready(&mut driver, &worker);
        let records = provider.0.lock().unwrap();
        let TaskEvent::Complete(result) = &records.events[1] else {
            panic!("missing completion")
        };
        assert_eq!(result.task_id, 7u64.into());
        assert_eq!(result.attempt, 1);
        assert_eq!(result.status, Status::Done);
        assert_eq!(result.result, Ok(8));
        assert_eq!(
            records.calls,
            ["register", "call", "execute", "events", "events"]
        );
    }

    #[tokio::test]
    async fn encoding_failure_is_recorded_without_replacing_successful_response() {
        let (_, mut driver, _) = fixture();
        let mut service = driver
            .layer(NumberCodec { fail: true }, 1)
            .persist_results(true)
            .layer(tower::service_fn(|_: Task<u32>| async {
                Ok::<_, io::Error>(42u32)
            }));
        poll_fn(|cx| service.poll_ready(cx)).await.unwrap();
        assert_eq!(service.call(task(1)).await.unwrap(), 42);
        let TaskEvent::Complete(result) = driver.receiver.as_mut().unwrap().try_recv().unwrap()
        else {
            panic!("missing completion")
        };
        assert_eq!(result.result, Err("encode".to_owned()));
        assert_eq!(result.status, Status::Pending);
    }

    #[tokio::test]
    async fn handler_error_is_returned_and_recorded_in_completion_event() {
        let (_, mut driver, _) = fixture();
        let mut service = driver
            .layer(NumberCodec { fail: false }, 1)
            .persist_results(true)
            .layer(tower::service_fn(|_: Task<u32>| async {
                Err::<u32, _>(io::Error::other("handler"))
            }));
        assert_eq!(
            service.call(task(1)).await.unwrap_err().to_string(),
            "handler"
        );
        let TaskEvent::Complete(result) = driver.receiver.as_mut().unwrap().try_recv().unwrap()
        else {
            panic!("missing completion")
        };
        assert_eq!(result.result, Err("handler".to_owned()));
    }

    #[tokio::test]
    async fn disconnected_channel_prevents_poll_ready_and_lock_delivery() {
        let (_, mut driver, _) = fixture();
        let executions = Arc::new(Mutex::new(0));
        let counted = executions.clone();
        let mut service = driver
            .layer(NumberCodec { fail: false }, 1)
            .lock_tasks(true)
            .layer(tower::service_fn(move |_: Task<u32>| {
                let counted = counted.clone();
                async move {
                    *counted.lock().unwrap() += 1;
                    Ok::<_, io::Error>(1u32)
                }
            }));
        poll_fn(|cx| service.poll_ready(cx)).await.unwrap();
        let future = service.call(task(1));
        driver.receiver = None;
        assert!(future.await.is_err());
        assert_eq!(*executions.lock().unwrap(), 0);
        assert!(matches!(
            service.poll_ready(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(Err(_))
        ));
    }

    #[tokio::test]
    async fn completion_send_failure_replaces_handler_result() {
        let (provider, mut driver, _) = fixture();
        let gate = provider.gate("execute");
        let recorded = provider.clone();
        let mut service = driver
            .layer(NumberCodec { fail: false }, 1)
            .persist_results(true)
            .layer(tower::service_fn(move |_: Task<u32>| {
                let recorded = recorded.clone();
                async move {
                    recorded.operation("execute").await?;
                    Ok::<_, io::Error>(42u32)
                }
            }));
        poll_fn(|cx| service.poll_ready(cx)).await.unwrap();
        let mut future = service.call(task(1));
        assert!(
            future
                .poll_unpin(&mut Context::from_waker(noop_waker_ref()))
                .is_pending()
        );
        driver.receiver = None;
        gate.send(()).unwrap();
        assert!(future.await.is_err());
        assert_eq!(provider.0.lock().unwrap().calls, ["execute"]);
    }

    #[tokio::test]
    async fn default_layer_requires_id_but_emits_no_events() {
        let (_, mut driver, _) = fixture();
        let mut service = driver
            .layer(NumberCodec { fail: false }, 1)
            .layer(tower::service_fn(|_: Task<u32>| async {
                Ok::<_, io::Error>(1u32)
            }));
        assert_eq!(service.call(task(1)).await.unwrap(), 1);
        assert!(driver.receiver.as_mut().unwrap().try_recv().is_err());
        assert!(
            std::panic::AssertUnwindSafe(service.call(Task::new(1)))
                .catch_unwind()
                .await
                .is_err()
        );
    }

    #[test]
    fn replacing_event_layer_disconnects_previous_services() {
        let (_, mut driver, _) = fixture();
        let old_layer = driver.layer(NumberCodec { fail: false }, 1);
        let mut service = old_layer.layer(tower::service_fn(|_: Task<u32>| async {
            Ok::<_, io::Error>(1u32)
        }));
        let _new_layer = driver.layer(NumberCodec { fail: false }, 1);
        assert!(matches!(
            service.poll_ready(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(Err(_))
        ));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn events_round_trip_through_serde_with_task_result_fields() {
        for (event, expected) in [
            (
                TaskEvent::Lock {
                    task_id: 1u64.into(),
                },
                serde_json::json!({"Lock": {"task_id": 1}}),
            ),
            (
                TaskEvent::Release {
                    task_id: 2u64.into(),
                },
                serde_json::json!({"Release": {"task_id": 2}}),
            ),
            (
                TaskEvent::Cancel {
                    task_id: 3u64.into(),
                },
                serde_json::json!({"Cancel": {"task_id": 3}}),
            ),
            (
                TaskEvent::Complete(crate::backend::TaskResult {
                    task_id: 4u64.into(),
                    attempt: 2,
                    status: Status::Done,
                    result: Ok(7u32),
                }),
                serde_json::json!({"Complete": {"task_id": 4, "attempt": 2, "status": "Done", "result": {"Ok": 7}}}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&event).unwrap(), expected);
            let decoded: TaskEvent<u32> = serde_json::from_value(expected.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
        }
    }
    #[tokio::test]
    async fn worker_adapter_persists_lifecycle_status_and_result_before_shutdown() {
        use crate::backend::{Backend, BackendConfig, WireFormatBackend, finalize::Durable};
        use crate::worker::builder::WorkerBuilder;
        struct Adapter {
            driver: Persisted<Provider>,
            codec: NumberCodec,
        }
        impl Backend for Adapter {
            type Task = Task<u32>;
            type Error = io::Error;
            fn poll_ready(
                &mut self,
                cx: &mut Context<'_>,
                worker: &WorkerContext,
            ) -> Poll<io::Result<()>> {
                self.driver.poll_ready(cx, worker, HEARTBEAT)
            }
            fn poll_next(
                &mut self,
                cx: &mut Context<'_>,
                worker: &WorkerContext,
            ) -> Poll<Option<io::Result<Self::Task>>> {
                self.driver.poll_next(cx, worker)
            }
            fn poll_close(
                &mut self,
                cx: &mut Context<'_>,
                worker: &WorkerContext,
            ) -> Poll<io::Result<()>> {
                self.driver.poll_close(cx, worker)
            }
        }
        impl WireFormatBackend for Adapter {
            type Codec = NumberCodec;
            type Compact = u32;
            fn codec(&self) -> &NumberCodec {
                &self.codec
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
                self.driver
                    .shared_layer(self.codec.clone(), 1)
                    .lock_tasks(true)
                    .persist_results(true)
            }
        }
        let (provider, driver, _) = fixture();
        provider.0.lock().unwrap().batches.push_back(vec![task(5)]);
        let worker = WorkerBuilder::new("persisted-worker")
            .backend(Adapter {
                driver,
                codec: NumberCodec { fail: false },
            })
            .build(|value: u32, worker: WorkerContext| async move {
                worker.stop().unwrap();
                Ok::<_, io::Error>(value + 1)
            });
        tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        let records = provider.0.lock().unwrap();
        assert!(matches!(records.events[0], TaskEvent::Lock { .. }));
        let TaskEvent::Complete(result) = &records.events[1] else {
            panic!("missing persisted result")
        };
        assert_eq!(result.result, Ok(6));
        assert_eq!(result.attempt, 1);
        assert_eq!(result.status, Status::Done);
        assert!(records.reenqueued.is_empty());
    }
    #[test]
    fn close_errors_discard_completed_registration_and_heartbeat_futures() {
        for operation in ["register", "heartbeat"] {
            let (provider, mut driver, worker) = fixture();
            if operation == "heartbeat" {
                ready(&mut driver, &worker);
            }
            provider.0.lock().unwrap().failure = Some(operation);
            let gate = provider.gate(operation);
            if operation == "heartbeat" {
                heartbeat(&mut driver, &worker);
            }
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(driver.poll_ready(&mut cx, &worker, HEARTBEAT).is_pending());
            gate.send(()).unwrap();
            let Poll::Ready(Err(error)) = driver.poll_close(&mut cx, &worker) else {
                panic!("expected close error")
            };
            assert_eq!(error.to_string(), operation);
            assert!(matches!(
                driver.poll_close(&mut cx, &worker),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(
                provider
                    .0
                    .lock()
                    .unwrap()
                    .calls
                    .iter()
                    .filter(|name| **name == operation)
                    .count(),
                1
            );
        }
    }
}
