//! Extension traits and combinators for [`Backend`] implementations.
//!
//! This module provides additional functionality for [`Backend`] implementations,
//! including:
//!
//! - [`BackendExt`]: Extension methods for transforming, composing, and managing
//!   backends.
//! - [`InspectErr`]: A wrapper that allows inspection of errors produced by a backend.
//! - [`MapErr`]: A wrapper that maps backend errors from one type to another.
//! - [`Pipe`]: A utility for piping tasks from one backend to another.
//! - [`BeforeStart`]: A lifecycle wrapper that runs an action before the backend starts.
//! - [`AfterStart`]: A lifecycle wrapper that runs an action after the backend starts.
//! - [`BeforeStop`]: A lifecycle wrapper that runs an action before the backend stops.
//! - [`AfterStop`]: A lifecycle wrapper that runs an action after the backend stops.
use std::task::{Context, Poll};

#[cfg(feature = "sleep")]
use std::time::Duration;

use futures_core::Stream;

#[cfg(feature = "tracing")]
use crate::backend::ext::instrument::Instrumented;
use crate::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        codec::Codec,
        ext::{
            inspect_err::InspectErr,
            interleave::Interleave,
            lifecycle::{AfterStart, AfterStop, BeforeStart, BeforeStop},
            map_err::MapErr,
            pipe::Pipe,
            poll_strategy::{PollStrategy, PollWith, StreamStrategy},
            wake_on_push::WakeOnPush,
            with_codec::WithCodec,
        },
    },
    error::BoxDynError,
    task::Task,
    worker::context::WorkerContext,
};

#[cfg(feature = "shared")]
use crate::backend::ext::shared::Shared;

#[cfg(feature = "sleep")]
use crate::backend::ext::poll_strategy::{BackoffConfig, BackoffStrategy, IntervalStrategy};

/// Macros for forwarding backend trait implementations.
#[macro_use]
pub mod delegate;
/// A wrapper that allows inspecting errors produced by a backend.
pub mod inspect_err;

/// Extension allowing backends to be instrumented with a [tracing::Span].
#[cfg(feature = "tracing")]
pub mod instrument;
/// A wrapper that allows merging a backend with a stream
pub mod interleave;

/// A wrapper that allows mapping the error type of a backend from `Self::Error` to another error type `E2`.
pub mod map_err;
pub mod pipe;
pub mod poll_strategy;
/// A wrapper that wakes the worker when a new item is fetched.
pub mod wake_on_push;
pub mod with_codec;

pub mod lifecycle;

/// A wrapper that makes a backend clonable
#[cfg(feature = "shared")]
pub mod shared;

/// An error returned while polling a backend and decoding task arguments.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PollNextArgsError<B: Backend> {
    /// The backend produced an error while polling for the next task.
    #[error("backend error: {0}")]
    BackendError(B::Error),
    /// The backend produced a task, but the task's arguments could not be decoded.
    #[error("failed to decode task args: {0}")]
    DecodeError(BoxDynError),
}

/// Extension trait for `Backend` that provides additional combinators and utilities.
pub trait BackendExt: Backend {
    /// A convenience method for calling `poll_next` and decoding the `Args` in one step,
    /// returning a `Task<Self::Args, ..>` instead of `Task<Self::Compact, ..>`.
    #[allow(clippy::type_complexity)]
    fn poll_next_args(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Args>, PollNextArgsError<Self>>>>
    where
        Self: Sized + BackendConfig + WireFormatBackend + Backend<Task = Task<Self::Compact>>,
        Self::Codec: Codec<Self::Args, Compact = Self::Compact>,
        <Self::Codec as Codec<Self::Args>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let next = self.poll_next(cx, worker);
        let codec = self.codec();
        next.map(move |item| match item {
            Some(Ok(task)) => {
                let task = task.try_map_args(|compact| codec.decode(&compact));
                Some(task.map_err(|e| PollNextArgsError::DecodeError(e.into())))
            }
            Some(Err(e)) => Some(Err(PollNextArgsError::BackendError(e))),
            None => None,
        })
    }

    /// Pipes every task polled from this backend into `sink`
    ///
    /// Useful for bridging two backend implementations — e.g. draining an
    /// ephemeral/legacy queue into a durable one, or fanning a lightweight
    /// source into a shared sink that multiple producers write into.
    fn pipe_to<Dst>(self, backend: Dst) -> Pipe<Dst, Self>
    where
        Self: Sized,
    {
        Pipe::new(self, backend)
    }

    /// Attaches a callback `F` to be run on each error produced while polling the backend.
    fn inspect_err<F>(self, f: F) -> InspectErr<Self, F>
    where
        Self: Sized,
        F: Fn(&Self::Error),
    {
        InspectErr { backend: self, f }
    }

    /// Maps errors produced by the backend from `Self::Error` into `E2`, useful for
    /// heterogeneous composed backends.
    fn map_err<F, E2>(self, f: F) -> MapErr<Self, F>
    where
        Self: Sized,
        F: Fn(Self::Error) -> E2,
    {
        MapErr { backend: self, f }
    }

    /// Swaps out the backend's serialization codec entirely (JSON,
    /// MessagePack, Protobuf, ...) without touching storage logic.
    fn with_codec<NewCodec>(self, codec: NewCodec) -> WithCodec<Self, NewCodec>
    where
        Self: Sized + BackendConfig,
        NewCodec: Codec<Self::Args>,
    {
        WithCodec::new(self, codec)
    }

    /// Wake the worker when a stream receives a new item
    fn poll_with_stream<S>(self, stream: S) -> PollWith<Self, StreamStrategy<S>>
    where
        Self: Sized,
        S: Stream + Unpin + Send + 'static,
    {
        let strategy = StreamStrategy::new(stream);
        PollWith::new(self, strategy)
    }

    /// Wake the worker periodically
    #[cfg(feature = "sleep")]
    fn poll_with_interval(self, duration: Duration) -> PollWith<Self, IntervalStrategy>
    where
        Self: Sized,
    {
        let strategy = IntervalStrategy::new(duration);
        PollWith::new(self, strategy)
    }

    /// Wake the worker periodically with a backoff
    #[cfg(feature = "sleep")]
    fn poll_with_backoff(
        self,
        interval: Duration,
        config: BackoffConfig,
    ) -> PollWith<Self, BackoffStrategy>
    where
        Self: Sized,
    {
        let strategy = IntervalStrategy::new(interval).with_backoff(config);
        PollWith::new(self, strategy)
    }

    /// Wake the worker with a custom strategy
    fn poll_with_strategy<S>(self, strategy: S) -> PollWith<Self, S>
    where
        Self: Sized,
        S: PollStrategy,
    {
        PollWith::new(self, strategy)
    }

    #[cfg(feature = "tracing")]
    /// Provides a span to decorate emitted events
    fn instrumented(self, span: tracing::Span) -> Instrumented<Self>
    where
        Self: Sized,
    {
        Instrumented::new(self, span)
    }

    /// Runs an async callback once, before the backend's first `poll_ready` is delegated.
    fn before_start<F, Fut>(self, f: F) -> BeforeStart<Self, Self::Error>
    where
        Self: Sized,
        F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        BeforeStart::new(self, f)
    }

    /// Runs an async callback once, before the backend's poll_close is called.
    fn before_stop<F, Fut>(self, f: F) -> BeforeStop<Self, Self::Error>
    where
        Self: Sized,
        F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        BeforeStop::new(self, f)
    }

    /// Runs an async callback once, after the backend's first `poll_ready` is successful.
    fn after_start<F, Fut>(self, f: F) -> AfterStart<Self, Self::Error>
    where
        Self: Sized,
        for<'c> F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        AfterStart::new(self, f)
    }

    /// Runs an async callback once, after the worker has stopped and backend has cleaned up.
    fn after_stop<F, Fut>(self, f: F) -> AfterStop<Self, Self::Error>
    where
        Self: Sized,
        F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        AfterStop::new(self, f)
    }

    /// Interleaves the external stream with the backend.
    fn interleave<S>(self, stream: S) -> Interleave<Self, S>
    where
        Self: Sized,
        S: Stream<Item = Result<Self::Task, Self::Error>> + Unpin,
    {
        Interleave::new(self, stream)
    }

    /// Wakes the worker when a new item is pushed
    fn wake_on_push(self) -> WakeOnPush<Self>
    where
        Self: Sized,
    {
        WakeOnPush::new(self)
    }

    /// Creates a cloneable handle to the inner backend.
    ///
    /// See [`Shared`] for configuration and pending-send buffer limitations.
    #[cfg(feature = "shared")]
    fn shared(self) -> Shared<Self>
    where
        Self: WireFormatBackend + Send,
        Self::Codec: Clone,
    {
        Shared::new(self)
    }
}

impl<B: Backend> BackendExt for B {}

#[cfg(test)]
mod composition_tests {
    use super::*;
    use crate::backend::{FetchById, finalize::Ephemeral};
    use crate::task::task_id::TaskId;
    use futures_sink::Sink;
    use futures_util::{SinkExt, StreamExt, poll, stream, task::noop_waker_ref};
    use std::{
        collections::VecDeque,
        io,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Debug, Default)]
    struct Probe {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail: Option<&'static str>,
        pending: Option<&'static str>,
        tasks: VecDeque<Task<u32>>,
    }

    impl Probe {
        fn record(&self, event: &'static str) -> io::Result<()> {
            self.events.lock().unwrap().push(event);
            if self.fail == Some(event) {
                Err(io::Error::other(event))
            } else {
                Ok(())
            }
        }
        fn poll(&self, event: &'static str) -> Poll<io::Result<()>> {
            if let Err(e) = self.record(event) {
                return Poll::Ready(Err(e));
            }
            if self.pending == Some(event) {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    impl Backend for Probe {
        type Task = Task<u32>;
        type Error = io::Error;
        fn poll_ready(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
            self.poll("ready")
        }
        fn poll_next(
            &mut self,
            _: &mut Context<'_>,
            _: &WorkerContext,
        ) -> Poll<Option<io::Result<Self::Task>>> {
            match self.poll("next") {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(())) => Poll::Ready(self.tasks.pop_front().map(Ok)),
            }
        }
        fn poll_close(&mut self, _: &mut Context<'_>, _: &WorkerContext) -> Poll<io::Result<()>> {
            self.poll("close")
        }
    }

    impl Sink<Task<u32>> for Probe {
        type Error = io::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll("sink_ready")
        }
        fn start_send(mut self: Pin<&mut Self>, task: Task<u32>) -> io::Result<()> {
            self.record("send")?;
            self.tasks.push_back(task);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll("flush")
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll("sink_close")
        }
    }

    impl BackendConfig for Probe {
        type Args = u32;
        type Id = String;
        type Kind = Ephemeral;
        type Config = ();
        type Layer = tower_layer::Identity;
        fn config(&self) -> &() {
            &()
        }
        fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
            tower_layer::Identity::new()
        }
    }

    impl FetchById for Probe {
        async fn fetch_by_id(&mut self, _: &TaskId) -> io::Result<Option<Task<u32>>> {
            self.record("fetch")?;
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DecodeNumber;
    impl Codec<u32> for DecodeNumber {
        type Compact = u32;
        type Error = io::Error;
        fn encode(&self, val: &u32) -> io::Result<u32> {
            Ok(*val)
        }
        fn decode(&self, val: &u32) -> io::Result<u32> {
            if *val == 0 {
                Err(io::Error::other("decode"))
            } else {
                Ok(val + 1)
            }
        }
    }

    #[tokio::test]
    async fn inspection_and_mapping_cover_backend_sink_and_management_errors() {
        let worker = WorkerContext::new("errors");
        let mut cx = Context::from_waker(noop_waker_ref());
        for phase in [
            "ready",
            "next",
            "close",
            "sink_ready",
            "send",
            "flush",
            "sink_close",
            "fetch",
        ] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let captured = seen.clone();
            let mut backend = Probe {
                fail: Some(phase),
                ..Probe::default()
            }
            .inspect_err(move |e| captured.lock().unwrap().push(e.to_string()))
            .map_err(|e| io::Error::other(format!("mapped: {e}")));
            let error = match phase {
                "ready" => Backend::poll_ready(&mut backend, &mut cx, &worker),
                "next" => backend
                    .poll_next(&mut cx, &worker)
                    .map(|r| r.unwrap().map(|_| ())),
                "close" => Backend::poll_close(&mut backend, &mut cx, &worker),
                "sink_ready" => backend.poll_ready_unpin(&mut cx),
                "send" => Poll::Ready(backend.start_send_unpin(Task::new(1))),
                "flush" => backend.poll_flush_unpin(&mut cx),
                "sink_close" => backend.poll_close_unpin(&mut cx),
                "fetch" => Poll::Ready(backend.fetch_by_id(&TaskId::from(1u64)).await.map(|_| ())),
                _ => unreachable!(),
            };
            let Poll::Ready(Err(error)) = error else {
                panic!("expected {phase} error")
            };
            assert_eq!(error.to_string(), format!("mapped: {phase}"));
            assert_eq!(*seen.lock().unwrap(), [phase]);
        }
    }

    #[test]
    fn codec_replacement_preserves_context_and_distinguishes_decode_errors() {
        let task = Task::new(7);
        let context = task.ctx().clone();
        let mut backend = Probe {
            tasks: VecDeque::from([task, Task::new(0)]),
            ..Probe::default()
        }
        .with_codec(DecodeNumber);
        let worker = WorkerContext::new("decode");
        let mut cx = Context::from_waker(noop_waker_ref());
        let Poll::Ready(Some(Ok(task))) = backend.poll_next_args(&mut cx, &worker) else {
            panic!("missing task")
        };
        assert_eq!(task.args, 8);
        assert!(Arc::ptr_eq(task.ctx(), &context));
        assert!(matches!(
            backend.poll_next_args(&mut cx, &worker),
            Poll::Ready(Some(Err(PollNextArgsError::DecodeError(_))))
        ));
        backend.fail = Some("next");
        assert!(matches!(
            backend.poll_next_args(&mut cx, &worker),
            Poll::Ready(Some(Err(PollNextArgsError::BackendError(_))))
        ));
        backend.fail = None;
        assert!(matches!(
            backend.poll_next_args(&mut cx, &worker),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn interleave_prioritizes_external_stream_and_stops_polling_after_exhaustion() {
        let mut ended = false;
        let external = stream::poll_fn(move |_| {
            assert!(!ended, "exhausted stream polled again");
            ended = true;
            Poll::Ready(None)
        });
        let probe = Probe {
            tasks: VecDeque::from([Task::new(2)]),
            ..Probe::default()
        };
        let mut backend = probe.interleave(stream::iter([Ok(Task::new(1))]).chain(external));
        let mut cx = Context::from_waker(noop_waker_ref());
        let worker = WorkerContext::new("interleave");
        for expected in [1, 2] {
            let Poll::Ready(Some(Ok(task))) = backend.poll_next(&mut cx, &worker) else {
                panic!("missing task")
            };
            assert_eq!(task.args, expected);
        }
        assert!(matches!(
            backend.poll_next(&mut cx, &worker),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn lifecycle_callbacks_run_in_order_once_and_clones_restart_hooks() {
        let probe = Probe::default();
        let events = probe.events.clone();
        let mut backend = probe
            .before_start(|b| std::future::ready(b.record("before_start")))
            .after_start(|b| std::future::ready(b.record("after_start")))
            .before_stop(|b| std::future::ready(b.record("before_stop")))
            .after_stop(|b| std::future::ready(b.record("after_stop")));
        let worker = WorkerContext::new("lifecycle");
        let mut cx = Context::from_waker(noop_waker_ref());
        for _ in 0..2 {
            assert!(Backend::poll_ready(&mut backend, &mut cx, &worker).is_ready());
        }
        for _ in 0..2 {
            assert!(Backend::poll_close(&mut backend, &mut cx, &worker).is_ready());
        }
        assert_eq!(
            *events.lock().unwrap(),
            [
                "before_start",
                "ready",
                "after_start",
                "ready",
                "before_stop",
                "close",
                "after_stop",
                "close"
            ]
        );
        let mut cloned = backend.clone();
        events.lock().unwrap().clear();
        assert!(Backend::poll_ready(&mut cloned, &mut cx, &worker).is_ready());
        assert_eq!(
            *events.lock().unwrap(),
            ["before_start", "ready", "after_start"]
        );
    }

    #[test]
    fn before_start_propagates_hook_failure_without_polling_backend() {
        let probe = Probe::default();
        let events = probe.events.clone();
        let mut backend = probe.before_start(|_| async { Err(io::Error::other("hook")) });
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            Backend::poll_ready(&mut backend, &mut cx, &WorkerContext::new("hook-error")),
            Poll::Ready(Err(_))
        ));
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn first_sink_readiness_waits_for_inner_sink_after_before_start() {
        let mut backend = Probe {
            pending: Some("sink_ready"),
            ..Probe::default()
        }
        .before_start(|b| std::future::ready(b.record("before_start")));
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(backend.poll_ready_unpin(&mut cx).is_pending());
        assert_eq!(
            *backend.events.lock().unwrap(),
            ["before_start", "sink_ready"]
        );
        backend.pending = None;
        assert!(matches!(
            backend.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            *backend.events.lock().unwrap(),
            ["before_start", "sink_ready", "sink_ready"]
        );
    }

    #[test]
    fn before_start_sink_propagates_hook_and_readiness_errors() {
        for fail in ["before_start", "sink_ready"] {
            let mut backend = Probe {
                fail: Some(fail),
                ..Probe::default()
            }
            .before_start(|b| std::future::ready(b.record("before_start")));
            let mut cx = Context::from_waker(noop_waker_ref());
            let Poll::Ready(Err(error)) = backend.poll_ready_unpin(&mut cx) else {
                panic!("expected {fail} error")
            };
            assert_eq!(error.to_string(), fail);
            let expected = if fail == "before_start" {
                vec!["before_start"]
            } else {
                vec!["before_start", "sink_ready"]
            };
            assert_eq!(*backend.events.lock().unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn before_start_feed_waits_for_delayed_hook() {
        let mut backend = Probe::default().before_start(|b| {
            let result = b.record("before_start");
            async move {
                tokio::task::yield_now().await;
                result
            }
        });
        let events = backend.events.clone();
        let mut feed = Box::pin(backend.feed(Task::new(7)));
        assert!(poll!(&mut feed).is_pending());
        assert_eq!(*events.lock().unwrap(), ["before_start"]);
        feed.await.unwrap();
        assert_eq!(
            *events.lock().unwrap(),
            ["before_start", "sink_ready", "send"]
        );

        let worker = WorkerContext::new("before-start-feed");
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            Backend::poll_ready(&mut backend, &mut cx, &worker),
            Poll::Ready(Ok(()))
        ));
        let Poll::Ready(Some(Ok(task))) = backend.poll_next(&mut cx, &worker) else {
            panic!("missing task")
        };
        assert_eq!(task.args, 7);
        assert!(matches!(
            backend.poll_next(&mut cx, &worker),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn pipe_preserves_readiness_and_shutdown_boundaries() {
        let source = Probe {
            tasks: VecDeque::from([Task::new(1)]),
            ..Probe::default()
        };
        let source_events = source.events.clone();
        let destination = Probe {
            pending: Some("ready"),
            ..Probe::default()
        };
        let mut backend = source.pipe_to(destination);
        let worker = WorkerContext::new("pipe");
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(Backend::poll_ready(&mut backend, &mut cx, &worker).is_pending());
        assert_eq!(*source_events.lock().unwrap(), ["ready"]);
        backend.pending = Some("flush");
        let Poll::Ready(Some(Ok(task))) = backend.poll_next(&mut cx, &worker) else {
            panic!("missing piped task")
        };
        assert_eq!(task.args, 1);
        assert!(Backend::poll_close(&mut backend, &mut cx, &worker).is_ready());
        assert!(!source_events.lock().unwrap().contains(&"close"));
        assert!(backend.events.lock().unwrap().contains(&"close"));
    }

    #[derive(Clone)]
    struct FixedStrategy {
        result: Poll<Option<()>>,
        snapshots: Arc<Mutex<Vec<poll_strategy::PollSnapshot>>>,
    }
    impl PollStrategy for FixedStrategy {
        fn poll_drive(
            &mut self,
            _: &mut Context<'_>,
            _: &poll_strategy::PollSnapshot,
        ) -> Poll<Option<()>> {
            self.result
        }
        fn on_poll(&mut self, snapshot: &poll_strategy::PollSnapshot) {
            self.snapshots.lock().unwrap().push(*snapshot);
        }
    }

    #[test]
    fn pending_or_exhausted_strategy_still_polls_backend_and_records_metrics() {
        for result in [Poll::Pending, Poll::Ready(None)] {
            let snapshots = Arc::new(Mutex::new(Vec::new()));
            let strategy = FixedStrategy {
                result,
                snapshots: snapshots.clone(),
            };
            let mut backend = Probe::default().poll_with_strategy(strategy);
            let mut cx = Context::from_waker(noop_waker_ref());
            let worker = WorkerContext::new("poll-with");
            assert!(matches!(
                backend.poll_next(&mut cx, &worker),
                Poll::Ready(None)
            ));
            backend.pending = Some("next");
            assert!(backend.poll_next(&mut cx, &worker).is_pending());
            let snapshots = snapshots.lock().unwrap();
            assert_eq!(snapshots[0].total_ready, 1);
            assert_eq!(
                snapshots[0].activity(),
                poll_strategy::ActivityState::WakingUp
            );
            assert_eq!(snapshots[1].total_pending, 1);
            assert_eq!(snapshots[1].activity(), poll_strategy::ActivityState::Idle);
        }
    }

    #[test]
    fn combined_strategy_treats_exhaustion_as_ready_without_forwarding_on_poll() {
        let snapshots = Arc::new(Mutex::new(Vec::new()));
        let mut strategy = poll_strategy::Strategy::new().apply(FixedStrategy {
            result: Poll::Ready(None),
            snapshots: snapshots.clone(),
        });
        let mut cx = Context::from_waker(noop_waker_ref());
        assert_eq!(
            strategy.poll_drive(&mut cx, &poll_strategy::PollSnapshot::new()),
            Poll::Ready(Some(()))
        );
        strategy.on_poll(&poll_strategy::PollSnapshot::new());
        assert!(snapshots.lock().unwrap().is_empty());
    }

    #[test]
    fn wake_on_push_registers_on_next_and_only_wakes_after_successful_send() {
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
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = count.clone().into();
        let mut cx = Context::from_waker(&waker);
        let worker = WorkerContext::new("wake-on-push");
        let mut backend = Probe::default().wake_on_push();
        assert!(Backend::poll_ready(&mut backend, &mut cx, &worker).is_ready());
        backend.start_send_unpin(Task::new(1)).unwrap();
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
        assert!(backend.poll_next(&mut cx, &worker).is_ready());
        backend.start_send_unpin(Task::new(2)).unwrap();
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        backend.fail = Some("send");
        assert!(backend.start_send_unpin(Task::new(3)).is_err());
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
    }
}
