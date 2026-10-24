//! # Pipe streams to backends
//!
//! This backend allows you to pipe tasks from any stream into another backend.
//! It is useful for connecting different backends together, such as piping tasks
//! from a cron stream into a database backend, or transforming and forwarding tasks
//! between systems.
//!
//! ## Example
//!
//! ```rust
//! # use futures_util::stream;
//! # use miju_core::backend::{ext::pipe::PipeExt, memory::MemoryStorage};
//! # use miju_core::worker::{builder::WorkerBuilder, context::WorkerContext};
//! # use miju_core::error::BoxDynError;
//! # use std::time::Duration;
//! # use futures_util::StreamExt;
//! # use miju_core::worker::ext::event_listener::EventListenerExt;
//! #[tokio::main]
//! async fn main() {
//!     let stm = stream::iter(0..10).map(|s| Ok::<_, std::io::Error>(s));
//!
//!     let in_memory = MemoryStorage::new();
//!     let backend = stm.pipe_to(in_memory);
//!
//!     async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//! #        if task == 9 {
//! #            worker.stop().unwrap();
//! #        }
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(backend)
//!         .on_event(|_worker, ev| {
//!             println!("On Event = {ev:?}");
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! This example pipes a stream of numbers into an in-memory backend and processes them with a worker.
//!
//! Readiness depends on both backends. An empty destination that reports
//! `Pending` from `Backend::poll_ready` can prevent the worker from reaching
//! the transfer step. Shutdown closes only the destination backend.
use std::fmt::Debug;
use std::fmt::{self};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::backend::*;
use crate::error::BoxDynError;
use crate::task::Task;
use crate::worker::context::WorkerContext;
use futures_core::stream::BoxStream;
use futures_core::{Stream, ready};
use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::TryStreamExt;

/// A pipe that transfers tasks from a source backend into a destination backend.
///
/// Use [`PipeExt`] to adapt a plain stream into the source backend.
#[doc = features_table! {
    setup = "{ unreachable!() }",
    TaskSink => supported("Ability to push new tasks", false),
    InheritsFeatures => limited("Inherits features from the underlying backend", false),
}]
pub struct Pipe<Dst, S> {
    pub(crate) from: S,
    pub(crate) into: Dst,
}

impl<S, Dst> Pipe<Dst, S> {
    /// Create a new `Pipe` from a raw `from` source and an `into` sink.
    /// Prefer [`PipeExt::pipe_to`] or [`BackendExt::pipe_to`] over calling
    /// this directly.
    ///
    /// [`BackendExt::pipe_to`]: crate::backend::ext::BackendExt
    pub fn new(from: S, into: Dst) -> Self {
        Self { from, into }
    }
}

impl<S: fmt::Debug, Dst: fmt::Debug> fmt::Debug for Pipe<Dst, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field("from", &self.from)
            .field("into", &self.into)
            .finish()
    }
}

impl<Dst: Clone, S: Clone> Clone for Pipe<Dst, S> {
    fn clone(&self) -> Self {
        Self {
            from: self.from.clone(),
            into: self.into.clone(),
        }
    }
}

impl<S, TSink, Args, Kind, Err> Backend for Pipe<TSink, S>
where
    S: Backend<Task = Task<Args>, Error = Err> + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    TSink: BackendConfig<Kind = Kind> + Backend + TaskSink<Args, Kind> + Unpin + Send + 'static,
    <TSink as Backend>::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static,
{
    type Task = TSink::Task;

    type Error = PipeError;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        wkr: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_ready: polling source");

        ready!(S::poll_ready(&mut self.from, cx, wkr)).map_err(|e| {
            trace!(error = ?e, "poll_ready: source returned error");
            PipeError::Inner(e.into())
        })?;

        trace!("poll_ready: source ready, polling destination");

        ready!(Backend::poll_ready(&mut self.into, cx, wkr)).map_err(|e| {
            trace!(error = ?e, "poll_ready: destination returned error");
            PipeError::Inner(e.into())
        })?;

        trace!("poll_ready: ready");

        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        loop {
            match TaskSink::poll_ready(Pin::new(&mut self.into), cx) {
                Poll::Ready(Ok(())) => {
                    trace!("poll_next: destination ready");
                }
                Poll::Ready(Err(e)) => {
                    trace!(error = ?e, "poll_next: destination poll_ready failed");
                    return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                }
                Poll::Pending => break,
            }

            match self.from.poll_next(cx, worker) {
                Poll::Ready(Some(Ok(task))) => {
                    trace!("poll_next: received task from source");

                    if let Err(e) = TaskSink::start_send(Pin::new(&mut self.into), task) {
                        trace!(error = ?e, "poll_next: destination start_send failed");
                        return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                    }

                    trace!("poll_next: task sent to destination");
                }
                Poll::Ready(Some(Err(e))) => {
                    trace!(error = ?e, "poll_next: source returned error");
                    return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                }
                Poll::Ready(None) => {
                    trace!("poll_next: source closed");
                    break;
                }
                Poll::Pending => break,
            }
        }

        match TSink::poll_flush(Pin::new(&mut self.into), cx) {
            Poll::Ready(Err(e)) => {
                trace!(error = ?e, "poll_next: destination flush failed");
                return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
            }
            Poll::Ready(Ok(())) => {
                trace!("poll_next: destination flushed");
            }
            Poll::Pending => {}
        }

        if let Err(e) = ready!(self.into.poll_ready(cx, worker)) {
            trace!(error = ?e, "poll_next: destination backend poll_ready failed");
            return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
        }

        self.into.poll_next(cx, worker).map_err(|e| {
            trace!(error = ?e, "poll_next: destination backend returned error");
            PipeError::Inner(e.into())
        })
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_close: closing destination");

        self.into.poll_close(cx, worker).map_err(|e| {
            trace!(error = ?e, "poll_close: destination returned error");
            PipeError::Inner(e.into())
        })
    }
}

/// Utility for piping a plain stream of `Result<Args, Err>` into a backend.
pub trait PipeExt<B, Args>
where
    B: Backend,
{
    /// Pipe the current stream into the provided sink backend.
    fn pipe_to(self, backend: B) -> Pipe<B, BoxStream<'static, Result<Args, PipeError>>>;
}

impl<B, Args, Err, S> PipeExt<B, Args> for S
where
    B: Backend + Unpin + Send + 'static,
    S: Stream<Item = Result<Args, Err>> + Send + Unpin + 'static,
    Err: Into<BoxDynError> + Send + Sync + 'static,
    Args: 'static,
{
    fn pipe_to(self, backend: B) -> Pipe<B, BoxStream<'static, Result<Args, PipeError>>> {
        Pipe::new(
            Box::pin(self.map_err(|e| PipeError::Inner(e.into()))),
            backend,
        )
    }
}

/// Error encountered while piping streams
#[derive(Debug, thiserror::Error)]
pub enum PipeError {
    /// An empty-stream error variant; the pipe does not emit it on source exhaustion.
    #[error("The inner stream provided a None")]
    EmptyStream,
    /// An inner stream error occurred
    #[error("The inner stream error: {0}")]
    Inner(BoxDynError),
}

impl<Dst, S, T, Err> Sink<T> for Pipe<Dst, S>
where
    Dst: Sink<T, Error = Err> + Unpin,
    S: Unpin,
    Err: Into<BoxDynError> + Send + Sync,
{
    type Error = PipeError;
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.get_mut()
            .into
            .start_send_unpin(item)
            .map_err(|e| PipeError::Inner(e.into()))
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .into
            .poll_ready_unpin(cx)
            .map_err(|e| PipeError::Inner(e.into()))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .into
            .poll_flush_unpin(cx)
            .map_err(|e| PipeError::Inner(e.into()))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .into
            .poll_close_unpin(cx)
            .map_err(|e| PipeError::Inner(e.into()))
    }
}

delegate_config!(Pipe<Dst, S>, into);

delegate_deref!(Pipe<Dst, S>, into);

delegate_codec!(Pipe<Dst, S>, into);

delegate_expose!(
    impl<B, S, Args, Kind> for Pipe<B, S>
    where {
        S: Backend<Task = Task<Args>> + Send + Sync + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
        B: BackendConfig<Kind = Kind>
        + Backend
        + TaskSink<Args, Kind>
        + Unpin
        + Send
        + 'static,
        <B as Backend>::Error: std::error::Error + Send + Sync + 'static,
        Args: Send + 'static,
    }
    => into,
    wrap = |this, result| result.map_err(|e| PipeError::Inner(e.into()))
);

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
        time::Duration,
    };

    use futures_channel::oneshot;
    use futures_util::{FutureExt, StreamExt, stream, task::noop_waker_ref};

    use crate::{
        backend::{
            dequeue::{VecDequeBackend, VecDequeError},
            ext::BackendExt,
            memory::MemoryStorage,
        },
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    use super::*;

    struct ReadinessGate<B> {
        inner: B,
        gate: Option<oneshot::Receiver<bool>>,
        next_calls: usize,
    }

    impl<B: Backend<Error = VecDequeError>> Backend for ReadinessGate<B> {
        type Task = B::Task;
        type Error = B::Error;

        fn poll_ready(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            if let Some(gate) = &mut self.gate {
                let allowed = ready!(gate.poll_unpin(cx)).unwrap();
                self.gate = None;
                if !allowed {
                    return Poll::Ready(Err(VecDequeError::PollError(Arc::new(
                        io::Error::other("readiness failed").into(),
                    ))));
                }
            }
            self.inner.poll_ready(cx, worker)
        }

        fn poll_next(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
            self.next_calls += 1;
            self.inner.poll_next(cx, worker)
        }

        fn poll_close(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_close(cx, worker)
        }
    }

    delegate_config!(ReadinessGate<B>, inner);
    delegate_sink!(ReadinessGate<B>, inner);

    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn destination_readiness_pending_blocks_next_until_woken() {
        let mut queue = VecDequeBackend::new();
        queue.start_send_unpin(Task::new(1u32)).unwrap();
        let destination = ReadinessGate {
            inner: queue,
            gate: None,
            next_calls: 0,
        };
        let mut pipe = stream::iter([Ok::<_, io::Error>(2u32)]).pipe_to(destination);
        let worker = WorkerContext::new("pipe-readiness");
        let wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = wake.clone().into();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            Backend::poll_ready(&mut pipe, &mut cx, &worker),
            Poll::Ready(Ok(()))
        ));

        let (tx, rx) = oneshot::channel();
        pipe.into.gate = Some(rx);
        assert!(pipe.poll_next(&mut cx, &worker).is_pending());
        assert_eq!(pipe.into.next_calls, 0);
        wake.0.store(0, Ordering::SeqCst);
        tx.send(true).unwrap();
        assert!(wake.0.load(Ordering::SeqCst) > 0);
        for expected in [1, 2] {
            let Poll::Ready(Some(Ok(task))) = pipe.poll_next(&mut cx, &worker) else {
                panic!("missing task after readiness resumed");
            };
            assert_eq!(task.args, expected);
        }
        assert_eq!(pipe.into.next_calls, 2);
    }

    #[test]
    fn destination_readiness_error_is_returned_without_polling_next() {
        let (tx, rx) = oneshot::channel();
        tx.send(false).unwrap();
        let destination = ReadinessGate {
            inner: VecDequeBackend::<u32>::new(),
            gate: Some(rx),
            next_calls: 0,
        };
        let mut pipe = stream::empty::<io::Result<u32>>().pipe_to(destination);
        let worker = WorkerContext::new("pipe-readiness-error");
        let mut cx = Context::from_waker(noop_waker_ref());
        let Poll::Ready(Some(Err(error))) = pipe.poll_next(&mut cx, &worker) else {
            panic!("missing readiness error");
        };
        assert!(error.to_string().contains("readiness failed"));
        assert_eq!(pipe.into.next_calls, 0);
    }

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_worker() {
        let stm = stream::iter(0..ITEMS).map(Ok::<_, io::Error>);
        let in_memory = MemoryStorage::new();

        let backend = stm.pipe_to(in_memory);

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                worker.stop().unwrap();
                return Err("Graceful Exit".into());
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_worker, ev| {
                println!("On Event = {ev:?}");
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn dequeue_to_memory_worker() {
        let dequeue = VecDequeBackend::new();

        let mut in_memory = MemoryStorage::new();

        in_memory.push(42).await.unwrap();

        let mut backend = in_memory.pipe_to(dequeue);

        backend.push(43).await.unwrap();

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == 42 {
                worker.stop().unwrap();
                return Err("Graceful Exit".into());
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_worker, ev| {
                println!("On Event = {ev:?}");
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
