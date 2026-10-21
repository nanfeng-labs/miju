//! A backend wrapper that drives a polling strategy and records poll statistics.
//!
//! Each `poll_next` drives the strategy, then polls the backend regardless of
//! whether the strategy returns `Pending`, a wakeup, or exhaustion. Strategies
//! provide wakeups; they do not gate calls to the backend. Every ready result,
//! including an error or exhaustion, counts as ready in the polling metrics.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{
    backend::{
        ext::poll_strategy::{PollMetrics, PollStrategy},
        *,
    },
    task::Task,
    worker::context::WorkerContext,
};

/// A backend that uses a polling strategy to wake the worker.
#[derive(Debug, Clone)]
pub struct PollWith<B, S> {
    backend: B,
    strategy: S,
    poll_metrics: PollMetrics,
}

impl<B, S> PollWith<B, S> {
    /// Create a new `PollWith` wrapping the given backend and strategy.
    pub fn new(backend: B, strategy: S) -> Self {
        Self {
            backend,
            strategy,
            poll_metrics: PollMetrics::default(),
        }
    }
}

impl<B, S> Backend for PollWith<B, S>
where
    B: Backend,
    S: PollStrategy,
{
    type Task = B::Task;
    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let snapshot = self.poll_metrics.snapshot();

        let _ = self.strategy.poll_drive(cx, &snapshot);

        match self.backend.poll_next(cx, worker) {
            Poll::Ready(task) => {
                self.poll_metrics.on_ready();
                self.strategy.on_poll(&self.poll_metrics.snapshot());
                Poll::Ready(task)
            }

            Poll::Pending => {
                self.poll_metrics.on_pending();
                self.strategy.on_poll(&self.poll_metrics.snapshot());
                Poll::Pending
            }
        }
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend.poll_close(cx, worker)
    }
}

delegate_sink!(PollWith<B, S>, backend);

delegate_codec!(PollWith<B, S>, backend);

delegate_deref!(PollWith<B, S>, backend);

delegate_config!(PollWith<B, S>, backend);

delegate_expose!(
    impl<B, S> for PollWith<B, S>
    where {
        B: Send + Sync,
        S: PollStrategy + Send + Sync,
        B::Task: Send
    }
    => backend
);
