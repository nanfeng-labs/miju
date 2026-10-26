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

#[cfg(all(test, feature = "sleep"))]
mod tests {
    use std::{
        sync::{Arc, mpsc},
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use futures_util::SinkExt;

    use crate::{
        backend::{
            Backend,
            ext::poll_strategy::{IntervalStrategy, PollStrategy, PollWith},
            memory::MemoryStorage,
        },
        task::Task,
        worker::context::WorkerContext,
    };

    struct Notify(mpsc::Sender<()>);

    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.0.send(());
        }
    }

    fn assert_idle_backend_keeps_waking(strategy: impl PollStrategy) {
        let mut backend = PollWith::new(MemoryStorage::<u32>::new(), strategy);
        let worker = WorkerContext::new("idle-polling");
        let (tx, rx) = mpsc::channel();
        let waker = Waker::from(Arc::new(Notify(tx)));
        let mut cx = Context::from_waker(&waker);
        assert!(backend.poll_ready(&mut cx, &worker).is_ready());

        for tick in 1..=3 {
            assert!(backend.poll_next(&mut cx, &worker).is_pending());
            assert!(
                rx.recv_timeout(Duration::from_secs(2)).is_ok(),
                "idle backend did not receive wake {tick}"
            );
        }

        assert!(backend.poll_ready_unpin(&mut cx).is_ready());
        backend.start_send_unpin(Task::new(42)).unwrap();
        let Poll::Ready(Some(Ok(task))) = backend.poll_next(&mut cx, &worker) else {
            panic!("queued task was not returned");
        };
        assert_eq!(task.args, 42);
    }

    #[test]
    fn interval_keeps_waking_an_idle_backend() {
        let interval = Duration::from_millis(10);
        assert_idle_backend_keeps_waking(IntervalStrategy::new(interval));
    }

    #[test]
    fn zero_interval_keeps_waking_an_idle_backend() {
        assert_idle_backend_keeps_waking(IntervalStrategy::new(Duration::ZERO));
    }
}
