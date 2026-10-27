use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures_sink::Sink;
use tower_layer::Identity;

use crate::{
    backend::{Backend, BackendConfig, finalize::Ephemeral},
    error::BoxDynError,
    task::{
        Task,
        task_id::{RandomId, TaskId},
    },
    worker::context::WorkerContext,
};

/// A simple in-memory backend that uses a `VecDeque` to store tasks.
///
/// This backend is primarily intended for testing and demonstration purposes. It does not persist tasks and is not suitable for production use.
#[derive(Debug, Clone)]
pub struct VecDequeBackend<T> {
    queue: Arc<Mutex<VecDeque<Task<T>>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Default for VecDequeBackend<T> {
    fn default() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            waker: Arc::new(Mutex::new(None)),
        }
    }
}

impl<T> VecDequeBackend<T> {
    /// Create a new `VecDequeBackend` with default capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new `VecDequeBackend` with a specified capacity for the internal queue.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            waker: Arc::new(Mutex::new(None)),
        }
    }
}

/// Error type for VecDequeBackend
#[derive(Debug, thiserror::Error, Clone)]
#[non_exhaustive]
pub enum VecDequeError {
    /// Error occurred during polling
    #[error("Polling error: {0}")]
    PollError(Arc<BoxDynError>),
    /// Error occurred during sending
    #[error("Sending error: {0}")]
    SendError(Arc<BoxDynError>),
}

impl<T> Backend for VecDequeBackend<T> {
    type Task = Task<T>;
    type Error = VecDequeError;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if self
            .queue
            .lock()
            .map_err(|e| VecDequeError::PollError(Arc::new(e.to_string().into())))?
            .is_empty()
        {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            // A producer may have sent before the waker was registered.
            if self
                .queue
                .lock()
                .map_err(|e| VecDequeError::PollError(Arc::new(e.to_string().into())))?
                .is_empty()
            {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let task = self
            .queue
            .lock()
            .map_err(|e| VecDequeError::PollError(Arc::new(e.to_string().into())))?
            .pop_front();
        if let Some(task) = task {
            return Poll::Ready(Some(Ok(task)));
        }

        // Waker callbacks may reenter the backend, so run them outside the locks.
        let waker = cx.waker().clone();
        let previous = self.waker.lock().unwrap().replace(waker);
        drop(previous);

        // Recheck after registering so a send cannot be missed.
        match self
            .queue
            .lock()
            .map_err(|e| VecDequeError::PollError(Arc::new(e.to_string().into())))?
            .pop_front()
        {
            Some(task) => Poll::Ready(Some(Ok(task))),
            None => Poll::Pending,
        }
    }

    fn poll_close(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<T> Sink<Task<T>> for VecDequeBackend<T>
where
    T: Send + Unpin + 'static,
{
    type Error = VecDequeError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, mut item: Task<T>) -> Result<(), Self::Error> {
        let this = self.get_mut();

        let mut tasks = this
            .queue
            .lock()
            .map_err(|e| VecDequeError::SendError(Arc::new(e.to_string().into())))?;

        if let Some(ref key) = item.idempotency_key() {
            let exists = tasks.iter().any(|task| {
                task.idempotency_key()
                    .as_ref()
                    .map(|existing| existing == key)
                    .unwrap_or(false)
            });

            if exists {
                return Ok(());
            }
        }

        if item.task_id().is_none() {
            item = item
                .into_builder()
                .task_id(TaskId::from_string(RandomId::default()))
                .build();
        }

        tasks.push_back(item);

        if let Some(waker) = this.waker.lock().unwrap().as_ref() {
            waker.wake_by_ref();
        }

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<Args> BackendConfig for VecDequeBackend<Args> {
    type Args = Args;
    type Id = RandomId;

    type Kind = Ephemeral;

    type Config = ();

    type Layer = Identity;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _worker: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}
