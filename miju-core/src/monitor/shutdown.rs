//! A shutdown monitor that can be used to signal shutdown to tasks and workers.
//!
//! ## Overview
//! It provides a future that resolves when a shutdown is initiated, allowing tasks to await shutdown
//! signals and perform cleanup operations.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use futures_core::Future;

/// A shutdown token that stops execution
#[derive(Debug)]
pub struct Shutdown {
    inner: Arc<ShutdownCtx>,
    key: usize,
}

impl Shutdown {
    /// Create a new shutdown handle
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownCtx::new()),
            key: 0,
        }
    }

    /// Set the future to await before shutting down
    pub fn shutdown_after<F: Future>(&self, f: F) -> impl Future<Output = F::Output> + use<F> {
        let handle = self.clone();
        async move {
            let result = f.await;
            handle.start_shutdown();
            result
        }
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        Self {
            key: self.inner.next_key.fetch_add(1, Ordering::Relaxed),
            inner: self.inner.clone(),
        }
    }
}

impl Drop for Shutdown {
    fn drop(&mut self) {
        let removed = self.inner.wakers.lock().unwrap().remove(&self.key);
        drop(removed);
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct ShutdownCtx {
    state: AtomicBool,
    wakers: Mutex<HashMap<usize, Waker>>,
    next_key: AtomicUsize,
}
impl ShutdownCtx {
    fn new() -> Self {
        Self {
            state: AtomicBool::default(),
            wakers: Mutex::default(),
            next_key: AtomicUsize::new(1),
        }
    }
    fn shutdown(&self) {
        self.state.store(true, Ordering::Release);
        self.wake();
    }

    fn is_shutting_down(&self) -> bool {
        self.state.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap());
        for waker in wakers.into_values() {
            waker.wake();
        }
    }
}

impl Shutdown {
    /// Check if the system is shutting down
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.inner.is_shutting_down()
    }

    /// Start the shutdown process
    pub fn start_shutdown(&self) {
        self.inner.shutdown()
    }

    pub(crate) fn poll_shutdown(&self, cx: &Context<'_>) -> Poll<()> {
        let (ready, replaced) = {
            let mut wakers = self.inner.wakers.lock().unwrap();
            if self.inner.is_shutting_down() {
                (true, wakers.remove(&self.key))
            } else {
                (false, wakers.insert(self.key, cx.waker().clone()))
            }
        };
        drop(replaced);
        if ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_shutdown(cx)
    }
}
