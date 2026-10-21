//! Backend lifecycle hooks.
//!
//! This module provides wrappers for running asynchronous callbacks at
//! different points in a backend's lifecycle.
//!
//! Hooks are executed at most once and receive a mutable reference to the
//! wrapped backend, allowing them to perform initialization or cleanup work.
//!
//! The hooks are executed in the following order:
//!
//! ```text
//! before_start
//!     ↓
//! first poll_ready
//!     ↓
//! after_start
//!     ↓
//! ... backend operation ...
//!     ↓
//! before_stop
//!     ↓
//! poll_close
//!     ↓
//! after_stop
//! ```
//!
//! # Hooks
//!
//! * [`BeforeStart`] runs once before the backend's first `poll_ready` call.
//! * [`AfterStart`] runs once after the backend's first successful `poll_ready`.
//! * [`BeforeStop`] runs once before the backend's `poll_close` call.
//! * [`AfterStop`] runs once after the backend has stopped and cleaned up.
//!
//! The [`BackendExt`] trait provides convenience methods for attaching these
//! hooks to a backend.
//!
//! # Examples
//!
//! A backend can be configured with lifecycle hooks using the extension
//! methods provided by [`BackendExt`]:
//!
//! ```rust
//! # use miju_core::backend::{dequeue::VecDequeBackend, ext::BackendExt};
//! # let backend = VecDequeBackend::<u32>::new();
//! let backend = backend
//!     .before_start(|backend| async {
//!         // Initialize the backend.
//!         Ok(())
//!     })
//!     .after_start(|backend| async {
//!         // Backend is ready.
//!         Ok(())
//!     })
//!     .before_stop(|backend| async {
//!         // Prepare for shutdown.
//!         Ok(())
//!     })
//!     .after_stop(|backend| async {
//!         // Perform final cleanup.
//!         Ok(())
//!     });
//! ```
//!
//! Errors returned by a hook are propagated as backend errors and can prevent
//! the corresponding lifecycle transition from completing. After a hook returns
//! an error, stop polling that wrapper: its completed future remains stored.
//! Cloning a wrapper resets its hook state, so the clone runs the hook again.
//!
//! [`BackendExt`]: super::BackendExt
use std::{
    sync::Arc,
    task::{Context, Poll},
};

mod after_start;
mod after_stop;
mod before_start;
mod before_stop;

pub use after_start::AfterStart;
pub use after_stop::AfterStop;
pub use before_start::BeforeStart;
pub use before_stop::BeforeStop;

use crate::backend::future::BoxSyncFuture;

type HookFuture<E> = BoxSyncFuture<Result<(), E>>;

/// One-shot lifecycle hook state, shared by all `*_hook` wrappers below.
enum HookState<E> {
    /// Not yet triggered.
    Pending,
    /// Hook future currently running.
    Running(HookFuture<E>),
    /// Hook completed successfully; delegate as normal from now on.
    Done,
}

impl<E> std::fmt::Debug for HookState<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("Pending"),
            Self::Running(_) => f.write_str("Running"),
            Self::Done => f.write_str("Done"),
        }
    }
}

impl<E> HookState<E> {
    /// Drives the hook to completion, invoking `make_fut` lazily on first poll.
    /// Returns `Poll::Pending` while the hook is running, `Poll::Ready(Err)` if
    /// it fails, and `Poll::Ready(Ok(()))` once done (including if already done).
    fn poll_hook(
        &mut self,
        cx: &mut Context<'_>,
        make_fut: impl FnOnce() -> HookFuture<E>,
    ) -> Poll<Result<(), E>> {
        if matches!(self, Self::Pending) {
            *self = Self::Running(make_fut());
        }
        if let Self::Running(fut) = self {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => *self = Self::Done,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

type FnHandler<B, Err> =
    Arc<dyn Fn(&mut B) -> BoxSyncFuture<Result<(), Err>> + 'static + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, future::poll_fn, task::noop_waker_ref};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn failed_hook_retains_future_for_subsequent_poll() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = polls.clone();
        let mut state = HookState::Pending;
        let mut cx = Context::from_waker(noop_waker_ref());
        let result = state.poll_hook(&mut cx, || {
            BoxSyncFuture::new(
                poll_fn(move |_| {
                    counted.fetch_add(1, Ordering::Relaxed);
                    Poll::Ready(Err::<(), _>("failed"))
                })
                .boxed(),
            )
        });
        assert_eq!(result, Poll::Ready(Err("failed")));
        assert!(matches!(state, HookState::Running(_)));
        assert_eq!(
            state.poll_hook(&mut cx, || panic!("factory invoked twice")),
            Poll::Ready(Err("failed"))
        );
        assert_eq!(polls.load(Ordering::Relaxed), 2);
    }
}
