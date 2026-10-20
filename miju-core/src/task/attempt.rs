//! Shared tracking of task execution attempts.
//!
//! [`Attempt`] holds a count that can be read and incremented through shared
//! references. Clones refer to the same counter, allowing a task and its
//! observers to see updates without replacing their execution contexts.
//!
//! The counter starts at zero unless restored from an existing value. It does
//! not count executions automatically or enforce an attempt limit; the caller
//! decides when an attempt begins and increments it accordingly.
//!
//! # Example
//!
//! ```
//! use miju_core::task::attempt::Attempt;
//!
//! let attempts = Attempt::new();
//! let observer = attempts.clone();
//! let previous = attempts.increment();
//!
//! assert_eq!(previous, 0);
//! assert_eq!(observer.current(), 1);
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// An atomic attempt count shared by all of its clones.
///
/// Use [`current`](Self::current) to obtain a numeric snapshot and
/// [`new_with_value`](Self::new_with_value) to create an independent counter
/// from a recorded count. Cloning preserves shared observation instead.
#[derive(Clone, Debug, Default)]
pub struct Attempt {
    count: Arc<AtomicUsize>,
}

impl Attempt {
    /// Starts a counter at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Restores a counter from a recorded value.
    ///
    /// The new counter is independent of any counter that supplied `value`.
    #[must_use]
    pub fn new_with_value(value: usize) -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(value)),
        }
    }

    /// Reads the current number of attempts.
    ///
    /// Another observer may increment the counter after this read.
    #[must_use]
    pub fn current(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Increments the counter and returns its previous value.
    ///
    /// The returned value is the count before this increment, even when other
    /// observers are updating the counter. Adding one to [`usize::MAX`] wraps
    /// the stored count to zero.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::attempt::Attempt;
    /// let attempts = Attempt::new_with_value(2);
    /// assert_eq!(attempts.increment(), 2);
    /// assert_eq!(attempts.current(), 3);
    /// ```
    #[allow(
        clippy::must_use_candidate,
        reason = "incrementing is useful without the previous value"
    )]
    pub fn increment(&self) -> usize {
        self.count.fetch_add(1, Ordering::SeqCst)
    }
}
