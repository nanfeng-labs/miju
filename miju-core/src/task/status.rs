//! Recorded task states and shared observation of state changes.
//!
//! [`Status`] is a copyable state value suitable for recording an outcome.
//! [`AtomicStatus`] holds a state shared between observers, so changes are
//! visible without replacing a task's execution context.
//!
//! These types represent progress but do not drive execution. Updating a cell
//! neither starts nor stops a task, and no lifecycle transitions are validated.
//! The execution integration decides when a transition is appropriate.
//!
//! # State Names
//!
//! Display and parsing use `Pending`, `Queued`, `Running`, `Done`, `Failed`, and
//! `Killed`. Parsing is case-sensitive and does not trim whitespace:
//!
//! ```
//! use miju_core::task::status::Status;
//!
//! assert_eq!("Running".parse::<Status>()?, Status::Running);
//! assert_eq!(Status::Done.to_string(), "Done");
//! assert!("running".parse::<Status>().is_err());
//! assert!("Done ".parse::<Status>().is_err());
//! # Ok::<(), miju_core::task::status::StatusError>(())
//! ```

use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

/// A task's recorded execution state.
///
/// The default is [`Pending`](Self::Pending). Copying this value produces a
/// snapshot; use [`AtomicStatus`] to observe later changes. Backends and workers
/// are responsible for enforcing allowed transitions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Status {
    /// Waiting to become eligible for execution.
    #[default]
    Pending = 0,
    /// Available for a worker to receive.
    Queued = 1,
    /// Being executed by a worker.
    Running = 2,
    /// Completed successfully.
    Done = 3,
    /// Completed with an error.
    Failed = 4,
    /// Execution was terminated.
    Killed = 5,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pending => "Pending",
            Self::Queued => "Queued",
            Self::Running => "Running",
            Self::Done => "Done",
            Self::Failed => "Failed",
            Self::Killed => "Killed",
        })
    }
}

/// An unrecognized, case-sensitive state name.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StatusError {
    /// Preserves the input that could not be parsed.
    UnknownState(String),
}

impl fmt::Display for StatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownState(value) => write!(f, "unknown task status: {value:?}"),
        }
    }
}

impl std::error::Error for StatusError {}

impl FromStr for Status {
    type Err = StatusError;

    /// Parses an exact state name as produced by [`Display`](fmt::Display).
    ///
    /// # Errors
    ///
    /// Returns [`StatusError::UnknownState`] for an unrecognized name, preserving
    /// the input without trimming or case normalization.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Pending" => Ok(Self::Pending),
            "Queued" => Ok(Self::Queued),
            "Running" => Ok(Self::Running),
            "Done" => Ok(Self::Done),
            "Failed" => Ok(Self::Failed),
            "Killed" => Ok(Self::Killed),
            _ => Err(StatusError::UnknownState(value.to_owned())),
        }
    }
}

/// An atomic status cell shared by all of its clones.
///
/// A default cell contains [`Status::Pending`]. Cloning the cell keeps observing
/// the same state; [`load`](Self::load) returns an independent value representing
/// the state at the time of the read.
///
/// # Example
///
/// ```
/// use miju_core::task::status::{AtomicStatus, Status};
///
/// let status = AtomicStatus::default();
/// let observer = status.clone();
/// let snapshot = status.load();
///
/// status.store(Status::Running);
/// assert_eq!(observer.load(), Status::Running);
/// assert_eq!(snapshot, Status::Pending);
/// ```
#[derive(Clone, Debug, Default)]
pub struct AtomicStatus {
    value: Arc<AtomicU8>,
}

impl AtomicStatus {
    /// Creates an independent status cell.
    ///
    /// Use [`Clone::clone`] to share an existing cell instead.
    #[must_use]
    pub fn new(status: Status) -> Self {
        Self {
            value: Arc::new(AtomicU8::new(status as u8)),
        }
    }

    /// Reads the current state.
    #[must_use]
    pub fn load(&self) -> Status {
        Self::decode(self.value.load(Ordering::SeqCst))
    }

    /// Replaces the current state without validating a lifecycle transition.
    pub fn store(&self, status: Status) {
        self.value.store(status as u8, Ordering::SeqCst);
    }

    /// Replaces the current state and returns the previous one.
    ///
    /// Like [`store`](Self::store), this accepts any [`Status`] without checking
    /// a lifecycle transition. The read and replacement are one atomic operation.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::status::{AtomicStatus, Status};
    /// let status = AtomicStatus::new(Status::Running);
    /// assert_eq!(status.swap(Status::Done), Status::Running);
    /// assert_eq!(status.load(), Status::Done);
    /// ```
    #[must_use]
    pub fn swap(&self, status: Status) -> Status {
        Self::decode(self.value.swap(status as u8, Ordering::SeqCst))
    }

    fn decode(value: u8) -> Status {
        // Only typed Status values enter the private cell, so each stored value has a variant.
        match value {
            0 => Status::Pending,
            1 => Status::Queued,
            2 => Status::Running,
            3 => Status::Done,
            4 => Status::Failed,
            5 => Status::Killed,
            _ => unreachable!("status cells only store Status discriminants"),
        }
    }
}

impl From<Status> for AtomicStatus {
    fn from(value: Status) -> Self {
        Self::new(value)
    }
}

impl From<AtomicStatus> for Status {
    fn from(value: AtomicStatus) -> Self {
        value.load()
    }
}
