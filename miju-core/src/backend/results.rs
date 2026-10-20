//! Recorded outcomes for individual execution attempts.
//!
//! [`TaskResult`] associates a task identifier and attempt number with a status
//! and either an output value or a failure description. Integrations can use
//! it when saving or returning completed attempt information.
//!
//! A result is an ordinary owned value. It does not observe live task state,
//! increment an attempt counter, or write an outcome to storage automatically.

use crate::task::{status::Status, task_id::TaskId};

/// The outcome of one execution attempt.
///
/// `T` is the successful output type; failures carry a string description. The
/// integration supplies all fields and keeps [`status`](Self::status) consistent
/// with [`result`](Self::result). Construction performs no such validation.
///
/// # Example
///
/// ```
/// use miju_core::{
///     backend::results::TaskResult,
///     task::{status::Status, task_id::TaskId},
/// };
///
/// let outcome = TaskResult {
///     task_id: TaskId::from_int(81),
///     attempt: 2,
///     status: Status::Failed,
///     result: Err::<u64, _>("report source unavailable".to_owned()),
/// };
///
/// assert_eq!(outcome.task_id.as_int(), Some(81));
/// assert_eq!(outcome.result.as_ref().unwrap_err(), "report source unavailable");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskResult<T> {
    /// The task whose execution produced this result.
    pub task_id: TaskId,
    /// The attempt number associated with the outcome.
    ///
    /// Supplied by the integration; this field is not a shared counter.
    pub attempt: usize,
    /// The recorded execution state.
    ///
    /// Changing it does not update an existing task's status cell.
    pub status: Status,
    /// The output or a stored description of the failure.
    pub result: Result<T, String>,
}
