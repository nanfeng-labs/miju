//! Utilities for task runtime data extension.
//!
//! # Overview
//!
//! - [`MissingDataError`]: Error type for missing or unavailable data in a task context.

/// Error type for missing data in a task's context.
#[derive(Debug, thiserror::Error, PartialEq)]
#[non_exhaustive]
pub enum MissingDataError {
    /// The type was not found in the task's data map
    #[error("the type for key `{0}` is not available")]
    NotFound(String),
}
