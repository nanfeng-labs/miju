//! Defines the `TaskId` type and related functionality.
//!
//! `TaskId` is an identifier for a task, supporting a fixed set of common
//! id representations (integer and string).
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

/// A wrapper type that defines a task id.
///
/// Supports a fixed set of common identifier representations.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", serde(untagged))]
#[non_exhaustive]
pub enum TaskId {
    /// An integer-based id
    Int(u64),
    /// A string-based id
    String(String),
}

impl TaskId {
    /// Construct a `TaskId` from an integer.
    #[must_use]
    pub fn from_int(id: u64) -> Self {
        Self::Int(id)
    }

    /// Construct a `TaskId` from a string.
    #[must_use]
    pub fn from_string(id: impl Into<String>) -> Self {
        Self::String(id.into())
    }

    /// Returns the inner value as an integer, if this is an `Int` variant.
    #[must_use]
    #[allow(clippy::match_wildcard_for_single_variants)]
    pub fn as_int(&self) -> Option<u64> {
        match self {
            Self::Int(id) => Some(*id),
            _ => None,
        }
    }

    /// Returns the inner value as a string slice, if this is a `String` variant.
    #[must_use]
    #[allow(clippy::match_wildcard_for_single_variants)]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(id) => Some(id.as_str()),
            _ => None,
        }
    }
}

/// Errors that can occur when parsing a `TaskId` from a string
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TaskIdError {
    /// The string did not match any known `TaskId` representation
    #[error("could not decode task_id: `{0}`")]
    Decode(String),
}

impl FromStr for TaskId {
    type Err = TaskIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(id) = u64::from_str(s) {
            return Ok(Self::Int(id));
        }

        if !s.is_empty() {
            return Ok(Self::String(s.to_owned()));
        }

        Err(TaskIdError::Decode(s.to_owned()))
    }
}

impl TryFrom<&'_ str> for TaskId {
    type Error = TaskIdError;

    fn try_from(value: &'_ str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(id) => Display::fmt(id, f),
            Self::String(id) => Display::fmt(id, f),
        }
    }
}

impl From<u64> for TaskId {
    fn from(id: u64) -> Self {
        Self::Int(id)
    }
}

impl From<String> for TaskId {
    fn from(id: String) -> Self {
        Self::String(id)
    }
}
