//! Task identifiers supplied by producers or assigned by storage.
//!
//! [`TaskId`] supports integer keys and opaque string keys. Choose an explicit
//! constructor when the storage representation is known, or parse text when
//! numeric input should be interpreted as an integer identifier.
//!
//! # Construction and Parsing
//!
//! [`TaskId::from_string`] and conversion from an owned [`String`] preserve text
//! as a string key, including empty strings. [`FromStr`] and `TryFrom<&str>`
//! reject empty input, attempt to parse a `u64`, and otherwise preserve the input
//! as a string key. Numeric parsing can therefore normalize input such as `0081`
//! to the integer `81`.
//!
//! ```
//! use miju_core::task::task_id::TaskId;
//!
//! let parsed: TaskId = "0081".parse()?;
//! let opaque = TaskId::from_string("0081");
//!
//! assert_eq!(parsed.as_int(), Some(81));
//! assert_eq!(opaque.as_string(), Some("0081"));
//! assert_ne!(parsed, opaque);
//! assert!("".parse::<TaskId>().is_err());
//! # Ok::<(), miju_core::task::task_id::TaskIdError>(())
//! ```
//!
//! Identifiers are not generated or checked for uniqueness here. Producers and
//! storage integrations must agree on their representation and allocation.

use std::{fmt, str::FromStr};

/// An integer or opaque string key identifying a task.
///
/// Equality and hashing include the variant. Integer and string keys therefore
/// remain distinct even if [`Display`](fmt::Display) produces the same text.
/// Displaying and reparsing an ID does not always preserve its original variant:
///
/// ```
/// use miju_core::task::task_id::TaskId;
///
/// let integer = TaskId::from_int(81);
/// let text = TaskId::from_string("81");
/// assert_eq!(integer.to_string(), text.to_string());
/// assert_ne!(integer, text);
/// assert_eq!(text.to_string().parse::<TaskId>()?, integer);
/// # Ok::<(), miju_core::task::task_id::TaskIdError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TaskId {
    /// An integer key assigned by a producer or store.
    Int(u64),
    /// An opaque string key.
    String(String),
}

impl TaskId {
    /// Constructs an integer identifier.
    #[must_use]
    pub fn from_int(id: u64) -> Self {
        Self::Int(id)
    }

    /// Constructs an opaque identifier without interpreting its contents.
    ///
    /// Numeric text stays a string, and an empty string is accepted. Use
    /// [`str::parse`] when numeric text should become an integer identifier.
    #[must_use]
    pub fn from_string(id: impl Into<String>) -> Self {
        Self::String(id.into())
    }

    /// Returns the integer only when this is an integer identifier.
    ///
    /// A string containing digits still returns `None`.
    #[must_use]
    pub fn as_int(&self) -> Option<u64> {
        match self {
            Self::Int(id) => Some(*id),
            Self::String(_) => None,
        }
    }

    /// Borrows the string only when this is a string identifier.
    ///
    /// Integer identifiers are not formatted into strings by this accessor.
    #[must_use]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(id) => Some(id),
            Self::Int(_) => None,
        }
    }
}

/// An error returned when parsing an identifier from text.
///
/// Currently only empty input is rejected. Nonempty text that cannot be parsed
/// as a `u64` is accepted as [`TaskId::String`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskIdError {
    /// The rejected input.
    Decode(String),
}

impl fmt::Display for TaskIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(value) => write!(f, "invalid task identifier: {value:?}"),
        }
    }
}

impl std::error::Error for TaskIdError {}

impl FromStr for TaskId {
    type Err = TaskIdError;

    /// Parses a nonempty identifier, preferring a `u64` when possible.
    ///
    /// Non-numeric input and integers outside the `u64` range become string
    /// identifiers. String fallback preserves whitespace and other characters.
    ///
    /// # Errors
    ///
    /// Returns [`TaskIdError::Decode`] only when `value` is empty.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(TaskIdError::Decode(value.to_owned()));
        }
        Ok(value
            .parse::<u64>()
            .map_or_else(|_| Self::from_string(value), Self::Int))
    }
}

impl TryFrom<&str> for TaskId {
    type Error = TaskIdError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<u64> for TaskId {
    fn from(value: u64) -> Self {
        Self::Int(value)
    }
}

impl From<String> for TaskId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(id) => id.fmt(f),
            Self::String(id) => f.write_str(id),
        }
    }
}
