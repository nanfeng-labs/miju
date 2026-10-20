//! Destination names carried in task execution context.
//!
//! [`Queue`] represents a queue's name. A producer can select one with
//! [`TaskBuilder::queue`](crate::task::builder::TaskBuilder::queue), and an
//! integration can read it through
//! [`ExecutionContext::queue`](crate::task::ExecutionContext::queue).
//!
//! The name does not create a queue or determine how it routes tasks. Storage
//! integrations define valid names, default destinations, and routing behavior.

use std::{convert::Infallible, fmt, str::FromStr};

/// An owned queue name preserved without normalization or validation.
///
/// Construction and parsing accept any string, including an empty name. The
/// backend decides whether the name identifies an available destination.
/// [`AsRef<str>`] borrows the stored name, and [`Display`](fmt::Display) emits
/// the same text.
///
/// # Example
///
/// ```
/// use miju_core::backend::queue::Queue;
///
/// let queue: Queue = "reports/monthly".parse()?;
/// assert_eq!(queue.as_ref(), "reports/monthly");
/// assert_eq!(queue.to_string(), "reports/monthly");
/// assert_eq!(Queue::from("").as_ref(), "");
/// # Ok::<(), std::convert::Infallible>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Queue(String);

impl From<String> for Queue {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Queue {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl FromStr for Queue {
    type Err = Infallible;

    /// Copies the input into a queue name without validation.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(value))
    }
}
