use miju_core::task::Task;
use tracing::{Level, Span};

use crate::layers::tracing::{DEFAULT_MESSAGE_LEVEL, MakeSpan};

/// Creates a task span under the currently active span.
///
/// Records `task_id` and the task attempt count, clamped to at least one.
/// [`TracingContext`] can be stored on a task, but this span factory does not
/// currently restore a remote parent from that metadata.
///
/// # Panics
///
/// Panics when the task has no ID, even if span recording is disabled.
///
/// [`TracingContext`]: super::TracingContext
#[derive(Debug, Clone)]
pub struct ContextualTaskSpan {
    level: Level,
}

impl ContextualTaskSpan {
    /// Create a new [`ContextualTaskSpan`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            level: DEFAULT_MESSAGE_LEVEL,
        }
    }

    /// Set the [`Level`] used for the [tracing span].
    ///
    /// Defaults to [`Level::DEBUG`].
    ///
    /// [tracing span]: https://docs.rs/tracing/latest/tracing/#spans
    #[must_use]
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }
}

impl Default for ContextualTaskSpan {
    fn default() -> Self {
        Self::new()
    }
}

impl<Args> MakeSpan<Args> for ContextualTaskSpan {
    fn make_span(&mut self, req: &Task<Args>) -> Span {
        let task_id = req.task_id().expect("A task must have an ID").to_string();
        let attempt = req.attempt();
        let span = Span::current();
        // The current attempt cannot be 0 since we are in an attempt.
        let current_attempt = std::cmp::max(1, attempt);

        macro_rules! make_span {
            ($level:expr) => {
                tracing::span!(
                    parent: span,
                    $level,
                    "task",
                    task_id = task_id,
                    attempt = current_attempt,
                )
            };
        }

        match self.level {
            Level::ERROR => make_span!(Level::ERROR),
            Level::WARN => make_span!(Level::WARN),
            Level::INFO => make_span!(Level::INFO),
            Level::DEBUG => make_span!(Level::DEBUG),
            Level::TRACE => make_span!(Level::TRACE),
        }
    }
}
