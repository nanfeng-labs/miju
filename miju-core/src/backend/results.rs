use crate::task::{status::Status, task_id::TaskId};

/// Represents the result of a task execution
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct TaskResult<T> {
    /// The unique identifier of the task
    pub task_id: TaskId,
    /// The most recent result
    pub attempt: usize,
    /// The status of the task
    pub status: Status,
    /// The result of the task execution
    pub result: Result<T, String>,
}

impl<T> TaskResult<T> {
    /// Get the ID of the task
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Get the status of the task
    pub fn status(&self) -> &Status {
        &self.status
    }

    /// Get the result of the task
    pub fn result(&self) -> &Result<T, String> {
        &self.result
    }

    /// Take the result of the task
    pub fn take(self) -> Result<T, String> {
        self.result
    }
}
