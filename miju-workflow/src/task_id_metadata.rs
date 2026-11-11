use miju_core::{error::BoxDynError, task::task_id::TaskId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum StoredTaskId {
    Int(u64),
    String(String),
    Typed(String),
}

impl From<&TaskId> for StoredTaskId {
    fn from(id: &TaskId) -> Self {
        match id {
            TaskId::Int(value) => Self::Int(*value),
            TaskId::String(value) => Self::String(value.clone()),
            _ => Self::Typed(id.to_string()),
        }
    }
}

impl StoredTaskId {
    pub(crate) fn into_task_id(self) -> Result<TaskId, BoxDynError> {
        match self {
            Self::Int(value) => Ok(TaskId::from_int(value)),
            Self::String(value) => Ok(TaskId::from_string(value)),
            Self::Typed(value) => {
                let id = value.parse()?;
                if matches!(id, TaskId::Int(_) | TaskId::String(_)) {
                    return Err("unsupported typed task ID".into());
                }
                Ok(id)
            }
        }
    }

    pub(crate) fn decode(value: &str) -> Result<TaskId, BoxDynError> {
        serde_json::from_str::<Self>(value)?.into_task_id()
    }

    pub(crate) fn encode(id: &TaskId) -> Result<String, serde_json::Error> {
        serde_json::to_string(&Self::from(id))
    }
}

pub(crate) fn is_tagged(version: Option<&str>) -> Result<bool, BoxDynError> {
    match version {
        None => Ok(false),
        Some("1") => Ok(true),
        Some(_) => Err("unsupported task ID metadata encoding".into()),
    }
}
