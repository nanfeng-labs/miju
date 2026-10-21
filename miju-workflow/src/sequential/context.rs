use std::num::ParseIntError;

use miju_core::{
    backend::WireFormatBackend,
    task::metadata::{Metadata, MetadataError, MetadataStore},
};
use serde::{Deserialize, Serialize};

/// Context information for the current step in the workflow
#[derive(Debug)]
pub struct StepContext<B>
where
    B: WireFormatBackend,
{
    /// Index of the current step
    pub current_step: usize,
    /// Backend associated with the current step
    pub backend: B,
    /// Indicates if there is a next step
    pub has_next: bool,
}

impl<B> Clone for StepContext<B>
where
    B: WireFormatBackend + Clone,
{
    fn clone(&self) -> Self {
        Self {
            current_step: self.current_step,
            backend: self.backend.clone(),
            has_next: self.has_next,
        }
    }
}
impl<B> StepContext<B>
where
    B: WireFormatBackend,
{
    /// Creates a new StepContext
    pub fn new(backend: B, idx: usize, has_next: bool) -> Self {
        Self {
            current_step: idx,
            backend,
            has_next,
        }
    }
}

/// Metadata stored in each task for workflow processing
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WorkflowContext {
    /// Index of the step in the workflow
    pub step_index: usize,
}

#[non_exhaustive]
/// Represents an invalid [`WorkflowContext`] state
#[derive(Debug, thiserror::Error)]
pub enum WorkflowContextError {
    /// An entry for the key is missing
    #[error("the data for key {WORKFLOW_CONTEXT_KEY} is missing")]
    MissingKey,
    /// Could not parse the value provided
    #[error("Could not parse key {WORKFLOW_CONTEXT_KEY}")]
    Parse(#[from] ParseIntError),

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

const WORKFLOW_CONTEXT_KEY: &str = "miju_workflow.context.step_index";

impl Metadata for WorkflowContext {
    type Error = WorkflowContextError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let step_index = map
            .get(WORKFLOW_CONTEXT_KEY)
            .ok_or(WorkflowContextError::MissingKey)?
            .parse::<usize>()
            .map_err(WorkflowContextError::Parse)?;

        Ok(Self { step_index })
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), WorkflowContextError> {
        map.insert(WORKFLOW_CONTEXT_KEY, self.step_index.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trip_preserves_step_indices() {
        assert_eq!(WorkflowContext::default().step_index, 0);
        for step_index in [0, 1, usize::MAX] {
            let context = WorkflowContext { step_index };
            let mut metadata = MetadataStore::new();
            context.inject(&mut metadata).unwrap();
            assert_eq!(
                WorkflowContext::extract(&metadata).unwrap().step_index,
                step_index
            );
            assert_eq!(
                metadata.get(WORKFLOW_CONTEXT_KEY),
                Some(step_index.to_string().as_str())
            );
            let encoded = serde_json::to_string(&context).unwrap();
            let decoded: WorkflowContext = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded.step_index, step_index);
        }
    }

    #[test]
    fn missing_and_invalid_indices_return_distinct_errors() {
        assert!(matches!(
            WorkflowContext::extract(&MetadataStore::new()),
            Err(WorkflowContextError::MissingKey)
        ));
        for value in ["", "-1", "step", "1.5", "18446744073709551616"] {
            let mut metadata = MetadataStore::new();
            metadata.insert(WORKFLOW_CONTEXT_KEY, value).unwrap();
            assert!(matches!(
                WorkflowContext::extract(&metadata),
                Err(WorkflowContextError::Parse(_))
            ));
        }
    }

    #[test]
    fn duplicate_injection_preserves_the_original_index() {
        let mut metadata = MetadataStore::new();
        WorkflowContext { step_index: 2 }
            .inject(&mut metadata)
            .unwrap();
        assert!(matches!(
            WorkflowContext { step_index: 3 }.inject(&mut metadata),
            Err(WorkflowContextError::DuplicateEntry(MetadataError::DuplicateKey(key)))
                if key == WORKFLOW_CONTEXT_KEY
        ));
        assert_eq!(WorkflowContext::extract(&metadata).unwrap().step_index, 2);
    }

    #[test]
    fn step_context_clone_preserves_backend_and_position() {
        #[derive(Clone, Debug)]
        struct Backend {
            codec: String,
        }
        impl WireFormatBackend for Backend {
            type Codec = String;
            type Compact = Vec<u8>;

            fn codec(&self) -> &Self::Codec {
                &self.codec
            }
        }
        let context = StepContext::new(
            Backend {
                codec: "test-codec".to_owned(),
            },
            4,
            true,
        );
        let cloned = context.clone();
        assert_eq!(cloned.current_step, 4);
        assert!(cloned.has_next);
        assert_eq!(cloned.backend.codec(), context.backend.codec());
        let last = StepContext::new(cloned.backend, 5, false);
        assert!(!last.has_next);
        assert_eq!(last.current_step, 5);
    }
}
