//! Sends after an explicit memory sink close keep returning errors.

use futures_util::SinkExt;
use miju_core::backend::{
    TaskSink, TaskSinkError,
    memory::{MemoryStorage, MemoryStorageError},
};

#[tokio::test]
async fn sends_after_sink_close_keep_returning_errors() {
    let mut storage = MemoryStorage::<u32>::new();
    SinkExt::close(&mut storage).await.unwrap();

    for value in 0..3 {
        assert!(matches!(
            storage.push(value).await,
            Err(TaskSinkError::PushError(MemoryStorageError::SendError(_)))
        ));
    }
}
