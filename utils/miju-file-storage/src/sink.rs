use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use miju_core::task::{
    Task,
    task_id::{RandomId, TaskId},
};
use serde_json::Value;

use crate::{
    Adapter, FileStorage, PendingChange, SyncPolicy, error::FileStorageError, util::RawTask,
};

impl<A: Adapter + Unpin, Args: Unpin> Sink<Task<Value>> for FileStorage<Args, A> {
    type Error = FileStorageError<A>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, mut item: Task<Value>) -> Result<(), Self::Error> {
        let idempotency_key = item.idempotency_key().map(|a| a.to_owned());
        if item.task_id().is_none() {
            item = item
                .into_builder()
                .task_id(TaskId::from_string(RandomId::default()))
                .build();
        }

        let entry = RawTask {
            task_id: item.task_id().unwrap().clone(),
            ctx: item.metadata().clone(),
            args: item.args,
            result: None,
            idempotency_key,
        };
        let this = self.get_mut();
        let mut entries = this
            .entries
            .try_write()
            .map_err(|_| FileStorageError::WouldBlockLock)?;
        // Enforce idempotency
        if entry.idempotency_key.is_some()
            && entries
                .iter()
                .any(|t| t.idempotency_key == entry.idempotency_key)
        {
            return Ok(());
        }
        let mut pending = this
            .pending
            .try_write()
            .map_err(|_| FileStorageError::WouldBlockLock)?;
        let line = A::from_entry(&entry).map_err(FileStorageError::AdapterError)?;
        let bytes = this
            .adapter
            .serialize(&line)
            .map_err(FileStorageError::AdapterError)?;
        let waiters = std::mem::take(
            &mut *this
                .waiters
                .lock()
                .map_err(|_| FileStorageError::WouldBlockLock)?,
        );
        entries.push(entry);
        pending.push(PendingChange::Append {
            bytes,
            offset: None,
        });
        drop(pending);
        drop(entries);

        for waiter in waiters {
            if let Some(waiter) = waiter.upgrade() {
                waiter.wake();
            }
        }

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.sync_policy.clone() {
            SyncPolicy::Instant | SyncPolicy::Periodic(_) => self.flush()?,
            SyncPolicy::Manual => {}
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
