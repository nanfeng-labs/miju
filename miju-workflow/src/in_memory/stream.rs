use std::{
    collections::HashSet,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
};

use futures_util::Stream;
use miju_core::{
    backend::{TaskResult, memory::MemoryStorageError},
    task::task_id::TaskId,
};
use serde::de::DeserializeOwned;

use crate::in_memory::result_store::ResultStore;

pub(crate) type WaitId = u64;

/// Stream of stored results for the requested task IDs.
#[derive(Debug)]
pub struct WaitForStream<O> {
    task_ids: Vec<TaskId>,
    completed: HashSet<TaskId>,
    store: Arc<ResultStore>,
    id: WaitId,
    _marker: PhantomData<O>,
}

impl<O> WaitForStream<O> {
    pub(super) fn new(task_ids: impl IntoIterator<Item = TaskId>, store: Arc<ResultStore>) -> Self {
        let id = store.next_id.fetch_add(1, Ordering::Relaxed);
        Self {
            task_ids: task_ids.into_iter().collect(),
            completed: HashSet::new(),
            store,
            id,
            _marker: PhantomData,
        }
    }
}

impl<O: Clone> Clone for WaitForStream<O> {
    fn clone(&self) -> Self {
        let mut stream = Self::new(self.task_ids.clone(), Arc::clone(&self.store));
        stream.completed = self.completed.clone();
        stream
    }
}

impl<O> Drop for WaitForStream<O> {
    fn drop(&mut self) {
        self.store.wakers.remove(&self.id);
    }
}

impl<O> Stream for WaitForStream<O>
where
    O: DeserializeOwned + Unpin,
{
    type Item = Result<TaskResult<O>, MemoryStorageError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        #[cfg(feature = "tracing")]
        tracing::trace!(
            completed = ?this.completed.len(),
            results = ?&this.store.results.len(),
            wakers = ?&this.store.wakers.len(),
            "Checking the results",
        );
        for task_id in &this.task_ids {
            if this.completed.contains(task_id) {
                continue;
            }

            if let Some(value) = this.store.get(task_id) {
                this.completed.insert(task_id.clone());
                let decoded = value
                    .result
                    .map(|a| O::deserialize(a).map_err(|e| e.to_string()))
                    .map_err(|e| MemoryStorageError::Other(e.into()))?;

                return Poll::Ready(Some(Ok(TaskResult {
                    task_id: value.task_id,
                    attempt: value.attempt,
                    status: value.status,
                    result: decoded,
                })));
            }
        }

        if this.completed.len() == this.task_ids.len() {
            return Poll::Ready(None);
        }
        this.store.register_waker(this.id, cx.waker());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    use futures_util::StreamExt;
    use miju_core::{
        backend::TaskResult,
        task::{
            status::Status,
            task_id::{GenerateId, RandomId, TaskId},
        },
    };
    use serde_json::json;

    use super::WaitForStream;
    use crate::in_memory::ResultStore;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn publish(store: &ResultStore, id: &TaskId, value: u32) {
        store.insert(
            id.clone(),
            TaskResult {
                task_id: id.clone(),
                attempt: 0,
                status: Status::Done,
                result: Ok(json!(value)),
            },
        );
    }

    #[test]
    fn cloned_waiters_preserve_progress_and_receive_independent_notifications() {
        let store = Arc::new(ResultStore::default());
        let ids = [RandomId::generate(), RandomId::generate()];
        let mut original = WaitForStream::<u32>::new(ids.clone(), store.clone());
        let original_counter = Arc::new(WakeCounter::default());
        let original_waker = Waker::from(original_counter.clone());
        publish(&store, &ids[0], 7);
        assert!(
            matches!(original.poll_next_unpin(&mut Context::from_waker(&original_waker)), Poll::Ready(Some(Ok(result))) if result.result == Ok(7))
        );
        let mut cloned = original.clone();
        let cloned_counter = Arc::new(WakeCounter::default());
        let cloned_waker = Waker::from(cloned_counter.clone());
        assert!(
            original
                .poll_next_unpin(&mut Context::from_waker(&original_waker))
                .is_pending()
        );
        assert!(
            cloned
                .poll_next_unpin(&mut Context::from_waker(&cloned_waker))
                .is_pending()
        );
        publish(&store, &ids[1], 9);
        assert!(
            original_counter.0.load(Ordering::SeqCst) > 0,
            "cloning must not replace the original observer's notification"
        );
        assert!(cloned_counter.0.load(Ordering::SeqCst) > 0);
        for (stream, waker) in [
            (&mut original, &original_waker),
            (&mut cloned, &cloned_waker),
        ] {
            assert!(
                matches!(stream.poll_next_unpin(&mut Context::from_waker(waker)), Poll::Ready(Some(Ok(result))) if result.task_id == ids[1] && result.result == Ok(9))
            );
            assert!(matches!(
                stream.poll_next_unpin(&mut Context::from_waker(waker)),
                Poll::Ready(None)
            ));
        }
    }

    #[test]
    fn dropping_polled_and_unpolled_clones_preserves_the_original_registration() {
        for poll_clone in [false, true] {
            let store = Arc::new(ResultStore::default());
            let id = RandomId::generate();
            let mut original = WaitForStream::<u32>::new([id.clone()], store.clone());
            let original_counter = Arc::new(WakeCounter::default());
            let original_waker = Waker::from(original_counter.clone());
            assert!(
                original
                    .poll_next_unpin(&mut Context::from_waker(&original_waker))
                    .is_pending()
            );
            let mut cloned = original.clone();
            let cloned_counter = Arc::new(WakeCounter::default());
            let cloned_waker = Waker::from(cloned_counter.clone());
            if poll_clone {
                assert!(
                    cloned
                        .poll_next_unpin(&mut Context::from_waker(&cloned_waker))
                        .is_pending()
                );
            }
            drop(cloned);
            publish(&store, &id, 9);
            assert!(
                original_counter.0.load(Ordering::SeqCst) > 0,
                "dropping a clone must preserve the original observer's notification"
            );
            assert_eq!(cloned_counter.0.load(Ordering::SeqCst), 0);
            assert!(
                matches!(original.poll_next_unpin(&mut Context::from_waker(&original_waker)), Poll::Ready(Some(Ok(result))) if result.result == Ok(9))
            );
        }
    }
}
