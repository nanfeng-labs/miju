//! Public contention behavior without buffering tasks or reserving readiness.
use futures_util::{Sink, SinkExt, lock::Mutex as AsyncMutex, task::noop_waker_ref};
use miju_core::{
    backend::memory::{MemorySink, MemoryStorageError},
    task::{Task, builder::TaskBuilder, extensions::Extensions},
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
type Inner =
    Arc<AsyncMutex<Box<dyn Sink<Task<u32>, Error = MemoryStorageError> + Send + Sync + Unpin>>>;
struct Recording {
    values: Arc<Mutex<Vec<u32>>>,
    fail: bool,
    commit_error: bool,
}
impl Sink<Task<u32>> for Recording {
    type Error = MemoryStorageError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, task: Task<u32>) -> Result<(), Self::Error> {
        if self.fail {
            self.fail = false;
            if self.commit_error {
                self.values.lock().unwrap().push(task.args);
            }
            return Err(MemoryStorageError::Other("underlying failure".into()));
        }
        self.values.lock().unwrap().push(task.args);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
fn setup(fail: bool, commit_error: bool) -> (MemorySink<u32>, Inner, Arc<Mutex<Vec<u32>>>) {
    let values = Arc::new(Mutex::new(Vec::new()));
    let inner: Inner = Arc::new(AsyncMutex::new(Box::new(Recording {
        values: values.clone(),
        fail,
        commit_error,
    })));
    (MemorySink::new(inner.clone()), inner, values)
}
fn ready(s: &mut MemorySink<u32>) {
    assert!(matches!(
        s.poll_ready_unpin(&mut Context::from_waker(noop_waker_ref())),
        Poll::Ready(Ok(()))
    ));
}
fn keyed(value: u32, key: &str) -> Task<u32> {
    TaskBuilder::new(value).idempotency_key(key).build()
}
#[test]
fn busy_send_returns_error_and_preserves_existing_key_recording_policy() {
    for with_key in [false, true] {
        let (mut s, inner, values) = setup(false, false);
        ready(&mut s);
        let held = inner.try_lock().unwrap();
        let task = if with_key {
            keyed(1, "new")
        } else {
            Task::new(1)
        };
        let result = catch_unwind(AssertUnwindSafe(|| s.start_send_unpin(task)));
        assert!(matches!(result, Ok(Err(MemoryStorageError::Other(_)))));
        assert!(values.lock().unwrap().is_empty());
        drop(held);
        ready(&mut s);
        s.start_send_unpin(if with_key {
            keyed(2, "new")
        } else {
            Task::new(2)
        })
        .unwrap();
        // Characterize the existing policy, not a claim that failed keyed sends recover.
        assert_eq!(
            *values.lock().unwrap(),
            if with_key { vec![] } else { vec![2] }
        );
    }
}
#[test]
fn known_duplicate_still_succeeds_while_external_owner_holds_sink() {
    let (mut s, inner, values) = setup(false, false);
    ready(&mut s);
    s.start_send_unpin(keyed(1, "known")).unwrap();
    ready(&mut s);
    let held = inner.try_lock().unwrap();
    s.start_send_unpin(keyed(2, "known")).unwrap();
    assert_eq!(*values.lock().unwrap(), vec![1]);
    drop(held);
}
#[test]
fn success_remains_immediately_visible_and_handle_drop_does_not_discard_it() {
    let (mut s, inner, values) = setup(false, false);
    ready(&mut s);
    s.start_send_unpin(Task::new(3)).unwrap();
    assert_eq!(*values.lock().unwrap(), vec![3]);
    assert!(inner.try_lock().is_some());
    drop(s);
    assert_eq!(*values.lock().unwrap(), vec![3]);
}
#[test]
fn actual_underlying_errors_remain_synchronous_and_keep_the_key() {
    for committed in [false, true] {
        let (mut s, _, values) = setup(true, committed);
        ready(&mut s);
        let err = s.start_send_unpin(keyed(1, "failed")).unwrap_err();
        assert!(err.to_string().contains("underlying failure"));
        ready(&mut s);
        s.start_send_unpin(keyed(2, "failed")).unwrap();
        assert_eq!(
            *values.lock().unwrap(),
            if committed { vec![1] } else { vec![] }
        );
    }
}
struct OnDrop(Mutex<Option<Box<dyn FnOnce() + Send>>>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        if let Some(action) = self.0.get_mut().unwrap().take() {
            action();
        }
    }
}
#[test]
fn contention_drops_task_after_releasing_the_key_registry_guard() {
    let (mut s, inner, _) = setup(false, false);
    ready(&mut s);
    s.start_send_unpin(keyed(1, "known")).unwrap();
    ready(&mut s);
    let mut peer = s.clone();
    ready(&mut peer);
    let observed = Arc::new(AtomicUsize::new(0));
    let mark = observed.clone();
    let mut data = Extensions::default();
    data.insert(Arc::new(OnDrop(Mutex::new(Some(Box::new(move || {
        peer.start_send_unpin(keyed(2, "known")).unwrap();
        mark.fetch_add(1, Ordering::SeqCst);
    }))))));
    let task = TaskBuilder::new(3)
        .idempotency_key("new")
        .with_data(data)
        .build();
    let held = inner.try_lock().unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| s.start_send_unpin(task)));
    assert!(matches!(result, Ok(Err(_))));
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    drop(held);
}

#[test]
fn missing_id_context_clone_stays_outside_locks_and_preserves_key_winner() {
    struct InspectClone {
        inner: Inner,
        peer: Arc<Mutex<Option<MemorySink<u32>>>>,
        calls: Arc<AtomicUsize>,
    }
    impl Clone for InspectClone {
        fn clone(&self) -> Self {
            assert!(self.inner.try_lock().is_some());
            self.peer
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .start_send_unpin(keyed(2, "outer"))
                .unwrap();
            self.calls.fetch_add(1, Ordering::SeqCst);
            Self {
                inner: self.inner.clone(),
                peer: self.peer.clone(),
                calls: self.calls.clone(),
            }
        }
    }
    let (mut s, inner, values) = setup(false, false);
    let mut peer = s.clone();
    ready(&mut peer);
    ready(&mut s);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut data = Extensions::default();
    data.insert(InspectClone {
        inner,
        peer: Arc::new(Mutex::new(Some(peer))),
        calls: calls.clone(),
    });
    let task = TaskBuilder::new(1)
        .idempotency_key("outer")
        .with_data(data)
        .build();
    assert!(task.task_id().is_none());
    let _shared_context = task.clone();
    s.start_send_unpin(task).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(*values.lock().unwrap(), vec![1]);
}
