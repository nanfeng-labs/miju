//! Shared file consumers receive backend wakes without acquiring separate cursors.

use futures_util::FutureExt;
use miju_core::{
    backend::{Backend, TaskSink, ext::BackendExt, ext::shared::Shared},
    error::WorkerError,
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt,
    },
};
use miju_file_storage::JsonStorage;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Notifications(AtomicUsize);

impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn settle<F: Future>(
    mut future: Pin<&mut F>,
    notifications: &Arc<Notifications>,
) -> Option<F::Output> {
    let waker = Waker::from(notifications.clone());
    let mut cx = Context::from_waker(&waker);
    for _ in 0..32 {
        notifications.0.store(0, Ordering::SeqCst);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return Some(output),
            Poll::Pending if notifications.0.load(Ordering::SeqCst) == 0 => return None,
            Poll::Pending => {}
        }
    }
    panic!("worker did not settle within its wake notification bound");
}

#[derive(Default)]
struct Trace {
    contexts: Mutex<HashMap<String, WorkerContext>>,
    seen: Mutex<Vec<(String, u32)>>,
}

fn worker(
    backend: Shared<JsonStorage<u32>>,
    name: &'static str,
    trace: Arc<Trace>,
) -> impl Future<Output = Result<(), WorkerError>> {
    let events = trace.clone();
    WorkerBuilder::new(name)
        .backend(backend)
        .on_event(move |worker, event| {
            if matches!(event, Event::Start) {
                events
                    .contexts
                    .lock()
                    .unwrap()
                    .insert(worker.name(), worker.clone());
            } else if let Event::Error(error) = event {
                panic!("unexpected worker error: {error}");
            }
        })
        .build(move |value: u32, worker: WorkerContext| {
            let trace = trace.clone();
            async move {
                trace.seen.lock().unwrap().push((worker.name(), value));
                value * 2
            }
        })
        .run()
}

#[test]
fn shared_json_worker_wakes_after_the_last_poller_stops_and_drops() {
    let directory = tempfile::tempdir().unwrap();
    let mut producer = JsonStorage::<u32>::new(directory.path().join("tasks.json"))
        .unwrap()
        .shared();
    let trace = Arc::new(Trace::default());
    let (a, b) = (
        Arc::new(Notifications::default()),
        Arc::new(Notifications::default()),
    );
    let mut first = Box::pin(worker(producer.clone(), "A", trace.clone()));
    let mut last = Box::pin(worker(producer.clone(), "B", trace.clone()));
    assert!(settle(first.as_mut(), &a).is_none());
    assert!(settle(last.as_mut(), &b).is_none());
    let context = |name: &str| trace.contexts.lock().unwrap()[name].clone();
    context("B").stop().unwrap();
    settle(last.as_mut(), &b)
        .expect("B failed to close")
        .unwrap();
    assert!(context("B").is_terminated());
    drop(last);
    a.0.store(0, Ordering::SeqCst);
    b.0.store(0, Ordering::SeqCst);

    producer.push(7).now_or_never().unwrap().unwrap();
    assert!(a.0.load(Ordering::SeqCst) > 0);
    assert_eq!(b.0.load(Ordering::SeqCst), 0);
    assert!(context("A").is_running());
    assert!(settle(first.as_mut(), &a).is_none());
    assert_eq!(*trace.seen.lock().unwrap(), [("A".to_owned(), 7)]);

    let c = Arc::new(Notifications::default());
    let mut later = Box::pin(worker(producer.clone(), "C", trace.clone()));
    assert!(settle(later.as_mut(), &c).is_none());
    assert_eq!(
        trace.seen.lock().unwrap().len(),
        1,
        "shared cursor replayed a row"
    );
    context("C").stop().unwrap();
    settle(later.as_mut(), &c)
        .expect("C failed to close")
        .unwrap();
    drop(later);
    context("A").stop().unwrap();
    settle(first.as_mut(), &a)
        .expect("A failed to close")
        .unwrap();
    drop(first);
    trace.contexts.lock().unwrap().clear();
}

#[test]
fn direct_json_clones_keep_live_wakes_and_independent_cursors() {
    let directory = tempfile::tempdir().unwrap();
    let mut producer = JsonStorage::<u32>::new(directory.path().join("tasks.json")).unwrap();
    let mut first = producer.clone();
    let mut last = producer.clone();
    let (a, b) = (
        Arc::new(Notifications::default()),
        Arc::new(Notifications::default()),
    );
    let (aw, bw) = (Waker::from(a.clone()), Waker::from(b.clone()));
    let worker = WorkerContext::new("direct-clone-control");
    for (backend, waker) in [(&mut first, &aw), (&mut last, &bw)] {
        let mut cx = Context::from_waker(waker);
        assert!(Backend::poll_ready(backend, &mut cx, &worker).is_ready());
        assert!(backend.poll_next(&mut cx, &worker).is_pending());
    }
    drop(last);
    producer.push(7).now_or_never().unwrap().unwrap();
    assert!(a.0.load(Ordering::SeqCst) > 0);
    assert_eq!(b.0.load(Ordering::SeqCst), 0);
    let mut cx = Context::from_waker(Waker::noop());
    let mut another = producer.clone();
    for backend in [&mut first, &mut another] {
        assert!(Backend::poll_ready(backend, &mut cx, &worker).is_ready());
        let Poll::Ready(Some(Ok(task))) = backend.poll_next(&mut cx, &worker) else {
            panic!("direct clone failed to replay its row");
        };
        assert_eq!(task.args, serde_json::json!(7));
    }
}
