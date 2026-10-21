//! Task extraction contracts and data middleware behavior.

use std::{
    any::type_name,
    future::{Future, Ready, poll_fn, ready},
    pin::pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use miju_core::{
    backend::queue::{Queue, QueueError},
    task::{
        Task,
        builder::TaskBuilder,
        data::{Data, MissingDataError},
        from_request::FromRequest,
        metadata::{Meta, Metadata, MetadataStore},
        status::Status,
        task_id::TaskId,
    },
};
use tower_layer::Layer;
use tower_service::Service;

fn ready_output<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("expected an immediately ready extractor or service"),
    }
}

#[test]
fn data_extraction_clones_the_requested_type_without_consuming_the_task() {
    let shared = Arc::new(AtomicUsize::new(7));
    let payload = String::from("borrowed payload");
    let task = TaskBuilder::new(payload.as_str())
        .data(String::from("original"))
        .data(shared.clone())
        .build();

    let mut text = ready_output(task.extract::<Data<String>>()).unwrap();
    text.push_str(" changed");
    assert_eq!(&*text, "original changed");
    assert_eq!(task.data().get::<String>().unwrap(), "original");
    assert_eq!(task.args, payload);

    let extracted = ready_output(task.extract::<Data<Arc<AtomicUsize>>>()).unwrap();
    assert!(Arc::ptr_eq(&extracted, &shared));
    extracted.store(9, Ordering::SeqCst);
    assert_eq!(shared.load(Ordering::SeqCst), 9);
    let error = ready_output(task.extract::<Data<usize>>()).unwrap_err();
    assert_eq!(
        error,
        MissingDataError::NotFound(type_name::<usize>().into())
    );
    assert!(
        ready_output(task.extract::<Option<Data<usize>>>())
            .unwrap()
            .is_none()
    );
}

#[test]
fn identifiers_and_queues_are_extracted_from_their_context_fields() {
    let task = TaskBuilder::new(())
        .task_id(TaskId::from_string("task-42"))
        .queue("jobs".into())
        .data(TaskId::from(99))
        .metadata(&Queue::from("metadata-queue"))
        .build();
    assert_eq!(
        ready_output(task.extract::<TaskId>()).unwrap(),
        TaskId::from_string("task-42")
    );
    assert_eq!(
        ready_output(task.extract::<Option<TaskId>>()).unwrap(),
        task.task_id().cloned()
    );
    assert_eq!(
        ready_output(task.extract::<Queue>()).unwrap().as_ref(),
        "jobs"
    );
    assert_eq!(
        Queue::extract(task.metadata()).unwrap().as_ref(),
        "metadata-queue"
    );

    let fields_missing = TaskBuilder::new(())
        .data(TaskId::from(99))
        .metadata(&Queue::from("metadata-queue"))
        .build();
    assert_eq!(
        ready_output(fields_missing.extract::<TaskId>()).unwrap_err(),
        MissingDataError::NotFound(type_name::<TaskId>().into())
    );
    assert!(matches!(
        ready_output(fields_missing.extract::<Queue>()),
        Err(QueueError::NotFound)
    ));
    assert!(
        ready_output(fields_missing.extract::<Option<Queue>>())
            .unwrap()
            .is_none()
    );
}

#[derive(Debug, PartialEq, Eq)]
struct Tenant(String);

impl Metadata for Tenant {
    type Error = &'static str;

    fn extract(store: &MetadataStore) -> Result<Self, Self::Error> {
        match store.get("tenant") {
            None => Err("missing tenant"),
            Some("") => Err("invalid tenant"),
            Some(value) => Ok(Self(value.into())),
        }
    }

    fn inject(&self, store: &mut MetadataStore) -> Result<(), Self::Error> {
        store
            .insert("tenant", &self.0)
            .map_err(|_| "duplicate tenant")
    }
}

#[test]
fn metadata_extraction_preserves_errors_and_clones_the_store() {
    let task = TaskBuilder::new(())
        .metadata(&Tenant("alpha".into()))
        .build();
    let tenant = ready_output(task.extract::<Meta<Tenant>>()).unwrap();
    assert_eq!(*tenant, Tenant("alpha".into()));
    let mut store = ready_output(task.extract::<MetadataStore>()).unwrap();
    store.replace("tenant", "beta");
    assert_eq!(task.metadata().get("tenant"), Some("alpha"));

    for (task, expected) in [
        (Task::new(()), "missing tenant"),
        (
            TaskBuilder::new(())
                .metadata(&Tenant(String::new()))
                .build(),
            "invalid tenant",
        ),
    ] {
        assert_eq!(
            ready_output(task.extract::<Meta<Tenant>>()).unwrap_err(),
            expected
        );
        assert!(
            ready_output(task.extract::<Option<Meta<Tenant>>>())
                .unwrap()
                .is_none()
        );
    }
}

#[derive(Debug)]
struct Deferred;

impl FromRequest<AtomicUsize> for Deferred {
    type Error = Rc<str>;

    fn from_request(
        request: &AtomicUsize,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send {
        poll_fn(move |cx| {
            if request.fetch_add(1, Ordering::SeqCst) == 0 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Err(Rc::from("invalid request")))
            }
        })
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn optional_extraction_forwards_pending_and_swallows_nonstandard_errors() {
    let polls = AtomicUsize::new(0);
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(Option::<Deferred>::from_request(&polls));
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        future.as_mut().poll(&mut cx),
        Poll::Ready(Ok(None))
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(
        &*ready_output(Deferred::from_request(&polls)).unwrap_err(),
        "invalid request"
    );
}

struct Echo {
    polls: Arc<AtomicUsize>,
}

impl<A> Service<Task<A>> for Echo {
    type Response = Task<A>;
    type Error = &'static str;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, request: Task<A>) -> Self::Future {
        ready(Ok(request))
    }
}

#[test]
fn layered_data_obeys_readiness_and_overwrites_without_changing_observers() {
    let polls = Arc::new(AtomicUsize::new(0));
    let mut layer = Data::new(String::from("inner"));
    let inner = layer.layer(Echo {
        polls: polls.clone(),
    });
    layer.push_str(" changed after layering");
    let mut service = Data::new(String::from("outer")).layer(inner);
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(Service::<Task<String>>::poll_ready(&mut service, &mut cx).is_pending());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        Service::<Task<String>>::poll_ready(&mut service, &mut cx),
        Poll::Ready(Ok(()))
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);

    let task = TaskBuilder::new(String::from("payload"))
        .data(String::from("original"))
        .data(17_usize)
        .metadata(&Tenant("alpha".into()))
        .build();
    let observer = task.clone();
    let future: Ready<Result<Task<String>, &'static str>> = service.call(task);
    let delivered = ready_output(future).unwrap();
    assert_eq!(delivered.args, "payload");
    assert_eq!(delivered.data().get::<String>().unwrap(), "inner");
    assert_eq!(observer.data().get::<String>().unwrap(), "original");
    assert_eq!(delivered.data().get::<usize>(), Some(&17));
    assert_eq!(delivered.metadata(), observer.metadata());
    assert!(!Arc::ptr_eq(delivered.ctx(), observer.ctx()));
    delivered.raw_status().store(Status::Running);
    assert_eq!(observer.status(), Status::Running);
}

struct Failing {
    readiness_error: bool,
}

impl Service<Task<()>> for Failing {
    type Response = ();
    type Error = &'static str;
    type Future = Ready<Result<(), Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(if self.readiness_error {
            Err("readiness")
        } else {
            Ok(())
        })
    }

    fn call(&mut self, _: Task<()>) -> Self::Future {
        ready(Err("call"))
    }
}

#[test]
fn data_middleware_preserves_readiness_and_call_errors() {
    let data = Data::new(7_usize);
    let mut unavailable = data.layer(Failing {
        readiness_error: true,
    });
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        unavailable.poll_ready(&mut cx),
        Poll::Ready(Err("readiness"))
    );
    let mut service = data.layer(Failing {
        readiness_error: false,
    });
    assert_eq!(service.poll_ready(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(ready_output(service.call(Task::new(()))), Err("call"));
    assert_eq!(*data, 7);
}
