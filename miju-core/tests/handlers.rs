//! Handler state, extraction order, and response conversion contracts.

use std::{
    cell::Cell,
    fmt,
    future::{Future, poll_fn, ready},
    pin::pin,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use miju_core::task::{
    Task, builder::TaskBuilder, data::Data, from_request::FromRequest, status::Status,
    task_fn::task_fn,
};
use tower_layer::Layer;
use tower_service::Service;

fn ready_output<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("expected an immediately ready handler"),
    }
}

struct LocalCounter(Rc<Cell<usize>>, usize);

impl LocalCounter {
    fn next(&mut self) -> usize {
        self.1 += 1;
        self.0.set(self.1);
        self.1
    }
}

#[test]
fn zero_extractors_keep_non_clone_local_handler_state() {
    let calls = Rc::new(Cell::new(0));
    let mut counter = LocalCounter(calls.clone(), 0);
    let mut service = task_fn(move |value: Rc<str>| {
        let count = counter.next();
        ready(Some((value, count)))
    });
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(calls.get(), 0);

    for count in 1..=2 {
        let value: Rc<str> = Rc::from("payload");
        let future = service.call(Task::new(value.clone()));
        assert_eq!(calls.get(), count);
        let (returned, recorded) = ready_output(future).unwrap().unwrap();
        assert!(Rc::ptr_eq(&returned, &value));
        assert_eq!(recorded, count);
    }
}

#[test]
fn extracting_handlers_clone_per_call_and_run_after_data_injection() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut per_handler_count = 0;
    let handler = move |payload: u32, state: Data<u32>| {
        per_handler_count += 1;
        calls.fetch_add(1, Ordering::SeqCst);
        ready(if payload == 0 {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        } else {
            Ok(payload + *state + per_handler_count)
        })
    };
    let mut service = Data::new(5_u32).layer(task_fn(handler));
    for count in 0..2 {
        assert!(matches!(
            service.poll_ready(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(()))
        ));
        let task = Task::new(10);
        let observer = task.clone();
        let future = service.call(task);
        assert_eq!(observed.load(Ordering::SeqCst), count);
        assert_eq!(ready_output(future).unwrap(), 16);
        assert_eq!(observed.load(Ordering::SeqCst), count + 1);
        assert_eq!(observer.attempt(), 0);
        assert_eq!(observer.status(), Status::Pending);
        assert!(observer.data().is_empty());
    }
    assert!(matches!(
        service.poll_ready(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    let error = ready_output(service.call(Task::new(0))).unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
}

struct Trace {
    events: Arc<Mutex<Vec<u8>>>,
    fail_at: Option<u8>,
}

struct Extract<const ID: u8>;

#[derive(Debug)]
struct ExtractError(u8);

impl fmt::Display for ExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "extractor {} failed", self.0)
    }
}

impl std::error::Error for ExtractError {}

impl<const ID: u8> FromRequest<Task<Trace>> for Extract<ID> {
    type Error = ExtractError;

    fn from_request(task: &Task<Trace>) -> impl Future<Output = Result<Self, Self::Error>> + Send {
        let mut started = false;
        poll_fn(move |cx| {
            if !started {
                started = true;
                task.args.events.lock().unwrap().push(ID * 10);
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                task.args.events.lock().unwrap().push(ID * 10 + 1);
                Poll::Ready(if task.args.fail_at == Some(ID) {
                    Err(ExtractError(ID))
                } else {
                    Ok(Self)
                })
            }
        })
    }
}

#[test]
fn extraction_is_sequential_and_failure_skips_later_extractors_and_handler() {
    for fail_at in [None, Some(2)] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut service = task_fn(
            |trace: Trace, _: Extract<1>, _: Extract<2>, _: Extract<3>| {
                trace.events.lock().unwrap().push(90);
                let mut started = false;
                poll_fn(move |cx| {
                    if !started {
                        started = true;
                        trace.events.lock().unwrap().push(91);
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        trace.events.lock().unwrap().push(92);
                        Poll::Ready(true)
                    }
                })
            },
        );
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        let mut future = pin!(service.call(Task::new(Trace {
            events: events.clone(),
            fail_at
        })));
        assert!(events.lock().unwrap().is_empty());
        let pending_polls = if fail_at.is_some() { 2 } else { 4 };
        for _ in 0..pending_polls {
            assert!(future.as_mut().poll(&mut cx).is_pending());
        }
        let Poll::Ready(result) = future.as_mut().poll(&mut cx) else {
            panic!("expected handler completion");
        };
        if fail_at.is_some() {
            assert_eq!(
                result
                    .unwrap_err()
                    .downcast_ref::<ExtractError>()
                    .unwrap()
                    .0,
                2
            );
            assert_eq!(*events.lock().unwrap(), [10, 11, 20, 21]);
        } else {
            assert!(result.unwrap());
            assert_eq!(
                *events.lock().unwrap(),
                [10, 11, 20, 21, 30, 31, 90, 91, 92]
            );
        }
    }
}

#[test]
fn sixteen_extractors_receive_the_task_data() {
    let mut service = task_fn(
        |payload: u32,
         a1: Data<u32>,
         a2: Data<u32>,
         a3: Data<u32>,
         a4: Data<u32>,
         a5: Data<u32>,
         a6: Data<u32>,
         a7: Data<u32>,
         a8: Data<u32>,
         a9: Data<u32>,
         a10: Data<u32>,
         a11: Data<u32>,
         a12: Data<u32>,
         a13: Data<u32>,
         a14: Data<u32>,
         a15: Data<u32>,
         a16: Data<u32>| {
            ready(
                payload
                    + [
                        a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16,
                    ]
                    .into_iter()
                    .map(|value| *value)
                    .sum::<u32>(),
            )
        },
    );
    assert!(matches!(
        service.poll_ready(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    let task = TaskBuilder::new(2).data(3_u32).build();
    assert_eq!(ready_output(service.call(task)).unwrap(), 50);
}
