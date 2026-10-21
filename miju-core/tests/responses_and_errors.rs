//! Response conversion and error-layer contracts.

use std::{
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use miju_core::{
    error::{AbortError, BoxDynError, DeferredError, ErrorHandlingLayer, RetryAfterError},
    task::into_response::IntoResponse,
};
use tower_layer::Layer;
use tower_service::Service;

#[derive(Debug)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for TestError {}

#[test]
fn response_containers_do_not_recursively_interpret_their_values() {
    assert!(true.into_response().unwrap());
    assert_eq!(
        false.into_response().unwrap_err().to_string(),
        "Task returned false"
    );
    assert_eq!(None::<bool>.into_response().unwrap(), None);
    assert_eq!(Some(false).into_response().unwrap(), Some(false));
    assert_eq!(vec![false, true].into_response().unwrap(), [false, true]);
    assert_eq!(
        vec![Err::<(), _>("failed item")].into_response().unwrap(),
        [Err("failed item")]
    );
    assert_eq!(Ok::<_, TestError>(42).into_response().unwrap(), 42);
    let error = Err::<(), _>(TestError("failed task"))
        .into_response()
        .unwrap_err();
    assert_eq!(error.downcast_ref::<TestError>().unwrap().0, "failed task");
    assert_eq!(
        Err::<(), _>("plain error")
            .into_response()
            .unwrap_err()
            .to_string(),
        "plain error"
    );
}

#[test]
fn scalar_and_custom_responses_preserve_their_output() {
    fn round_trip<T: IntoResponse<Output = T> + PartialEq + fmt::Debug + Clone>(value: T) {
        let expected = value.clone();
        assert_eq!(value.into_response().unwrap(), expected);
    }
    round_trip(());
    round_trip(1_u8);
    round_trip(2_u16);
    round_trip(3_u32);
    round_trip(4_u64);
    round_trip(5_usize);
    round_trip(-1_i8);
    round_trip(-2_i16);
    round_trip(-3_i32);
    round_trip(-4_i64);
    round_trip(-5_isize);
    round_trip(1.5_f32);
    round_trip(2.5_f64);
    round_trip(String::from("owned"));
    round_trip("static");

    struct Custom(u32);
    impl IntoResponse for Custom {
        type Output = String;
        fn into_response(self) -> Result<Self::Output, BoxDynError> {
            Ok(self.0.to_string())
        }
    }
    assert_eq!(Custom(42).into_response().unwrap(), "42");
}

#[test]
fn task_error_markers_preserve_their_source_and_retry_duration() {
    let abort = AbortError::new(TestError("abort"));
    assert_eq!(abort.to_string(), "AbortError: abort");
    assert_eq!(
        abort
            .source()
            .unwrap()
            .downcast_ref::<TestError>()
            .unwrap()
            .0,
        "abort"
    );
    let boxed: BoxDynError = Box::new(abort);
    assert!(boxed.is::<AbortError>());

    let duration = Duration::from_millis(150);
    let retry = RetryAfterError::new(TestError("later"), duration);
    assert_eq!(retry.get_duration(), duration);
    assert_eq!(retry.to_string(), "RetryError: later");
    assert_eq!(
        retry
            .source()
            .unwrap()
            .downcast_ref::<TestError>()
            .unwrap()
            .0,
        "later"
    );

    let deferred = DeferredError::new(TestError("deferred"));
    assert_eq!(deferred.to_string(), "DeferredError: deferred");
    assert_eq!(
        deferred
            .source()
            .unwrap()
            .downcast_ref::<TestError>()
            .unwrap()
            .0,
        "deferred"
    );
}

#[derive(Clone)]
struct InnerService {
    ready_polls: usize,
    ready_error: bool,
    call_error: bool,
}

struct ResponseFuture {
    pending: bool,
    result: Option<Result<u8, TestError>>,
}

impl Future for ResponseFuture {
    type Output = Result<u8, TestError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.pending {
            self.pending = false;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(
                self.result
                    .take()
                    .expect("response polled after completion"),
            )
        }
    }
}

impl Service<u8> for InnerService {
    type Response = u8;
    type Error = TestError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ready_polls += 1;
        if self.ready_polls == 1 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(if self.ready_error {
                Err(TestError("readiness"))
            } else {
                Ok(())
            })
        }
    }

    fn call(&mut self, request: u8) -> Self::Future {
        ResponseFuture {
            pending: true,
            result: Some(if self.call_error {
                Err(TestError("call"))
            } else {
                Ok(request)
            }),
        }
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
fn error_layer_forwards_pending_success_and_typed_errors() {
    for (ready_error, call_error) in [(true, false), (false, true), (false, false)] {
        let mut service = ErrorHandlingLayer::default().layer(InnerService {
            ready_polls: 0,
            ready_error,
            call_error,
        });
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(service.poll_ready(&mut cx).is_pending());
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(ready) = service.poll_ready(&mut cx) else {
            panic!("expected readiness result");
        };
        if ready_error {
            assert_eq!(
                ready.unwrap_err().downcast_ref::<TestError>().unwrap().0,
                "readiness"
            );
            continue;
        }
        ready.unwrap();
        let mut response = service.call(7);
        assert!(response.as_mut().poll(&mut cx).is_pending());
        assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
        let Poll::Ready(result) = response.as_mut().poll(&mut cx) else {
            panic!("expected response result");
        };
        if call_error {
            assert_eq!(
                result.unwrap_err().downcast_ref::<TestError>().unwrap().0,
                "call"
            );
        } else {
            assert_eq!(result.unwrap(), 7);
        }
    }
}
