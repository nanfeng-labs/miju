//! Regression tests for circuit recovery notifications.

use futures_util::{StreamExt, future::poll_fn};
use miju_core::{
    backend::{TaskSink, memory::MemoryStorage},
    error::BoxDynError,
    worker::{
        builder::WorkerBuilder,
        event::Event,
        ext::circuit_breaker::{CircuitBreaker, config::CircuitBreakerConfig},
    },
};
use std::{
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
use tower::{Service, service_fn};
use tower_layer::Layer;

struct Notify(mpsc::Sender<()>);

impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn notification() -> (Waker, mpsc::Receiver<()>) {
    let (tx, rx) = mpsc::channel();
    (Waker::from(Arc::new(Notify(tx))), rx)
}

fn circuit(timeout: Duration) -> impl Service<bool, Response = (), Error = BoxDynError> + Clone {
    let mut service = None;
    let _builder = WorkerBuilder::new("recovery-probe")
        .backend(MemoryStorage::<bool>::new())
        .break_circuit_with(
            CircuitBreakerConfig::default()
                .failure_threshold(5)
                .recovery_timeout(timeout),
        )
        .chain(|layer| {
            service = Some(layer.layer(service_fn(|succeeds: bool| async move {
                if succeeds {
                    Ok(())
                } else {
                    Err("expected failure")
                }
            })));
            layer
        });
    service.unwrap()
}

async fn open<S: Service<bool, Error = BoxDynError>>(service: &mut S) {
    for _ in 0..5 {
        poll_fn(|cx| service.poll_ready(cx)).await.unwrap();
        assert!(service.call(false).await.is_err());
    }
}

fn wait_for_readiness<S: Service<bool, Error = BoxDynError>>(
    service: &mut S,
    waker: &Waker,
    rx: &mpsc::Receiver<()>,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(
            rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .is_ok(),
            "recovery deadline elapsed without notifying readiness"
        );
        if let Poll::Ready(result) = service.poll_ready(&mut Context::from_waker(waker)) {
            result.unwrap();
            return;
        }
    }
}

#[tokio::test]
async fn recovery_deadline_notifies_a_waiting_worker() {
    let mut backend = MemoryStorage::new();
    for succeeds in [false, false, false, false, false, true] {
        backend.push(succeeds).await.unwrap();
    }
    let mut events = WorkerBuilder::new("recovery-wake")
        .backend(backend)
        .break_circuit_with(
            CircuitBreakerConfig::default()
                .failure_threshold(5)
                .recovery_timeout(Duration::from_millis(30)),
        )
        .build(|succeeds: bool| async move {
            if succeeds {
                Ok(())
            } else {
                Err("expected failure")
            }
        })
        .stream();
    assert!(matches!(events.next().await, Some(Ok(Event::Start))));
    for _ in 0..5 {
        assert!(matches!(events.next().await, Some(Ok(Event::Error(_)))));
    }
    let (waker, rx) = notification();
    let mut cx = Context::from_waker(&waker);
    assert!(events.poll_next_unpin(&mut cx).is_pending());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(
            rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .is_ok(),
            "recovery deadline elapsed without notifying the waiting worker"
        );
        match events.poll_next_unpin(&mut cx) {
            Poll::Ready(Some(Ok(Event::Success(_)))) => break,
            Poll::Pending => {}
            other @ Poll::Ready(_) => panic!("unexpected recovery event: {other:?}"),
        }
    }
}

#[tokio::test]
async fn cloned_services_notify_each_pending_caller() {
    let mut first = circuit(Duration::from_millis(30));
    open(&mut first).await;
    let (a, arx) = notification();
    assert!(first.poll_ready(&mut Context::from_waker(&a)).is_pending());
    let mut second = first.clone();
    let (b, brx) = notification();
    assert!(second.poll_ready(&mut Context::from_waker(&b)).is_pending());
    wait_for_readiness(&mut first, &a, &arx);
    wait_for_readiness(&mut second, &b, &brx);
}

#[tokio::test]
async fn dropping_a_clone_does_not_cancel_another_recovery_notification() {
    let mut first = circuit(Duration::from_millis(30));
    open(&mut first).await;
    let mut second = first.clone();
    let (a, _) = notification();
    let (b, brx) = notification();
    assert!(first.poll_ready(&mut Context::from_waker(&a)).is_pending());
    assert!(second.poll_ready(&mut Context::from_waker(&b)).is_pending());
    drop(first);
    wait_for_readiness(&mut second, &b, &brx);
}

#[tokio::test]
async fn a_late_failure_extends_the_cooldown_without_stranding_the_waiter() {
    let timeout = Duration::from_millis(100);
    let mut service = circuit(timeout);
    poll_fn(|cx| service.poll_ready(cx)).await.unwrap();
    let late_failure = service.call(false);
    open(&mut service).await;
    let (waker, rx) = notification();
    assert!(
        service
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    std::thread::sleep(Duration::from_millis(50));
    let latest_failure = Instant::now();
    assert!(late_failure.await.is_err());
    wait_for_readiness(&mut service, &waker, &rx);
    assert!(
        latest_failure.elapsed() >= timeout,
        "an older timer must not bypass the latest failure's cooldown"
    );
}

#[tokio::test]
async fn repolling_updates_the_registered_waker() {
    let mut service = circuit(Duration::from_millis(50));
    open(&mut service).await;
    let (old, _) = notification();
    let (current, rx) = notification();
    assert!(
        service
            .poll_ready(&mut Context::from_waker(&old))
            .is_pending()
    );
    assert!(
        service
            .poll_ready(&mut Context::from_waker(&current))
            .is_pending()
    );
    wait_for_readiness(&mut service, &current, &rx);
}

#[tokio::test]
async fn zero_cooldown_remains_immediately_ready() {
    let mut service = circuit(Duration::ZERO);
    open(&mut service).await;
    let (waker, _) = notification();
    assert!(matches!(
        service.poll_ready(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(()))
    ));
}

#[tokio::test]
async fn very_large_cooldown_remains_pending_without_panicking() {
    let mut service = circuit(Duration::MAX);
    open(&mut service).await;
    let (waker, _) = notification();
    assert!(
        service
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
}
