//! Regression tests for bounded half-open recovery.

use futures_util::future::poll_fn;
use miju_core::{
    backend::memory::MemoryStorage,
    error::BoxDynError,
    worker::{
        builder::WorkerBuilder,
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

fn circuit(
    timeout: Duration,
    max: u32,
) -> impl Service<bool, Response = (), Error = BoxDynError> + Clone {
    let mut service = None;
    let _builder = WorkerBuilder::new("budget")
        .backend(MemoryStorage::<bool>::new())
        .break_circuit_with(
            CircuitBreakerConfig::default()
                .failure_threshold(5)
                .success_threshold(0.5)
                .half_open_max_calls(max)
                .recovery_timeout(timeout),
        )
        .chain(|layer| {
            service = Some(layer.layer(service_fn(|succeeds: bool| async move {
                if succeeds { Ok(()) } else { Err("failure") }
            })));
            layer
        });
    service.unwrap()
}

async fn open<S: Service<bool, Error = BoxDynError>>(s: &mut S) {
    for _ in 0..5 {
        poll_fn(|cx| s.poll_ready(cx)).await.unwrap();
        assert!(s.call(false).await.is_err());
    }
}

async fn success<S: Service<bool, Error = BoxDynError>>(s: &mut S) {
    poll_fn(|cx| s.poll_ready(cx)).await.unwrap();
    assert!(s.call(true).await.is_ok());
}

#[tokio::test]
async fn exhausted_successful_budget_recovers() {
    let mut s = circuit(Duration::from_millis(20), 3);
    open(&mut s).await;
    for _ in 0..4 {
        success(&mut s).await;
    }
    tokio::time::timeout(Duration::from_secs(1), success(&mut s))
        .await
        .expect("exhausted successful probes must renew");
}

#[tokio::test]
async fn cancellation_does_not_permanently_consume_budget() {
    let mut s = circuit(Duration::from_millis(20), 1);
    open(&mut s).await;
    for _ in 0..2 {
        poll_fn(|cx| s.poll_ready(cx)).await.unwrap();
        drop(s.call(true));
    }
    tokio::time::timeout(Duration::from_secs(1), success(&mut s))
        .await
        .expect("dropped probes must renew");
}

#[tokio::test]
async fn zero_budget_keeps_the_first_probe_each_round() {
    let mut s = circuit(Duration::ZERO, 0);
    open(&mut s).await;
    for _ in 0..6 {
        tokio::time::timeout(Duration::from_secs(1), success(&mut s))
            .await
            .expect("zero budget first probe");
    }
}

#[tokio::test]
async fn cloned_waiters_are_not_stranded_by_shared_renewal() {
    let mut s = circuit(Duration::from_millis(20), 1);
    open(&mut s).await;
    success(&mut s).await;
    success(&mut s).await;
    let mut b = s.clone();
    let mut c = s.clone();
    let (a, arx) = notification();
    let (bw, brx) = notification();
    let (cw, crx) = notification();
    assert!(s.poll_ready(&mut Context::from_waker(&a)).is_pending());
    assert!(b.poll_ready(&mut Context::from_waker(&bw)).is_pending());
    assert!(c.poll_ready(&mut Context::from_waker(&cw)).is_pending());
    for (waiter, w, rx) in [(&mut s, a, arx), (&mut b, bw, brx), (&mut c, cw, crx)] {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("timer wake");
            if let Poll::Ready(r) = waiter.poll_ready(&mut Context::from_waker(&w)) {
                r.unwrap();
                break;
            }
        }
        assert!(waiter.call(true).await.is_ok());
    }
}

#[tokio::test]
async fn late_success_does_not_strand_existing_cooldown_waiters() {
    let mut s = circuit(Duration::from_millis(300), 3);
    poll_fn(|cx| s.poll_ready(cx)).await.unwrap();
    let late = s.call(true);
    open(&mut s).await;
    for _ in 0..4 {
        success(&mut s).await;
    }
    let mut other = s.clone();
    let (a, arx) = notification();
    let (b, brx) = notification();
    assert!(s.poll_ready(&mut Context::from_waker(&a)).is_pending());
    assert!(other.poll_ready(&mut Context::from_waker(&b)).is_pending());
    assert!(late.await.is_ok());
    arx.recv_timeout(Duration::from_secs(1))
        .expect("finite cooldown must notify first waiter");
    brx.recv_timeout(Duration::from_secs(1))
        .expect("finite cooldown must notify other waiter");
}

#[tokio::test]
async fn late_failure_starts_its_own_recovery_timeout() {
    let timeout = Duration::from_millis(40);
    let mut service = circuit(timeout, 1);
    poll_fn(|cx| service.poll_ready(cx)).await.unwrap();
    let late_failure = service.call(false);
    open(&mut service).await;
    success(&mut service).await;
    success(&mut service).await;
    let (waker, _) = notification();
    assert!(
        service
            .poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(late_failure.await.is_err());
    let failed_at = Instant::now();
    tokio::time::timeout(Duration::from_secs(1), success(&mut service))
        .await
        .unwrap();
    assert!(failed_at.elapsed() >= timeout);
}

#[tokio::test]
async fn canceled_probes_do_not_change_failure_history() {
    let _builder = WorkerBuilder::new("history")
        .backend(MemoryStorage::<bool>::new())
        .break_circuit_with(
            CircuitBreakerConfig::default()
                .failure_threshold(5)
                .recovery_timeout(Duration::ZERO)
                .half_open_max_calls(1),
        )
        .chain(|layer| {
            let mut service =
                layer.layer(service_fn(|_: bool| async { Ok::<(), BoxDynError>(()) }));
            for _ in 0..5 {
                service.record_failure();
            }
            let before = service.get_stats();
            let (waker, _) = notification();
            let mut cx = Context::from_waker(&waker);
            assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
            drop(service.call(true));
            assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
            drop(service.call(true));
            assert!(
                service
                    .poll_ready(&mut Context::from_waker(&waker))
                    .is_ready()
            );
            drop(service.call(true));
            let after = service.get_stats();
            assert_eq!(after.failure_count, before.failure_count);
            assert_eq!(after.last_failure_time, before.last_failure_time);
            assert_eq!(after.success_count, 0);
            layer
        });
}
