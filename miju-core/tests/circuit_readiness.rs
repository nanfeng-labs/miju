//! Readiness remains resolved until dispatch while shared circuit policy can change.

use std::{
    future::ready,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use miju_core::{
    backend::memory::MemoryStorage,
    error::BoxDynError,
    worker::{
        builder::WorkerBuilder,
        ext::circuit_breaker::{CircuitBreaker, config::CircuitBreakerConfig},
    },
};
use tower::{Service, service_fn};
use tower_layer::Layer;

fn circuit<S>(
    inner: S,
    config: CircuitBreakerConfig,
) -> impl Service<bool, Response = (), Error = BoxDynError> + Clone
where
    S: Service<bool, Response = (), Error = io::Error> + Clone,
    S::Future: Send + 'static,
{
    let mut service = None;
    let _builder = WorkerBuilder::new("readiness")
        .backend(MemoryStorage::<bool>::new())
        .break_circuit_with(config)
        .chain(|layer| {
            service = Some(layer.layer(inner));
            layer
        });
    service.unwrap()
}

#[tokio::test]
async fn ready_instance_remains_ready_when_a_peer_opens_the_circuit() {
    for succeeds in [true, false] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let inner = service_fn(move |succeeds: bool| {
            observed.fetch_add(1, Ordering::SeqCst);
            ready(if succeeds {
                Ok(())
            } else {
                Err(io::Error::other("failure"))
            })
        });
        let mut peer = circuit(
            inner,
            CircuitBreakerConfig::default()
                .failure_threshold(1)
                .recovery_timeout(Duration::MAX),
        );
        let mut admitted = peer.clone();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(admitted.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(peer.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        assert_eq!(peer.call(succeeds).await.is_ok(), succeeds);

        let later = admitted.poll_ready(&mut cx);
        assert!(
            matches!(later, Poll::Ready(Ok(()))),
            "readiness must stay resolved before call: {later:?}"
        );
        let mut fresh = admitted.clone();
        if !succeeds {
            assert!(
                fresh.poll_ready(&mut cx).is_pending(),
                "clones must not inherit readiness"
            );
        }
        assert_eq!(admitted.call(true).await.is_ok(), succeeds);
        assert_eq!(calls.load(Ordering::SeqCst), if succeeds { 2 } else { 1 });
        if !succeeds {
            assert!(
                admitted.poll_ready(&mut cx).is_pending(),
                "call must consume the readiness latch"
            );
        }
    }
}

#[tokio::test]
async fn inner_readiness_is_not_inherited_and_unused_permits_are_released_on_drop() {
    let inner = tower::limit::ConcurrencyLimit::new(
        service_fn(|_: bool| ready(Ok::<(), io::Error>(()))),
        1,
    );
    let mut first = circuit(inner, CircuitBreakerConfig::default());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(first.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    let mut second = first.clone();
    assert!(second.poll_ready(&mut cx).is_pending());
    assert!(second.poll_ready(&mut cx).is_pending());
    drop(first);
    assert!(matches!(second.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    let mut third = second.clone();
    assert!(third.poll_ready(&mut cx).is_pending());
    drop(second);
    assert!(matches!(third.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert!(third.call(true).await.is_ok());
}
