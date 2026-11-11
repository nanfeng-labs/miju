//! Readiness remains resolved until dispatch and admits the call it was ready for,
//! even after shared circuit policy changes.

use std::{
    future::ready,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use miju_core::{
    backend::{
        TaskSink,
        memory::{MemorySink, MemoryStorage},
    },
    error::{BoxDynError, WorkerError},
    task::Task,
    worker::{
        builder::WorkerBuilder,
        event::Event,
        ext::{
            circuit_breaker::{CircuitBreaker, config::CircuitBreakerConfig},
            event_listener::EventListenerExt,
        },
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
        // The call that readiness admitted runs even though the peer opened the circuit.
        assert!(admitted.call(true).await.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
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

#[tokio::test]
async fn task_taken_after_the_circuit_opens_runs_instead_of_failing() {
    let (sender, receiver) = futures_channel::mpsc::unbounded::<Task<bool>>();
    let mut producer = MemorySink::new(Arc::new(futures_util::lock::Mutex::new(Box::new(
        sender.sink_map_err(Into::into),
    ))));
    let mut backend = MemoryStorage::new_with(producer.clone(), receiver.boxed());
    backend.push(false).await.unwrap();
    let (calls, errors) = (
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
    );
    let (seen, logged) = (calls.clone(), errors.clone());
    WorkerBuilder::new("circuit-queue")
        .backend(backend)
        .break_circuit_with(
            CircuitBreakerConfig::default()
                .failure_threshold(1)
                .recovery_timeout(Duration::from_secs(60)),
        )
        .on_event(move |_, event| {
            if let Event::Error(error) = event {
                logged.lock().unwrap().push(error.to_string());
            }
        })
        .build(move |succeeds: bool| {
            seen.lock().unwrap().push(succeeds);
            async move {
                if succeeds {
                    Ok(())
                } else {
                    // Fail after the idle worker has already reported readiness again.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Err::<(), BoxDynError>("boom".into())
                }
            }
        })
        .run_until(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            for _ in 0..2 {
                producer.send(Task::new(true)).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<(), WorkerError>(())
        })
        .await
        .unwrap();

    // The task taken on the retained readiness runs; the next waits for recovery.
    assert_eq!(*calls.lock().unwrap(), [false, true]);
    assert_eq!(*errors.lock().unwrap(), ["boom"]);
}

#[tokio::test]
async fn call_without_readiness_is_rejected_while_the_circuit_is_open() {
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
    let mut service = circuit(
        inner,
        CircuitBreakerConfig::default()
            .failure_threshold(1)
            .recovery_timeout(Duration::MAX),
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert!(service.call(false).await.is_err());

    // A clone that never reported readiness has nothing admitted.
    let error = service.clone().call(true).await.unwrap_err();
    assert_eq!(error.to_string(), "Circuit breaker is open");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
