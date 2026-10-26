//! Circuit Breaker extension for workers.
//!
//! The [`CircuitBreaker`] trait and related types enable circuit breaker
//! functionality for workers. The circuit breaker pattern helps prevent repeated failures
//! by temporarily halting task processing when a configurable error threshold is reached.
//!
//! # Features
//! - Automatically breaks the circuit when a failure threshold is hit.
//! - Configurable recovery timeout, success threshold, and half-open state behavior.
//! - Integrates with worker middleware stack.
//!
//! # Example
//!
//! ```rust
//! # use miju_core::worker::ext::circuit_breaker::{CircuitBreaker, config::CircuitBreakerConfig};
//! # use miju_core::worker::builder::WorkerBuilder;
//! # use miju_core::backend::memory::MemoryStorage;
//! # use miju_core::worker::context::WorkerContext;
//! # use miju_core::backend::TaskSink;
//! # use miju_core::error::BoxDynError;
//! use std::time::Duration;
//!
//! const ITEMS: u32 = 10;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     for i in 0..ITEMS {
//!         in_memory.push(i).await.unwrap();
//!     }
//!
//!     async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!         if task == ITEMS - 1 {
//!             worker.stop().unwrap();
//!             Err("Worker stopped!")?;
//!         }
//!         if task % 3 == 0 {
//!             return Ok(());
//!         } else {
//!             return Err("Expected Error stopped!")?;
//!         }
//!     }
//!
//!     let config = CircuitBreakerConfig::default()
//!         .failure_threshold(1)
//!         .recovery_timeout(Duration::from_secs(1))
//!         .success_threshold(0.5)
//!         .half_open_max_calls(1);
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .break_circuit_with(config)
//!         .build(task);
//!
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! This example demonstrates how to configure a worker with a circuit breaker, set custom thresholds,
//! and process tasks with automatic circuit breaking on repeated failures.
//!
//! See [`CircuitBreakerConfig`] for configuration options.
use tower_layer::{Layer, Stack};

use crate::{
    backend::Backend,
    worker::{
        builder::WorkerBuilder,
        ext::circuit_breaker::{config::CircuitBreakerConfig, service::CircuitBreakerLayer},
    },
};

/// Configuration options for the circuit breaker.
pub mod config;
mod service;
/// Allows breaking the circuit if an error threshold is hit
///
/// See [module level documentation](self) for more details.
pub trait CircuitBreaker<Args, Source, Middleware>: Sized {
    /// Allows the worker to break the circuit in case of failures
    /// Uses default configuration
    fn break_circuit(self) -> WorkerBuilder<Args, Source, Stack<CircuitBreakerLayer, Middleware>> {
        self.break_circuit_with(CircuitBreakerConfig::default())
    }

    /// Allows the worker to break the circuit w in case of failures
    /// Allows customizing the CircuitBreakerConfig
    fn break_circuit_with(
        self,
        cfg: CircuitBreakerConfig,
    ) -> WorkerBuilder<Args, Source, Stack<CircuitBreakerLayer, Middleware>>;
}

impl<Args, P, M> CircuitBreaker<Args, P, M> for WorkerBuilder<Args, P, M>
where
    P: Backend,
    M: Layer<CircuitBreakerLayer>,
{
    fn break_circuit_with(
        self,
        config: CircuitBreakerConfig,
    ) -> WorkerBuilder<Args, P, Stack<CircuitBreakerLayer, M>> {
        let this = self.layer(CircuitBreakerLayer::new(config));
        WorkerBuilder {
            context: this.context,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{task::Poll, time::Duration};

    use futures_util::{StreamExt, poll};
    use tower::limit::ConcurrencyLimitLayer;

    use crate::{
        backend::{TaskSink, memory::MemoryStorage},
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            event::Event,
            ext::{circuit_breaker::CircuitBreaker, event_listener::EventListenerExt},
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn worker_uses_configured_failure_threshold() {
        for failure_threshold in [1, 7, 50] {
            let mut backend = MemoryStorage::new();
            for _ in 0..=failure_threshold {
                backend.push(()).await.unwrap();
            }
            let config = CircuitBreakerConfig::default()
                .failure_threshold(failure_threshold)
                .recovery_timeout(Duration::from_secs(60));
            let mut events = WorkerBuilder::new("configured-failure-threshold")
                .backend(backend)
                .break_circuit_with(config)
                .build(|_: ()| async { Err::<(), _>("expected failure") })
                .stream();

            assert!(matches!(events.next().await, Some(Ok(Event::Start))));
            for _ in 0..failure_threshold {
                assert!(matches!(
                    poll!(events.next()),
                    Poll::Ready(Some(Ok(Event::Error(_))))
                ));
            }
            assert!(poll!(events.next()).is_pending());
        }
    }

    #[tokio::test]
    async fn worker_uses_configured_success_threshold() {
        let mut backend = MemoryStorage::new();
        for succeeds in [false, false, false, false, false, true, true, true] {
            backend.push(succeeds).await.unwrap();
        }
        let config = CircuitBreakerConfig::default()
            .failure_threshold(5)
            .recovery_timeout(Duration::ZERO)
            .success_threshold(0.125)
            .half_open_max_calls(1);
        let mut events = WorkerBuilder::new("configured-success-threshold")
            .backend(backend)
            .break_circuit_with(config)
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
            assert!(matches!(
                poll!(events.next()),
                Poll::Ready(Some(Ok(Event::Error(_))))
            ));
        }
        for _ in 0..3 {
            assert!(matches!(
                poll!(events.next()),
                Poll::Ready(Some(Ok(Event::Success(_))))
            ));
        }
    }

    #[tokio::test]
    async fn basic_worker() {
        let mut in_memory = MemoryStorage::new();
        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                Err("Worker stopped!")?;
            }
            if task == 8 {
                worker.stop().unwrap();
            }
            if task % 3 == 0 {
                Ok(())
            } else {
                return Err("Expected Error stopped!")?;
            }
        }

        let config = CircuitBreakerConfig::default()
            .failure_threshold(1)
            .recovery_timeout(Duration::from_secs(1))
            .success_threshold(0.5)
            .half_open_max_calls(1);

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .break_circuit_with(config)
            .layer(ConcurrencyLimitLayer::new(3))
            .on_event(|_worker, ev| {
                println!("On Event = {ev:?}");
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
