use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use miju_core::{backend::TaskResult, error::BoxDynError, task::Task};
use serde::Serialize;
use tower::{Layer, Service};

use crate::in_memory::result_store::ResultStore;

/// A layer for a service that stores results in memory
#[derive(Debug, Clone)]
pub struct StoreResultsLayer {
    store: Arc<ResultStore>,
}

impl StoreResultsLayer {
    /// Create middleware that writes to the given result store.
    #[must_use]
    pub fn new(store: Arc<ResultStore>) -> Self {
        Self { store }
    }

    /// Clone the shared result store handle.
    #[must_use]
    pub fn store(&self) -> Arc<ResultStore> {
        Arc::clone(&self.store)
    }
}

impl<S> Layer<S> for StoreResultsLayer {
    type Service = StoreResultsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        StoreResultsService {
            service,
            store: Arc::clone(&self.store),
        }
    }
}

/// A service that stores results in memory
#[derive(Debug, Clone)]
pub struct StoreResultsService<S> {
    service: S,
    store: Arc<ResultStore>,
}

impl<S, Args, Res> Service<Task<Args>> for StoreResultsService<S>
where
    S: Service<Task<Args>, Response = Res>,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError> + Send + Sync + 'static,
    Res: Serialize,
{
    type Response = Res;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        let task_id = task.task_id().cloned().unwrap();
        let attempt = task.raw_attempt().clone();
        let status = task.raw_status().clone();
        let store = Arc::clone(&self.store);

        let future = self.service.call(task);

        Box::pin(async move {
            let res: Result<_, BoxDynError> = future.await.map_err(Into::into).and_then(|o| {
                let value = serde_json::to_value(&o)?;
                Ok((o, value))
            });

            match res {
                Ok((o, value)) => {
                    store.insert(
                        task_id.clone(),
                        TaskResult {
                            task_id,
                            attempt: attempt.current(),
                            status: status.load(),
                            result: Ok(value),
                        },
                    );

                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        results = ?&store.results.len(),
                        wakers = ?&store.wakers.len(),
                        "Stored a new success",
                    );
                    Ok(o)
                }
                Err(error) => {
                    let value = error.to_string();
                    store.insert(
                        task_id.clone(),
                        TaskResult {
                            task_id,
                            attempt: attempt.current(),
                            status: status.load(),
                            result: Err(value),
                        },
                    );
                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        results = ?&store.results.len(),
                        "Stored a new failure",
                    );
                    Err(error)
                }
            }
        })
    }
}
