use std::task::{Context, Poll};

use miju::prelude::*;

use crate::client::EmailClient;

#[derive(Clone, Debug)]
pub(crate) struct ClientLayer(EmailClient);

impl ClientLayer {
    pub(crate) fn new(client: EmailClient) -> Self {
        Self(client)
    }
}

impl<S> Layer<S> for ClientLayer {
    type Service = ClientService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ClientService {
            inner,
            client: self.0.clone(),
            worker: None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClientService<S> {
    inner: S,
    client: EmailClient,
    worker: Option<WorkerContext>,
}

impl<S, Args> Service<Task<Args>> for ClientService<S>
where
    S: Service<Task<Args>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.client.remaining() == 0
            && !self
                .worker
                .as_ref()
                .is_some_and(WorkerContext::is_shutting_down)
        {
            tracing::warn!("API quota exhausted; worker is waiting for shutdown");
            // This layer runs in the worker driver, outside spawned retries.
            // Shutdown wakes that driver. A refill must also save and wake cx.waker().
            return Poll::Pending;
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut task: Task<Args>) -> Self::Future {
        if self.worker.is_none() {
            self.worker = Some(
                task.data()
                    .get::<WorkerContext>()
                    .expect("the worker injects its context before calling middleware")
                    .clone(),
            );
        }
        task.inject_data(self.client.clone());
        self.inner.call(task)
    }
}
