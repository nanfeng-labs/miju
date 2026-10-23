//! Queue heterogeneous payloads through generated enum dispatch.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use email_service::{Email, example_good_email, send_email};
use enum_dispatch::enum_dispatch;
use futures_util::future::BoxFuture;
use miju::prelude::*;

#[enum_dispatch]
trait ServiceHandler {
    fn handle(self) -> BoxFuture<'static, Result<(), BoxDynError>>;
}

#[enum_dispatch(ServiceHandler)]
#[derive(Debug, Clone)]
enum Job {
    Email,
    String,
}

impl ServiceHandler for Email {
    fn handle(self) -> BoxFuture<'static, Result<(), BoxDynError>> {
        Box::pin(send_email(self))
    }
}

impl ServiceHandler for String {
    fn handle(self) -> BoxFuture<'static, Result<(), BoxDynError>> {
        Box::pin(async move {
            tracing::info!(message = self, "Handled text variant");
            Ok(())
        })
    }
}

async fn handle(
    job: Job,
    completed: Data<Arc<AtomicUsize>>,
    worker: WorkerContext,
) -> Result<(), BoxDynError> {
    // enum_dispatch supplies the match; the worker sees one queue payload type.
    job.handle().await?;
    if completed.fetch_add(1, Ordering::SeqCst) == 1 {
        worker.stop()?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut backend = MemoryStorage::<Job>::new();
    backend.push(example_good_email().into()).await?;
    backend.push("Hello mixed queue".to_owned().into()).await?;
    let completed = Arc::new(AtomicUsize::new(0));
    WorkerBuilder::new("mixed-payloads")
        .backend(backend)
        .data(completed.clone())
        .enable_tracing()
        .build(handle)
        .run()
        .await?;
    assert_eq!(completed.load(Ordering::SeqCst), 2);
    Ok(())
}
