//! Reuse a statically dispatched worker factory for different payload types.

use email_service::{Email, example_good_email, send_email};
use miju::prelude::*;
use tracing::{Instrument, Span};

trait ServiceHandler {
    fn handle(self) -> impl Future<Output = Result<(), BoxDynError>> + Send;
}

impl ServiceHandler for Email {
    async fn handle(self) -> Result<(), BoxDynError> {
        send_email(self).await
    }
}

impl ServiceHandler for String {
    async fn handle(self) -> Result<(), BoxDynError> {
        tracing::info!(message = self, "Handled text payload");
        Ok(())
    }
}

async fn handle<T: ServiceHandler>(input: T) -> Result<(), BoxDynError> {
    // Instrument the future, rather than holding an entered span across await.
    input.handle().instrument(Span::current()).await
}

async fn run_worker<T>(input: T) -> Result<(), BoxDynError>
where
    T: ServiceHandler + Send + Unpin + Clone + 'static,
{
    let mut backend = MemoryStorage::<T>::new();
    backend.push(input).await?;
    let context = WorkerContext::new(&format!("worker-{}", std::any::type_name::<T>()));
    WorkerBuilder::new(&context)
        .backend(backend)
        .enable_tracing()
        .build(move |input: T| {
            let context = context.clone();
            async move {
                handle(input).await?;
                context.stop()?;
                Ok::<(), BoxDynError>(())
            }
        })
        .run()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // Separate typed queues and workers share the same generic implementation.
    tokio::try_join!(
        run_worker(example_good_email()),
        run_worker("Hello worker".to_owned())
    )?;
    Ok(())
}
