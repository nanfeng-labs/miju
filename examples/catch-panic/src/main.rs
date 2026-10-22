//! Capture panics from email tasks and observe errors without stopping the worker.

use miju::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct Email {
    recipient: String,
}

async fn send_email(email: Email) {
    // Deliberately fail without sending mail; the panic hook still prints a message.
    panic!("Simulated email failure for {}", email.recipient);
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let mut backend = MemoryStorage::new();
    for recipient in ["first@example.com", "second@example.com"] {
        backend
            .push(Email {
                recipient: recipient.to_owned(),
            })
            .await?;
    }

    info!("Two email tasks will panic; press Ctrl+C to stop the worker afterwards");
    WorkerBuilder::new("catch-panic")
        .backend(backend)
        // Keep the concurrency permit for the whole task, including retries.
        .concurrency(2)
        // Panic capture boxes its AbortError. This policy does not unpack it,
        // so each task panics once, retries once, then reports the final error.
        .retry(RetryPolicy::retries(1))
        // Poll the handler inside this layer so unwinding panics are caught here.
        .catch_panic()
        .enable_tracing()
        .on_event(|_, event| info!(%event, "Worker event"))
        .build(send_email)
        .run_until(tokio::signal::ctrl_c())
        .await?;

    Ok(())
}
