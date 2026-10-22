//! Compare successful, retried, and aborted email tasks using a bounded policy.

use std::time::Duration;

use email_service::{Email, send_email};
use miju::{
    layers::retry::{HasherRng, backoff::ExponentialBackoffMaker, backoff::MakeBackoff},
    prelude::*,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

async fn process(email: Email, attempt: Attempt) -> Result<(), BoxDynError> {
    info!(
        recipient = email.to,
        attempt = attempt.current(),
        "Delivery attempt"
    );
    send_email(email).await
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
    for recipient in [
        "reader@example.com", // Success: one attempt.
        "reader.example.com", // Ordinary error: initial attempt plus three retries.
        "A@b@c@example.com",  // Invalid character: abort without retrying.
    ] {
        backend
            .push(Email {
                to: recipient.to_owned(),
                subject: "Retry demonstration".to_owned(),
                text: "A simulated email job".to_owned(),
            })
            .await?;
    }

    let backoff = ExponentialBackoffMaker::new(
        Duration::from_secs(1),
        Duration::from_secs(5),
        1.25,
        HasherRng::default(),
    )?
    .make_backoff();

    WorkerBuilder::new("retries")
        .backend(backend)
        .retry(
            RetryPolicy::retries(3)
                .with_backoff(backoff)
                .retry_if(|error: &BoxDynError| error.downcast_ref::<AbortError>().is_none()),
        )
        .enable_tracing()
        .on_event(|worker, event| info!(worker = worker.name(), %event, "Worker event"))
        .build(process)
        .run_until(tokio::signal::ctrl_c())
        .await?;

    Ok(())
}
