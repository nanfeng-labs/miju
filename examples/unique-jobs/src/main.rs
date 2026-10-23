//! Use a recipient as an idempotency key to suppress a duplicate email job.

use email_service::{Email, send_email};
use miju::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

async fn deliver(email: Email) -> Result<(), BoxDynError> {
    info!(subject = email.subject, "Processing unique email");
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
    let recipient = "reader@example.com";
    for subject in ["First notification", "Duplicate notification"] {
        let email = Email {
            to: recipient.to_owned(),
            subject: subject.to_owned(),
            text: "Only the first task should execute".to_owned(),
        };
        // The key, rather than the payload or generated task ID, determines uniqueness.
        let task = TaskBuilder::new(email).idempotency_key(recipient).build();
        backend.push_task(task).await?;
    }

    // Both pushes return Ok, but the duplicate is discarded before reaching the worker.
    // Keys live with this in-memory backend; a fresh backend starts a new set.
    WorkerBuilder::new("unique-jobs")
        .backend(backend)
        .enable_tracing()
        .build(deliver)
        .run_until(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}
