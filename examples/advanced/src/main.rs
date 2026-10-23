//! Combine application middleware, shared resources, and deferred cache maintenance.

pub mod cache;
mod client;
mod error;
mod layer;

use email_service::Email;
use miju::{layers::catch_panic::CatchPanicLayer, prelude::*};
use tracing::{Instrument, Span, info};
use tracing_subscriber::EnvFilter;

use crate::{
    cache::ValidationCache, client::EmailClient, error::DeliveryError, layer::ClientLayer,
};

async fn deliver(
    email: Email,
    client: Data<EmailClient>,
    cache: Data<ValidationCache>,
    task: TaskContext,
) -> Result<(), DeliveryError> {
    let recipient = email.to.clone();
    if let Some(validation) = cache.get(&recipient) {
        return if validation.valid {
            client.send_unchecked(email).await
        } else {
            Err(DeliveryError::InvalidEmail)
        };
    }

    client.send(email).await?;
    let completion = task.clone();
    tokio::spawn(
        async move {
            // executed() also resolves on cancellation; it is not a success receipt.
            completion.executed().await;
            cache::refresh(recipient, &cache).await;
        }
        .instrument(Span::current()),
    );
    // This deliberately requests cancellation after the simulated side effect.
    // Cancellation cannot undo a delivery; the follow-up is independent of the task.
    task.cancel()
        .expect("the handler's task context is still running");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
        )
        .with_target(false)
        .init();

    let concurrency = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    let mut backend = MemoryStorage::new();
    for index in 0..10 {
        backend
            .push(Email {
                to: format!("reader{index}@example.com"),
                subject: "Shared client demonstration".to_owned(),
                text: "No network delivery takes place".to_owned(),
            })
            .await?;
    }

    WorkerBuilder::new("advanced")
        .backend(&mut backend)
        // Gate new admissions in the worker driver, outside spawned retry futures.
        .layer(ClientLayer::new(EmailClient::new()))
        .parallelize(tokio::spawn)
        .concurrency(concurrency)
        .retry(RetryPolicy::retries(3))
        .layer(CatchPanicLayer::with_panic_handler(|payload| {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_owned())
                })
                .unwrap_or_else(|| "non-string panic payload".to_owned());
            AbortError::new(DeliveryError::Panic(message))
        }))
        .enable_tracing()
        .data(ValidationCache::new())
        .on_event(|worker, event| info!(worker = worker.name(), %event, "Worker event"))
        .build(deliver)
        .run_until(tokio::signal::ctrl_c())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;

    #[tokio::test]
    async fn cached_addresses_take_the_cheap_or_rejected_path() {
        let cache = ValidationCache::new();
        let mut backend = MemoryStorage::new();
        for (recipient, valid) in [("valid@example.com", true), ("invalid@example.com", false)] {
            cache.insert(
                recipient,
                cache::Validation {
                    valid,
                    disposable: false,
                },
            );
            backend
                .push(Email {
                    to: recipient.to_owned(),
                    ..email_service::example_good_email()
                })
                .await
                .unwrap();
        }
        let client = EmailClient::new();
        let results = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&results);
        let worker = WorkerBuilder::new("cache-paths")
            .backend(backend)
            .layer(ClientLayer::new(client.clone()))
            .data(cache)
            .on_event(move |worker, event| {
                let result = match event {
                    Event::Success(_) => Ok(()),
                    Event::Error(error) => Err(error.to_string()),
                    _ => return,
                };
                let mut observed = observed.lock().unwrap();
                observed.push(result);
                if observed.len() == 2 {
                    worker.stop().unwrap();
                }
            })
            .build(deliver);
        tokio::time::timeout(Duration::from_secs(3), worker.run())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *results.lock().unwrap(),
            vec![Ok(()), Err("cached address is invalid".to_owned())]
        );
        assert_eq!(client.remaining(), 9);
    }
}
