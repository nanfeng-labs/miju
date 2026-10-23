//! Report simulated failures with per-task hubs, breadcrumbs, and performance spans.

use std::{fmt, time::Duration};

use email_service::{Email, example_good_email};
use miju::{layers::sentry::SentryLayer, prelude::*};
use sentry_tower::NewSentryLayer;
use tracing_subscriber::prelude::*;

#[derive(Debug)]
struct DeliveryRejected(String);

impl fmt::Display for DeliveryRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "simulated delivery rejected for {}", self.0)
    }
}

impl std::error::Error for DeliveryRejected {}

macro_rules! progress {
    ($message:literal, $percent:expr) => {
        tracing::info!(progress = $percent, $message);
    };
}

async fn deliver(email: Email) -> Result<(), DeliveryRejected> {
    let parent = sentry::configure_scope(|scope| scope.get_span());
    let transaction = sentry::start_transaction(sentry::TransactionContext::continue_from_span(
        "email.send",
        "miju.job",
        parent.clone(),
    ));
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));
    tracing::info!(recipient = email.to, "Starting simulated delivery");

    let dns = transaction.start_child("dns", "Look up mail configuration");
    progress!("Checking mail configuration", 10);
    tokio::time::sleep(Duration::from_millis(1_008)).await;
    progress!("Mail configuration found", 20);
    dns.finish();

    let provider = transaction.start_child("provider", "Read delivery provider settings");
    tokio::time::sleep(Duration::from_millis(712)).await;
    progress!("Provider settings loaded", 30);
    provider.finish();

    let user = transaction.start_child("user", "Load recipient details");
    tokio::time::sleep(Duration::from_millis(100)).await;
    progress!("Recipient found", 50);
    // These spans are children of the user lookup, not siblings on the transaction.
    let database = user.start_child("user.database", "Read recipient database");
    tracing::warn!("Inspecting additional recipient records");
    tokio::time::sleep(Duration::from_millis(209)).await;
    database.finish();
    let lookup = user.start_child("user.by_id", "Resolve recipient identifier");
    tokio::time::sleep(Duration::from_millis(120)).await;
    lookup.finish();
    user.finish();

    tracing::warn!("Simulated delivery rejected");
    transaction.set_status(sentry::protocol::SpanStatus::InvalidArgument);
    transaction.finish();
    // Restore the outer task transaction before SentryLayer captures the error.
    sentry::configure_scope(|scope| scope.set_span(parent));
    Err(DeliveryRejected(email.to))
}

async fn run_worker(
    signal: impl Future<Output = std::io::Result<()>> + Send + 'static,
) -> Result<(), BoxDynError> {
    let mut backend = MemoryStorage::new();
    for recipient in ["first@example.com", "second@example.com"] {
        backend
            .push(Email {
                to: recipient.to_owned(),
                ..example_good_email()
            })
            .await?;
    }
    WorkerBuilder::new("email-errors")
        // Consume the very backend populated above.
        .backend(backend)
        // A separate hub for every task prevents concurrent breadcrumbs and
        // task metadata from leaking into each other's events.
        .layer(NewSentryLayer::new_from_top())
        .layer(SentryLayer::new())
        .enable_tracing()
        .concurrency(2)
        .build(deliver)
        .run_until(signal)
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let dsn: sentry::types::Dsn = std::env::var("SENTRY_DSN")
        .map_err(|_| "Set SENTRY_DSN before running this example")?
        .parse()?;
    // Sample every event and transaction so both demonstration tasks are visible.
    // Keep the guard until after worker shutdown so the transport can drain.
    let _guard = sentry::init((
        dsn,
        sentry::ClientOptions::new()
            .sample_rate(1.0)
            .traces_sample_rate(1.0)
            .maybe_release(sentry::release_name!()),
    ));
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_target(false))
        // SentryLayer reports handler errors; tracing adds breadcrumbs, avoiding
        // a duplicate event for the same failure logged by the tracing middleware.
        .with(sentry_tracing::layer().event_filter(|_| sentry_tracing::EventFilter::Breadcrumb))
        .init();
    tracing::info!("Two simulated tasks will fail; press Ctrl+C after their progress logs");
    run_worker(tokio::signal::ctrl_c()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentry::{SentryFutureExt, protocol::EnvelopeItem};
    use std::sync::Arc;
    use tracing::instrument::WithSubscriber;

    #[tokio::test]
    async fn errors_breadcrumbs_and_nested_spans_are_captured_without_network() {
        let transport = sentry::test::TestTransport::new();
        let options = sentry::ClientOptions::new()
            .dsn("https://public@example.invalid/1")
            .transport(transport.clone())
            .sample_rate(1.0)
            .traces_sample_rate(1.0);
        let hub = Arc::new(sentry::Hub::new(
            Some(Arc::new(options.into())),
            Arc::new(Default::default()),
        ));
        let subscriber = tracing_subscriber::registry().with(
            sentry_tracing::layer().event_filter(|_| sentry_tracing::EventFilter::Breadcrumb),
        );
        let signal = async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        };
        run_worker(signal)
            .bind_hub(hub)
            .with_subscriber(subscriber)
            .await
            .unwrap();
        let items: Vec<_> = transport
            .fetch_and_clear_envelopes()
            .into_iter()
            .flat_map(sentry::Envelope::into_items)
            .collect();
        let events: Vec<_> = items
            .iter()
            .filter_map(|item| match item {
                EnvelopeItem::Event(event) => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(events.len(), 2);
        assert_ne!(events[0].event_id, events[1].event_id);
        for event in events {
            assert!(event.contexts.contains_key("task"));
            assert!(event.tags.contains_key("queue"));
            let recipients: Vec<_> = event
                .breadcrumbs
                .iter()
                .filter_map(|crumb| crumb.data.get("recipient"))
                .collect();
            assert_eq!(
                recipients.len(),
                1,
                "each task must have only its own recipient breadcrumb"
            );
            assert!(
                event
                    .breadcrumbs
                    .iter()
                    .any(|crumb| crumb.data.contains_key("progress"))
            );
        }
        let transactions: Vec<_> = items
            .iter()
            .filter_map(|item| match item {
                EnvelopeItem::Transaction(transaction) => Some(transaction),
                _ => None,
            })
            .collect();
        assert_eq!(transactions.len(), 4);
        let deliveries: Vec<_> = transactions
            .iter()
            .filter(|tx| tx.name.as_deref() == Some("email.send"))
            .collect();
        assert_eq!(deliveries.len(), 2);
        for transaction in deliveries {
            assert_eq!(transaction.spans.len(), 5);
            let user = transaction
                .spans
                .iter()
                .find(|span| span.op.as_deref() == Some("user"))
                .unwrap();
            let nested: Vec<_> = transaction
                .spans
                .iter()
                .filter(|span| span.parent_span_id == Some(user.span_id))
                .collect();
            assert_eq!(nested.len(), 2);
        }
    }
}
