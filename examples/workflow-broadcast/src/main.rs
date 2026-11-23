//! Publish each order once to three workers that each decide whether to handle it.
//!
//! Every worker receives every order on its own queue and runs on its own Tokio
//! task. The main task then reads each worker's decision from its result store.

use std::time::Duration;

use futures_util::StreamExt;
use miju::prelude::*;
use miju_workflow::broadcast::{BroadcastFlow, Decision, when};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: u64,
    amount_cents: u64,
    country: String,
}

const EU: [&str; 3] = ["DE", "FR", "NL"];

async fn record_audit(order: OrderPlaced) -> Result<(), BoxDynError> {
    info!(order_id = order.order_id, thread = ?std::thread::current().id(), "Audit recorded");
    Ok(())
}

// Handlers are ordinary task functions, so injected arguments still work.
async fn review_fraud(order: OrderPlaced, worker: WorkerContext) -> Result<(), BoxDynError> {
    info!(
        order_id = order.order_id,
        worker = worker.name(),
        thread = ?std::thread::current().id(),
        "Fraud review queued"
    );
    Ok(())
}

async fn charge_eu_vat(order: OrderPlaced) -> Result<u64, BoxDynError> {
    let vat = order.amount_cents * 19 / 100;
    info!(order_id = order.order_id, vat, "EU VAT charged");
    Ok(vat)
}

async fn stop_signal(mut rx: watch::Receiver<()>) -> Result<(), std::io::Error> {
    let _ = rx.changed().await;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    // One in-memory queue and result store per subscriber.
    let orders = BroadcastFlow::<OrderPlaced>::new("orders")
        .subscriber("audit")
        .subscriber("fraud")
        .subscriber("eu-vat")
        .in_memory()?;

    // Each worker decides on its own whether to handle an order.
    let audit = WorkerBuilder::new("audit")
        .backend(orders.backend("audit").expect("declared above"))
        .catch_panic()
        .build(when(|_: &OrderPlaced| true, record_audit));
    let fraud = WorkerBuilder::new("fraud")
        .backend(orders.backend("fraud").expect("declared above"))
        .catch_panic()
        .build(when(
            |order: &OrderPlaced| order.amount_cents >= 100_000,
            review_fraud,
        ));
    let eu_vat = WorkerBuilder::new("eu-vat")
        .backend(orders.backend("eu-vat").expect("declared above"))
        .catch_panic()
        .build(when(
            |order: &OrderPlaced| EU.contains(&order.country.as_str()),
            charge_eu_vat,
        ));

    let (stop, stopped) = watch::channel(());
    let running = [
        tokio::spawn(audit.run_until(stop_signal(stopped.clone()))),
        tokio::spawn(fraud.run_until(stop_signal(stopped.clone()))),
        tokio::spawn(eu_vat.run_until(stop_signal(stopped))),
    ];

    // Each order is published once; every subscriber gets its own delivery.
    let mut publisher = orders.publisher();
    let mut receipts = Vec::new();
    for (order_id, amount_cents, country) in [(7, 250_000, "US"), (8, 4_500, "DE")] {
        let order = OrderPlaced {
            order_id,
            amount_cents,
            country: country.to_owned(),
        };
        let receipt = publisher.publish(order).await?;
        info!(
            order_id,
            deliveries = receipt.deliveries.len(),
            "Order published"
        );
        receipts.push((order_id, receipt));
    }

    for (order_id, receipt) in &receipts {
        for delivery in &receipt.deliveries {
            let mut backend = orders
                .backend(&delivery.subscriber)
                .expect("declared above");
            let mut results = WaitForCompletion::<Decision<serde_json::Value>>::wait_for_single(
                &mut backend,
                delivery.task_id.clone(),
            );
            let outcome = tokio::time::timeout(Duration::from_secs(5), results.next())
                .await?
                .expect("one result per delivery")?;
            info!(order_id, subscriber = %delivery.subscriber, decision = ?outcome.result, "Decision");
        }
    }

    stop.send(())?;
    for worker in running {
        worker.await??;
    }
    Ok(())
}
