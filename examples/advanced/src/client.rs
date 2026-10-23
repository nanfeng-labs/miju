use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use email_service::Email;

use crate::error::DeliveryError;

#[derive(Debug)]
struct Connection;

#[derive(Clone, Debug)]
pub(crate) struct EmailClient {
    connection: Arc<Connection>,
    remaining: Arc<AtomicUsize>,
}

impl EmailClient {
    pub(crate) fn new() -> Self {
        Self {
            connection: Arc::new(Connection),
            remaining: Arc::new(AtomicUsize::new(10)),
        }
    }

    fn charge(&self, cost: usize) -> Result<(), DeliveryError> {
        // Cloned clients share one budget; checking and decrementing must be atomic.
        // Compare/exchange keeps the update compatible with Rust 1.85.
        let mut remaining = self.remaining.load(Ordering::Relaxed);
        while let Some(next) = remaining.checked_sub(cost) {
            match self.remaining.compare_exchange_weak(
                remaining,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => remaining = current,
            }
        }
        Err(DeliveryError::ApiRateLimit)
    }

    pub(crate) async fn send(&self, email: Email) -> Result<(), DeliveryError> {
        self.charge(2)?;
        tracing::info!(recipient = email.to, client = ?self.connection, "Simulated validation and delivery");
        Ok(())
    }

    pub(crate) async fn send_unchecked(&self, email: Email) -> Result<(), DeliveryError> {
        self.charge(1)?;
        tracing::info!(recipient = email.to, client = ?self.connection, "Simulated cached delivery");
        Ok(())
    }

    pub(crate) fn remaining(&self) -> usize {
        self.remaining.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_charge_preserves_the_remaining_budget() {
        let client = EmailClient::new();
        client.charge(9).unwrap();
        assert!(matches!(client.charge(2), Err(DeliveryError::ApiRateLimit)));
        assert_eq!(client.remaining(), 1);
        client.charge(1).unwrap();
        assert_eq!(client.remaining(), 0);
        assert!(matches!(client.charge(1), Err(DeliveryError::ApiRateLimit)));
        assert_eq!(client.remaining(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cloned_clients_share_a_bounded_budget() {
        let client = EmailClient::new();
        let mut calls = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let client = client.clone();
            calls.spawn(async move { client.send(email_service::example_good_email()).await });
        }
        let mut accepted = 0;
        while let Some(result) = calls.join_next().await {
            match result.unwrap() {
                Ok(()) => accepted += 1,
                Err(DeliveryError::ApiRateLimit) => (),
                Err(error) => panic!("unexpected delivery error: {error}"),
            }
        }
        assert_eq!(accepted, 5);
        assert_eq!(client.remaining(), 0);

        let cached_client = EmailClient::new();
        cached_client
            .send_unchecked(email_service::example_good_email())
            .await
            .unwrap();
        assert_eq!(cached_client.remaining(), 9);
    }
}
