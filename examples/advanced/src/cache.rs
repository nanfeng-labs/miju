//! Shared validation results, copied out before any asynchronous operation.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

/// A simulated address-validation response.
#[derive(Clone, Debug)]
pub struct Validation {
    /// Whether delivery is permitted.
    pub valid: bool,
    /// Whether the address belongs to a disposable mailbox provider.
    pub disposable: bool,
}

/// A cache whose clones share the same entries.
#[derive(Clone, Debug, Default)]
pub struct ValidationCache(Arc<Mutex<HashMap<String, Validation>>>);

impl ValidationCache {
    /// Create an empty shared cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Copy the cached response, releasing the lock before returning.
    #[must_use]
    pub fn get(&self, address: &str) -> Option<Validation> {
        self.0.lock().unwrap().get(address).cloned()
    }

    /// Check whether an address has a cached response.
    #[must_use]
    pub fn contains(&self, address: &str) -> bool {
        self.0.lock().unwrap().contains_key(address)
    }

    /// Insert or replace a validation result.
    pub fn insert(&self, address: impl Into<String>, validation: Validation) {
        self.0.lock().unwrap().insert(address.into(), validation);
    }

    /// Remove an address and return its previous validation result.
    #[must_use]
    pub fn remove(&self, address: &str) -> Option<Validation> {
        self.0.lock().unwrap().remove(address)
    }

    /// Remove every cached response.
    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    /// Count cached addresses.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// Report whether the cache has no addresses.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.lock().unwrap().is_empty()
    }
}

/// Simulate a slow refresh without holding the cache lock across the wait.
pub async fn refresh(address: String, cache: &ValidationCache) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    let result = Validation {
        valid: true,
        disposable: false,
    };
    tracing::debug!(
        recipient = address,
        valid = result.valid,
        disposable = result.disposable,
        "Cache refreshed"
    );
    cache.insert(address, result);
}
