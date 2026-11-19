use std::{collections::HashSet, fmt, marker::PhantomData};

use miju_core::backend::{BackendConfig, ext::shared::Shared};

use super::Publisher;
use crate::in_memory::InMemoryWorkflow;

/// Declares a broadcast and the subscribers that receive every publication.
///
/// Membership is fixed when the broadcast is built: subscribers cannot be added
/// or removed afterwards.
///
/// ```rust
/// use miju_workflow::broadcast::BroadcastFlow;
///
/// let orders = BroadcastFlow::<u32>::new("orders")
///     .subscriber("audit")
///     .subscriber("fraud")
///     .in_memory()?;
/// assert_eq!(orders.subscribers().collect::<Vec<_>>(), ["audit", "fraud"]);
/// assert!(orders.backend("audit").is_some());
/// # Ok::<(), miju_workflow::broadcast::BroadcastError>(())
/// ```
pub struct BroadcastFlow<Args> {
    name: String,
    subscribers: Vec<String>,
    _marker: PhantomData<fn(Args)>,
}

impl<Args> BroadcastFlow<Args> {
    /// Starts declaring a broadcast called `name`.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            subscribers: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Adds a subscriber. Names must be unique; a duplicate is rejected when
    /// the broadcast is built.
    #[must_use]
    pub fn subscriber(mut self, name: &str) -> Self {
        self.subscribers.push(name.to_owned());
        self
    }

    /// Builds the broadcast, calling `make` once per subscriber in declaration
    /// order to create its backend.
    ///
    /// Each backend must take `Args` as its argument type, which is checked at
    /// compile time. Every backend must also use the same wire format, because
    /// [`Publisher::publish`] encodes once with the first subscriber's codec.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastError::DuplicateSubscriber`] if a name was declared
    /// more than once. No backend is created in that case.
    pub fn build_with<B>(
        self,
        mut make: impl FnMut(&str) -> B,
    ) -> Result<Broadcast<Args, B>, BroadcastError>
    where
        B: BackendConfig<Args = Args>,
    {
        let mut seen = HashSet::new();
        if let Some(duplicate) = self
            .subscribers
            .iter()
            .find(|name| !seen.insert(name.as_str()))
        {
            return Err(BroadcastError::DuplicateSubscriber(duplicate.clone()));
        }
        let members = self
            .subscribers
            .into_iter()
            .map(|name| {
                let backend = make(&name);
                (name, backend)
            })
            .collect();
        Ok(Broadcast {
            name: self.name,
            members,
            _marker: PhantomData,
        })
    }

    /// Builds the broadcast with one [`InMemoryWorkflow`] queue and result
    /// store per subscriber.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastError::DuplicateSubscriber`] if a name was declared
    /// more than once.
    pub fn in_memory(
        self,
    ) -> Result<Broadcast<Args, Shared<InMemoryWorkflow<Args>>>, BroadcastError> {
        self.build_with(|_| InMemoryWorkflow::create())
    }
}

impl<Args> fmt::Debug for BroadcastFlow<Args> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastFlow")
            .field("name", &self.name)
            .field("subscribers", &self.subscribers)
            .finish()
    }
}

/// A built broadcast: one backend per subscriber.
///
/// Give each subscriber's [`backend`](Self::backend) to its worker, and create
/// a [`Publisher`] for each producer.
pub struct Broadcast<Args, B> {
    name: String,
    members: Vec<(String, B)>,
    _marker: PhantomData<fn(Args)>,
}

impl<Args, B> Broadcast<Args, B> {
    /// The broadcast's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Subscriber names in declaration order.
    pub fn subscribers(&self) -> impl ExactSizeIterator<Item = &str> {
        self.members.iter().map(|(name, _)| name.as_str())
    }
}

impl<Args, B: Clone> Broadcast<Args, B> {
    /// A clone of the subscriber's backend handle, for
    /// `WorkerBuilder::backend`, or `None` for an unknown name.
    ///
    /// Whether clones share one queue depends on the backend's `Clone`
    /// implementation; [`Shared`] handles do.
    #[must_use]
    pub fn backend(&self, subscriber: &str) -> Option<B> {
        self.members
            .iter()
            .find(|(name, _)| name == subscriber)
            .map(|(_, backend)| backend.clone())
    }

    /// A new publisher that owns fresh clones of every subscriber's backend
    /// handle.
    ///
    /// Create one publisher per producer task; publishers do not share a lock
    /// with each other in this layer. A new publisher also tries backends that
    /// an earlier publisher stopped using after an error.
    #[must_use]
    pub fn publisher(&self) -> Publisher<Args, B> {
        Publisher::new(self.members.clone())
    }
}

impl<Args, B: Clone> Clone for Broadcast<Args, B> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            members: self.members.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Args, B> fmt::Debug for Broadcast<Args, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Broadcast")
            .field("name", &self.name)
            .field("subscribers", &self.subscribers().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// Errors returned when building a broadcast.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BroadcastError {
    /// The same subscriber name was declared more than once.
    #[error("subscriber `{0}` is declared more than once")]
    DuplicateSubscriber(String),
}
