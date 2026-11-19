use std::{fmt, marker::PhantomData};

use futures_sink::Sink;
use futures_util::SinkExt;
use miju_core::{
    backend::{BackendConfig, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::{
        Task,
        builder::TaskBuilder,
        task_id::{GenerateId, TaskId},
    },
};

/// Sends each publication to every subscriber of a broadcast.
///
/// Created by [`Broadcast::publisher`](super::Broadcast::publisher). It owns its
/// own clones of the subscriber backends, so publishers on different threads do
/// not contend for a lock in this layer.
pub struct Publisher<Args, B> {
    members: Vec<(String, B)>,
    _marker: PhantomData<fn(Args)>,
}

impl<Args, B> Publisher<Args, B> {
    pub(super) fn new(members: Vec<(String, B)>) -> Self {
        Self {
            members,
            _marker: PhantomData,
        }
    }
}

impl<Args, B, Compact> Publisher<Args, B>
where
    B: Sink<Task<Compact>> + WireFormatBackend<Compact = Compact> + BackendConfig + Unpin,
    B::Error: Into<BoxDynError>,
    B::Codec: Codec<Args, Compact = Compact>,
    <B::Codec as Codec<Args>>::Error: Into<BoxDynError>,
    B::Id: GenerateId,
    Compact: Clone,
{
    /// Publishes `args` once: encodes it once and sends one task, with a fresh
    /// task ID, to every subscriber in declaration order.
    ///
    /// Success means each listed delivery was accepted by its backend, not that
    /// any handler ran. A subscriber whose send fails is recorded in
    /// [`PublishReceipt::failures`] and does not stop delivery to the others;
    /// there is no rollback. Sends are awaited one at a time, so a backend that
    /// applies backpressure delays the subscribers after it. Dropping the
    /// future before it completes can leave the task with some subscribers and
    /// not others, and the receipt is lost. A broadcast without subscribers
    /// returns an empty receipt.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError::Codec`] if encoding fails. Nothing is sent in
    /// that case.
    pub async fn publish(&mut self, args: Args) -> Result<PublishReceipt, PublishError> {
        let publication_id = B::Id::generate();
        let mut receipt = PublishReceipt {
            publication_id,
            deliveries: Vec::with_capacity(self.members.len()),
            failures: Vec::new(),
        };
        let Some((_, first)) = self.members.first() else {
            return Ok(receipt);
        };
        let compact = first
            .codec()
            .encode(&args)
            .map_err(|error| PublishError::Codec(error.into()))?;
        drop(args);
        for (subscriber, backend) in &mut self.members {
            let task_id = B::Id::generate();
            let task = TaskBuilder::new(compact.clone())
                .task_id(task_id.clone())
                .build();
            match backend.send(task).await {
                Ok(()) => receipt.deliveries.push(Delivery {
                    subscriber: subscriber.clone(),
                    task_id,
                }),
                Err(error) => receipt.failures.push(DeliveryFailure {
                    subscriber: subscriber.clone(),
                    error: error.into(),
                }),
            }
        }
        Ok(receipt)
    }
}

impl<Args, B: Clone> Clone for Publisher<Args, B> {
    fn clone(&self) -> Self {
        Self::new(self.members.clone())
    }
}

impl<Args, B> fmt::Debug for Publisher<Args, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let subscribers: Vec<_> = self.members.iter().map(|(name, _)| name).collect();
        f.debug_struct("Publisher")
            .field("subscribers", &subscribers)
            .finish_non_exhaustive()
    }
}

/// The result of one [`Publisher::publish`] call.
#[derive(Debug)]
pub struct PublishReceipt {
    /// Identity of the publication. Deliveries have their own task IDs.
    pub publication_id: TaskId,
    /// Subscribers whose backend accepted the task, in declaration order.
    pub deliveries: Vec<Delivery>,
    /// Subscribers whose backend rejected the task.
    pub failures: Vec<DeliveryFailure>,
}

/// One task sent to one subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The subscriber's name.
    pub subscriber: String,
    /// The task ID of this delivery, for example to wait for its result.
    pub task_id: TaskId,
}

/// A subscriber whose backend rejected the task.
#[derive(Debug)]
pub struct DeliveryFailure {
    /// The subscriber's name.
    pub subscriber: String,
    /// The backend's send error.
    pub error: BoxDynError,
}

/// Errors returned by [`Publisher::publish`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublishError {
    /// Encoding the arguments failed; nothing was sent.
    #[error("failed to encode the publication: {0}")]
    Codec(#[source] BoxDynError),
}
