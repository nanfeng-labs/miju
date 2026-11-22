use std::{
    fmt,
    future::poll_fn,
    marker::PhantomData,
    sync::{Arc, OnceLock},
};

use futures_sink::Sink;
use futures_util::{SinkExt, lock::Mutex};
use miju_core::{
    backend::{
        BackendConfig, WireFormatBackend,
        codec::Codec,
        finalize::{DurableBackend, FinalizeBackend},
    },
    error::BoxDynError,
    task::{
        Task,
        builder::TaskBuilder,
        task_id::{GenerateId, TaskId},
    },
};

/// Sends each publication to every subscriber of a broadcast.
///
/// Created by [`Broadcast::publisher`](super::Broadcast::publisher). Publishers
/// can run on different threads. They take turns submitting to each subscriber
/// so each receipt describes its own send.
pub struct Publisher<Args, B> {
    members: Arc<[Member<B>]>,
    encoder: Option<B>,
    _marker: PhantomData<fn(Args)>,
}

// Initialize the sending handles when a cloneable broadcast first needs them.
// Broadcast clones made before the first publisher share the same handles too.
pub(super) type SharedSenders<B> = Arc<OnceLock<Arc<[Member<B>]>>>;

pub(super) struct Member<B> {
    subscriber: String,
    sender: Mutex<Option<Sender<B>>>,
}

struct Sender<B> {
    backend: B,
    // Remains set if the publisher is cancelled during flush.
    flushing: bool,
}

impl<Args, B: Clone> Publisher<Args, B> {
    pub(super) fn new(backends: &[(String, B)], senders: &SharedSenders<B>) -> Self {
        let members = senders.get_or_init(|| {
            backends
                .iter()
                .map(|(subscriber, backend)| Member {
                    subscriber: subscriber.clone(),
                    sender: Mutex::new(Some(Sender {
                        backend: backend.clone(),
                        flushing: false,
                    })),
                })
                .collect()
        });
        Self {
            members: Arc::clone(members),
            encoder: backends.first().map(|(_, backend)| backend.clone()),
            _marker: PhantomData,
        }
    }
}

impl<Args, B, Compact> Publisher<Args, B>
where
    B: Sink<Task<Compact>>
        + WireFormatBackend<Compact = Compact>
        + BackendConfig<Args = Args>
        + Unpin,
    B::Error: Into<BoxDynError>,
    B::Codec: Codec<Args, Compact = Compact>,
    <B::Codec as Codec<Args>>::Error: Into<BoxDynError>,
    B::Kind: FinalizeBackend<B, Args, Backend = DurableBackend<B, Args>>,
    B::Id: GenerateId,
    Compact: Clone,
{
    /// Publishes `args` once: encodes it once and sends one task, with a fresh
    /// task ID, to every subscriber in declaration order.
    ///
    /// Success means each listed delivery was accepted by its backend, not that
    /// any handler ran. A subscriber whose send fails is recorded in
    /// [`PublishReceipt::failures`], with the task ID and a [`FailureKind`]
    /// saying whether the backend may still hold the task. A failure does not
    /// stop delivery to the others, and there is no rollback.
    ///
    /// A backend error disables that sending handle for all publishers of this
    /// broadcast and its clones. Later publications record that subscriber as
    /// [`FailureKind::Disabled`]. To resume delivery, build a broadcast with a
    /// working backend for the affected queue.
    ///
    /// Sends are awaited one at a time, so a backend that applies backpressure
    /// delays the subscribers after it. Dropping the future before it completes
    /// can leave the task with some subscribers and not others, and the receipt
    /// is lost. If a send had already started, the next publisher finishes its
    /// flush before starting another task for that subscriber. A failure of that
    /// flush marks the new task as [`FailureKind::Disabled`]. A broadcast
    /// without subscribers returns an empty receipt.
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
        let Some(encoder) = &self.encoder else {
            return Ok(receipt);
        };
        let compact = encoder
            .codec()
            .encode(&args)
            .map_err(|error| PublishError::Codec(error.into()))?;
        drop(args);
        for member in self.members.iter() {
            let subscriber = member.subscriber.clone();
            let task_id = B::Id::generate();
            let mut slot = member.sender.lock().await;
            let (kind, error) = if let Some(sender) = slot.as_mut() {
                let task = TaskBuilder::new(compact.clone())
                    .task_id(task_id.clone())
                    .build();
                match sender.send(task).await {
                    Ok(()) => {
                        receipt.deliveries.push(Delivery {
                            subscriber,
                            task_id,
                        });
                        continue;
                    }
                    Err((kind, error)) => {
                        *slot = None;
                        (kind, error.into())
                    }
                }
            } else {
                (
                    FailureKind::Disabled,
                    "the backend returned an error during an earlier publication".into(),
                )
            };
            receipt.failures.push(DeliveryFailure {
                subscriber,
                task_id,
                kind,
                error,
            });
        }
        Ok(receipt)
    }
}

impl<B> Sender<B> {
    async fn send<T>(&mut self, task: T) -> Result<(), (FailureKind, B::Error)>
    where
        B: Sink<T> + Unpin,
    {
        // A previous caller may have dropped its future after start_send.
        // Settle that task before assigning any flush result to this one.
        if self.flushing {
            self.backend
                .flush()
                .await
                .map_err(|e| (FailureKind::Disabled, e))?;
            self.flushing = false;
        }
        poll_fn(|cx| self.backend.poll_ready_unpin(cx))
            .await
            .map_err(|e| (FailureKind::NotSent, e))?;
        // A failed start_send can still have accepted the task.
        self.backend
            .start_send_unpin(task)
            .map_err(|e| (FailureKind::Unconfirmed, e))?;
        self.flushing = true;
        self.backend
            .flush()
            .await
            .map_err(|e| (FailureKind::Unconfirmed, e))?;
        self.flushing = false;
        Ok(())
    }
}

impl<Args, B: Clone> Clone for Publisher<Args, B> {
    fn clone(&self) -> Self {
        Self {
            members: Arc::clone(&self.members),
            encoder: self.encoder.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Args, B> fmt::Debug for Publisher<Args, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let subscribers: Vec<_> = self.members.iter().map(|m| &m.subscriber).collect();
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
    /// Subscribers whose delivery failed, in declaration order.
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

/// A subscriber whose delivery failed.
#[derive(Debug)]
pub struct DeliveryFailure {
    /// The subscriber's name.
    pub subscriber: String,
    /// The task ID this delivery was given. Only an
    /// [`Unconfirmed`](FailureKind::Unconfirmed) task can exist under it.
    pub task_id: TaskId,
    /// How far the send got.
    pub kind: FailureKind,
    /// The backend's error, or a description for
    /// [`Disabled`](FailureKind::Disabled).
    pub error: BoxDynError,
}

/// How far a failed delivery got, which decides whether it is safe to send
/// the task again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FailureKind {
    /// The backend reported an error before taking the task, so nothing was
    /// enqueued. Sending it again cannot create a duplicate.
    NotSent,
    /// The backend took the task and then reported an error, so the task may
    /// still be delivered. Look it up by [`DeliveryFailure::task_id`] before
    /// sending it again.
    Unconfirmed,
    /// The subscriber's sending handle failed on a previous task. This task
    /// was never passed to the backend.
    Disabled,
}

/// Errors returned by [`Publisher::publish`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublishError {
    /// Encoding the arguments failed; nothing was sent.
    #[error("failed to encode the publication: {0}")]
    Codec(#[source] BoxDynError),
}
