use std::{fmt, future::poll_fn, marker::PhantomData};

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
    members: Vec<Member<B>>,
    _marker: PhantomData<fn(Args)>,
}

#[derive(Clone)]
struct Member<B> {
    subscriber: String,
    backend: B,
    /// Set once the backend returns an error. The backend is not polled again:
    /// a sink may panic if it is used after an error.
    failed: bool,
}

impl<Args, B> Publisher<Args, B> {
    pub(super) fn new(members: Vec<(String, B)>) -> Self {
        let members = members
            .into_iter()
            .map(|(subscriber, backend)| Member {
                subscriber,
                backend,
                failed: false,
            })
            .collect();
        Self {
            members,
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
    /// After a backend returns an error, this publisher does not poll it again,
    /// because a sink may panic or misbehave when used after an error. Later
    /// publications record that subscriber as [`FailureKind::Disabled`]. Create
    /// a new publisher with
    /// [`Broadcast::publisher`](super::Broadcast::publisher) to try the backend
    /// again; cloning this publisher keeps the disabled state.
    ///
    /// Sends are awaited one at a time, so a backend that applies backpressure
    /// delays the subscribers after it. Dropping the future before it completes
    /// can leave the task with some subscribers and not others, and the receipt
    /// is lost. A broadcast without subscribers returns an empty receipt.
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
        let Some(first) = self.members.first() else {
            return Ok(receipt);
        };
        let compact = first
            .backend
            .codec()
            .encode(&args)
            .map_err(|error| PublishError::Codec(error.into()))?;
        drop(args);
        for member in &mut self.members {
            let subscriber = member.subscriber.clone();
            let task_id = B::Id::generate();
            if member.failed {
                receipt.failures.push(DeliveryFailure {
                    subscriber,
                    task_id,
                    kind: FailureKind::Disabled,
                    error: "the backend returned an error during an earlier publication".into(),
                });
                continue;
            }
            let task = TaskBuilder::new(compact.clone())
                .task_id(task_id.clone())
                .build();
            match send(&mut member.backend, task).await {
                Ok(()) => receipt.deliveries.push(Delivery {
                    subscriber,
                    task_id,
                }),
                Err((kind, error)) => {
                    member.failed = true;
                    receipt.failures.push(DeliveryFailure {
                        subscriber,
                        task_id,
                        kind,
                        error: error.into(),
                    });
                }
            }
        }
        Ok(receipt)
    }
}

/// Sends one task. An error before the backend takes the task is
/// [`FailureKind::NotSent`]; an error after it is [`FailureKind::Unconfirmed`].
/// `start_send` takes the task, and the `Sink` contract does not say whether a
/// failed `start_send` kept it, so that error counts as unconfirmed.
async fn send<B, T>(backend: &mut B, task: T) -> Result<(), (FailureKind, B::Error)>
where
    B: Sink<T> + Unpin,
{
    poll_fn(|cx| backend.poll_ready_unpin(cx))
        .await
        .map_err(|error| (FailureKind::NotSent, error))?;
    backend
        .start_send_unpin(task)
        .map_err(|error| (FailureKind::Unconfirmed, error))?;
    backend
        .flush()
        .await
        .map_err(|error| (FailureKind::Unconfirmed, error))
}

impl<Args, B: Clone> Clone for Publisher<Args, B> {
    fn clone(&self) -> Self {
        Self {
            members: self.members.clone(),
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
    /// The backend returned an error during an earlier publication, so this
    /// publisher did not try to send. Nothing was enqueued.
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
