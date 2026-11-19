//! Deliver one publication to several workers, each of which decides whether
//! to handle it.
//!
//! - [`BroadcastFlow`] declares the subscribers and builds one backend (queue)
//!   per subscriber.
//! - [`Publisher::publish`] encodes the arguments once and sends one task to
//!   every subscriber's backend.
//! - [`when`] wraps a handler so each worker evaluates its own predicate and
//!   records [`Decision::Skipped`] instead of an error when it declines.
//!
//! A publication is a single `publish` call. Delivery is one task per declared
//! subscriber, each with a fresh task ID. Execution happens independently in
//! each subscriber's worker: zero, one or several workers may handle the same
//! publication. Delivery is not atomic across subscribers, and the in-memory
//! backend is neither persistent nor bounded. See the crate README for the full
//! list of guarantees and limits.

mod flow;
mod publisher;
mod when;

use serde::{Deserialize, Serialize};

pub use flow::{Broadcast, BroadcastError, BroadcastFlow};
pub use publisher::{
    Delivery, DeliveryFailure, FailureKind, PublishError, PublishReceipt, Publisher,
};
pub use when::{When, when};

/// The outcome of one delivery at one worker.
///
/// Both variants are successful results. Handler failures remain errors and
/// follow the worker's usual error, retry and event handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Decision<R> {
    /// The predicate accepted the task and the handler returned this value.
    Handled(R),
    /// The predicate declined the task; the handler did not run.
    Skipped,
}
