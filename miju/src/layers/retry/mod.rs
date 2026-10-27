//! Retry Policies for miju
//!
//! This module provides flexible retry strategies for tasks in miju workers.
//!
//! # Approaches
//!
//! ## 1. Fixed Retry Count
//!
//! Use [RetryPolicy] to retry a task a fixed number of times.
//!
//! ```rust
//! use miju::layers::retry::RetryPolicy;
//!
//! let policy = RetryPolicy::retries(3);
//! ```
//!
//! ## 2. Retry with Backoff
//!
//! Use [BackoffRetryPolicy] to retry with a backoff strategy (e.g., exponential).
//!
//! ```rust
//! use miju::layers::retry::{RetryPolicy, BackoffRetryPolicy};
//! use tower::retry::backoff::ExponentialBackoffMaker;
//! use tower::retry::backoff::MakeBackoff;
//!
//! let backoff = ExponentialBackoffMaker::default().make_backoff();
//! let policy = RetryPolicy::retries(5).with_backoff(backoff);
//! ```
//!
//! ## 3. Conditional Retry
//!
//! Use [RetryIfPolicy] to retry only if a predicate matches the error.
//!
//! ```rust
//! use std::io::{self, ErrorKind};
//! use miju::layers::retry::RetryPolicy;
//!
//! fn is_transient(e: &io::Error) -> bool {
//!     matches!(e.kind(),
//!         ErrorKind::ConnectionRefused |
//!         ErrorKind::ConnectionReset |
//!         ErrorKind::TimedOut |
//!         ErrorKind::Interrupted
//!     )
//! }
//!
//! let policy = RetryPolicy::retries(3).retry_if(is_transient);
//! ```
//!
//! ## 4. Retry Based on Task Metadata
//!
//! Use [`FromTaskConfigPolicy`] to respect per-task retry configuration or else default to the underlying policy.
//!
//! ```rust
//! use miju::layers::retry::{RetryPolicy, RetryConfig};
//!
//! let policy = RetryPolicy::default().from_task_config();
//! // Attach RetryConfig to your task's metadata
//! ```
//!
//! # Example: Worker with Retry Policy
//!
//! ```rust
//! # use miju::layers::retry::RetryPolicy;
//! # use miju_core::worker::builder::WorkerBuilder;
//! # use miju_core::backend::memory::MemoryStorage;
//! # use miju::layers::WorkerBuilderExt;
//! # async fn my_task_fn(task: u32) {
//! #    // Task implementation
//! # }
//! let worker = WorkerBuilder::new("my-worker")
//!     .backend(MemoryStorage::new())
//!     .retry(RetryPolicy::retries(3))
//!     .build(my_task_fn);
//! ```
//!
//! See individual types for more details.
//!
//! # Execution requirements
//!
//! These policies require a [`WorkerContext`] in the task's data and read the
//! task's shared attempt counter. A worker advances that counter on each execution;
//! the policy itself does not increment it. Failures before execution also consume
//! the retry budget without changing that counter. This accounting follows the
//! retained retry request and resets when a dispatched task is submitted again.
//! Each policy owns its pending retry decisions; cloned policies consume their
//! copies independently. When driving [`Policy`] directly, call
//! [`Policy::clone_request`] before dispatching each attempt and retain its returned
//! request for the retry decision.
//! Requests must have cloneable arguments.
//! Backoff timers may require a runtime; Tower's exponential backoff uses Tokio.
//!
//! [`RetryPolicy::retries`] allows retries after the first execution. The
//! [`FromTaskConfigPolicy`] wrapper additionally stops once the attempt count
//! reaches [`RetryConfig::retries`]; it still applies the inner policy's limit.
//! Missing or invalid metadata falls back to the inner policy.
//!
//! # Converted errors and shutdown
//!
//! Both policies recognize [`AbortError`] directly and inside [`BoxDynError`].
//! For other error wrappers, use `retry_if` to inspect the error and decide
//! whether it should be retried.
//!
//! Shutdown is checked when deciding on a retry. If a worker stops during an
//! already scheduled backoff, the retry can remain waiting for service readiness.
//! Bound the worker's run future when shutdown requires a deadline.

use miju_core::error::{AbortError, BoxDynError};

use miju_core::task::Task;
use miju_core::task::builder::TaskBuilder;
use miju_core::task::metadata::{Metadata, MetadataError, MetadataStore};
use miju_core::task::status::Status;
use miju_core::worker::context::WorkerContext;
use std::any::Any;
use std::fmt::Debug;
use std::num::ParseIntError;
use std::sync::{Arc, Weak};
use tower::retry::backoff::Backoff;

/// Re-exports from [`tower::retry`]
pub use tower::retry::*;
/// Re-exports from [`tower::util`]
pub use tower::util::rng::HasherRng;

// Snapshots follow retained requests; approval belongs to the policy that issued it.
#[derive(Clone, Debug)]
struct RetryAttempts {
    before_call: usize,
    rejected: Option<usize>,
}

impl RetryAttempts {
    fn rejected(&self, attempt: usize) -> Option<usize> {
        self.rejected?
            .checked_add(usize::from(self.before_call == attempt))
    }

    fn failed<T>(req: &Task<T>) -> Option<usize> {
        let rejected = match req.data().get::<Arc<Self>>() {
            Some(attempts) => attempts.rejected(req.attempt())?,
            None => 0,
        };
        req.attempt().checked_add(rejected)
    }

    fn take_retry<T>(pending: &mut Vec<Weak<Self>>, req: &Task<T>) -> bool {
        let previous = req.data().get::<Arc<Self>>();
        let mut retrying = false;
        pending.retain(|entry| {
            if previous.is_some_and(|old| entry.as_ptr() == Arc::as_ptr(old)) {
                retrying = true;
                false
            } else {
                entry.strong_count() != 0
            }
        });
        retrying
    }

    fn set_retrying<T>(pending: &mut Vec<Weak<Self>>, req: &Task<T>, retrying: bool) {
        Self::take_retry(pending, req);
        if retrying {
            if let Some(attempts) = req.data().get::<Arc<Self>>() {
                pending.push(Arc::downgrade(attempts));
            }
        }
    }

    fn cancel<T>(req: &mut Task<T>) {
        if let Some(previous) = req.data().get::<Arc<Self>>() {
            // Invalidate an inner policy's prior approval without changing aliases.
            req.inject_data(Arc::new((**previous).clone()));
        }
    }

    fn clone_request<T>(req: &Task<T>, mut cloned: Task<T>, retrying: bool) -> Task<T> {
        let previous = req.data().get::<Arc<Self>>();
        if let Some(prepared) = cloned.data().get::<Arc<Self>>() {
            if previous.is_none_or(|old| !Arc::ptr_eq(old, prepared))
                && (!retrying || RetryConfig::extract(req.metadata()).is_err())
            {
                // An active metadata cap must retain its own budget.
                return cloned;
            }
        }
        let rejected = match previous.filter(|_| retrying) {
            Some(old) => old.rejected(req.attempt()),
            None => Some(0),
        };
        cloned.inject_data(Arc::new(Self {
            before_call: cloned.attempt(),
            rejected,
        }));
        cloned
    }
}

/// Retries a task with backoff
#[derive(Clone, Debug)]
pub struct BackoffRetryPolicy<B> {
    retries: usize,
    backoff: B,
    retrying: Vec<Weak<RetryAttempts>>,
}

impl<B> BackoffRetryPolicy<B> {
    /// Build a new retry policy with backoff
    #[must_use]
    pub fn new(retries: usize, backoff: B) -> Self {
        Self {
            retries,
            backoff,
            retrying: Vec::new(),
        }
    }

    /// Retry the task if the predicate returns true
    #[must_use]
    pub fn retry_if<F, Err>(self, predicate: F) -> RetryIfPolicy<Self, F>
    where
        F: Fn(&Err) -> bool + Send + Sync + 'static,
    {
        RetryIfPolicy::new(self, predicate)
    }

    /// Retry the task based on [`RetryConfig`] metadata if exists
    ///
    /// Falls back to the [`RetryPolicy`] if no metadata is found.
    #[must_use]
    pub fn from_task_config(self) -> FromTaskConfigPolicy<Self> {
        FromTaskConfigPolicy::new(self)
    }
}

impl<T, Res, B, Err: Any> Policy<Task<T>, Res, Err> for BackoffRetryPolicy<B>
where
    T: Clone,
    B: Backoff,
    B::Future: Send + 'static,
{
    type Future = B::Future;

    fn retry(&mut self, req: &mut Task<T>, result: &mut Result<Res, Err>) -> Option<Self::Future> {
        RetryAttempts::set_retrying(&mut self.retrying, req, false);
        let status = req.status();
        let worker = req.data().get::<WorkerContext>()?;
        if worker.is_shutting_down() {
            return None;
        }
        match result.as_mut() {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if self.retries == 0 => None,
            Err(_) if status == Status::Killed => None,
            // Helps handle the case where the user explicitly wants to abort retries
            Err(err) if (err as &dyn Any).downcast_ref::<AbortError>().is_some() => None,
            Err(err)
                if (err as &dyn Any)
                    .downcast_ref::<BoxDynError>()
                    .is_some_and(|error| error.is::<AbortError>()) =>
            {
                None
            }

            Err(_) => {
                let attempt = RetryAttempts::failed(req)?;
                (self.retries >= attempt).then(|| {
                    RetryAttempts::set_retrying(&mut self.retrying, req, true);
                    self.backoff.next_backoff()
                })
            }
        }
    }

    fn clone_request(&mut self, req: &Task<T>) -> Option<Task<T>> {
        let retrying = RetryAttempts::take_retry(&mut self.retrying, req);
        Some(RetryAttempts::clone_request(req, req.clone(), retrying))
    }
}

/// Retries a task instantly for `retries`
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    retries: usize,
    retrying: Vec<Weak<RetryAttempts>>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::retries(5)
    }
}

impl RetryPolicy {
    /// Set the number of retries
    #[must_use]
    pub fn retries(retries: usize) -> Self {
        Self {
            retries,
            retrying: Vec::new(),
        }
    }

    /// Include a backoff to the retry policy
    #[must_use]
    pub fn with_backoff<B: Backoff>(self, backoff: B) -> BackoffRetryPolicy<B> {
        BackoffRetryPolicy {
            retries: self.retries,
            backoff,
            retrying: self.retrying,
        }
    }
    /// Retry the task if the predicate returns true
    #[must_use]
    pub fn retry_if<F, Err>(self, predicate: F) -> RetryIfPolicy<Self, F>
    where
        F: Fn(&Err) -> bool + Send + Sync + 'static,
    {
        RetryIfPolicy::new(self, predicate)
    }
    /// Retry the task based on [`RetryConfig`] metadata if exists
    ///
    /// Falls back to the [`RetryPolicy`] if no metadata is found.
    #[must_use]
    pub fn from_task_config(self) -> FromTaskConfigPolicy<Self> {
        FromTaskConfigPolicy::new(self)
    }
}

impl<T, Res, Err: Any> Policy<Task<T>, Res, Err> for RetryPolicy
where
    T: Clone,
{
    type Future = std::future::Ready<()>;

    fn retry(&mut self, req: &mut Task<T>, result: &mut Result<Res, Err>) -> Option<Self::Future> {
        RetryAttempts::set_retrying(&mut self.retrying, req, false);
        let status = req.status();
        let worker = req.data().get::<WorkerContext>()?;
        if worker.is_shutting_down() {
            return None;
        }
        match result.as_mut() {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if self.retries == 0 => None,
            Err(_) if status == Status::Killed => None,
            Err(err) if (err as &dyn Any).downcast_ref::<AbortError>().is_some() => None,
            Err(err)
                if (err as &dyn Any)
                    .downcast_ref::<BoxDynError>()
                    .is_some_and(|error| error.is::<AbortError>()) =>
            {
                None
            }
            Err(_) => {
                let attempt = RetryAttempts::failed(req)?;
                (self.retries >= attempt).then(|| {
                    RetryAttempts::set_retrying(&mut self.retrying, req, true);
                    std::future::ready(())
                })
            }
        }
    }

    fn clone_request(&mut self, req: &Task<T>) -> Option<Task<T>> {
        let retrying = RetryAttempts::take_retry(&mut self.retrying, req);
        Some(RetryAttempts::clone_request(req, req.clone(), retrying))
    }
}

/// Retry the task if the predicate returns true
#[derive(Debug, Clone)]
pub struct RetryIfPolicy<P, F> {
    inner: P,
    predicate: F,
}

impl<P, F> RetryIfPolicy<P, F> {
    /// Build a new retry if policy
    #[must_use]
    pub fn new(inner: P, predicate: F) -> Self {
        Self { inner, predicate }
    }

    /// Retry the task based on [`RetryConfig`] metadata
    ///
    /// Falls back to the [`RetryIfPolicy`] if no metadata is found.
    #[must_use]
    pub fn from_task_config(self) -> FromTaskConfigPolicy<Self> {
        FromTaskConfigPolicy::new(self)
    }
}
impl<T, Res, P, F, Err> Policy<Task<T>, Res, Err> for RetryIfPolicy<P, F>
where
    T: Clone,
    P: Policy<Task<T>, Res, Err>,
    F: Fn(&Err) -> bool + Send + Sync + 'static,
{
    type Future = P::Future;

    fn retry(&mut self, req: &mut Task<T>, result: &mut Result<Res, Err>) -> Option<Self::Future> {
        if req
            .data()
            .get::<WorkerContext>()
            .is_none_or(WorkerContext::is_shutting_down)
        {
            RetryAttempts::cancel(req);
            return None;
        }
        match result {
            Ok(_) => {
                RetryAttempts::cancel(req);
                None
            }
            Err(err) => {
                if !(self.predicate)(err) {
                    RetryAttempts::cancel(req);
                    return None;
                }
                self.inner.retry(req, result)
            }
        }
    }

    fn clone_request(&mut self, req: &Task<T>) -> Option<Task<T>> {
        self.inner.clone_request(req)
    }
}
/// Retry configuration for tasks
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// The configured retry limit.
    ///
    /// [`FromTaskConfigPolicy`] compares this value with the total attempt count,
    /// including the first execution, and also applies its inner policy's limit.
    pub retries: usize,
}

/// An error that represents an invalid [`RetryConfig`]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RetryConfigError {
    /// The retry config key is missing
    #[error("the data for key {RETRY_CONFIG_KEY} is missing")]
    MissingKey,

    /// Could not parse the retry config key
    #[error("Could not parse key {RETRY_CONFIG_KEY}")]
    Parse(#[from] ParseIntError),

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

const RETRY_CONFIG_KEY: &str = "miju.retries.config";

impl Metadata for RetryConfig {
    type Error = RetryConfigError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let retries = map
            .get(RETRY_CONFIG_KEY)
            .ok_or(RetryConfigError::MissingKey)?
            .parse::<usize>()
            .map_err(RetryConfigError::Parse)?;

        Ok(Self { retries })
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), RetryConfigError> {
        map.insert(RETRY_CONFIG_KEY, self.retries.to_string())?;
        Ok(())
    }
}

/// Retry the task based on the [`RetryConfig`] metadata
#[derive(Debug, Clone)]
pub struct FromTaskConfigPolicy<P> {
    inner: P,
    retrying: Vec<Weak<RetryAttempts>>,
}

impl<P> FromTaskConfigPolicy<P> {
    /// Build a policy with metadata configuration
    #[must_use]
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            retrying: Vec::new(),
        }
    }

    /// Retry the task if the predicate returns true
    #[must_use]
    pub fn retry_if<F, Err>(self, predicate: F) -> RetryIfPolicy<Self, F>
    where
        F: Fn(&Err) -> bool + Send + Sync + 'static,
    {
        RetryIfPolicy::new(self, predicate)
    }
}

impl Default for FromTaskConfigPolicy<RetryPolicy> {
    fn default() -> Self {
        Self {
            inner: RetryPolicy::retries(0),
            retrying: Vec::new(),
        }
    }
}

impl<T, Res, P, Err> Policy<Task<T>, Res, Err> for FromTaskConfigPolicy<P>
where
    T: Clone,
    P: Policy<Task<T>, Res, Err>,
{
    type Future = P::Future;

    fn retry(&mut self, req: &mut Task<T>, result: &mut Result<Res, Err>) -> Option<Self::Future> {
        RetryAttempts::set_retrying(&mut self.retrying, req, false);
        if req
            .data()
            .get::<WorkerContext>()
            .is_none_or(WorkerContext::is_shutting_down)
        {
            RetryAttempts::cancel(req);
            return None;
        }
        if result.is_ok() {
            RetryAttempts::cancel(req);
            None
        } else {
            let attempt = RetryAttempts::failed(req);
            // If we have a retry config, we need to respect it
            if let Ok(cfg) = RetryConfig::extract(req.metadata()) {
                if attempt.is_none_or(|attempt| cfg.retries <= attempt) {
                    RetryAttempts::cancel(req);
                    return None;
                }
            };

            let retry = self.inner.retry(req, result);
            RetryAttempts::set_retrying(&mut self.retrying, req, retry.is_some());
            retry
        }
    }

    fn clone_request(&mut self, req: &Task<T>) -> Option<Task<T>> {
        let retrying = RetryAttempts::take_retry(&mut self.retrying, req);
        let cloned = self.inner.clone_request(req)?;
        Some(RetryAttempts::clone_request(req, cloned, retrying))
    }
}

/// Retry metadata extension to include [`RetryConfig`]
pub trait RetryMetadataExt {
    /// Set [`RetryConfig::retries`] in the task metadata.
    ///
    /// [`FromTaskConfigPolicy`] compares this limit with the total attempt count,
    /// including the first execution, and also applies its inner policy's limit.
    #[must_use]
    fn retries(self, retries: usize) -> Self;
}

impl<Args> RetryMetadataExt for TaskBuilder<Args> {
    /// Set [`RetryConfig::retries`] in the task metadata.
    fn retries(self, retries: usize) -> Self {
        self.metadata(&RetryConfig { retries })
    }
}
