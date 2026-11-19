//! One publication reaches every subscriber, and each worker decides on its own
//! whether to handle it.

use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures_sink::Sink;
use futures_util::StreamExt;
use miju::prelude::{RetryPolicy, WorkerBuilderExt};
use miju_codec::json::JsonCodec;
use miju_core::{
    backend::{BackendConfig, WaitForCompletion, WireFormatBackend, ext::shared::Shared},
    error::{BoxDynError, WorkerError},
    task::{
        Task,
        data::Data,
        task_id::{RandomId, TaskId},
    },
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt,
    },
};
use miju_workflow::{
    BroadcastFlow,
    broadcast::{BroadcastError, Decision, FailureKind, PublishError, PublishReceipt, When, when},
    in_memory::InMemoryWorkflow,
};
use tower::{Layer, Service, ServiceExt, limit::GlobalConcurrencyLimitLayer};

const DEADLINE: Duration = Duration::from_secs(5);

type Queue = Shared<InMemoryWorkflow<u32>>;

#[derive(Default)]
struct Log {
    decided: Mutex<Vec<(&'static str, u32)>>,
    handled: Mutex<Vec<(&'static str, u32)>>,
    errors: AtomicUsize,
}

impl Log {
    fn decided_by(&self, worker: &str) -> Vec<u32> {
        let decided = self.decided.lock().unwrap();
        decided
            .iter()
            .filter(|(w, _)| *w == worker)
            .map(|(_, id)| *id)
            .collect()
    }

    fn handled_by(&self, worker: &str) -> HashSet<u32> {
        let handled = self.handled.lock().unwrap();
        handled
            .iter()
            .filter(|(w, _)| *w == worker)
            .map(|(_, id)| *id)
            .collect()
    }
}

/// Spawns a worker on its own Tokio task. Its predicate records every decision
/// and its handler records every handled ID.
fn spawn_worker(
    name: &'static str,
    backend: Queue,
    accept: fn(u32) -> bool,
    log: &Arc<Log>,
) -> tokio::task::JoinHandle<Result<(), WorkerError>> {
    let (decisions, handled, errors) = (log.clone(), log.clone(), log.clone());
    let worker = WorkerBuilder::new(name)
        .backend(backend)
        .on_event(move |_, event| {
            if matches!(event, Event::Error(_)) {
                errors.errors.fetch_add(1, Ordering::SeqCst);
            }
        })
        .catch_panic()
        .build(when(
            move |id: &u32| {
                decisions.decided.lock().unwrap().push((name, *id));
                accept(*id)
            },
            move |id: u32| {
                let handled = handled.clone();
                async move { handled.handled.lock().unwrap().push((name, id)) }
            },
        ));
    tokio::spawn(worker.run())
}

async fn eventually(mut done: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition reached before the deadline");
}

/// Waits for the stored result of every delivery to `subscriber`.
async fn decisions(
    backend: &mut Queue,
    receipts: &[PublishReceipt],
    subscriber: &str,
) -> (HashMap<TaskId, Decision<()>>, usize) {
    let ids: Vec<_> = receipts
        .iter()
        .flat_map(|r| r.deliveries.iter())
        .filter(|d| d.subscriber == subscriber)
        .map(|d| d.task_id.clone())
        .collect();
    let mut stream = WaitForCompletion::<Decision<()>>::wait_for(backend, ids);
    let mut stored = HashMap::new();
    let mut failed = 0;
    while let Some(item) = tokio::time::timeout(DEADLINE, stream.next())
        .await
        .expect("results arrive before the deadline")
    {
        match item {
            Ok(result) => {
                stored.insert(result.task_id, result.result.expect("decoded decision"));
            }
            Err(_) => failed += 1,
        }
    }
    (stored, failed)
}

fn audit(id: u32) -> bool {
    id % 2 == 0
}

fn fraud(id: u32) -> bool {
    id % 3 == 0
}

fn vat(id: u32) -> bool {
    id % 5 == 0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_publication_reaches_every_subscriber_and_each_worker_decides() {
    let orders = BroadcastFlow::<u32>::new("orders")
        .subscriber("audit")
        .subscriber("fraud")
        .subscriber("vat")
        .in_memory()
        .unwrap();
    let log = Arc::new(Log::default());
    let workers = [
        spawn_worker("audit", orders.backend("audit").unwrap(), audit, &log),
        spawn_worker("fraud", orders.backend("fraud").unwrap(), fraud, &log),
        spawn_worker("vat", orders.backend("vat").unwrap(), vat, &log),
    ];

    let mut publisher = orders.publisher();
    let mut receipts = Vec::new();
    for id in 0..60 {
        let receipt = publisher.publish(id).await.unwrap();
        let subscribers: Vec<_> = receipt
            .deliveries
            .iter()
            .map(|d| d.subscriber.as_str())
            .collect();
        assert_eq!(subscribers, ["audit", "fraud", "vat"]);
        assert!(receipt.failures.is_empty());
        receipts.push(receipt);
    }
    eventually(|| log.decided.lock().unwrap().len() == 180).await;

    let all: Vec<u32> = (0..60).collect();
    let rules: [(&str, fn(u32) -> bool); 3] = [("audit", audit), ("fraud", fraud), ("vat", vat)];
    for (worker, accept) in rules {
        // Every worker decided every publication exactly once, in publication order.
        assert_eq!(log.decided_by(worker), all, "{worker} decisions");
        let expected: HashSet<u32> = all.iter().copied().filter(|id| accept(*id)).collect();
        eventually(|| log.handled_by(worker).len() == expected.len()).await;
        assert_eq!(log.handled_by(worker), expected, "{worker} handled");

        let mut backend = orders.backend(worker).unwrap();
        let (stored, failed) = decisions(&mut backend, &receipts, worker).await;
        assert_eq!(failed, 0);
        let handled = stored
            .values()
            .filter(|d| **d == Decision::Handled(()))
            .count();
        assert_eq!(handled, expected.len(), "{worker} stored Handled");
        assert_eq!(
            stored.len() - handled,
            all.len() - expected.len(),
            "{worker} stored Skipped"
        );
    }

    // Zero, one, two and three workers handled some of the publications.
    let mut handlers_per_id: HashMap<u32, usize> = HashMap::new();
    for (_, id) in log.handled.lock().unwrap().iter() {
        *handlers_per_id.entry(*id).or_default() += 1;
    }
    let counts: HashSet<usize> = all
        .iter()
        .map(|id| handlers_per_id.get(id).copied().unwrap_or(0))
        .collect();
    assert_eq!(counts, HashSet::from([0, 1, 2, 3]));
    assert_eq!(log.errors.load(Ordering::SeqCst), 0);
    workers.iter().for_each(tokio::task::JoinHandle::abort);
}

#[tokio::test]
async fn declining_is_a_success_and_is_not_retried() {
    let orders = BroadcastFlow::<u32>::new("orders")
        .subscriber("even")
        .in_memory()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let (counted, error_count) = (calls.clone(), errors.clone());
    let worker = WorkerBuilder::new("even")
        .backend(orders.backend("even").unwrap())
        .on_event(move |_, event| {
            if matches!(event, Event::Error(_)) {
                error_count.fetch_add(1, Ordering::SeqCst);
            }
        })
        .retry(RetryPolicy::retries(2))
        .build(when(
            move |id: &u32| {
                counted.fetch_add(1, Ordering::SeqCst);
                id % 2 == 0
            },
            |_: u32| async {},
        ));
    let worker = tokio::spawn(worker.run());

    let mut publisher = orders.publisher();
    let mut receipts = Vec::new();
    for id in 0..10 {
        receipts.push(publisher.publish(id).await.unwrap());
    }
    let mut backend = orders.backend("even").unwrap();
    let (stored, failed) = decisions(&mut backend, &receipts, "even").await;

    assert_eq!(failed, 0);
    let skipped = stored.values().filter(|d| **d == Decision::Skipped).count();
    assert_eq!(skipped, 5);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        10,
        "declined tasks are not retried"
    );
    assert_eq!(
        errors.load(Ordering::SeqCst),
        0,
        "declining emits no error event"
    );
    worker.abort();
}

#[tokio::test]
async fn a_retry_after_a_handler_error_runs_the_predicate_again() {
    let orders = BroadcastFlow::<u32>::new("orders")
        .subscriber("flaky")
        .in_memory()
        .unwrap();
    let predicate_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let (predicates, handlers) = (predicate_calls.clone(), handler_calls.clone());
    let worker = WorkerBuilder::new("flaky")
        .backend(orders.backend("flaky").unwrap())
        .retry(RetryPolicy::retries(1))
        .build(when(
            move |_: &u32| {
                predicates.fetch_add(1, Ordering::SeqCst);
                true
            },
            move |_: u32| {
                let attempt = handlers.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err::<(), BoxDynError>("transient".into())
                    } else {
                        Ok(())
                    }
                }
            },
        ));
    let worker = tokio::spawn(worker.run());

    let receipt = orders.publisher().publish(1).await.unwrap();
    let mut backend = orders.backend("flaky").unwrap();
    let (stored, failed) = decisions(&mut backend, &[receipt], "flaky").await;

    assert_eq!(failed, 0);
    assert_eq!(
        stored.into_values().collect::<Vec<_>>(),
        [Decision::Handled(())]
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 2);
    assert_eq!(predicate_calls.load(Ordering::SeqCst), 2);
    worker.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_predicate_fails_only_that_delivery() {
    fn panics_on_three(id: u32) -> bool {
        assert_ne!(id, 3, "predicate panics on 3");
        true
    }
    let orders = BroadcastFlow::<u32>::new("orders")
        .subscriber("steady")
        .subscriber("fragile")
        .in_memory()
        .unwrap();
    let log = Arc::new(Log::default());
    let workers = [
        spawn_worker("steady", orders.backend("steady").unwrap(), |_| true, &log),
        spawn_worker(
            "fragile",
            orders.backend("fragile").unwrap(),
            panics_on_three,
            &log,
        ),
    ];

    let mut publisher = orders.publisher();
    let mut receipts = Vec::new();
    for id in 0..6 {
        receipts.push(publisher.publish(id).await.unwrap());
    }
    let mut steady = orders.backend("steady").unwrap();
    let mut fragile = orders.backend("fragile").unwrap();
    let (steady_results, steady_failed) = decisions(&mut steady, &receipts, "steady").await;
    let (fragile_results, fragile_failed) = decisions(&mut fragile, &receipts, "fragile").await;

    assert_eq!((steady_results.len(), steady_failed), (6, 0));
    assert_eq!((fragile_results.len(), fragile_failed), (5, 1));
    assert_eq!(log.handled_by("steady"), (0..6).collect());
    assert_eq!(log.handled_by("fragile"), HashSet::from([0, 1, 2, 4, 5]));
    eventually(|| log.errors.load(Ordering::SeqCst) == 1).await;
    workers.iter().for_each(tokio::task::JoinHandle::abort);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_keep_their_own_order() {
    let orders = BroadcastFlow::<u32>::new("orders")
        .subscriber("left")
        .subscriber("right")
        .in_memory()
        .unwrap();
    let log = Arc::new(Log::default());
    let workers = [
        spawn_worker("left", orders.backend("left").unwrap(), |_| true, &log),
        spawn_worker("right", orders.backend("right").unwrap(), |_| true, &log),
    ];

    let publishers: Vec<_> = [0, 1]
        .into_iter()
        .map(|parity| {
            let mut publisher = orders.publisher();
            tokio::spawn(async move {
                for id in (0..400).filter(|id| id % 2 == parity) {
                    let receipt = publisher.publish(id).await.unwrap();
                    assert_eq!(receipt.deliveries.len(), 2);
                }
            })
        })
        .collect();
    for publisher in publishers {
        publisher.await.unwrap();
    }
    eventually(|| log.decided.lock().unwrap().len() == 800).await;

    for worker in ["left", "right"] {
        let decided = log.decided_by(worker);
        assert_eq!(
            decided.iter().copied().collect::<HashSet<_>>(),
            (0..400).collect()
        );
        for parity in [0, 1] {
            let ordered: Vec<_> = decided
                .iter()
                .copied()
                .filter(|id| id % 2 == parity)
                .collect();
            assert!(
                ordered.is_sorted(),
                "{worker} keeps publisher {parity}'s order"
            );
        }
    }
    workers.iter().for_each(tokio::task::JoinHandle::abort);
}

#[tokio::test]
async fn publications_before_a_worker_starts_wait_in_its_queue() {
    let orders = BroadcastFlow::<u32>::new("orders")
        .subscriber("late")
        .in_memory()
        .unwrap();
    let mut publisher = orders.publisher();
    for id in 0..5 {
        publisher.publish(id).await.unwrap();
    }
    let log = Arc::new(Log::default());
    let worker = spawn_worker("late", orders.backend("late").unwrap(), |_| true, &log);
    eventually(|| log.handled_by("late").len() == 5).await;
    worker.abort();
}

#[tokio::test]
async fn handlers_keep_their_extractors() {
    let orders = BroadcastFlow::<u32>::new("orders")
        .subscriber("extract")
        .in_memory()
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let worker = WorkerBuilder::new("extract")
        .backend(orders.backend("extract").unwrap())
        .data(seen.clone())
        .build(when(
            |_: &u32| true,
            |id: u32, worker: WorkerContext, seen: Data<Arc<Mutex<Vec<String>>>>| async move {
                seen.lock().unwrap().push(format!("{}:{id}", worker.name()));
            },
        ));
    let worker = tokio::spawn(worker.run());
    orders.publisher().publish(9).await.unwrap();
    eventually(|| !seen.lock().unwrap().is_empty()).await;
    assert_eq!(*seen.lock().unwrap(), ["extract:9"]);
    worker.abort();
}

#[tokio::test]
async fn zero_subscribers_is_a_no_op() {
    let empty = BroadcastFlow::<u32>::new("empty").in_memory().unwrap();
    let receipt = empty.publisher().publish(1).await.unwrap();
    assert!(receipt.deliveries.is_empty());
    assert!(receipt.failures.is_empty());
}

#[test]
fn duplicate_subscribers_are_rejected_before_any_backend_is_created() {
    let mut created = 0;
    let result = BroadcastFlow::<u32>::new("orders")
        .subscriber("audit")
        .subscriber("audit")
        .build_with(|_| {
            created += 1;
            RecordingSink::new(Mode::Accept)
        });
    assert!(matches!(result, Err(BroadcastError::DuplicateSubscriber(name)) if name == "audit"));
    assert_eq!(created, 0);
}

#[tokio::test]
async fn an_encoding_failure_sends_nothing() {
    let sink = RecordingSink::new(Mode::Accept);
    let broadcast = BroadcastFlow::<HashMap<(u8, u8), u8>>::new("bad")
        .subscriber("a")
        .build_with(|_| sink.clone())
        .unwrap();
    let mut publisher = broadcast.publisher();
    let result = publisher.publish(HashMap::from([((1, 2), 3)])).await;
    assert!(matches!(result, Err(PublishError::Codec(_))));
    assert!(sink.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_failing_subscriber_does_not_block_the_others() {
    let accepting = RecordingSink::new(Mode::Accept);
    let broadcast = BroadcastFlow::<u32>::new("orders")
        .subscriber("first")
        .subscriber("broken")
        .subscriber("last")
        .build_with(|name| {
            if name == "broken" {
                RecordingSink::new(Mode::FailReady)
            } else {
                accepting.clone()
            }
        })
        .unwrap();
    let receipt = broadcast.publisher().publish(7).await.unwrap();

    let delivered: Vec<_> = receipt
        .deliveries
        .iter()
        .map(|d| d.subscriber.as_str())
        .collect();
    assert_eq!(delivered, ["first", "last"]);
    assert_eq!(receipt.failures.len(), 1);
    assert_eq!(receipt.failures[0].subscriber, "broken");
    assert_eq!(receipt.failures[0].kind, FailureKind::NotSent);
    let sent = accepting.sent.lock().unwrap();
    let expected: Vec<_> = receipt
        .deliveries
        .iter()
        .map(|d| d.task_id.clone())
        .collect();
    assert_eq!(*sent, expected);
}

#[tokio::test]
async fn an_error_after_the_backend_took_the_task_is_unconfirmed() {
    let sink = RecordingSink::new(Mode::FailFlush);
    let broadcast = BroadcastFlow::<u32>::new("orders")
        .subscriber("files")
        .build_with(|_| sink.clone())
        .unwrap();
    let receipt = broadcast.publisher().publish(7).await.unwrap();

    assert!(receipt.deliveries.is_empty());
    let [failure] = receipt.failures.as_slice() else {
        panic!("expected one failure, got {:?}", receipt.failures);
    };
    assert_eq!(failure.kind, FailureKind::Unconfirmed);
    // The backend holds the task under the reported ID, so the caller can look
    // it up instead of sending a duplicate.
    assert_eq!(
        *sink.sent.lock().unwrap(),
        std::slice::from_ref(&failure.task_id)
    );
}

#[tokio::test]
async fn a_backend_that_failed_is_not_polled_again() {
    let healthy = RecordingSink::new(Mode::Accept);
    let broadcast = BroadcastFlow::<u32>::new("orders")
        .subscriber("closed")
        .subscriber("healthy")
        .build_with(|name| {
            if name == "closed" {
                RecordingSink::new(Mode::FailReady)
            } else {
                healthy.clone()
            }
        })
        .unwrap();
    let mut publisher = broadcast.publisher();

    let first = publisher.publish(1).await.unwrap();
    assert_eq!(first.failures[0].kind, FailureKind::NotSent);
    // The failed handle panics if it is polled again.
    let second = publisher.publish(2).await.unwrap();
    assert_eq!(second.failures[0].subscriber, "closed");
    assert_eq!(second.failures[0].kind, FailureKind::Disabled);
    assert_eq!(healthy.sent.lock().unwrap().len(), 2);

    // A new publisher starts from the broadcast's handles and tries again.
    let third = broadcast.publisher().publish(3).await.unwrap();
    assert_eq!(third.failures[0].kind, FailureKind::NotSent);
    assert_eq!(healthy.sent.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_skipped_task_releases_capacity_the_handler_reserved() {
    // Both handlers share one concurrency permit.
    let permit = GlobalConcurrencyLimitLayer::new(1);
    let handler =
        tower::service_fn(|task: Task<u32>| async move { Ok::<_, BoxDynError>(task.args) });
    let mut skipping = When::new(|_: &u32| false, permit.layer(handler));
    let mut accepting = When::new(|_: &u32| true, permit.layer(handler));

    let skipped = skipping.ready().await.unwrap().call(Task::new(1)).await;
    assert_eq!(skipped.unwrap(), Decision::Skipped);
    let ready = tokio::time::timeout(DEADLINE, accepting.ready())
        .await
        .expect("the skipped task still holds the permit")
        .unwrap();
    assert_eq!(
        ready.call(Task::new(2)).await.unwrap(),
        Decision::Handled(2)
    );
}

/// The step at which a [`RecordingSink`] fails.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Accept,
    FailReady,
    FailFlush,
}

/// A sink backend that records the task IDs it takes, or fails at one step.
/// Like `SinkMapErr`, a handle panics if it is polled after returning an error.
#[derive(Clone)]
struct RecordingSink<A> {
    mode: Mode,
    failed: bool,
    sent: Arc<Mutex<Vec<TaskId>>>,
    codec: JsonCodec,
    _args: PhantomData<fn(A)>,
}

impl<A> RecordingSink<A> {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            failed: false,
            sent: Arc::default(),
            codec: JsonCodec::default(),
            _args: PhantomData,
        }
    }

    fn step(&mut self, fails: Mode) -> Result<(), std::io::Error> {
        assert!(!self.failed, "polled after an error");
        if self.mode == fails {
            self.failed = true;
            return Err(std::io::Error::other("rejected"));
        }
        Ok(())
    }
}

impl<A> Sink<Task<Vec<u8>>> for RecordingSink<A> {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(self.get_mut().step(Mode::FailReady))
    }

    fn start_send(self: Pin<&mut Self>, task: Task<Vec<u8>>) -> Result<(), Self::Error> {
        self.sent
            .lock()
            .unwrap()
            .push(task.task_id().cloned().unwrap());
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(self.get_mut().step(Mode::FailFlush))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<A> WireFormatBackend for RecordingSink<A> {
    type Codec = JsonCodec;
    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<A> BackendConfig for RecordingSink<A> {
    type Args = A;
    type Id = RandomId;
    type Kind = ();
    type Config = ();
    type Layer = tower::layer::util::Identity;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        tower::layer::util::Identity::new()
    }
}
