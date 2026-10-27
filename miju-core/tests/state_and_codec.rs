//! Shared state, identifiers, and storage representation contracts.

use std::{collections::HashSet, num::ParseIntError};

use miju_core::{
    backend::{TaskResult, codec::Codec, queue::Queue},
    task::{
        metadata::{Metadata, MetadataStore},
        runs::Run,
        status::{AtomicStatus, Status, StatusError},
        task_id::{TaskId, TaskIdError},
    },
};

#[test]
fn statuses_round_trip_and_shared_cells_accept_each_state() {
    let status = AtomicStatus::default();
    let shared = status.clone();
    let mut previous = Status::Pending;
    for next in [
        Status::Queued,
        Status::Running,
        Status::Done,
        Status::Failed,
        Status::Killed,
        Status::Pending,
    ] {
        assert_eq!(next.to_string().parse::<Status>().unwrap(), next);
        assert_eq!(status.swap(next.clone()), previous);
        assert_eq!(shared.load(), next);
        previous = next;
    }
    std::thread::spawn(move || shared.store(Status::Done))
        .join()
        .unwrap();
    assert_eq!(Status::from(status), Status::Done);
    assert_eq!(AtomicStatus::from(Status::Failed).load(), Status::Failed);
    for invalid in ["", "running", " Done", "Queued ", "unknown"] {
        let error = invalid.parse::<Status>().unwrap_err();
        assert!(matches!(&error, StatusError::UnknownState(value) if value == invalid));
        assert_eq!(error.to_string(), format!("Unknown state: {invalid}"));
    }
}

#[test]
fn identifier_parsing_distinguishes_typed_keys_from_text() {
    let error = TaskId::try_from("").unwrap_err();
    assert!(matches!(&error, TaskIdError::Decode(value) if value.is_empty()));
    assert_eq!(error.to_string(), "could not decode task_id: ``");
    for (input, expected) in [
        ("0", 0),
        ("0042", 42),
        ("+42", 42),
        ("18446744073709551615", u64::MAX),
    ] {
        assert_eq!(input.parse::<TaskId>().unwrap(), TaskId::Int(expected));
    }
    for input in ["job-42", "-1", "18446744073709551616", " 42 ", "任务"] {
        assert_eq!(
            TaskId::try_from(input).unwrap(),
            TaskId::String(input.to_owned())
        );
    }
    assert!(TaskId::Int(u64::MAX) < TaskId::String(String::new()));
    let integer = TaskId::from_int(42);
    let opaque = TaskId::from_string("42");
    assert_eq!(integer.to_string(), opaque.to_string());
    assert_ne!(integer, opaque);
    assert_eq!(integer.as_int(), Some(42));
    assert!(integer.as_string().is_none());
    assert_eq!(opaque.as_string(), Some("42"));
    assert!(opaque.as_int().is_none());
    assert_eq!(HashSet::from([integer, opaque]).len(), 2);
    assert_eq!(TaskId::from(String::new()).as_string(), Some(""));
}

#[test]
fn generated_ids_preserve_their_representation_and_counter_suffix() {
    use miju_core::task::task_id::{GenerateId, RandomId};

    let workers: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                (0..64)
                    .map(|_| RandomId::generate().to_string())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let ids: Vec<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
    let mut counters = HashSet::new();
    for id in &ids {
        assert!(id.len() > 11);
        assert!(
            id.as_bytes()[..11]
                .iter()
                .all(|byte| b"abcdefghijkmnopqrstuvwxyz23456789-".contains(byte))
        );
        assert!(counters.insert(id[11..].parse::<u64>().unwrap()));
    }
    assert!(matches!(u64::generate(), TaskId::Int(_)));
    assert!(matches!(RandomId::generate(), TaskId::String(_)));
    for text in ["", "not-a-generated-id", "任务"] {
        let id = RandomId::try_from(text).unwrap();
        assert_eq!(id.to_string(), text);
        assert_eq!(String::from(id), text);
    }
}

#[test]
fn coalescing_ids_quotes_display_values_without_json_escaping() {
    use miju_core::task::task_id::coalesce_ids;

    assert_eq!(coalesce_ids(std::iter::empty::<u64>()), "[]");
    assert_eq!(
        coalesce_ids([TaskId::Int(42), TaskId::from_string("job")]),
        r#"["42","job"]"#
    );
    let unescaped = coalesce_ids(["a\"b", "c\\d"]);
    assert_eq!(unescaped, r#"["a"b","c\d"]"#);
    assert!(serde_json::from_str::<Vec<String>>(&unescaped).is_err());
}

#[test]
fn clearing_task_ids_retains_context_and_existing_old_id_metadata() {
    use miju_core::task::builder::TaskBuilder;
    use std::sync::Arc;

    struct Payload(u8);
    for (id, old, expected) in [
        (Some(42), None, "42"),
        (None, None, ""),
        (Some(99), Some("previous"), "previous"),
    ] {
        let mut metadata: MetadataStore = [("region", "west")].into_iter().collect();
        if let Some(old) = old {
            metadata.insert("miju_core.transform.old_id", old).unwrap();
        }
        let mut builder = TaskBuilder::new(Payload(7))
            .with_metadata(metadata)
            .data(vec![1_u8, 2])
            .attempt(3)
            .status(Status::Running)
            .run_at_timestamp(10)
            .done_at(Some(30))
            .lock_at(Some(20))
            .lock_by(Some("worker".to_owned()))
            .idempotency_key("key")
            .max_attempts(4)
            .priority(2)
            .queue("reports".into());
        if let Some(id) = id {
            builder = builder.task_id(TaskId::Int(id));
        }
        let task = builder.build();
        let observer = Arc::clone(task.ctx());
        let mapped = task.map_id_type();
        assert!(mapped.task_id().is_none());
        assert_eq!(
            observer.task_id(),
            id.as_ref().map(|id| TaskId::Int(*id)).as_ref()
        );
        assert!(!Arc::ptr_eq(&observer, mapped.ctx()));
        assert_eq!(mapped.args.0, 7);
        assert_eq!(
            mapped.metadata().get("miju_core.transform.old_id"),
            Some(expected)
        );
        assert_eq!(observer.metadata().get("miju_core.transform.old_id"), old);
        assert_eq!(mapped.metadata().get("region"), Some("west"));
        assert_eq!(mapped.data().get::<Vec<u8>>(), Some(&vec![1, 2]));
        assert_eq!((mapped.attempt(), mapped.status()), (3, Status::Running));
        assert_eq!(
            (mapped.run_at(), mapped.done_at(), mapped.lock_at()),
            (Some(10), Some(30), Some(20))
        );
        assert_eq!(
            (mapped.lock_by(), mapped.idempotency_key()),
            (Some("worker"), Some("key"))
        );
        assert_eq!(
            (mapped.max_attempts(), mapped.priority()),
            (Some(4), Some(2))
        );
        assert_eq!(mapped.queue().map(AsRef::as_ref), Some("reports"));
        mapped.raw_status().store(Status::Done);
        assert_eq!(observer.status(), Status::Done);
        let mapped_again = mapped.map_id_type();
        assert_eq!(
            mapped_again.metadata().get("miju_core.transform.old_id"),
            Some(expected)
        );
    }
}

#[test]
fn identifier_parsing_follows_the_enabled_formats() {
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    let ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let ambiguous = "00000000000000000000000001";

    #[cfg(feature = "uuid")]
    assert!(matches!(uuid.parse::<TaskId>().unwrap(), TaskId::Uuid(_)));
    #[cfg(not(feature = "uuid"))]
    assert_eq!(uuid.parse::<TaskId>().unwrap(), TaskId::from_string(uuid));
    #[cfg(feature = "ulid")]
    {
        assert!(matches!(ulid.parse::<TaskId>().unwrap(), TaskId::Ulid(_)));
        assert!(matches!(
            ambiguous.parse::<TaskId>().unwrap(),
            TaskId::Ulid(_)
        ));
    }
    #[cfg(not(feature = "ulid"))]
    {
        assert_eq!(ulid.parse::<TaskId>().unwrap(), TaskId::from_string(ulid));
        assert_eq!(ambiguous.parse::<TaskId>().unwrap(), TaskId::Int(1));
    }
    for value in [uuid, ulid, ambiguous] {
        assert_eq!(TaskId::from_string(value).as_string(), Some(value));
    }
}

#[cfg(feature = "uuid")]
#[test]
fn uuid_generation_and_mapping_keep_the_fixed_namespace_and_input_bytes() {
    use miju_core::task::task_id::GenerateId;

    let value = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let id = TaskId::from_uuid(value);
    assert_eq!(id, TaskId::from(value));
    assert_eq!(id.as_uuid(), Some(value));
    assert_eq!(id.to_uuid(), value);
    assert!(id.as_string().is_none());
    assert_eq!(TaskId::Int(42).as_uuid(), None);
    assert_eq!(
        TaskId::Int(42).to_uuid().to_string(),
        "19057bc1-954c-5263-a0d8-3345a634754f"
    );
    assert_eq!(
        TaskId::from_string("42").to_uuid().to_string(),
        "7c411b5e-9d3f-50b5-9c28-62096e41c4ed"
    );
    let generated = <uuid::Uuid as GenerateId>::generate().as_uuid().unwrap();
    assert_eq!(generated.get_version_num(), 4);
    assert_eq!(generated.get_variant(), uuid::Variant::RFC4122);
}

#[cfg(feature = "ulid")]
#[test]
fn ulid_generation_and_conversion_preserve_the_typed_id() {
    use miju_core::task::task_id::GenerateId;

    let value = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse::<ulid::Ulid>().unwrap();
    let id = TaskId::from_ulid(value);
    assert_eq!(id, TaskId::from(value));
    assert_eq!(id.as_ulid(), Some(value));
    assert_eq!(id.to_string(), value.to_string());
    assert!(id.as_string().is_none());
    assert_eq!(TaskId::Int(42).as_ulid(), None);
    assert!(matches!(
        <ulid::Ulid as GenerateId>::generate(),
        TaskId::Ulid(_)
    ));
    #[cfg(feature = "uuid")]
    assert_eq!(id.to_uuid().as_u128(), u128::from(value));
}

#[derive(Debug)]
struct Decimal;

impl Codec<u64> for Decimal {
    type Compact = String;
    type Error = ParseIntError;

    fn encode(&self, value: &u64) -> Result<Self::Compact, Self::Error> {
        Ok(value.to_string())
    }

    fn decode(&self, value: &Self::Compact) -> Result<u64, Self::Error> {
        value.parse()
    }
}

#[test]
fn codec_converts_borrowed_values_and_propagates_decode_failures() {
    let codec = Decimal;
    let encoded = codec.encode(&123).unwrap();
    assert_eq!(codec.decode(&encoded), Ok(123));
    assert_eq!(encoded, "123");
    let invalid = "not an integer".to_owned();
    assert_eq!(
        codec.decode(&invalid).unwrap_err(),
        invalid.parse::<u64>().unwrap_err()
    );
}

#[test]
fn queue_names_and_recorded_results_preserve_storage_data() {
    let queue: Queue = "tasks/日报".parse().unwrap();
    assert_eq!(queue.as_ref(), "tasks/日报");
    assert_eq!(Queue::from(queue.to_string()), queue);
    assert_eq!(Queue::from("").as_ref(), "");
    let mut metadata = MetadataStore::new();
    assert!(Queue::extract(&metadata).is_err());
    queue.inject(&mut metadata).unwrap();
    assert_eq!(Queue::extract(&metadata).unwrap(), queue);
    let duplicate = Queue::from("replacement")
        .inject(&mut metadata)
        .unwrap_err();
    assert_eq!(
        duplicate.to_string(),
        "Queue data not found in task context. This is likely a bug. Please report it."
    );
    assert_eq!(metadata.get("queue"), Some(queue.as_ref()));
    let outcome = TaskResult {
        task_id: TaskId::from(8),
        attempt: 2,
        status: Status::Failed,
        result: Err::<Vec<u8>, _>("temporary failure".to_owned()),
    };
    let mut snapshot = outcome.clone();
    snapshot.status = Status::Done;
    snapshot.result = Ok(vec![1, 2]);
    assert_eq!(outcome.status, Status::Failed);
    assert_eq!(outcome.result.as_ref().unwrap_err(), "temporary failure");
    assert_eq!(snapshot.task_id, outcome.task_id);
    assert_eq!(snapshot.attempt, outcome.attempt);
    assert_eq!(snapshot.task_id(), &TaskId::Int(8));
    assert_eq!(snapshot.status(), &Status::Done);
    assert_eq!(snapshot.result(), &Ok(vec![1, 2]));
    let run = Run {
        result: Some(snapshot),
        lock_at: -1,
        lock_by: "worker".to_owned(),
        done_at: Some(2),
    };
    assert_eq!(run.lock_at, -1);
    assert_eq!(run.clone().result.unwrap().take(), Ok(vec![1, 2]));
    assert_eq!(run.result.as_ref().unwrap().result(), &Ok(vec![1, 2]));
    assert_eq!(outcome.take(), Err("temporary failure".to_owned()));
}

#[tokio::test]
async fn memory_submission_keeps_ids_and_remembers_idempotency_keys_after_fetch() {
    use futures_util::{FutureExt, StreamExt};
    use miju_core::{
        backend::{TaskSink, memory::MemoryStorage},
        task::builder::TaskBuilder,
    };
    let mut backend = MemoryStorage::new();
    let task = TaskBuilder::new(7u32)
        .task_id("explicit".to_owned().into())
        .idempotency_key("once")
        .build();
    backend.push_task(task.clone()).await.unwrap();
    backend.push_task(task.clone()).await.unwrap();
    let fetched = backend.next().await.unwrap();
    assert_eq!(fetched.task_id(), task.task_id());
    assert_eq!(fetched.args, 7);
    backend.push_task(task).await.unwrap();
    assert!(backend.next().now_or_never().is_none());
    backend.push(9).await.unwrap();
    let generated = backend.next().await.unwrap();
    assert!(generated.task_id().is_some());
    assert_eq!(generated.args, 9);
}

#[tokio::test]
async fn dequeue_idempotency_only_covers_queued_tasks_and_push_wakes_receiver() {
    use miju_core::{
        backend::{Backend, TaskSink, dequeue::VecDequeBackend},
        task::builder::TaskBuilder,
        worker::context::WorkerContext,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };
    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let worker = WorkerContext::new("dequeue");
    let mut backend = VecDequeBackend::new();
    assert!(Backend::poll_ready(&mut backend, &mut cx, &worker).is_pending());
    let task = TaskBuilder::new(3u32).idempotency_key("queued").build();
    backend.push_task(task.clone()).await.unwrap();
    backend.push_task(task.clone()).await.unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let Poll::Ready(Some(Ok(fetched))) = backend.poll_next(&mut cx, &worker) else {
        panic!("queued task missing")
    };
    assert!(fetched.task_id().is_some());
    assert_eq!(fetched.args, 3);
    assert!(backend.poll_next(&mut cx, &worker).is_pending());
    backend.push_task(task).await.unwrap();
    assert!(matches!(
        backend.poll_next(&mut cx, &worker),
        Poll::Ready(Some(Ok(_)))
    ));
}

mod durable_submission {
    use super::*;
    use futures_sink::Sink;
    use futures_util::SinkExt;
    use miju_core::{
        backend::{
            Backend, BackendConfig, TaskSink, TaskSinkError, WireFormatBackend,
            dequeue::VecDequeBackend,
            finalize::{Durable, DurableError, FinalizeBackend},
        },
        task::builder::TaskBuilder,
        worker::context::WorkerContext,
    };
    use std::{
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use tower_layer::Identity;

    #[derive(Clone)]
    struct NumberCodec;
    impl Codec<u32> for NumberCodec {
        type Compact = String;
        type Error = std::io::Error;
        fn encode(&self, value: &u32) -> Result<String, Self::Error> {
            if *value == u32::MAX {
                return Err(std::io::Error::other("encode"));
            }
            Ok(value.to_string())
        }
        fn decode(&self, value: &String) -> Result<u32, Self::Error> {
            value.parse().map_err(std::io::Error::other)
        }
    }

    struct Encoded<B> {
        backend: B,
    }
    miju_core::delegate_sink!(Encoded<B>, backend);

    impl<B: Backend> Backend for Encoded<B> {
        type Task = B::Task;
        type Error = B::Error;
        fn poll_ready(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            self.backend.poll_ready(cx, worker)
        }
        fn poll_next(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
            self.backend.poll_next(cx, worker)
        }
        fn poll_close(
            &mut self,
            cx: &mut Context<'_>,
            worker: &WorkerContext,
        ) -> Poll<Result<(), Self::Error>> {
            self.backend.poll_close(cx, worker)
        }
    }
    impl<B> BackendConfig for Encoded<B> {
        type Args = u32;
        type Kind = Durable;
        type Id = String;
        type Config = ();
        type Layer = Identity;
        fn config(&self) -> &() {
            &()
        }
        fn middleware(&mut self, _: &mut WorkerContext) -> Identity {
            Identity::new()
        }
    }
    impl<B> WireFormatBackend for Encoded<B> {
        type Compact = String;
        type Codec = NumberCodec;
        fn codec(&self) -> &NumberCodec {
            &NumberCodec
        }
    }

    #[tokio::test]
    async fn durable_submission_encodes_preserves_context_and_reports_codec_errors() {
        let mut backend = Encoded {
            backend: VecDequeBackend::<String>::new(),
        };
        let task = TaskBuilder::new(42u32)
            .task_id("durable".to_owned().into())
            .priority(9)
            .build();
        backend.push_task(task.clone()).await.unwrap();
        assert!(matches!(
            backend.push(u32::MAX).await,
            Err(TaskSinkError::CodecError(_))
        ));
        backend.backend.push("invalid".to_owned()).await.unwrap();
        let mut sender = backend.backend.clone();
        let mut finalized = <Durable as FinalizeBackend<_, u32>>::finalize(backend);
        let worker = WorkerContext::new("decode");
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Some(Ok(decoded))) = finalized.poll_next(&mut cx, &worker) else {
            panic!("decoded task missing")
        };
        assert_eq!(decoded.args, 42);
        assert!(std::sync::Arc::ptr_eq(decoded.ctx(), task.ctx()));
        assert_eq!(decoded.priority(), Some(9));
        assert!(matches!(
            finalized.poll_next(&mut cx, &worker),
            Poll::Ready(Some(Err(DurableError::CodecError(_))))
        ));
        assert!(finalized.poll_next(&mut cx, &worker).is_pending());
        sender.push("43".to_owned()).await.unwrap();
        let Poll::Ready(Some(Ok(decoded))) = finalized.poll_next(&mut cx, &worker) else {
            panic!("a later decoded task must remain available")
        };
        assert_eq!(decoded.args, 43);
    }
}
