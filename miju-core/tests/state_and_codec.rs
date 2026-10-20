//! Shared state, identifiers, and storage representation contracts.

use std::{collections::HashSet, num::ParseIntError};

use miju_core::{
    backend::{codec::Codec, queue::Queue, results::TaskResult},
    task::{
        attempt::Attempt,
        status::{AtomicStatus, Status, StatusError},
        task_id::{TaskId, TaskIdError},
    },
};

#[test]
fn concurrent_attempts_return_each_previous_value_once() {
    let attempts = Attempt::new_with_value(7);
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let counter = attempts.clone();
            std::thread::spawn(move || (0..500).map(|_| counter.increment()).collect::<Vec<_>>())
        })
        .collect();
    let mut observed: Vec<_> = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect();
    observed.sort_unstable();
    assert_eq!(observed, (7..4_007).collect::<Vec<_>>());
    assert_eq!(attempts.current(), 4_007);
    assert_eq!(Attempt::new().current(), 0);
}

#[test]
fn attempt_overflow_matches_atomic_wrapping_semantics() {
    let attempts = Attempt::new_with_value(usize::MAX);
    assert_eq!(attempts.increment(), usize::MAX);
    assert_eq!(attempts.current(), 0);
}

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
        assert_eq!(next.to_string().parse::<Status>(), Ok(next));
        assert_eq!(status.swap(next), previous);
        assert_eq!(shared.load(), next);
        previous = next;
    }
    std::thread::spawn(move || shared.store(Status::Done))
        .join()
        .unwrap();
    assert_eq!(Status::from(status), Status::Done);
    assert_eq!(AtomicStatus::from(Status::Failed).load(), Status::Failed);
    for invalid in ["", "running", " Done", "Queued ", "unknown"] {
        assert_eq!(
            invalid.parse::<Status>(),
            Err(StatusError::UnknownState(invalid.to_owned()))
        );
    }
}

#[test]
fn identifier_parsing_distinguishes_typed_keys_from_text() {
    assert_eq!(
        TaskId::try_from(""),
        Err(TaskIdError::Decode(String::new()))
    );
    for (input, expected) in [
        ("0", 0),
        ("0042", 42),
        ("+42", 42),
        ("18446744073709551615", u64::MAX),
    ] {
        assert_eq!(input.parse::<TaskId>(), Ok(TaskId::Int(expected)));
    }
    for input in ["job-42", "-1", "18446744073709551616", " 42 ", "任务"] {
        assert_eq!(
            TaskId::try_from(input),
            Ok(TaskId::String(input.to_owned()))
        );
    }
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
}
