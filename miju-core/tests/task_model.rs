//! Task construction, payload conversion, and context sharing behavior.

use std::sync::Arc;

use miju_core::task::{
    Task,
    builder::TaskBuilder,
    metadata::{Metadata, MetadataStore},
    status::Status,
    task_id::TaskId,
};

#[derive(Debug, PartialEq, Eq)]
struct Tenant(String);

impl Metadata for Tenant {
    type Error = &'static str;

    fn extract(store: &MetadataStore) -> Result<Self, Self::Error> {
        let value = store.get("tenant").ok_or("missing tenant")?;
        if value.is_empty() {
            return Err("empty tenant");
        }
        Ok(Self(value.to_owned()))
    }

    fn inject(&self, store: &mut MetadataStore) -> Result<(), Self::Error> {
        store
            .insert("tenant", self.0.clone())
            .map_err(|_| "duplicate tenant")?;
        // Exercise a caller that writes before discovering an error.
        if self.0.is_empty() {
            return Err("empty tenant");
        }
        Ok(())
    }
}

#[test]
fn new_tasks_are_pending_and_have_no_storage_or_scheduling_defaults() {
    let task = Task::new("payload");
    assert_eq!(task.args, "payload");
    assert!(task.task_id().is_none());
    assert_eq!(task.status(), Status::Pending);
    assert_eq!(task.attempt(), 0);
    assert!(task.data().is_empty());
    assert!(task.metadata().is_empty());
    assert_eq!(
        (task.run_at(), task.max_attempts(), task.priority()),
        (None, None, None)
    );
    assert!(task.queue().is_none());

    task.raw_attempt().increment();
    task.raw_status().store(Status::Running);
    let unrelated = Task::new("another payload");
    assert_eq!(unrelated.attempt(), 0);
    assert_eq!(unrelated.status(), Status::Pending);
}

#[test]
fn builder_mapping_preserves_configuration_and_does_not_require_clone() {
    #[derive(Debug)]
    struct Payload(u64);

    let task = TaskBuilder::new(Payload(7))
        .task_id(42)
        .queue("reports")
        .run_at_timestamp(1_800_000_000)
        .max_attempts(0)
        .priority(0)
        .attempt(2)
        .status(Status::Queued)
        .data(vec![1_u8, 2])
        .metadata(&Tenant("east".to_owned()))
        .unwrap()
        .map_args(|payload| payload.0.to_string())
        .try_map_args(|payload| payload.parse::<u64>())
        .unwrap()
        .build();

    assert_eq!(task.args, 7);
    assert_eq!(task.task_id(), Some(&TaskId::Int(42)));
    assert_eq!(task.queue().map(AsRef::as_ref), Some("reports"));
    assert_eq!(task.run_at(), Some(1_800_000_000));
    assert_eq!((task.max_attempts(), task.priority()), (Some(0), Some(0)));
    assert_eq!((task.attempt(), task.status()), (2, Status::Queued));
    assert_eq!(task.data().get::<Vec<u8>>().unwrap(), &[1, 2]);
    assert_eq!(
        Tenant::extract(task.metadata()),
        Ok(Tenant("east".to_owned()))
    );
}

#[test]
fn task_clones_isolate_data_but_continue_to_observe_shared_execution() {
    let original = TaskBuilder::new(vec![1_u8]).data(vec![10_u32]).build();
    let mut copy = original.clone();
    assert!(Arc::ptr_eq(original.ctx(), copy.ctx()));

    copy.args.push(2);
    assert_eq!(copy.inject_data(vec![20_u32]), Some(vec![10]));
    copy.inject_metadata(&Tenant("north".to_owned())).unwrap();
    assert!(!Arc::ptr_eq(original.ctx(), copy.ctx()));
    assert_eq!(original.args, [1]);
    assert_eq!(copy.args, [1, 2]);
    assert_eq!(original.data().get::<Vec<u32>>().unwrap(), &[10]);
    assert_eq!(copy.data().get::<Vec<u32>>().unwrap(), &[20]);
    assert!(original.metadata().is_empty());

    std::thread::spawn(move || {
        assert_eq!(copy.raw_attempt().increment(), 0);
        copy.raw_status().store(Status::Running);
    })
    .join()
    .unwrap();
    assert_eq!(original.attempt(), 1);
    assert_eq!(original.status(), Status::Running);
}

#[test]
fn failed_metadata_injection_preserves_values_and_context_identity() {
    let mut task = Task::new(());
    let observer = task.clone();
    assert_eq!(
        task.inject_metadata(&Tenant(String::new())),
        Err("empty tenant")
    );
    assert!(task.metadata().is_empty());
    assert!(Arc::ptr_eq(task.ctx(), observer.ctx()));

    task.inject_metadata(&Tenant("west".to_owned())).unwrap();
    let before = Arc::clone(task.ctx());
    assert_eq!(
        task.inject_metadata(&Tenant("east".to_owned())),
        Err("duplicate tenant")
    );
    assert!(Arc::ptr_eq(task.ctx(), &before));
    assert_eq!(
        Tenant::extract(task.metadata()),
        Ok(Tenant("west".to_owned()))
    );
    assert!(observer.metadata().is_empty());
}

#[test]
fn builder_propagates_metadata_errors() {
    let invalid = TaskBuilder::new(()).metadata(&Tenant(String::new()));
    assert_eq!(invalid.unwrap_err(), "empty tenant");
    let duplicate = TaskBuilder::new(())
        .metadata(&Tenant("first".to_owned()))
        .unwrap()
        .metadata(&Tenant("second".to_owned()));
    assert_eq!(duplicate.unwrap_err(), "duplicate tenant");
    assert_eq!(
        Tenant::extract(&MetadataStore::new()),
        Err("missing tenant")
    );
}

#[test]
fn mapping_and_taking_payloads_preserve_the_original_context() {
    let source = TaskBuilder::new("42".to_owned()).task_id(5).build();
    let observer = Arc::clone(source.ctx());
    let parsed = source.try_map_args(|args| args.parse::<u64>()).unwrap();
    assert!(Arc::ptr_eq(&observer, parsed.ctx()));
    let formatted = parsed.map_args(|args| format!("id={args}"));
    assert!(Arc::ptr_eq(&observer, formatted.ctx()));
    let (args, context) = formatted.take();
    assert_eq!(args, "id=42");
    assert!(Arc::ptr_eq(&observer, &context));
    let restored = Task::new_with_ctx(args, context);
    assert_eq!(restored.task_id(), Some(&TaskId::Int(5)));
}

#[test]
fn payload_conversion_errors_are_returned_unchanged() {
    let task_error = Task::new("invalid")
        .try_map_args(|args| args.parse::<u64>())
        .unwrap_err();
    let builder_error = TaskBuilder::new("invalid")
        .try_map_args(|args| args.parse::<u64>())
        .unwrap_err();
    let expected = "invalid".parse::<u64>().unwrap_err();
    assert_eq!(task_error, expected);
    assert_eq!(builder_error, expected);
}

#[test]
fn rebuilding_retains_context_but_explicit_resets_do_not_change_observers() {
    let original = TaskBuilder::new(7)
        .task_id(9)
        .data(vec![1_u8])
        .metadata(&Tenant("south".to_owned()))
        .unwrap()
        .queue("batch")
        .run_at_timestamp(123)
        .max_attempts(4)
        .attempt(2)
        .status(Status::Running)
        .build();

    let changed = original.clone().into_builder().priority(8).build();
    assert_eq!(original.priority(), None);
    assert_eq!(changed.priority(), Some(8));
    assert_eq!(changed.task_id(), original.task_id());
    assert_eq!(changed.metadata(), original.metadata());
    assert_eq!(
        changed.data().get::<Vec<u8>>(),
        original.data().get::<Vec<u8>>()
    );
    assert_eq!(changed.queue(), original.queue());
    assert_eq!(changed.run_at(), Some(123));
    assert_eq!(changed.max_attempts(), Some(4));
    changed.raw_attempt().increment();
    changed.raw_status().store(Status::Done);
    assert_eq!((original.attempt(), original.status()), (3, Status::Done));

    let reset = changed
        .into_builder()
        .attempt(0)
        .status(Status::Pending)
        .build();
    reset.raw_attempt().increment();
    reset.raw_status().store(Status::Queued);
    assert_eq!((reset.attempt(), reset.status()), (1, Status::Queued));
    assert_eq!((original.attempt(), original.status()), (3, Status::Done));
}
