//! Serialization is a value snapshot and does not preserve live shared cells.

#![cfg(feature = "serde")]

use miju_core::{
    backend::{TaskResult, queue::Queue},
    task::{
        ExecutionContext,
        attempt::Attempt,
        builder::TaskBuilder,
        metadata::MetadataStore,
        runs::Run,
        status::{AtomicStatus, Status},
        task_id::TaskId,
    },
};
use serde_json::{from_value, json, to_value};

#[test]
fn context_and_builder_snapshots_preserve_fields_and_skip_runtime_data() {
    let builder = TaskBuilder::new("payload".to_owned())
        .task_id(TaskId::Int(42))
        .data(99_u32)
        .with_metadata([("region", "west")].into_iter().collect())
        .attempt(2)
        .status(Status::Running)
        .run_at_timestamp(10)
        .done_at(Some(30))
        .lock_at(Some(20))
        .lock_by(Some("worker".to_owned()))
        .idempotency_key("key")
        .max_attempts(0)
        .priority(0)
        .queue(Queue::from("reports"));
    let context = json!({
        "task_id":42, "attempt":2, "status":"Running", "run_at":10,
        "done_at":30, "lock_at":20, "lock_by":"worker", "idempotency_key":"key",
        "metadata":{"region":"west"}, "max_attempts":0, "priority":0,
        "queue":"reports", "runs":[]
    });
    let encoded = to_value(&builder).unwrap();
    assert_eq!(encoded, json!({"args":"payload", "ctx":context}));
    let restored: TaskBuilder<String> = from_value(encoded).unwrap();
    let original = builder.build();
    let restored = restored.build();
    assert_eq!(to_value(original.ctx().as_ref()).unwrap(), context);
    assert_eq!(to_value(restored.ctx().as_ref()).unwrap(), context);
    assert_eq!(restored.args, "payload");
    assert!(restored.data().is_empty());
    assert_eq!(original.data().get::<u32>(), Some(&99));
    original.raw_status().store(Status::Done);
    assert_eq!(restored.status(), Status::Running);
    let snapshot: ExecutionContext = from_value(context).unwrap();
    assert_eq!(snapshot.status(), Status::Running);
    assert!(snapshot.data().is_empty());
}

#[test]
fn scalar_and_history_wire_formats_match_the_data_contracts() {
    assert_eq!(to_value(Attempt::new_with_value(7)).unwrap(), json!(7));
    assert_eq!(from_value::<Attempt>(json!(7)).unwrap().current(), 7);
    assert!(from_value::<Attempt>(json!(-1)).is_err());
    assert_eq!(
        to_value(AtomicStatus::new(Status::Failed)).unwrap(),
        json!("Failed")
    );
    assert_eq!(
        from_value::<AtomicStatus>(json!("Failed")).unwrap().load(),
        Status::Failed
    );
    assert!(
        from_value::<AtomicStatus>(json!("failed"))
            .unwrap_err()
            .to_string()
            .contains("Unknown state: failed")
    );
    assert_eq!(to_value(Status::Done).unwrap(), json!("Done"));
    assert_eq!(from_value::<Status>(json!("Done")).unwrap(), Status::Done);
    assert_eq!(to_value(TaskId::Int(42)).unwrap(), json!(42));
    assert_eq!(
        to_value(TaskId::String("42".to_owned())).unwrap(),
        json!("42")
    );
    assert_eq!(
        from_value::<TaskId>(json!("42")).unwrap(),
        TaskId::String("42".to_owned())
    );
    assert_eq!(
        from_value::<TaskId>(json!("")).unwrap(),
        TaskId::String(String::new())
    );
    assert!(from_value::<TaskId>(json!(-1)).is_err());
    assert_eq!(to_value(Queue::from("reports")).unwrap(), json!("reports"));
    assert_eq!(
        from_value::<Queue>(json!("reports")).unwrap().as_ref(),
        "reports"
    );
    let metadata: MetadataStore = from_value(json!({"key":"value"})).unwrap();
    assert_eq!(to_value(metadata).unwrap(), json!({"key":"value"}));

    let run: Run = Run {
        result: Some(TaskResult {
            task_id: TaskId::Int(1),
            attempt: 2,
            status: Status::Done,
            result: Ok(vec![3]),
        }),
        lock_at: -1,
        lock_by: "worker".to_owned(),
        done_at: Some(4),
    };
    let expected = json!({"result":{"task_id":1,"attempt":2,"status":"Done","result":{"Ok":[3]}},"lock_at":-1,"lock_by":"worker","done_at":4});
    assert_eq!(to_value(&run).unwrap(), expected);
    let restored: Run = from_value(expected.clone()).unwrap();
    assert_eq!(restored.result.unwrap().take(), Ok(vec![3]));
    let mut context = to_value(ExecutionContext::default()).unwrap();
    context["runs"] = json!([expected]);
    let context: ExecutionContext = from_value(context).unwrap();
    assert_eq!(context.runs()[0].lock_at, -1);
    assert_eq!(context.runs()[0].result.as_ref().unwrap().attempt, 2);
    let failed = TaskResult {
        task_id: TaskId::from_int(1),
        attempt: 3,
        status: Status::Failed,
        result: Err::<Vec<u8>, _>("failure".to_owned()),
    };
    assert_eq!(
        to_value(&failed).unwrap()["result"],
        json!({"Err":"failure"})
    );
    assert_eq!(
        from_value::<TaskResult<Vec<u8>>>(to_value(failed).unwrap())
            .unwrap()
            .take(),
        Err("failure".to_owned())
    );
}
