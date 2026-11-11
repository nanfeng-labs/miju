//! A join runs in its deepest predecessor's task, and fails loudly when that
//! task cannot know every predecessor's task ID.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::SinkExt;
use miju_core::{
    backend::ext::shared::Shared,
    error::WorkerError,
    task::{builder::TaskBuilder, task_id::TaskId},
    worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
};
use miju_workflow::{GraphFlow, in_memory::InMemoryWorkflow};

type Backend = Shared<InMemoryWorkflow<Vec<u8>>>;

/// Run `graph` from input 7 for a while and return its join inputs and worker errors.
async fn run(
    graph: GraphFlow<Backend>,
    joined: Arc<Mutex<Vec<(u32, u32)>>>,
) -> (Vec<(u32, u32)>, Vec<String>) {
    let backend = InMemoryWorkflow::<Vec<u8>>::create();
    let mut producer = backend.clone();
    producer
        .send(
            TaskBuilder::new(b"7".to_vec())
                .task_id(TaskId::from_string("root"))
                .build(),
        )
        .await
        .unwrap();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let seen = errors.clone();
    let worker = WorkerBuilder::new("graph")
        .backend(backend)
        .on_event(move |_, event| {
            if let Event::Error(error) = event {
                seen.lock().unwrap().push(error.to_string());
            }
        })
        .build(graph);
    worker
        .run_until(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<(), WorkerError>(())
        })
        .await
        .unwrap();
    let joined = joined.lock().unwrap().clone();
    let errors = errors.lock().unwrap().clone();
    (joined, errors)
}

#[tokio::test]
async fn join_across_unequal_branches_runs_once_for_any_node_order() {
    for short_branch_last in [false, true] {
        let graph = GraphFlow::<Backend>::new("unequal");
        let entry = graph.add_task(|value: u32| async move { value });
        let add_long = |graph: &GraphFlow<Backend>| {
            let first = graph
                .add_task(|value: u32| async move { value + 1 })
                .depends_on(&entry);
            let second = graph
                .add_task(|value: u32| async move { value + 1 })
                .depends_on(&first);
            graph
                .add_task(|value: u32| async move { value + 1 })
                .depends_on(&second)
        };
        let add_short = |graph: &GraphFlow<Backend>| {
            graph
                .add_task(|value: u32| async move { value * 2 })
                .depends_on(&entry)
        };
        // Node indices follow insertion order, so this decides which predecessor
        // has the higher index.
        let (short, long) = if short_branch_last {
            let long = add_long(&graph);
            (add_short(&graph), long)
        } else {
            let short = add_short(&graph);
            (short, add_long(&graph))
        };
        let joined = Arc::new(Mutex::new(Vec::new()));
        let record = joined.clone();
        let _join = graph
            .add_task(move |(short, long): (u32, u32)| {
                record.lock().unwrap().push((short, long));
                async move { short + long }
            })
            .depends_on((&short, &long));

        let (joined, errors) = run(graph, joined).await;
        assert_eq!(
            joined,
            vec![(14, 10)],
            "short_branch_last={short_branch_last}"
        );
        assert!(errors.is_empty(), "{errors:?}");
    }
}

#[tokio::test]
async fn join_without_a_recorded_predecessor_id_reports_an_error() {
    // The deepest predecessor descends from `x`, but `c` gets its task ID later in
    // the other branch, so the join cannot locate `c`'s result.
    let graph = GraphFlow::<Backend>::new("unrecorded");
    let entry = graph.add_task(|value: u32| async move { value });
    let b = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&entry);
    let x = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&entry);
    let c = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&b);
    let y = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&x);
    let z = graph
        .add_task(|value: u32| async move { value })
        .depends_on(&y);
    let joined = Arc::new(Mutex::new(Vec::new()));
    let record = joined.clone();
    let _join = graph
        .add_task(move |(c, z): (u32, u32)| {
            record.lock().unwrap().push((c, z));
            async move { c + z }
        })
        .depends_on((&c, &z));

    let (joined, errors) = run(graph, joined).await;
    assert!(joined.is_empty());
    assert!(
        errors
            .iter()
            .any(|error| error.contains("Missing TaskId for incoming node")),
        "{errors:?}"
    );
}
