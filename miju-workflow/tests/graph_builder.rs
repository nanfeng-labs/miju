//! Graph registration, typed dependency wiring and construction-time limits.

use std::{
    future::ready,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use miju_core::task::{Task, task_fn::task_fn};
use miju_workflow::{
    GraphFlow,
    graph::{DepsCheck, NodeHandle, error::GraphFlowError},
    in_memory::InMemoryWorkflow,
};
use petgraph::graph::{EdgeIndex, NodeIndex};
use tower::service_fn;

type Graph = GraphFlow<InMemoryWorkflow<u32>>;

async fn increment(value: u32) -> u32 {
    value + 1
}

#[test]
fn constructors_accept_empty_graphs_and_display_their_name() {
    let ordinary = Graph::new("empty");
    let typed = Graph::new_typed("empty");
    ordinary.validate().unwrap();
    typed.validate().unwrap();
    assert_eq!(ordinary.to_dot(), typed.to_dot());
    assert!(ordinary.to_dot().contains("digraph"));
    assert_eq!(
        ordinary.to_string(),
        format!("Graph name: empty\nDot format:\n{}", ordinary.to_dot())
    );
}

#[test]
fn services_tasks_and_routes_register_without_running_handlers() {
    let graph = Graph::new("registration");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let text = graph.add_node(
        "text",
        service_fn(move |task: Task<u32>| {
            observed.fetch_add(1, Ordering::SeqCst);
            ready(Ok::<_, io::Error>(task.args.to_string()))
        }),
    );
    let length = graph
        .add_task(|text: String| async move { text.len() })
        .depends_on(&text);
    let observed = Arc::clone(&calls);
    let route = graph
        .route(move |length: usize| {
            observed.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, io::Error>(NodeIndex::new(length)) }
        })
        .depends_on(&length);
    assert_eq!(length.id(), NodeIndex::new(1));
    assert_eq!(length.edges(), [EdgeIndex::new(0)]);
    assert_eq!(route.id(), NodeIndex::new(2));
    assert_eq!(route.edges(), [EdgeIndex::new(1)]);
    graph.validate().unwrap();
    let dot = graph.to_dot();
    assert!(dot.contains("label=\"text\""));
    assert!(dot.contains("0 -> 1"));
    assert!(dot.contains("1 -> 2"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn tuple_and_vector_dependencies_preserve_insertion_order() {
    let graph = Graph::new("fan-in");
    let first = graph.add_node("first", task_fn(increment));
    let second = graph.add_node("second", task_fn(increment));
    let tuple = graph
        .add_task(|(a, b): (u32, u32)| async move { a + b })
        .depends_on((&second, &first));
    let vector = graph
        .add_task(|items: Vec<u32>| async move { items.len() })
        .depends_on(vec![&first, &second]);
    assert_eq!(tuple.edges(), [EdgeIndex::new(0), EdgeIndex::new(1)]);
    assert_eq!(vector.edges(), [EdgeIndex::new(2), EdgeIndex::new(3)]);
    let dot = graph.to_dot();
    assert!(dot.find("1 -> 2").unwrap() < dot.find("0 -> 2").unwrap());
    assert!(dot.find("0 -> 3").unwrap() < dot.find("1 -> 3").unwrap());

    let eight = graph
        .add_task(|values: (u32, u32, u32, u32, u32, u32, u32, u32)| async move { values.0 })
        .depends_on((
            &tuple, &tuple, &tuple, &tuple, &tuple, &tuple, &tuple, &tuple,
        ));
    assert_eq!(eight.edges().len(), 8);
    assert_eq!(
        DepsCheck::<(u32, u32)>::to_node_indices(&(&first, &second)),
        [NodeIndex::new(0), NodeIndex::new(1)]
    );
    graph.validate().unwrap();
}

#[test]
fn single_dependency_forms_preserve_scalar_and_singleton_tuple_types() {
    let graph = Graph::new("single");
    let first = graph.add_task(increment);
    let handle = graph.add_task(increment).depends_on(&first);
    let expected = vec![NodeIndex::new(0)];
    assert_eq!(DepsCheck::<u32>::to_node_indices(&first), expected);
    assert_eq!(DepsCheck::<u32>::to_node_indices(&&first), expected);
    assert_eq!(
        DepsCheck::<u32>::to_node_indices(&(first.clone(),)),
        expected
    );
    assert_eq!(DepsCheck::<u32>::to_node_indices(&(&first,)), expected);
    assert_eq!(DepsCheck::<(u32,)>::to_node_indices(&(&first,)), expected);

    let expected = vec![NodeIndex::new(1)];
    assert_eq!(DepsCheck::<u32>::to_node_indices(&handle), expected);
    assert_eq!(DepsCheck::<u32>::to_node_indices(&&handle), expected);
    assert_eq!(
        DepsCheck::<u32>::to_node_indices(&(handle.clone(),)),
        expected
    );
    assert_eq!(DepsCheck::<u32>::to_node_indices(&(&handle,)), expected);
    assert_eq!(DepsCheck::<(u32,)>::to_node_indices(&(&handle,)), expected);
    let _scalar = graph.add_task(increment).depends_on((&handle,));
    let _tuple = graph
        .add_task(|(value,): (u32,)| async move { value })
        .depends_on((&handle,));
    graph.validate().unwrap();
}

#[test]
fn cloned_builders_append_edges_and_handles_report_only_their_own_edges() {
    let graph = Graph::new("parallel-edges");
    let entry = graph.add_task(increment);
    let target = graph.add_task(increment);
    let first = target.clone().depends_on(&entry);
    let second = target.depends_on(&entry);
    assert_eq!(first.id(), second.id());
    assert_eq!(first.edges(), [EdgeIndex::new(0)]);
    assert_eq!(second.edges(), [EdgeIndex::new(1)]);
    let cloned = second.clone();
    assert_eq!(cloned.id(), second.id());
    assert_eq!(cloned.edges(), second.edges());
    assert_eq!(graph.to_dot().matches("0 -> 1").count(), 2);
    graph.validate().unwrap();
}

#[test]
fn validation_reports_cycles_after_edges_have_been_added() {
    for self_loop in [true, false] {
        let graph = Graph::new("cycles");
        let first = graph.add_task(increment);
        let second = graph.add_task(increment);
        if self_loop {
            let _loop = first.clone().depends_on(&first);
        } else {
            let _forward = second.clone().depends_on(&first);
            let _backward = first.depends_on(&second);
        }
        let GraphFlowError::CyclicDAG(cycle) = graph.validate().unwrap_err() else {
            panic!("expected a cycle error");
        };
        if self_loop {
            assert_eq!(cycle.node_id(), NodeIndex::new(0));
        } else {
            assert!(cycle.node_id().index() < 2);
        }
    }
}

#[test]
fn short_vectors_panic_without_poisoning_the_graph() {
    let graph = Graph::new("vector-cardinality");
    let first = graph.add_task(increment);
    let second = graph.add_task(increment);
    let collector = graph.add_task(|values: Vec<u32>| async move { values.len() });
    for deps in [vec![], vec![&first]] {
        assert!(catch_unwind(AssertUnwindSafe(|| collector.clone().depends_on(deps))).is_err());
        graph.validate().unwrap();
        assert!(!graph.to_dot().contains(" -> "));
    }
    let handle = collector.depends_on(vec![&first, &second]);
    assert_eq!(handle.edges(), [EdgeIndex::new(0), EdgeIndex::new(1)]);
    graph.validate().unwrap();
}

#[test]
fn foreign_handles_can_outlive_their_graph_and_connect_a_local_index() {
    let foreign: NodeHandle<u32, u32> = {
        let original = Graph::new("original");
        let entry = original.add_task(increment);
        original.add_task(increment).depends_on(&entry)
    };
    assert_eq!(foreign.id(), NodeIndex::new(1));

    let destination = Graph::new("destination");
    let _first = destination.add_node("local-first", task_fn(increment));
    let _second = destination.add_node("local-second", task_fn(increment));
    let target = destination.add_task(increment).depends_on(&foreign);
    assert_eq!(target.id(), NodeIndex::new(2));
    assert_eq!(target.edges(), [EdgeIndex::new(0)]);
    let dot = destination.to_dot();
    assert!(dot.contains("1 -> 2"));
    assert!(!dot.contains("0 -> 2"));
    destination.validate().unwrap();
}

#[test]
fn custom_dependency_indices_allow_empty_input_but_invalid_indices_poison_the_lock() {
    struct Indices(Vec<NodeIndex>);
    impl DepsCheck<u32> for Indices {
        fn to_node_indices(&self) -> Vec<NodeIndex> {
            self.0.clone()
        }
    }

    let graph = Graph::new("custom-deps");
    let entry = graph.add_task(increment).depends_on(Indices(vec![]));
    assert!(entry.edges().is_empty());
    let target = graph.add_task(increment);
    let deps = Indices(vec![entry.id(), NodeIndex::new(999)]);
    assert!(catch_unwind(AssertUnwindSafe(|| target.depends_on(deps))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| graph.validate())).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| graph.to_dot())).is_err());
}

#[test]
fn duplicate_names_keep_nodes_but_only_label_the_newest_and_labels_are_not_escaped() {
    let graph = Graph::new("labels");
    let _first = graph.add_node("same", task_fn(increment));
    let _second = graph.add_node("same", task_fn(increment));
    let _quoted = graph.add_node("unescaped\"label", task_fn(increment));
    let dot = graph.to_dot();
    let first = dot
        .lines()
        .find(|line| line.trim_start().starts_with("0 ["))
        .unwrap();
    let second = dot
        .lines()
        .find(|line| line.trim_start().starts_with("1 ["))
        .unwrap();
    assert!(first.contains("label=\"\""));
    assert!(second.contains("label=\"same\""));
    assert!(dot.contains("label=\"unescaped\"label\""));
    graph.validate().unwrap();
}
