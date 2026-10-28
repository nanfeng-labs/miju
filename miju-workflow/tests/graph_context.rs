//! Graph metadata, response serialization and error contracts.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
};

use miju_core::task::{
    metadata::{Metadata, MetadataError, MetadataStore},
    task_id::TaskId,
};
use miju_workflow::graph::{
    GraphFlowContext,
    context::GraphFlowContextError,
    error::{GraphFlowError, GraphServiceError},
    response::GraphNodeResponse,
};
use petgraph::graph::NodeIndex;
use serde_json::json;

fn assert_context_eq(actual: &GraphFlowContext, expected: &GraphFlowContext) {
    assert_eq!(actual.prev_node, expected.prev_node);
    assert_eq!(actual.current_node, expected.current_node);
    assert_eq!(actual.completed_nodes, expected.completed_nodes);
    assert_eq!(actual.node_task_ids, expected.node_task_ids);
    assert_eq!(actual.current_position, expected.current_position);
    assert_eq!(actual.is_initial, expected.is_initial);
    assert_eq!(actual.root_task_id, expected.root_task_id);
}

fn required_metadata() -> MetadataStore {
    let mut metadata = MetadataStore::new();
    metadata
        .insert("miju_workflow.graph.current_node", "2")
        .unwrap();
    metadata
        .insert("miju_workflow.graph.current_position", "1")
        .unwrap();
    metadata
}

#[test]
fn graph_progress_round_trips_through_metadata_and_json() {
    let context = GraphFlowContext {
        prev_node: Some(NodeIndex::new(1)),
        current_node: NodeIndex::new(2),
        completed_nodes: HashSet::from([NodeIndex::new(0), NodeIndex::new(1)]),
        node_task_ids: HashMap::from([
            (NodeIndex::new(0), TaskId::from_int(17)),
            (NodeIndex::new(1), TaskId::from_string("task-one")),
        ]),
        current_position: 2,
        is_initial: false,
        root_task_id: Some(TaskId::from_string("root")),
    };
    let mut metadata = MetadataStore::new();
    context.inject(&mut metadata).unwrap();
    assert_context_eq(&GraphFlowContext::extract(&metadata).unwrap(), &context);
    let json = serde_json::to_vec(&context).unwrap();
    let decoded: GraphFlowContext = serde_json::from_slice(&json).unwrap();
    assert_context_eq(&decoded, &context);
}

#[test]
fn dependency_lookup_omits_unknown_nodes_and_clones_are_independent() {
    let mut original = GraphFlowContext::new(Some(TaskId::from_string("root")));
    original.completed_nodes.insert(NodeIndex::new(0));
    original.node_task_ids.extend([
        (NodeIndex::new(0), TaskId::from_int(10)),
        (NodeIndex::new(1), TaskId::from_int(11)),
    ]);
    let mut cloned = original.clone();
    assert_context_eq(&cloned, &original);
    let mut ids =
        cloned.get_dependency_task_ids(&[NodeIndex::new(1), NodeIndex::new(1), NodeIndex::new(9)]);
    assert_eq!(
        ids,
        HashMap::from([(NodeIndex::new(1), TaskId::from_int(11))])
    );
    assert!(!cloned.completed_nodes.contains(&NodeIndex::new(1)));
    ids.clear();
    cloned.node_task_ids.clear();
    cloned.completed_nodes.clear();
    cloned.root_task_id = None;
    assert_eq!(original.node_task_ids.len(), 2);
    assert_eq!(original.completed_nodes.len(), 1);
    assert!(original.root_task_id.is_some());
}

#[test]
fn construction_and_optional_metadata_preserve_current_initial_flag_defaults() {
    let initial = GraphFlowContext::new(None);
    assert_eq!(initial.current_node, NodeIndex::new(0));
    assert_eq!(initial.current_position, 0);
    assert!(initial.is_initial);
    assert!(!GraphFlowContext::default().is_initial);
    let mut expected = initial;
    expected.current_node = NodeIndex::new(2);
    expected.current_position = 1;
    assert_context_eq(
        &GraphFlowContext::extract(&required_metadata()).unwrap(),
        &expected,
    );
    for value in ["true", "invalid", "", "FALSE"] {
        let mut metadata = required_metadata();
        metadata
            .insert("miju_workflow.graph.is_initial", value)
            .unwrap();
        assert!(GraphFlowContext::extract(&metadata).unwrap().is_initial);
    }
    let mut metadata = required_metadata();
    metadata
        .insert("miju_workflow.graph.is_initial", "false")
        .unwrap();
    assert!(!GraphFlowContext::extract(&metadata).unwrap().is_initial);
}

#[test]
fn missing_required_fields_and_invalid_indices_or_task_ids_are_reported() {
    let mut missing = MetadataStore::new();
    assert!(matches!(
        GraphFlowContext::extract(&missing),
        Err(GraphFlowContextError::MissingCurrentNode)
    ));
    missing
        .insert("miju_workflow.graph.current_node", "0")
        .unwrap();
    assert!(matches!(
        GraphFlowContext::extract(&missing),
        Err(GraphFlowContextError::MissingCurrentPosition)
    ));
    for (key, value) in [
        ("miju_workflow.graph.current_node", "invalid"),
        ("miju_workflow.graph.current_position", "-1"),
        ("miju_workflow.graph.prev_node", "invalid"),
        ("miju_workflow.graph.completed_nodes", "0,no-node"),
        ("miju_workflow.graph.node_task_ids", "invalid=task"),
    ] {
        let mut metadata = required_metadata();
        metadata.as_mut().insert(key.to_owned(), value.to_owned());
        assert!(
            matches!(
                GraphFlowContext::extract(&metadata),
                Err(GraphFlowContextError::ParseNodeIndex(_))
            ),
            "{key}"
        );
    }
    for (key, value) in [
        ("miju_workflow.graph.node_task_ids", "no-delimiter"),
        ("miju_workflow.graph.node_task_ids", "0="),
        ("miju_workflow.graph.root_task_id", ""),
    ] {
        let mut metadata = required_metadata();
        metadata.insert(key, value).unwrap();
        assert!(
            matches!(
                GraphFlowContext::extract(&metadata),
                Err(GraphFlowContextError::ParseTaskId(_))
            ),
            "{key}"
        );
    }
}

#[test]
fn node_indices_reject_values_above_their_storage_range() {
    for (key, value) in [
        ("miju_workflow.graph.current_node", "4294967296"),
        ("miju_workflow.graph.prev_node", "4294967296"),
        ("miju_workflow.graph.completed_nodes", "0,4294967296"),
        ("miju_workflow.graph.node_task_ids", "4294967296=task"),
    ] {
        let mut metadata = required_metadata();
        metadata.replace(key, value);
        assert!(
            matches!(
                GraphFlowContext::extract(&metadata),
                Err(GraphFlowContextError::ParseNodeIndex(_))
            ),
            "{key} must not alias a different node"
        );
    }
}

#[test]
fn largest_node_index_and_host_sized_position_round_trip() {
    let context = GraphFlowContext {
        prev_node: Some(NodeIndex::from(u32::MAX)),
        current_node: NodeIndex::from(u32::MAX),
        completed_nodes: HashSet::from([NodeIndex::from(u32::MAX)]),
        node_task_ids: HashMap::from([(NodeIndex::from(u32::MAX), TaskId::from_int(42))]),
        current_position: usize::MAX,
        ..GraphFlowContext::default()
    };
    let mut metadata = MetadataStore::new();
    context.inject(&mut metadata).unwrap();
    assert_context_eq(&GraphFlowContext::extract(&metadata).unwrap(), &context);
}

#[test]
fn duplicate_keys_return_errors_without_overwriting_existing_values() {
    let context = GraphFlowContext::new(Some(TaskId::from_string("root")));
    let mut metadata = MetadataStore::new();
    context.inject(&mut metadata).unwrap();
    assert!(matches!(
        context.inject(&mut metadata),
        Err(GraphFlowContextError::DuplicateEntry(_))
    ));
    assert_context_eq(&GraphFlowContext::extract(&metadata).unwrap(), &context);

    let mut metadata = MetadataStore::new();
    metadata
        .insert("miju_workflow.graph.is_initial", "false")
        .unwrap();
    assert!(matches!(
        context.inject(&mut metadata),
        Err(GraphFlowContextError::DuplicateEntry(MetadataError::DuplicateKey(key)))
            if key == "miju_workflow.graph.is_initial"
    ));
    assert_eq!(
        metadata.get("miju_workflow.graph.is_initial"),
        Some("false")
    );
    assert_eq!(metadata.get("miju_workflow.graph.current_node"), Some("0"));
    assert_eq!(
        metadata.get("miju_workflow.graph.current_position"),
        Some("0")
    );
    assert!(metadata.get("miju_workflow.graph.root_task_id").is_none());
}

#[test]
fn graph_responses_preserve_variant_payloads_and_node_task_mappings() {
    let ids = HashMap::from([
        (NodeIndex::new(1), TaskId::from_int(17)),
        (NodeIndex::new(2), TaskId::from_string("task-two")),
    ]);
    let responses = [
        GraphNodeResponse::EntryFanOut {
            node_task_ids: ids.clone(),
        },
        GraphNodeResponse::FanOut {
            response: json!({"items": [1, 2]}),
            node_task_ids: ids.clone(),
        },
        GraphNodeResponse::EnqueuedNext { result: json!(7) },
        GraphNodeResponse::WaitingForDependencies {
            pending_dependencies: ids,
        },
        GraphNodeResponse::Complete {
            result: json!("finished"),
        },
    ];
    for response in responses {
        let encoded = serde_json::to_vec(&response).unwrap();
        let decoded: GraphNodeResponse = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, response);
    }
    let response: GraphNodeResponse =
        serde_json::from_value(json!({"EnqueuedNext": {"result": 7}})).unwrap();
    assert_eq!(
        response,
        GraphNodeResponse::EnqueuedNext { result: json!(7) }
    );
    assert!(serde_json::from_value::<GraphNodeResponse>(json!({"Unknown": {}})).is_err());
}

#[test]
fn graph_errors_preserve_wrapped_sources_and_context_conversions() {
    let service = GraphServiceError::Codec(std::io::Error::other("decode failed").into());
    let flow = GraphFlowError::from(service);
    assert!(matches!(&flow, GraphFlowError::Service(_)));
    let source = flow.source().unwrap();
    assert!(source.is::<GraphServiceError>());
    let cause = source
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.to_string(), "decode failed");

    let metadata = GraphFlowError::from(GraphFlowContextError::MissingCurrentNode);
    assert!(matches!(&metadata, GraphFlowError::MetadataError(_)));
    assert!(metadata.source().unwrap().is::<GraphFlowContextError>());
}
