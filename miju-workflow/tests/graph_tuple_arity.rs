//! Tuple input failures return errors without running node handlers.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::Debug,
    future::ready,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::FutureExt;
use miju_codec::json::JsonCodec;
use miju_core::task::{Task, builder::TaskBuilder};
use miju_file_storage::JsonStorage;
use miju_workflow::{
    graph::{NodeInput, decode::GraphCodec, error::GraphFlowError, node::GraphNodeService},
    in_memory::InMemoryWorkflow,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tower::{ServiceExt, service_fn};

type BytesBackend = InMemoryWorkflow<u32>;
type ValueBackend = JsonStorage<u32>;
type Pair = (u32, u32);
type Tuple3 = (u32, u32, u32);
type Tuple4 = (u32, u32, u32, u32);
type Tuple5 = (u32, u32, u32, u32, u32);
type Tuple6 = (u32, u32, u32, u32, u32, u32);
type Tuple7 = (u32, u32, u32, u32, u32, u32, u32);
type Tuple8 = (u32, u32, u32, u32, u32, u32, u32, u32);
type Tuple9 = (u32, u32, u32, u32, u32, u32, u32, u32, u32);
type Tuple10 = (u32, u32, u32, u32, u32, u32, u32, u32, u32, u32);
type Tuple11 = (u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32);
type Twelve = (u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32);

macro_rules! for_each_tuple {
    ($check:ident) => {
        $check!(Pair, 2, (1_u32, 2_u32));
        $check!(Tuple3, 3, (1_u32, 2_u32, 3_u32));
        $check!(Tuple4, 4, (1_u32, 2_u32, 3_u32, 4_u32));
        $check!(Tuple5, 5, (1_u32, 2_u32, 3_u32, 4_u32, 5_u32));
        $check!(Tuple6, 6, (1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32));
        $check!(Tuple7, 7, (1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32));
        $check!(
            Tuple8,
            8,
            (1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32)
        );
        $check!(
            Tuple9,
            9,
            (
                1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32, 9_u32
            )
        );
        $check!(
            Tuple10,
            10,
            (
                1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32, 9_u32, 10_u32
            )
        );
        $check!(
            Tuple11,
            11,
            (
                1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32, 9_u32, 10_u32, 11_u32
            )
        );
        $check!(
            Twelve,
            12,
            (
                1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32, 9_u32, 10_u32, 11_u32,
                12_u32
            )
        );
    };
}

fn byte_inputs(count: usize) -> Vec<u8> {
    let mut values = vec![b"1".to_vec(); count];
    if let Some(first) = values.first_mut() {
        *first = b"]".to_vec();
    }
    serde_json::to_vec(&values).unwrap()
}

fn assert_count_error(error: &GraphFlowError, expected: usize, actual: usize) {
    match error {
        GraphFlowError::InputCountMismatch {
            expected: found_expected,
            actual: found_actual,
        } => assert_eq!((*found_expected, *found_actual), (expected, actual)),
        other => panic!("expected input-count mismatch, got {other:?}"),
    }
}

fn assert_bytes_arity_error<T: GraphCodec<BytesBackend, Error = GraphFlowError>>(
    count: usize,
    expected: usize,
) {
    let compact = byte_inputs(count);
    let codec = JsonCodec::default();
    let result = catch_unwind(|| T::decode(&compact, &codec));
    let Err(error) = result.expect("wrong tuple lengths must return rather than unwind") else {
        panic!("wrong tuple lengths must be rejected");
    };
    assert_count_error(&error, expected, count);
}

fn assert_value_arity_error<T: GraphCodec<ValueBackend, Error = GraphFlowError>>(
    count: usize,
    expected: usize,
) {
    let mut values = vec![json!(1_u32); count];
    if let Some(first) = values.first_mut() {
        *first = json!(true);
    }
    let compact = Value::Array(values);
    let codec = JsonCodec::<Value>::default();
    let result = catch_unwind(|| T::decode(&compact, &codec));
    let Err(error) = result.expect("wrong tuple lengths must return rather than unwind") else {
        panic!("wrong tuple lengths must be rejected");
    };
    assert_count_error(&error, expected, count);
}

#[test]
fn every_tuple_arity_rejects_each_wrong_length_before_decoding_elements() {
    macro_rules! check {
        ($tuple:ty, $arity:expr, $value:expr) => {{
            for actual in 0..=14 {
                if actual != $arity {
                    assert_bytes_arity_error::<$tuple>(actual, $arity);
                    assert_value_arity_error::<$tuple>(actual, $arity);
                }
            }
        }};
    }
    for_each_tuple!(check);
}

fn assert_tuple_round_trip<T>(expected: T, arity: usize)
where
    T: Copy
        + Debug
        + PartialEq
        + GraphCodec<BytesBackend, Error = GraphFlowError>
        + GraphCodec<ValueBackend, Error = GraphFlowError>,
{
    let bytes = JsonCodec::default();
    let byte_wire = <T as GraphCodec<BytesBackend>>::encode(expected, &bytes).unwrap();
    let encoded_elements: Vec<Vec<u8>> = (1..=arity)
        .map(|value| serde_json::to_vec(&value).unwrap())
        .collect();
    assert_eq!(byte_wire, serde_json::to_vec(&encoded_elements).unwrap());
    assert_eq!(
        <T as GraphCodec<BytesBackend>>::decode(&byte_wire, &bytes).unwrap(),
        expected
    );

    let values = JsonCodec::<Value>::default();
    let value_wire = <T as GraphCodec<ValueBackend>>::encode(expected, &values).unwrap();
    let expected_value = Value::Array((1..=arity).map(|value| json!(value)).collect());
    assert_eq!(value_wire, expected_value);
    assert_eq!(
        <T as GraphCodec<ValueBackend>>::decode(&value_wire, &values).unwrap(),
        expected
    );
}

#[test]
fn all_tuple_arities_preserve_the_existing_wire_layout_and_round_trip() {
    macro_rules! check {
        ($tuple:ty, $arity:expr, $value:expr) => {
            assert_tuple_round_trip::<$tuple>($value, $arity);
        };
    }
    for_each_tuple!(check);
}

fn json_error<'a>(mut error: &'a (dyn Error + 'static)) -> &'a serde_json::Error {
    loop {
        if let Some(json) = error.downcast_ref::<serde_json::Error>() {
            return json;
        }
        error = error
            .source()
            .expect("the original JSON error must remain in the source chain");
    }
}

fn assert_wrapped_codec_error(error: &GraphFlowError) -> &serde_json::Error {
    let GraphFlowError::Codec(source) = error else {
        panic!("expected GraphFlowError::Codec, got {error:?}");
    };
    json_error(source.as_ref())
}

#[test]
fn control_envelope_and_element_codec_errors_keep_their_sources() {
    let codec = JsonCodec::default();
    let malformed_envelope = b"]".to_vec();
    let result =
        catch_unwind(|| <Pair as GraphCodec<BytesBackend>>::decode(&malformed_envelope, &codec));
    let error = result
        .expect("an envelope codec error must not panic")
        .unwrap_err();
    assert!(assert_wrapped_codec_error(&error).is_syntax());

    let wrong_first_element = b"[[116,114,117,101],[50]]".to_vec();
    let result =
        catch_unwind(|| <Pair as GraphCodec<BytesBackend>>::decode(&wrong_first_element, &codec));
    let error = result
        .expect("an element codec error must not panic")
        .unwrap_err();
    assert!(assert_wrapped_codec_error(&error).is_data());

    let wrong_last_element = b"[[49],[116,114,117,101]]".to_vec();
    let error =
        <Pair as GraphCodec<BytesBackend>>::decode(&wrong_last_element, &codec).unwrap_err();
    assert!(assert_wrapped_codec_error(&error).is_data());
}

fn assert_bytes_element_error<T: GraphCodec<BytesBackend, Error = GraphFlowError> + Debug>(
    arity: usize,
    invalid_index: usize,
) {
    let mut values = vec![b"1".to_vec(); arity];
    values[invalid_index] = b"true".to_vec();
    let compact = serde_json::to_vec(&values).unwrap();
    let error = T::decode(&compact, &JsonCodec::default()).unwrap_err();
    assert!(assert_wrapped_codec_error(&error).is_data());
}

fn assert_value_element_error<T: GraphCodec<ValueBackend, Error = GraphFlowError> + Debug>(
    arity: usize,
    invalid_index: usize,
) {
    let mut values = vec![json!(1_u32); arity];
    values[invalid_index] = json!(true);
    let error = T::decode(&Value::Array(values), &JsonCodec::<Value>::default()).unwrap_err();
    assert!(assert_wrapped_codec_error(&error).is_data());
}

#[test]
fn every_tuple_arity_preserves_element_errors_at_every_position() {
    macro_rules! check {
        ($tuple:ty, $arity:expr, $value:expr) => {{
            for invalid_index in 0..$arity {
                assert_bytes_element_error::<$tuple>($arity, invalid_index);
                assert_value_element_error::<$tuple>($arity, invalid_index);
            }
        }};
    }
    for_each_tuple!(check);
}

#[test]
fn singleton_tuple_keeps_the_original_codec_error_type() {
    let codec = JsonCodec::default();
    let error: serde_json::Error =
        <(u32,) as GraphCodec<BytesBackend>>::decode(&b"]".to_vec(), &codec).unwrap_err();
    assert!(error.is_syntax());
}

#[test]
fn tuple_encode_errors_keep_original_codec_sources_for_each_position() {
    type InvalidMap = BTreeMap<Vec<u8>, u32>;
    let invalid = BTreeMap::from([(vec![1_u8], 2_u32)]);
    let codec = JsonCodec::default();

    let first_error =
        <(InvalidMap, u32) as GraphCodec<BytesBackend>>::encode((invalid.clone(), 3), &codec)
            .unwrap_err();
    assert!(
        assert_wrapped_codec_error(&first_error)
            .to_string()
            .contains("key must be a string")
    );

    let last_error =
        <(u32, InvalidMap) as GraphCodec<BytesBackend>>::encode((3, invalid), &codec).unwrap_err();
    assert!(
        assert_wrapped_codec_error(&last_error)
            .to_string()
            .contains("key must be a string")
    );
}

fn input(compact: Vec<u8>) -> Task<NodeInput<Vec<u8>>> {
    TaskBuilder::new(NodeInput::Single(compact))
        .data(JsonCodec::<Vec<u8>>::default())
        .build()
}

async fn assert_node_arity_error<T>(actual: usize, expected: usize)
where
    T: GraphCodec<BytesBackend, Error = GraphFlowError>
        + DeserializeOwned
        + Serialize
        + Send
        + Sync
        + 'static,
{
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let handler = service_fn(move |_: Task<T>| {
        observed.fetch_add(1, Ordering::SeqCst);
        ready(Ok::<_, io::Error>(7_u32))
    });
    let node = GraphNodeService::<_, BytesBackend, T>::new(handler);
    let result = AssertUnwindSafe(node.oneshot(input(byte_inputs(actual))))
        .catch_unwind()
        .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "invalid tuple must not reach the handler"
    );
    let error = result
        .expect("GraphNodeService must return an error without unwinding")
        .expect_err("invalid tuple must return an error");
    let graph_error = error
        .downcast_ref::<GraphFlowError>()
        .expect("GraphFlowError must remain the public error wrapper");
    assert_count_error(graph_error, expected, actual);
}

#[tokio::test]
async fn every_tuple_arity_rejects_wrong_node_inputs_before_handler_call() {
    macro_rules! check {
        ($tuple:ty, $arity:expr, $value:expr) => {{
            for actual in 0..=14 {
                if actual != $arity {
                    assert_node_arity_error::<$tuple>(actual, $arity).await;
                }
            }
        }};
    }
    for_each_tuple!(check);
}

#[tokio::test]
async fn control_node_codec_errors_skip_handler_and_legal_tuple_runs_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let handler = service_fn(move |task: Task<Pair>| {
        observed.fetch_add(1, Ordering::SeqCst);
        let ((first, second), _) = task.take();
        ready(Ok::<_, io::Error>(first + second))
    });
    let node = GraphNodeService::<_, BytesBackend, Pair>::new(handler);
    for compact in [b"]".to_vec(), b"[[49],[116,114,117,101]]".to_vec()] {
        let result = AssertUnwindSafe(node.clone().oneshot(input(compact)))
            .catch_unwind()
            .await;
        let error = result.expect("codec failures must not panic").unwrap_err();
        let graph_error = error
            .downcast_ref::<GraphFlowError>()
            .expect("GraphFlowError must remain the public error wrapper");
        let _original = assert_wrapped_codec_error(graph_error);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let result = node.oneshot(input(b"[[49],[50]]".to_vec())).await.unwrap();
    assert_eq!(result, (b"3".to_vec(), json!(3)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
