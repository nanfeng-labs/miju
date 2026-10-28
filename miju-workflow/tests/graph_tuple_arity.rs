//! Tuple input failures return errors without running node handlers.

use std::{
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
    graph::{NodeInput, decode::GraphCodec, node::GraphNodeService},
    in_memory::InMemoryWorkflow,
};
use serde_json::{Value, json};
use tower::{ServiceExt, service_fn};

type BytesBackend = InMemoryWorkflow<u32>;
type ValueBackend = JsonStorage<u32>;
type Pair = (u32, u32);
type Twelve = (u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32);

fn byte_inputs(count: usize) -> Vec<u8> {
    serde_json::to_vec(&vec![b"1".to_vec(); count]).unwrap()
}

fn assert_bytes_arity_error<T: GraphCodec<BytesBackend>>(count: usize)
where
    T::Error: Debug,
{
    let compact = byte_inputs(count);
    let codec = JsonCodec::default();
    let result = catch_unwind(|| T::decode(&compact, &codec));
    assert!(
        result.is_ok(),
        "GraphCodec must return an error without unwinding for {count} inputs"
    );
    assert!(
        result.unwrap().is_err(),
        "wrong tuple length must be rejected"
    );
}

fn assert_value_arity_error<T: GraphCodec<ValueBackend>>(count: usize)
where
    T::Error: Debug,
{
    let compact = json!(vec![1_u32; count]);
    let codec = JsonCodec::<Value>::default();
    let result = catch_unwind(|| T::decode(&compact, &codec));
    assert!(
        result.is_ok(),
        "GraphCodec must return an error without unwinding for {count} inputs"
    );
    assert!(
        result.unwrap().is_err(),
        "wrong tuple length must be rejected"
    );
}

macro_rules! arity_case {
    ($name:ident, $tuple:ty, $count:expr, $check:ident) => {
        #[test]
        fn $name() {
            $check::<$tuple>($count);
        }
    };
}

arity_case!(
    bytes_pair_empty_returns_error,
    Pair,
    0,
    assert_bytes_arity_error
);
arity_case!(
    bytes_pair_short_returns_error,
    Pair,
    1,
    assert_bytes_arity_error
);
arity_case!(
    bytes_pair_long_returns_error,
    Pair,
    3,
    assert_bytes_arity_error
);
arity_case!(
    bytes_twelve_empty_returns_error,
    Twelve,
    0,
    assert_bytes_arity_error
);
arity_case!(
    bytes_twelve_short_returns_error,
    Twelve,
    11,
    assert_bytes_arity_error
);
arity_case!(
    bytes_twelve_long_returns_error,
    Twelve,
    13,
    assert_bytes_arity_error
);
arity_case!(
    value_pair_empty_returns_error,
    Pair,
    0,
    assert_value_arity_error
);
arity_case!(
    value_pair_short_returns_error,
    Pair,
    1,
    assert_value_arity_error
);
arity_case!(
    value_pair_long_returns_error,
    Pair,
    3,
    assert_value_arity_error
);
arity_case!(
    value_twelve_empty_returns_error,
    Twelve,
    0,
    assert_value_arity_error
);
arity_case!(
    value_twelve_short_returns_error,
    Twelve,
    11,
    assert_value_arity_error
);
arity_case!(
    value_twelve_long_returns_error,
    Twelve,
    13,
    assert_value_arity_error
);

#[test]
fn control_legal_tuple_encoding_and_decoding_keep_values_and_bytes() {
    let bytes = JsonCodec::default();
    let pair_wire = b"[[49],[50]]".to_vec();
    assert_eq!(
        <Pair as GraphCodec<BytesBackend>>::encode((1, 2), &bytes).unwrap(),
        pair_wire
    );
    assert_eq!(
        <Pair as GraphCodec<BytesBackend>>::decode(&pair_wire, &bytes).unwrap(),
        (1, 2)
    );

    let twelve = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    let twelve_wire =
        b"[[49],[50],[51],[52],[53],[54],[55],[56],[57],[49,48],[49,49],[49,50]]".to_vec();
    assert_eq!(
        <Twelve as GraphCodec<BytesBackend>>::encode(twelve, &bytes).unwrap(),
        twelve_wire
    );
    assert_eq!(
        <Twelve as GraphCodec<BytesBackend>>::decode(&twelve_wire, &bytes).unwrap(),
        twelve
    );

    let values = JsonCodec::<Value>::default();
    assert_eq!(
        <Pair as GraphCodec<ValueBackend>>::encode((1, 2), &values).unwrap(),
        json!([1, 2])
    );
    assert_eq!(
        <Pair as GraphCodec<ValueBackend>>::decode(&json!([1, 2]), &values).unwrap(),
        (1, 2)
    );
    assert_eq!(
        <Twelve as GraphCodec<ValueBackend>>::encode(twelve, &values).unwrap(),
        json!([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    );
    assert_eq!(
        <Twelve as GraphCodec<ValueBackend>>::decode(
            &json!([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            &values
        )
        .unwrap(),
        twelve
    );
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

#[test]
fn control_envelope_and_element_codec_errors_keep_their_sources() {
    let codec = JsonCodec::default();
    let malformed_envelope = b"]".to_vec();
    let result =
        catch_unwind(|| <Pair as GraphCodec<BytesBackend>>::decode(&malformed_envelope, &codec));
    let error = result
        .expect("an envelope codec error must not panic")
        .unwrap_err();
    assert!(json_error(&error).is_syntax());

    let wrong_first_element = b"[[116,114,117,101],[50]]".to_vec();
    let result =
        catch_unwind(|| <Pair as GraphCodec<BytesBackend>>::decode(&wrong_first_element, &codec));
    let error = result
        .expect("an element codec error must not panic")
        .unwrap_err();
    assert!(json_error(&error).is_data());

    let wrong_last_element = b"[[49],[116,114,117,101]]".to_vec();
    let error =
        <Pair as GraphCodec<BytesBackend>>::decode(&wrong_last_element, &codec).unwrap_err();
    assert!(json_error(&error).is_data());
}

fn input(compact: Vec<u8>) -> Task<NodeInput<Vec<u8>>> {
    TaskBuilder::new(NodeInput::Single(compact))
        .data(JsonCodec::<Vec<u8>>::default())
        .build()
}

macro_rules! node_arity_case {
    ($name:ident, $tuple:ty, $count:expr) => {
        #[tokio::test]
        async fn $name() {
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&calls);
            let handler = service_fn(move |_: Task<$tuple>| {
                observed.fetch_add(1, Ordering::SeqCst);
                ready(Ok::<_, io::Error>(7_u32))
            });
            let node = GraphNodeService::<_, BytesBackend, $tuple>::new(handler);
            let result = AssertUnwindSafe(node.oneshot(input(byte_inputs($count))))
                .catch_unwind()
                .await;
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "invalid tuple must not reach the handler"
            );
            assert!(
                result.is_ok(),
                "GraphNodeService must return an error without unwinding"
            );
            assert!(
                result.unwrap().is_err(),
                "invalid tuple must return an error"
            );
        }
    };
}

node_arity_case!(
    node_pair_short_returns_error_without_calling_handler,
    Pair,
    1
);
node_arity_case!(
    node_pair_long_returns_error_without_calling_handler,
    Pair,
    3
);
node_arity_case!(
    node_twelve_short_returns_error_without_calling_handler,
    Twelve,
    11
);
node_arity_case!(
    node_twelve_long_returns_error_without_calling_handler,
    Twelve,
    13
);

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
        let _original = json_error(error.as_ref());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let result = node.oneshot(input(b"[[49],[50]]".to_vec())).await.unwrap();
    assert_eq!(result, (b"3".to_vec(), json!(3)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
