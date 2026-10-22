//! Graph input wire layouts with byte and structured JSON backends.

use std::{collections::BTreeMap, fmt::Debug, panic::catch_unwind};

use miju_codec::json::JsonCodec;
use miju_core::backend::codec::Codec;
use miju_file_storage::JsonStorage;
use miju_workflow::{graph::decode::GraphCodec, in_memory::InMemoryWorkflow};
use serde_json::{Value, json};

type BytesBackend = InMemoryWorkflow<u32>;
type ValueBackend = JsonStorage<u32>;

fn assert_bytes_round_trip<T>(value: T)
where
    T: GraphCodec<BytesBackend, Error = serde_json::Error> + Clone + PartialEq + Debug,
{
    let codec = JsonCodec::default();
    let expected = value.clone();
    let encoded = <T as GraphCodec<BytesBackend>>::encode(value, &codec).unwrap();
    let decoded = <T as GraphCodec<BytesBackend>>::decode(&encoded, &codec).unwrap();
    assert_eq!(decoded, expected);
}

#[test]
fn scalar_and_wrapped_values_use_the_backend_representation() {
    assert_bytes_round_trip("ready".to_owned());
    assert_bytes_round_trip(true);
    assert_bytes_round_trip('界');
    assert_bytes_round_trip(());
    assert_bytes_round_trip(u128::MAX);
    assert_bytes_round_trip(i128::MIN);
    assert_bytes_round_trip(1.25_f64);
    assert_bytes_round_trip(Some(vec![3_u32, 7]));
    assert_bytes_round_trip(None::<u32>);
    assert_bytes_round_trip(Ok::<u32, String>(7));
    assert_bytes_round_trip(Err::<u32, String>("failed".to_owned()));

    let codec = JsonCodec::default();
    let encoded =
        <Option<Vec<u32>> as GraphCodec<BytesBackend>>::encode(Some(vec![3, 7]), &codec).unwrap();
    assert_eq!(encoded, b"[3,7]");
}

#[test]
fn multi_element_tuples_encode_each_element_and_round_trip() {
    assert_bytes_round_trip((7_u32, "ready".to_owned()));
    assert_bytes_round_trip((7_u32, false, Some(3_u32)));
    assert_bytes_round_trip((
        1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 7_u8, 8_u8, 9_u8, 10_u8, 11_u8, 12_u8,
    ));

    type Pair = (u32, String);
    let codec = JsonCodec::default();
    let encoded =
        <Pair as GraphCodec<BytesBackend>>::encode((7, "ready".to_owned()), &codec).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&encoded).unwrap(),
        json!([b"7".to_vec(), b"\"ready\"".to_vec()])
    );

    let codec = JsonCodec::<Value>::default();
    let encoded =
        <Pair as GraphCodec<ValueBackend>>::encode((7, "ready".to_owned()), &codec).unwrap();
    assert_eq!(encoded, json!([7, "ready"]));
    assert_eq!(
        <Pair as GraphCodec<ValueBackend>>::decode(&encoded, &codec).unwrap(),
        (7, "ready".to_owned())
    );
}

#[test]
fn vector_and_singleton_layouts_preserve_their_current_asymmetry() {
    let bytes = JsonCodec::default();
    let encoded = <Vec<u32> as GraphCodec<BytesBackend>>::encode(vec![1, 2], &bytes).unwrap();
    assert_eq!(encoded, b"[[49],[50]]");
    assert!(<Vec<u32> as GraphCodec<BytesBackend>>::decode(&encoded, &bytes).is_err());
    assert_eq!(
        <Vec<u32> as GraphCodec<BytesBackend>>::decode(&b"[1,2]".to_vec(), &bytes).unwrap(),
        [1, 2]
    );
    assert_bytes_round_trip(Vec::<u32>::new());

    let encoded = <(u32,) as GraphCodec<BytesBackend>>::encode((7,), &bytes).unwrap();
    assert_eq!(encoded, b"[[91,55,93]]");
    assert!(<(u32,) as GraphCodec<BytesBackend>>::decode(&encoded, &bytes).is_err());
    assert_eq!(
        <(u32,) as GraphCodec<BytesBackend>>::decode(&b"[7]".to_vec(), &bytes).unwrap(),
        (7,)
    );

    let values = JsonCodec::<Value>::default();
    let encoded = <Vec<u32> as GraphCodec<ValueBackend>>::encode(vec![1, 2], &values).unwrap();
    assert_eq!(encoded, json!([1, 2]));
    assert_eq!(
        <Vec<u32> as GraphCodec<ValueBackend>>::decode(&encoded, &values).unwrap(),
        [1, 2]
    );
    let encoded = <(u32,) as GraphCodec<ValueBackend>>::encode((7,), &values).unwrap();
    assert_eq!(encoded, json!([[7]]));
    assert!(<(u32,) as GraphCodec<ValueBackend>>::decode(&encoded, &values).is_err());
}

#[test]
fn codec_errors_from_elements_and_envelopes_are_returned() {
    let codec = JsonCodec::default();
    type InvalidMap = BTreeMap<Vec<u8>, u32>;
    let invalid = BTreeMap::from([(vec![1_u8], 2_u32)]);
    let error =
        <Vec<InvalidMap> as GraphCodec<BytesBackend>>::encode(vec![invalid], &codec).unwrap_err();
    assert!(error.to_string().contains("key must be a string"));

    type Pair = (u32, String);
    assert!(
        <Pair as GraphCodec<BytesBackend>>::decode(&b"]".to_vec(), &codec)
            .unwrap_err()
            .is_syntax()
    );
    let encoded = Codec::encode(
        &codec,
        &vec![b"\"wrong type\"".to_vec(), b"\"name\"".to_vec()],
    )
    .unwrap();
    assert!(
        <Pair as GraphCodec<BytesBackend>>::decode(&encoded, &codec)
            .unwrap_err()
            .is_data()
    );
}

#[test]
fn tuple_length_mismatches_currently_panic() {
    let codec = JsonCodec::default();
    for count in [0, 1, 3] {
        let encoded = Codec::encode(&codec, &vec![b"1".to_vec(); count]).unwrap();
        let panic =
            catch_unwind(|| <(u32, u32) as GraphCodec<BytesBackend>>::decode(&encoded, &codec))
                .unwrap_err();
        assert_eq!(
            panic.downcast_ref::<String>().unwrap(),
            &format!("Expected 2 elements, got {count}")
        );
    }
}
