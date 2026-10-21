//! JSON codecs for byte buffers, strings, and structured values.
//!
//! ```rust
//! use miju_codec::json::JsonCodec;
//! use miju_core::backend::codec::Codec;
//!
//! let codec = JsonCodec::<Vec<u8>>::default();
//! let bytes = codec.encode(&vec![1_u32, 2, 3])?;
//! let decoded: Vec<u32> = codec.decode(&bytes)?;
//! assert_eq!(decoded, vec![1, 2, 3]);
//! # Ok::<(), serde_json::Error>(())
//! ```

use std::marker::PhantomData;

use miju_core::backend::codec::Codec;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Json encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct JsonCodec<Output = Vec<u8>> {
    _o: PhantomData<Output>,
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for JsonCodec<Vec<u8>> {
    type Compact = Vec<u8>;
    type Error = serde_json::Error;
    fn encode(&self, input: &T) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(input)
    }

    fn decode(&self, compact: &Vec<u8>) -> Result<T, Self::Error> {
        serde_json::from_slice(compact)
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for JsonCodec<String> {
    type Compact = String;
    type Error = serde_json::Error;
    fn encode(&self, input: &T) -> Result<String, Self::Error> {
        serde_json::to_string(input)
    }
    fn decode(&self, compact: &String) -> Result<T, Self::Error> {
        serde_json::from_str(compact)
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for JsonCodec<Value> {
    type Compact = Value;
    type Error = serde_json::Error;
    fn encode(&self, input: &T) -> Result<Value, Self::Error> {
        serde_json::to_value(input)
    }

    fn decode(&self, compact: &Value) -> Result<T, Self::Error> {
        T::deserialize(compact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        id: u32,
        name: String,
    }

    #[test]
    fn test_json_codec_vec_u8_roundtrip() {
        let original = TestStruct {
            id: 1,
            name: "Test".to_owned(),
        };
        let codec = JsonCodec::<Vec<u8>>::default();
        let encoded = codec.encode(&original).unwrap();
        let decoded: TestStruct = codec.decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_json_codec_string_roundtrip() {
        let original = TestStruct {
            id: 2,
            name: "Example".to_owned(),
        };
        let codec = JsonCodec::<String>::default();
        let encoded = codec.encode(&original).unwrap();
        let decoded: TestStruct = codec.decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_json_codec_value_roundtrip() {
        let original = TestStruct {
            id: 3,
            name: "Sample".to_owned(),
        };
        let codec = JsonCodec::<Value>::default();
        let encoded = codec.encode(&original).unwrap();
        let decoded: TestStruct = codec.decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_invalid_input() {
        let bytes: Result<TestStruct, _> = JsonCodec::<Vec<u8>>::default().decode(&b"]".to_vec());
        let text: Result<TestStruct, _> = JsonCodec::<String>::default().decode(&"]".to_owned());
        let value: Result<TestStruct, _> = JsonCodec::<Value>::default().decode(&Value::Bool(true));
        assert!(bytes.unwrap_err().is_syntax());
        assert!(text.unwrap_err().is_syntax());
        assert!(value.unwrap_err().is_data());
    }

    #[test]
    fn test_encode_invalid_map_key() {
        let invalid = std::collections::BTreeMap::from([(vec![1_u8], 2_u8)]);
        assert!(JsonCodec::<Vec<u8>>::default().encode(&invalid).is_err());
        assert!(JsonCodec::<String>::default().encode(&invalid).is_err());
        assert!(JsonCodec::<Value>::default().encode(&invalid).is_err());
    }
}
