//! MessagePack encoding through Serde, with structs encoded as positional arrays.
//!
//! ```rust
//! use miju_codec::msgpack::{MsgPackCodec, MsgPackCodecError};
//! use miju_core::backend::codec::Codec;
//!
//! let bytes = MsgPackCodec.encode(&vec![1_u32, 2, 3])?;
//! let decoded: Vec<u32> = MsgPackCodec.decode(&bytes)?;
//! assert_eq!(decoded, vec![1, 2, 3]);
//! # Ok::<(), MsgPackCodecError>(())
//! ```

use miju_core::backend::codec::Codec;
use serde::{Deserialize, Serialize};

/// MsgPack encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct MsgPackCodec;

/// Errors that can occur during MsgPack encoding/decoding
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum MsgPackCodecError {
    /// Error during encoding
    #[error("Encoding error: {0}")]
    EncodeError(#[from] rmp_serde::encode::Error),
    /// Error during decoding
    #[error("Decoding error: {0}")]
    DecodeError(#[from] rmp_serde::decode::Error),
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for MsgPackCodec {
    type Compact = Vec<u8>;
    type Error = MsgPackCodecError;
    fn encode(&self, input: &T) -> Result<Vec<u8>, Self::Error> {
        Ok(rmp_serde::to_vec(input)?)
    }

    fn decode(&self, compact: &Vec<u8>) -> Result<T, Self::Error> {
        Ok(rmp_serde::from_slice(compact)?)
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
    fn test_encode_decode_roundtrip() {
        let original = TestStruct {
            id: 1,
            name: "Test".to_owned(),
        };

        // Encode the original struct
        let encoded = MsgPackCodec.encode(&original).expect("Encoding failed");
        assert_eq!(encoded, vec![0x92, 0x01, 0xa4, b'T', b'e', b's', b't']);

        // Decode back to struct
        let decoded: TestStruct = MsgPackCodec.decode(&encoded).expect("Decoding failed");

        // Assert that the original and decoded structs are equal
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_invalid_input() {
        let decoded: Result<TestStruct, _> = MsgPackCodec.decode(&vec![]);
        assert!(matches!(decoded, Err(MsgPackCodecError::DecodeError(_))));
    }

    #[test]
    fn test_encode_error() {
        #[derive(Deserialize)]
        struct FailingValue;

        impl Serialize for FailingValue {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("encoding rejected"))
            }
        }

        assert!(matches!(
            MsgPackCodec.encode(&FailingValue),
            Err(MsgPackCodecError::EncodeError(_))
        ));
    }
}
