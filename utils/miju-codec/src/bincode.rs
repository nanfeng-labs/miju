//! Binary encoding with Bincode's native traits and standard configuration.
//!
//! Decoding returns the first value and accepts trailing bytes.
//!
//! ```rust
//! use miju_codec::bincode::{BincodeCodec, BincodeCodecError};
//! use miju_core::backend::codec::Codec;
//!
//! let bytes = BincodeCodec.encode(&42_u32)?;
//! let decoded: u32 = BincodeCodec.decode(&bytes)?;
//! assert_eq!(decoded, 42);
//! # Ok::<(), BincodeCodecError>(())
//! ```

use bincode::{Decode, Encode};
use miju_core::backend::codec::Codec;

/// Bincode encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct BincodeCodec;

/// Errors that can occur during Bincode encoding/decoding
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum BincodeCodecError {
    /// Error during encoding
    #[error("Encoding error: {0}")]
    EncodeError(#[from] bincode::error::EncodeError),
    /// Error during decoding
    #[error("Decoding error: {0}")]
    DecodeError(#[from] bincode::error::DecodeError),
}

impl<T: Encode + Decode<()>> Codec<T> for BincodeCodec {
    type Compact = Vec<u8>;
    type Error = BincodeCodecError;
    fn encode(&self, input: &T) -> Result<Vec<u8>, Self::Error> {
        let config = bincode::config::standard();
        Ok(bincode::encode_to_vec(input, config)?)
    }

    fn decode(&self, compact: &Vec<u8>) -> Result<T, Self::Error> {
        let config = bincode::config::standard();
        Ok(bincode::decode_from_slice(compact, config)?.0)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Encode, Decode, Debug, PartialEq)]
    struct TestData {
        id: u32,
        name: String,
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = TestData {
            id: 42,
            name: "test".to_owned(),
        };

        let encoded = BincodeCodec.encode(&original).expect("encoding failed");
        let decoded: TestData = BincodeCodec.decode(&encoded).expect("decoding failed");

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_truncated_input() {
        let decoded: Result<TestData, _> = BincodeCodec.decode(&vec![]);
        assert!(matches!(decoded, Err(BincodeCodecError::DecodeError(_))));
    }

    #[test]
    fn test_decode_accepts_trailing_bytes() {
        let mut encoded = BincodeCodec.encode(&42_u32).unwrap();
        assert_eq!(encoded, vec![42]);
        encoded.extend_from_slice(&[0xff, 0xff]);
        let decoded: u32 = BincodeCodec.decode(&encoded).unwrap();
        assert_eq!(decoded, 42);
    }

    #[test]
    fn test_encode_error() {
        #[derive(Decode)]
        struct FailingValue;

        impl Encode for FailingValue {
            fn encode<E: bincode::enc::Encoder>(
                &self,
                _encoder: &mut E,
            ) -> Result<(), bincode::error::EncodeError> {
                Err(bincode::error::EncodeError::Other("encoding rejected"))
            }
        }

        assert!(matches!(
            BincodeCodec.encode(&FailingValue),
            Err(BincodeCodecError::EncodeError(_))
        ));
    }
}
