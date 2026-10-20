//! Conversion between task arguments and a storage representation.
//!
//! Implement [`Codec`] when a storage or transport format differs from the
//! application's payload type. Both directions borrow their input, allowing the
//! caller to retain the original value while preparing or reading stored data.
//!
//! # Choosing a Representation
//!
//! [`Codec::Compact`] is the encoded type understood by the integration. It may
//! be text, bytes, or a structured value; the name does not require compression
//! or a smaller in-memory size. The implementation also chooses one error type
//! capable of describing encoding and decoding failures.
//!
//! # Example
//!
//! This codec stores an unsigned integer as four little-endian bytes. Its fixed
//! input sizes make both conversions infallible:
//!
//! ```
//! use std::convert::Infallible;
//! use miju_core::backend::codec::Codec;
//!
//! struct WordCodec;
//!
//! impl Codec<u32> for WordCodec {
//!     type Compact = [u8; 4];
//!     type Error = Infallible;
//!
//!     fn encode(&self, value: &u32) -> Result<Self::Compact, Self::Error> {
//!         Ok(value.to_le_bytes())
//!     }
//!
//!     fn decode(&self, value: &Self::Compact) -> Result<u32, Self::Error> {
//!         Ok(u32::from_le_bytes(*value))
//!     }
//! }
//!
//! let codec = WordCodec;
//! let stored = codec.encode(&513)?;
//! assert_eq!(stored, [1, 2, 0, 0]);
//! assert_eq!(codec.decode(&stored)?, 513);
//! # Ok::<(), Infallible>(())
//! ```

/// Encodes borrowed arguments and reconstructs them from borrowed storage data.
///
/// Implementations choose both the storage representation and the error type.
/// Encoding and decoding define a format together; an integration must use a
/// compatible codec when reading data written earlier. The trait itself does
/// not persist values or manage format versions.
///
/// # Type Parameter
///
/// `T` is the decoded application value. It does not need to implement a
/// particular serialization trait unless the codec implementation requires one.
/// See the [module documentation](self) for an implementation example.
pub trait Codec<T> {
    /// The representation accepted by the backing store.
    ///
    /// This type is independent of the application's decoded type `T`.
    type Compact;
    /// An encoding or decoding failure reported by this implementation.
    type Error;

    /// Produces a storage value without consuming the arguments.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the input cannot be represented in this codec's
    /// format. Validation and supported values are implementation-specific.
    fn encode(&self, value: &T) -> Result<Self::Compact, Self::Error>;

    /// Reconstructs arguments without consuming the storage value.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] if the stored value cannot be decoded as `T`, for
    /// example because its format is invalid or incompatible with this codec.
    fn decode(&self, value: &Self::Compact) -> Result<T, Self::Error>;
}
