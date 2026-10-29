//! Encode graph inputs and decode compact values for node services.
//!
//! [`GraphCodec`] uses the backend's [`Codec`], with special collection layouts.
//! Scalars, `Option<T>` and `Result<T, E>` delegate directly to that codec.
//! Tuples with two to twelve elements encode each element separately, then encode
//! the list of compact elements; decoding reverses those two layers.
//!
//! `Vec<T>` also encodes each element separately, but decodes the whole compact
//! value directly as `Vec<T>`. A one-element tuple encodes the entire tuple inside
//! a list and decodes directly as that tuple. These asymmetric layouts do not
//! guarantee round-trips: byte-based JSON fails for a nonempty `Vec<u32>` and for
//! `(u32,)`, while JSON values support the vector case.
//!
//! ```rust
//! use miju_codec::json::JsonCodec;
//! use miju_workflow::{graph::decode::GraphCodec, in_memory::InMemoryWorkflow};
//!
//! type Backend = InMemoryWorkflow<u32>;
//! type Input = (u32, String);
//! let codec = JsonCodec::default();
//! let compact = <Input as GraphCodec<Backend>>::encode((7, "ready".to_owned()), &codec)?;
//! let restored = <Input as GraphCodec<Backend>>::decode(&compact, &codec)?;
//! assert_eq!(restored, (7, "ready".to_owned()));
//! # Ok::<(), miju_workflow::graph::error::GraphFlowError>(())
//! ```

use miju_core::{
    backend::{Backend, WireFormatBackend, codec::Codec},
    error::BoxDynError,
};

use crate::graph::error::GraphFlowError;

/// Convert graph inputs using a backend's compact representation.
///
/// Collection implementations have distinct wire layouts; see the
/// [module documentation](self) before combining them with ordinary codec output.
/// Custom input types can implement this trait using their backend's codec.
pub trait GraphCodec<B: WireFormatBackend>: Sized {
    /// The codec error
    type Error;

    /// Encode the input, including any collection envelope used by its implementation.
    ///
    /// Tuple implementations with two or more elements wrap codec errors in
    /// [`GraphFlowError::Codec`]; other built-in implementations retain their codec error type.
    fn encode(self, codec: &B::Codec) -> Result<B::Compact, Self::Error>;

    /// Decode a compact input according to this type's wire layout.
    ///
    /// Tuple implementations with two or more elements return
    /// [`GraphFlowError::InputCountMismatch`] when the decoded list has the wrong
    /// length. Their codec errors remain available through [`GraphFlowError::Codec`].
    fn decode(response: &B::Compact, codec: &B::Codec) -> Result<Self, Self::Error>;
}

impl<B, T, Err> GraphCodec<B> for Vec<T>
where
    B: Backend + WireFormatBackend,
    B::Codec: Codec<T, Compact = B::Compact, Error = Err>
        + Codec<Vec<B::Compact>, Compact = B::Compact, Error = Err>
        + Codec<Self, Compact = B::Compact, Error = Err>,
{
    type Error = Err;
    fn encode(self, codec: &B::Codec) -> Result<B::Compact, <B::Codec as Codec<Self>>::Error> {
        let mut result = Vec::new();
        for input in self {
            result.push(B::Codec::encode(codec, &input)?);
        }
        let compact = B::Codec::encode(codec, &result)?;
        Ok(compact)
    }

    fn decode(response: &B::Compact, codec: &B::Codec) -> Result<Self, Err> {
        let decoded = B::Codec::decode(codec, response)?;
        Ok(decoded)
    }
}

macro_rules! impl_entry_for_tuple {
    // First, implement for the first type to extract the error type
    ($T1:ident) => {
        impl<B, Err, $T1> GraphCodec<B> for ($T1,)
        where
            B: Backend + WireFormatBackend,
            B::Codec: Codec<($T1,), Compact = B::Compact, Error = Err> + Codec<Vec<B::Compact>, Compact = B::Compact, Error = Err>,
        {
            type Error = Err;

            fn encode(self, codec: &B::Codec) -> Result<B::Compact, Self::Error> {
                let result = vec![B::Codec::encode(codec, &self)?];
                let compact = B::Codec::encode(codec, &result)?;
                Ok(compact)
            }
            fn decode(response: &B::Compact, codec: &B::Codec) -> Result<Self, Err> {
                let decoded = B::Codec::decode(codec, response)?;
                Ok(decoded)
            }
        }
    };

    // For 2+ types, use the first type's error
    ($T1:ident, $($T:ident),+) => {
        impl<B, Err, $T1, $($T),+> GraphCodec<B> for ($T1, $($T),+)
        where
            B: Backend + WireFormatBackend,
            B::Codec: Codec<$T1, Compact = B::Compact, Error = Err> + Codec<Vec<B::Compact>, Compact = B::Compact, Error = Err>,
            $(B::Codec: Codec<$T, Compact = B::Compact, Error = Err>,)+
            Err: Into<BoxDynError>,
            // Ensure all errors are the same type as T1's error
            $(
                <B::Codec as Codec<$T>>::Error: Into<<B::Codec as Codec<$T1>>::Error>,
            )+
        {
            type Error = GraphFlowError;

            fn encode(self, codec: &B::Codec) -> Result<B::Compact, Self::Error> {
                #[allow(non_snake_case)]
                let ($T1, $($T),+) = self;

                let mut result = vec![B::Codec::encode(codec, &$T1).map_err(|e: Err| GraphFlowError::Codec(e.into()))?];
                $(
                    result.push(B::Codec::encode(codec, &$T).map_err(|e: Err| GraphFlowError::Codec(e.into()))?);
                )+
                let compact = B::Codec::encode(codec, &result).map_err(|e: Err| GraphFlowError::Codec(e.into()))?;
                Ok(compact)
            }

            fn decode(response: &B::Compact, codec: &B::Codec) -> Result<Self, Self::Error> {
                let decoded: Vec<B::Compact> = B::Codec::decode(codec, response).map_err(|e: Err| GraphFlowError::Codec(e.into()))?;
                // Count the number of types in the tuple
                let expected_len = 1 $(+ {let _ = stringify!($T); 1})+;
                if decoded.len() != expected_len {
                    return Err(GraphFlowError::InputCountMismatch {
                        expected: expected_len,
                        actual: decoded.len(),
                    });
                }

                let mut iter = decoded.into_iter();

                #[allow(non_snake_case)]
                let $T1 = B::Codec::decode(codec, &iter.next().unwrap()).map_err(|e: Err| GraphFlowError::Codec(e.into()))?;
                $(
                    #[allow(non_snake_case)]
                    let $T = B::Codec::decode(codec, &iter.next().unwrap()).map_err(|e: Err| GraphFlowError::Codec(e.into()))?;
                )+

                Ok(($T1, $($T),+))
            }
        }
    };
}

impl_entry_for_tuple!(T1);
impl_entry_for_tuple!(T1, T2);
impl_entry_for_tuple!(T1, T2, T3);
impl_entry_for_tuple!(T1, T2, T3, T4);
impl_entry_for_tuple!(T1, T2, T3, T4, T5);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

macro_rules! impl_entry_passthrough {
    ($($T:ty),+) => {
        $(
            impl<B, Err> GraphCodec<B> for $T
            where
                B: Backend + WireFormatBackend,
                B::Codec: Codec<$T, Compact = B::Compact, Error = Err>,
            {
                type Error = Err;

                fn encode(self, codec: &B::Codec) -> Result<B::Compact, Self::Error> {
                    B::Codec::encode(codec, &self)
                }

                fn decode(response: &B::Compact, codec: &B::Codec) -> Result<Self, Self::Error> {
                    B::Codec::decode(codec, response)
                }
            }
        )+
    };
}

impl_entry_passthrough!(
    String,
    &'static str,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    bool,
    char,
    ()
);

impl<B, T, Err> GraphCodec<B> for Option<T>
where
    B: Backend + WireFormatBackend,
    B::Codec: Codec<Self, Compact = B::Compact, Error = Err>,
{
    type Error = Err;

    fn encode(self, codec: &B::Codec) -> Result<B::Compact, Self::Error> {
        B::Codec::encode(codec, &self)
    }

    fn decode(response: &B::Compact, codec: &B::Codec) -> Result<Self, Self::Error> {
        B::Codec::decode(codec, response)
    }
}

impl<B, T, E, Err> GraphCodec<B> for Result<T, E>
where
    B: Backend + WireFormatBackend,
    B::Codec: Codec<Self, Compact = B::Compact, Error = Err>,
{
    type Error = Err;

    fn encode(self, codec: &B::Codec) -> Result<B::Compact, Self::Error> {
        B::Codec::encode(codec, &self)
    }

    fn decode(response: &B::Compact, codec: &B::Codec) -> Result<Self, Self::Error> {
        B::Codec::decode(codec, response)
    }
}
