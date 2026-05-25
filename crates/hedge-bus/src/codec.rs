//! Pluggable wire codecs for typed bus payloads.
//!
//! The bus does not hard-code a single serialization format because the
//! Hot_Path uses two:
//!
//! * **FlatBuffers** for `md.*`, `of.*`, `feat.*`, `sig.*`, `risk.*`,
//!   `exec.*`, `pos.*` (R1.5). These payloads are accessed in place via
//!   `flatbuffers::root::<T>(&bytes)` so the receive path is zero-copy.
//! * **JSON** for `ai.*`, `mem.*`, `trader.*`, `ops.*`, `obs.*` — domains
//!   where developer ergonomics dominate latency. The Warm_AI_Pipeline is
//!   Python so JSON is the lingua franca.
//!
//! Both formats funnel through the [`Codec`] trait below. Concrete typed
//! [`Subject<Tick_v1>`](crate::Subject) bindings ship in `hedge-schemas` as
//! part of task **4.1**; until then the bus only needs the trait shape and
//! the two reference implementations.

use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::BusError;

/// A pluggable codec mapping a typed payload `T` to and from raw bytes.
///
/// The trait is intentionally `Send + Sync + 'static` so codec values can
/// live inside long-running tokio tasks without lifetime gymnastics. All
/// methods are stateless on `&self` — implementations are zero-sized in the
/// reference cases ([`JsonCodec`] and [`FlatBuffersCodec`]).
pub trait Codec<T>: Send + Sync + 'static {
    /// Serialize `value` into a [`Bytes`] payload ready for wire transmission.
    ///
    /// `Bytes` (and not `Vec<u8>`) is chosen so codecs that can avoid the
    /// final copy — notably FlatBuffers when given a pre-built buffer —
    /// surface that fact to the publisher.
    fn encode(&self, value: &T) -> Result<Bytes, BusError>;

    /// Decode a payload from the wire bytes.
    ///
    /// The slice is borrowed; FlatBuffers implementations should verify and
    /// return a borrowed view (when `T` is itself a `&[u8]` newtype) or copy
    /// out into an owned struct, depending on `T`.
    fn decode(&self, bytes: &[u8]) -> Result<T, BusError>;
}

/// JSON codec. Intended for `ai.*`/`mem.*`/`trader.*`/`ops.*`/`obs.*`
/// subjects whose payloads are defined as JSON Schema in the design.
///
/// The codec is generic over `T` and zero-sized — every instance is
/// indistinguishable from any other.
pub struct JsonCodec<T>(PhantomData<fn() -> T>);

impl<T> Default for JsonCodec<T> {
    #[inline]
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> JsonCodec<T> {
    /// Construct a JSON codec for payload `T`.
    #[inline]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Clone for JsonCodec<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<T> Copy for JsonCodec<T> {}

impl<T> Codec<T> for JsonCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn encode(&self, value: &T) -> Result<Bytes, BusError> {
        let v = serde_json::to_vec(value).map_err(|e| BusError::Encode(e.to_string()))?;
        Ok(Bytes::from(v))
    }

    fn decode(&self, bytes: &[u8]) -> Result<T, BusError> {
        serde_json::from_slice(bytes).map_err(|e| BusError::Decode(e.to_string()))
    }
}

/// Newtype wrapper around `Bytes` used as the return type for the FlatBuffers
/// codec placeholder. Once `hedge-schemas` ships in task 4.1, callers will
/// usually parameterise [`Subject`](crate::Subject) over the concrete
/// FlatBuffers root type and use a typed `FlatBuffersCodec<Tick_v1>` instead.
///
/// `RawBytes` exists today so the receive path is exercised end-to-end in
/// tests without depending on a generated FlatBuffers binding.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawBytes(pub Bytes);

impl RawBytes {
    /// Borrow as a slice for FlatBuffers verifiers (`flatbuffers::root::<T>`).
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// Length of the payload, in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the payload is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Bytes> for RawBytes {
    #[inline]
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl From<Vec<u8>> for RawBytes {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

/// Placeholder FlatBuffers codec.
///
/// This codec is the zero-copy pivot for the Hot_Path:
///
/// * [`encode`](Codec::encode) takes an already-built FlatBuffers byte buffer
///   wrapped in [`RawBytes`] and returns the underlying [`Bytes`] handle
///   without copying. Producers build the buffer once with
///   `flatbuffers::FlatBufferBuilder` and hand it off.
/// * [`decode`](Codec::decode) returns a `RawBytes` wrapping a *copy* of the
///   wire slice. The zero-copy receive path proper bypasses this trait
///   entirely and reads the [`Bytes`] returned by
///   [`NatsSubscriber::recv_bytes`](crate::nats::NatsSubscriber::recv_bytes)
///   directly.
///
/// Real typed codecs (e.g. `FlatBuffersCodec<Tick_v1>`) ship in task 4.1.
#[derive(Copy, Clone, Debug, Default)]
pub struct FlatBuffersCodec;

impl Codec<RawBytes> for FlatBuffersCodec {
    fn encode(&self, value: &RawBytes) -> Result<Bytes, BusError> {
        // Cheap clone: `Bytes` is refcounted, so this does not copy the buffer.
        Ok(value.0.clone())
    }

    fn decode(&self, bytes: &[u8]) -> Result<RawBytes, BusError> {
        // Hand the bytes back to the caller. We *must* copy here because the
        // borrowed slice's lifetime ends at the trait method boundary; the
        // zero-copy fast path lives on `NatsSubscriber::recv_bytes`, which
        // returns `Bytes` owned by the NATS library's wire buffer.
        Ok(RawBytes(Bytes::copy_from_slice(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        name: String,
        score: i64,
        flag: bool,
    }

    #[test]
    fn json_codec_round_trips_payload() {
        let codec: JsonCodec<Sample> = JsonCodec::new();
        let original = Sample {
            name: "alpha".into(),
            score: -42,
            flag: true,
        };

        let bytes = codec.encode(&original).expect("encode");
        // Sanity-check the wire form is JSON.
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.contains("\"alpha\""), "wire form not json: {}", s);
        assert!(s.contains("-42"));

        let decoded = codec.decode(&bytes).expect("decode");
        assert_eq!(decoded, original);
    }

    #[test]
    fn json_codec_decode_returns_decode_error_on_garbage() {
        let codec: JsonCodec<Sample> = JsonCodec::new();
        let err = codec.decode(b"not json").unwrap_err();
        match err {
            BusError::Decode(_) => {}
            other => panic!("expected Decode, got {:?}", other),
        }
    }

    #[test]
    fn flatbuffers_codec_passes_bytes_through_without_modification() {
        let codec = FlatBuffersCodec;
        let payload = RawBytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let encoded = codec.encode(&payload).expect("encode");
        assert_eq!(encoded.as_ref(), &[0xDE, 0xAD, 0xBE, 0xEF]);

        let decoded = codec.decode(&encoded).expect("decode");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn flatbuffers_codec_encode_does_not_clone_underlying_buffer() {
        // The cheap-clone property is what makes FlatBuffersCodec zero-copy
        // in spirit: encoding twice should yield two `Bytes` handles that
        // share storage. We verify by checking the underlying pointer of
        // each handle's `as_ref()` slice.
        let codec = FlatBuffersCodec;
        let payload = RawBytes::from(vec![1u8; 64]);

        let a = codec.encode(&payload).unwrap();
        let b = codec.encode(&payload).unwrap();

        assert_eq!(a.as_ref().as_ptr(), b.as_ref().as_ptr());
    }

    #[test]
    fn raw_bytes_helpers() {
        let rb = RawBytes::from(vec![1, 2, 3]);
        assert_eq!(rb.as_slice(), &[1, 2, 3]);
        assert_eq!(rb.len(), 3);
        assert!(!rb.is_empty());
        assert!(RawBytes::default().is_empty());
    }
}
