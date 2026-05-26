//! `rkyv` codec adapter for the [`hedge_bus::Codec`] trait.
//!
//! [`hedge_bus::RedisStreamProducer`] is generic over `Codec<T>` so the
//! producer can encode whatever payload the caller hands it. The Hot_Path
//! Redis streams (`hedge.hot.signals`, `.approvals`, `.fills`) all carry
//! FlatBuffers-encoded payloads via [`hedge_bus::FlatBuffersCodec`]. The
//! replay ledger ([`hedge_bus::STREAM_HOT_REPLAY_RECORD`]) is different:
//! every entry carries an rkyv-encoded [`ReplayRecord`] (R22.1 — design
//! § Components § Replay_Engine).
//!
//! [`ReplayRecordCodec`] is the zero-sized adapter that lets us reuse
//! the typed Redis Stream wrapper for our rkyv payloads without
//! widening `hedge-bus`'s public API.

use bytes::Bytes;
use hedge_bus::{BusError, Codec};

use crate::record::{decode_record, encode_record, ReplayRecord};

/// rkyv codec for [`ReplayRecord`]. Zero-sized; instances are
/// indistinguishable.
#[derive(Copy, Clone, Debug, Default)]
pub struct ReplayRecordCodec;

impl Codec<ReplayRecord> for ReplayRecordCodec {
    fn encode(&self, value: &ReplayRecord) -> Result<Bytes, BusError> {
        Ok(encode_record(value))
    }

    fn decode(&self, bytes: &[u8]) -> Result<ReplayRecord, BusError> {
        decode_record(bytes)
            .ok_or_else(|| BusError::Decode("rkyv check_archived_root failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordKind;

    fn sample_record() -> ReplayRecord {
        ReplayRecord {
            session_id: 20251130,
            sequence_no: 0,
            monotonic_ns: 1_000_000,
            wallclock_utc: 1_700_000_000_000_000_000,
            kind: RecordKind::Tick,
            payload: vec![0x01, 0x02, 0x03],
        }
    }

    #[test]
    fn round_trips_through_codec() {
        let codec = ReplayRecordCodec;
        let r = sample_record();
        let bytes = codec.encode(&r).unwrap();
        let back = codec.decode(&bytes).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn decode_rejects_garbage() {
        let codec = ReplayRecordCodec;
        let err = codec.decode(b"not rkyv").unwrap_err();
        match err {
            BusError::Decode(_) => {}
            other => panic!("expected Decode, got {:?}", other),
        }
    }
}
