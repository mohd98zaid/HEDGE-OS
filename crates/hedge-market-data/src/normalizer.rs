//! Tick normalizer.
//!
//! Converts a [`RawTick`] (vendor-agnostic JSON / future binary form) into
//! the canonical FlatBuffers [`Tick`] (=`Tick_v1`) wire payload (R1.5,
//! design § Components § Market_Data_Engine).
//!
//! Normalization performs three jobs:
//!
//! 1. **Symbol interning** — resolves the string ticker to a `SymbolId`
//!    via the shared [`SymbolInterner`]. Idempotent for already-known
//!    symbols and allocation-free in steady state.
//! 2. **Timestamp stamping** — sets `ts_recv_ns = hedge_core::now_ns()`,
//!    the monotonic per-process counter consumed by every downstream
//!    `LatencyTracer` budget check.
//! 3. **Correlation id minting** — allocates a fresh [`CorrelationId`]
//!    (ULID) so every event spawned from this tick downstream
//!    (orderflow event, feature update, signal, risk approval, fill) can
//!    be threaded back through the same `correlation_id` (R27.4).

use std::sync::Arc;

use hedge_core::{now_ns, CorrelationId};
use hedge_schemas::Tick;
use tracing::instrument;

use crate::interner::SymbolInterner;
use crate::protocol::RawTick;

/// Exchange discriminant for `Tick_v1.exchange` (`0 = NSE`, `1 = BSE`).
///
/// The schema declares `exchange: byte`. We expose this typed enum so the
/// normalizer's call sites read clearly; the wire form is a single byte.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(i8)]
pub enum Exchange {
    /// National Stock Exchange of India.
    Nse = 0,
    /// Bombay Stock Exchange.
    Bse = 1,
}

impl Exchange {
    /// Wire-form byte value used to populate `Tick_v1.exchange`.
    #[inline]
    pub const fn as_byte(self) -> i8 {
        self as i8
    }
}

/// Stateless tick normalizer.
///
/// Internally holds an `Arc<SymbolInterner>` so multiple adapter tasks can
/// share the same id space without coupling their lifetimes.
#[derive(Debug, Clone)]
pub struct TickNormalizer {
    interner: Arc<SymbolInterner>,
}

impl TickNormalizer {
    /// Construct a normalizer backed by the supplied interner.
    pub fn new(interner: Arc<SymbolInterner>) -> Self {
        Self { interner }
    }

    /// Borrow the underlying interner. Useful for tests that pre-populate
    /// known symbols and assert id stability.
    #[inline]
    pub fn interner(&self) -> &Arc<SymbolInterner> {
        &self.interner
    }

    /// Normalize a raw tick into the canonical [`Tick`] wire payload.
    ///
    /// Mints a fresh per-tick [`CorrelationId`] and stamps `ts_recv_ns` to
    /// the current monotonic timestamp.
    #[instrument(level = "trace", skip_all, fields(symbol = %raw.symbol, exchange = ?exchange))]
    pub fn normalize(&self, raw: &RawTick, exchange: Exchange) -> Tick {
        let cid = CorrelationId::new();
        self.normalize_with_correlation(raw, exchange, cid)
    }

    /// Variant that takes a pre-minted [`CorrelationId`].
    ///
    /// This is the entry point used by the [`crate::engine::MarketDataEngine`]
    /// when it wraps the normalize step inside a `LatencyTracer::start`
    /// scope: the tracer is keyed on the same correlation id that travels
    /// downstream.
    #[instrument(level = "trace", skip_all, fields(symbol = %raw.symbol))]
    pub fn normalize_with_correlation(
        &self,
        raw: &RawTick,
        exchange: Exchange,
        correlation_id: CorrelationId,
    ) -> Tick {
        let symbol_id = self.interner.intern(&raw.symbol);
        let mut cid_bytes = [0u8; 16];
        cid_bytes.copy_from_slice(&correlation_id.as_u128().to_be_bytes());

        Tick {
            correlation_id: cid_bytes,
            symbol: symbol_id.raw(),
            exchange: exchange.as_byte(),
            ltp_paise: raw.ltp_paise,
            bid_paise: raw.bid_paise,
            ask_paise: raw.ask_paise,
            ltq: raw.ltq,
            total_buy_qty: raw.total_buy_qty,
            total_sell_qty: raw.total_sell_qty,
            // The schema field is documented as nanoseconds; the raw payload
            // delivers milliseconds, so we widen by 1e6 here.
            ts_exchange_ns: raw.ts_exchange_ms.saturating_mul(1_000_000),
            ts_recv_ns: now_ns(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_raw() -> RawTick {
        RawTick {
            symbol: "RELIANCE".into(),
            ltp_paise: 250_000,
            bid_paise: 249_950,
            ask_paise: 250_050,
            ltq: 100,
            total_buy_qty: 5_000,
            total_sell_qty: 4_500,
            ts_exchange_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn normalize_copies_paise_fields_verbatim() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(interner);
        let raw = sample_raw();
        let tick = n.normalize(&raw, Exchange::Nse);

        assert_eq!(tick.ltp_paise, raw.ltp_paise);
        assert_eq!(tick.bid_paise, raw.bid_paise);
        assert_eq!(tick.ask_paise, raw.ask_paise);
        assert_eq!(tick.ltq, raw.ltq);
        assert_eq!(tick.total_buy_qty, raw.total_buy_qty);
        assert_eq!(tick.total_sell_qty, raw.total_sell_qty);
    }

    #[test]
    fn normalize_stamps_exchange_byte() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(interner);
        let raw = sample_raw();
        assert_eq!(n.normalize(&raw, Exchange::Nse).exchange, 0);
        assert_eq!(n.normalize(&raw, Exchange::Bse).exchange, 1);
    }

    #[test]
    fn normalize_widens_exchange_ms_to_ns() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(interner);
        let raw = sample_raw();
        let tick = n.normalize(&raw, Exchange::Nse);
        assert_eq!(tick.ts_exchange_ns, raw.ts_exchange_ms * 1_000_000);
    }

    #[test]
    fn normalize_assigns_monotonic_recv_timestamp() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(interner);
        let raw = sample_raw();
        let a = n.normalize(&raw, Exchange::Nse);
        let b = n.normalize(&raw, Exchange::Nse);
        assert!(b.ts_recv_ns >= a.ts_recv_ns, "ts_recv_ns is monotonic");
    }

    #[test]
    fn normalize_mints_unique_correlation_ids() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(interner);
        let raw = sample_raw();
        let a = n.normalize(&raw, Exchange::Nse);
        let b = n.normalize(&raw, Exchange::Nse);
        assert_ne!(
            a.correlation_id, b.correlation_id,
            "every tick must carry a fresh correlation_id"
        );
    }

    #[test]
    fn normalize_with_correlation_preserves_supplied_id() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(interner);
        let cid = CorrelationId(0x1122_3344_5566_7788_99AA_BBCC_DDEE_FF00u128);
        let tick = n.normalize_with_correlation(&sample_raw(), Exchange::Nse, cid);
        assert_eq!(
            u128::from_be_bytes(tick.correlation_id),
            cid.as_u128()
        );
    }

    #[test]
    fn normalize_resolves_symbol_via_interner() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(Arc::clone(&interner));
        let raw = sample_raw();
        let tick = n.normalize(&raw, Exchange::Nse);
        let id = interner.get("RELIANCE").expect("interned");
        assert_eq!(tick.symbol, id.raw());
        // Same symbol must resolve to the same id on a second normalize.
        let tick2 = n.normalize(&raw, Exchange::Nse);
        assert_eq!(tick2.symbol, tick.symbol);
    }

    #[test]
    fn normalize_distinct_symbols_get_distinct_ids() {
        let interner = Arc::new(SymbolInterner::new());
        let n = TickNormalizer::new(Arc::clone(&interner));
        let mut a = sample_raw();
        a.symbol = "RELIANCE".into();
        let mut b = sample_raw();
        b.symbol = "TCS".into();
        let ta = n.normalize(&a, Exchange::Nse);
        let tb = n.normalize(&b, Exchange::Nse);
        assert_ne!(ta.symbol, tb.symbol);
    }
}
