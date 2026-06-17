//! Static demo basket: 5 NSE large-caps the synth uses for every per-symbol
//! generator. Matches the Phase B `symbol_id_for` static table planned for
//! `hedge-bus` and the default `HEDGE_UPSTOX_INSTRUMENTS` basket.
//!
//! Add to this list to widen the basket. Order is stable (used by RNG
//! splits and rolling-LTP buffers).

/// Realistic anchor LTPs in paise (₹×100). Used as the starting price in
/// the synth's deterministic random walk when no live LTP has been seen.
/// Numbers are loose mid-2025 quotes — exact values don't matter, only the
/// magnitudes (so the dashboard looks plausible).
pub struct DemoSymbol {
    pub trading_symbol: &'static str,
    pub instrument_key: &'static str,
    pub anchor_paise: i64,
    pub sector: &'static str,
}

pub const DEMO_BASKET: &[DemoSymbol] = &[
    DemoSymbol {
        trading_symbol: "RELIANCE",
        instrument_key: "NSE_EQ|INE002A01018",
        anchor_paise: 135_500,
        sector: "Energy",
    },
    DemoSymbol {
        trading_symbol: "INFY",
        instrument_key: "NSE_EQ|INE009A01021",
        anchor_paise: 116_300,
        sector: "IT",
    },
    DemoSymbol {
        trading_symbol: "SBIN",
        instrument_key: "NSE_EQ|INE062A01020",
        anchor_paise: 96_900,
        sector: "Banks",
    },
    DemoSymbol {
        trading_symbol: "HDFCBANK",
        instrument_key: "NSE_EQ|INE040A01034",
        anchor_paise: 76_200,
        sector: "Banks",
    },
    DemoSymbol {
        trading_symbol: "ICICIBANK",
        instrument_key: "NSE_EQ|INE090A01021",
        anchor_paise: 128_500,
        sector: "Banks",
    },
];

/// All distinct sector names in [`DEMO_BASKET`], deduplicated and sorted
/// for stable iteration across runs.
pub fn sectors() -> Vec<&'static str> {
    let mut s: Vec<&'static str> = DEMO_BASKET.iter().map(|d| d.sector).collect();
    s.sort_unstable();
    s.dedup();
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basket_is_nonempty_and_unique() {
        assert!(!DEMO_BASKET.is_empty());
        let symbols: Vec<&str> = DEMO_BASKET.iter().map(|d| d.trading_symbol).collect();
        let mut sorted = symbols.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), symbols.len(), "duplicate symbol in basket");
    }

    #[test]
    fn sectors_are_deduplicated() {
        let s = sectors();
        let mut sorted = s.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), s.len());
    }
}
