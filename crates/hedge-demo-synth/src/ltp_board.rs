//! Shared rolling LTP board.
//!
//! Every generator that derives downstream events from "current price"
//! reads from a single in-memory board. The board is updated from two
//! sources:
//!
//! 1. The synth's own tick generator (when no real publisher is alive).
//! 2. Live `md.tick.*` payloads observed by the suppression subscriber —
//!    when a real publisher is producing, downstream synth events follow
//!    the real prices instead of synth's random walk (REQ-3.3).
//!
//! The board is `DashMap`-backed so concurrent reads/writes from many
//! generator tasks are lock-free at the API surface.

use std::sync::Arc;

use dashmap::DashMap;

#[derive(Copy, Clone, Debug)]
pub struct Quote {
    pub ltp_paise: i64,
    pub bid_paise: i64,
    pub ask_paise: i64,
    /// Wall-clock ns when this quote was recorded.
    pub ts_ns: i64,
}

#[derive(Clone, Debug, Default)]
pub struct LtpBoard {
    inner: Arc<DashMap<String, Quote>>,
}

impl LtpBoard {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn set(&self, symbol: &str, q: Quote) {
        self.inner.insert(symbol.to_string(), q);
    }

    pub fn get(&self, symbol: &str) -> Option<Quote> {
        self.inner.get(symbol).map(|kv| *kv.value())
    }

    /// Snapshot every symbol/quote pair currently on the board. Returns
    /// pairs in arbitrary order.
    pub fn snapshot(&self) -> Vec<(String, Quote)> {
        self.inner
            .iter()
            .map(|kv| (kv.key().clone(), *kv.value()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(ltp: i64) -> Quote {
        Quote {
            ltp_paise: ltp,
            bid_paise: ltp - 5,
            ask_paise: ltp + 5,
            ts_ns: 0,
        }
    }

    #[test]
    fn set_then_get_round_trips() {
        let b = LtpBoard::new();
        b.set("RELIANCE", q(135500));
        assert_eq!(b.get("RELIANCE").unwrap().ltp_paise, 135500);
        assert!(b.get("MISSING").is_none());
    }

    #[test]
    fn snapshot_returns_all_entries() {
        let b = LtpBoard::new();
        b.set("A", q(1));
        b.set("B", q(2));
        let s = b.snapshot();
        assert_eq!(s.len(), 2);
    }
}
