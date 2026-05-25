//! Shared test utilities.
//!
//! Indicator-level proptests reach into [`test_helpers`] for cheap
//! `Tick` builders so they do not have to repeat the boilerplate of
//! initialising every field of `Tick_v1` on every test.

#[cfg(test)]
pub mod test_helpers {
    use hedge_schemas::Tick;

    /// Build a minimal `Tick` carrying just price + last-trade quantity.
    /// All other fields are zeroed.
    pub fn tick(ltp_paise: i64, ltq: u64) -> Tick {
        Tick {
            correlation_id: [0u8; 16],
            symbol: 1,
            exchange: 0,
            ltp_paise,
            bid_paise: ltp_paise.saturating_sub(50),
            ask_paise: ltp_paise.saturating_add(50),
            ltq,
            total_buy_qty: 0,
            total_sell_qty: 0,
            ts_exchange_ns: 0,
            ts_recv_ns: 0,
        }
    }

    /// Build a `Tick` with an explicit tick count encoded into the
    /// `ts_exchange_ns` field — useful for ATR / EMA sequence tests
    /// that inspect monotonic timestamps.
    pub fn tick_with_count(ltp_paise: i64, count: u64) -> Tick {
        let mut t = tick(ltp_paise, 1);
        t.ts_exchange_ns = count;
        t.ts_recv_ns = count;
        t
    }

    /// Build a fully-specified `Tick` carrying buy/sell book quantities
    /// and a monotonic timestamp.
    pub fn tick_full(
        ltp_paise: i64,
        ltq: u64,
        total_buy_qty: u64,
        total_sell_qty: u64,
        ts_exchange_ns: u64,
        ts_recv_ns: u64,
    ) -> Tick {
        Tick {
            correlation_id: [0u8; 16],
            symbol: 1,
            exchange: 0,
            ltp_paise,
            bid_paise: ltp_paise.saturating_sub(50),
            ask_paise: ltp_paise.saturating_add(50),
            ltq,
            total_buy_qty,
            total_sell_qty,
            ts_exchange_ns,
            ts_recv_ns,
        }
    }
}
