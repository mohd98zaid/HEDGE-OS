//! Per-symbol broadcast distributor.
//!
//! Implements `Distributor` from design § Components § Market_Data_Engine
//! (R1.8): "fans out to per-symbol `tokio::broadcast` channels (lock-free,
//! zero-copy). Subscribers register at startup; no polling."
//!
//! ### Why broadcast (and not MPSC)
//!
//! Multiple Hot_Path consumers may listen to the same symbol — the
//! Orderflow_Engine, the Feature_Extraction_Engine, and the UI gateway
//! all want every tick. `tokio::broadcast` is single-publisher,
//! multi-consumer, and intentionally **lossy on slow consumers**: a
//! receiver that falls behind is dropped from the per-tick fan-out,
//! never blocking the publisher. For market data this is the correct
//! semantics — a stale tick is worthless and the alternative (back-
//! pressuring the publisher) would cascade into the source WebSocket.
//!
//! Channels are created lazily on first `subscribe`. This avoids
//! pre-allocating a channel for every symbol the interner has ever seen
//! when the consumer set is small.

use dashmap::DashMap;
use hedge_core::SymbolId;
use hedge_schemas::Tick;
use tokio::sync::broadcast;
use tracing::instrument;

/// Default per-symbol broadcast channel capacity.
///
/// 1024 ticks at the design's 2 ms ingest budget gives ~2 seconds of
/// burst tolerance per consumer before lossy semantics kick in. The
/// numeric value is fixed by the task brief ("Capacity 1024 per channel").
pub const CHANNEL_CAPACITY: usize = 1024;

/// Per-symbol fan-out distributor.
///
/// Holds one `broadcast::Sender<Tick>` per [`SymbolId`] in a
/// [`DashMap`]. Senders are created on demand by [`Distributor::subscribe`]
/// and reused by [`Distributor::broadcast`].
#[derive(Debug, Default)]
pub struct Distributor {
    channels: DashMap<SymbolId, broadcast::Sender<Tick>>,
    capacity: usize,
}

impl Distributor {
    /// Construct an empty distributor with the documented [`CHANNEL_CAPACITY`].
    pub fn new() -> Self {
        Self::with_capacity(CHANNEL_CAPACITY)
    }

    /// Construct an empty distributor with a custom per-channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            channels: DashMap::new(),
            capacity,
        }
    }

    /// Number of distinct symbols currently registered.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Subscribe to ticks for `sym`. Lazily creates the channel if no
    /// previous subscriber registered for the symbol.
    #[instrument(level = "debug", skip(self), fields(symbol = sym.raw()))]
    pub fn subscribe(&self, sym: SymbolId) -> broadcast::Receiver<Tick> {
        // We use `entry().or_insert_with` so concurrent calls for the same
        // symbol race safely — only one task constructs the channel and
        // every other concurrent subscriber observes the same sender.
        let sender = self
            .channels
            .entry(sym)
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel::<Tick>(self.capacity);
                tx
            })
            .clone();
        sender.subscribe()
    }

    /// Broadcast `tick` on its symbol's channel.
    ///
    /// Returns `true` when the tick was delivered to at least one
    /// receiver, `false` otherwise (channel does not yet exist, or every
    /// receiver has been dropped). Lossy: if the channel is full, the
    /// oldest queued tick is dropped per `broadcast::Sender::send`.
    #[instrument(level = "trace", skip_all, fields(symbol = tick.symbol))]
    pub fn broadcast(&self, tick: &Tick) -> bool {
        let sym = SymbolId::new(tick.symbol);
        match self.channels.get(&sym) {
            Some(sender) => sender.send(*tick).is_ok(),
            None => false,
        }
    }

    /// Eagerly create a channel for `sym` without subscribing. Useful
    /// when the engine wants to pre-warm channels for a known symbol set.
    pub fn ensure_channel(&self, sym: SymbolId) {
        self.channels.entry(sym).or_insert_with(|| {
            let (tx, _rx) = broadcast::channel::<Tick>(self.capacity);
            tx
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_tick(symbol_id: u32) -> Tick {
        Tick {
            correlation_id: [0u8; 16],
            symbol: symbol_id,
            exchange: 0,
            ltp_paise: 100,
            bid_paise: 99,
            ask_paise: 101,
            ltq: 1,
            total_buy_qty: 0,
            total_sell_qty: 0,
            ts_exchange_ns: 0,
            ts_recv_ns: 0,
        }
    }

    #[tokio::test]
    async fn subscribe_creates_channel_lazily() {
        let d = Distributor::new();
        assert_eq!(d.channel_count(), 0);
        let _rx = d.subscribe(SymbolId::new(7));
        assert_eq!(d.channel_count(), 1);
    }

    #[tokio::test]
    async fn broadcast_delivers_to_every_active_subscriber() {
        let d = Distributor::new();
        let mut rx_a = d.subscribe(SymbolId::new(42));
        let mut rx_b = d.subscribe(SymbolId::new(42));
        let mut rx_c = d.subscribe(SymbolId::new(42));

        let t = make_tick(42);
        assert!(d.broadcast(&t));

        let a = rx_a.recv().await.expect("a");
        let b = rx_b.recv().await.expect("b");
        let c = rx_c.recv().await.expect("c");
        assert_eq!(a, t);
        assert_eq!(b, t);
        assert_eq!(c, t);
    }

    #[tokio::test]
    async fn broadcast_does_not_cross_symbols() {
        let d = Distributor::new();
        let mut rx_a = d.subscribe(SymbolId::new(1));
        let _rx_b = d.subscribe(SymbolId::new(2));

        d.broadcast(&make_tick(2)); // routed to symbol 2 only

        // rx_a (symbol 1) sees nothing — try_recv returns Empty.
        match rx_a.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
            other => panic!("expected Empty, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn broadcast_returns_false_when_no_subscriber() {
        let d = Distributor::new();
        // Channel is never created — broadcast cannot find it.
        assert!(!d.broadcast(&make_tick(99)));
    }

    #[tokio::test]
    async fn broadcast_returns_false_when_all_subscribers_dropped() {
        let d = Distributor::new();
        let rx = d.subscribe(SymbolId::new(7));
        drop(rx);
        // The Sender is still held inside the dashmap, but no Receivers
        // exist; `send` returns Err(SendError) which `is_ok` reads as false.
        assert!(!d.broadcast(&make_tick(7)));
    }

    #[tokio::test]
    async fn ensure_channel_pre_warms_without_subscriber() {
        let d = Distributor::new();
        d.ensure_channel(SymbolId::new(11));
        assert_eq!(d.channel_count(), 1);
        // Subsequent subscribe reuses the prewarmed channel.
        let _rx = d.subscribe(SymbolId::new(11));
        assert_eq!(d.channel_count(), 1);
    }

    #[tokio::test]
    async fn concurrent_subscribers_share_one_channel() {
        // Property: a race between many tasks subscribing to the same
        // symbol must not produce two independent channels.
        let d = Arc::new(Distributor::new());
        let mut handles = Vec::new();
        for _ in 0..16 {
            let d = Arc::clone(&d);
            handles.push(tokio::spawn(async move {
                let _rx = d.subscribe(SymbolId::new(5));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(d.channel_count(), 1, "exactly one channel for symbol 5");
    }
}
