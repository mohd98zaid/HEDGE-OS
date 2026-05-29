//! Cross-generator in-process channel for synthesised signal correlation_ids.
//!
//! The signal generator emits `sig.emitted` events with a fresh
//! `correlation_id` and pushes the same id onto this bounded channel.
//! Downstream generators (`ai_rank`, `risk`, `exec`, `position`) pop ids
//! and produce events that reference them — so the cockpit's Signal /
//! Risk / Exec / Position panels show end-to-end correlated lifecycles.

use std::sync::Arc;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct SignalEvent {
    pub correlation_id: String,
    pub symbol: &'static str,
    pub side: &'static str,   // "buy" or "sell"
    pub strategy: &'static str,
    pub ltp_paise: i64,
    pub base_probability: f64,
    pub confidence: f64,
}

#[derive(Clone)]
pub struct SignalBus {
    tx: Sender<SignalEvent>,
    rx: Arc<Mutex<Receiver<SignalEvent>>>,
}

impl SignalBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = channel(capacity);
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    pub fn sender(&self) -> Sender<SignalEvent> {
        self.tx.clone()
    }

    /// Receive the next signal event. Multiple consumers serialise on the
    /// same receiver via the inner Mutex — fine for synth load (one event
    /// every few seconds).
    pub async fn recv(&self) -> Option<SignalEvent> {
        let mut guard = self.rx.lock().await;
        guard.recv().await
    }
}
