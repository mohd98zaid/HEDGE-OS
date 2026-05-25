//! Generated FlatBuffers bindings — fallback module.
//!
//! When `flatc` is available at build time, `build.rs` regenerates the
//! `*_generated.rs` files inside this directory from `schemas/*.fbs`.
//! When `flatc` is not on `PATH` (most CI environments), the committed
//! files in this directory are used unchanged. They contain typed POD
//! mirrors of every schema so the workspace builds and consumers can
//! reference `hedge_schemas::Tick`, `hedge_schemas::Signal`, etc.
//!
//! Full FlatBuffers wire-format encode/decode lands in task 4.2 alongside
//! the round-trip property tests; the structs in this module are
//! deliberately simple Rust types so that downstream Hot_Path crates have
//! something to type-check against today.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(clippy::module_name_repetitions)]

pub mod tick_generated;
pub mod orderbook_generated;
pub mod oi_generated;
pub mod features_generated;
pub mod signal_generated;
pub mod order_generated;
pub mod risk_generated;
pub mod latency_generated;

/// `namespace hedge.v1;` — the canonical schema namespace declared by every
/// `.fbs` file. Re-exported through `hedge_schemas::v1` for ergonomic access.
pub mod hedge {
    pub mod v1 {
        pub use super::super::tick_generated::Tick_v1;
        pub use super::super::orderbook_generated::{BookLevel, OrderBook_v1};
        pub use super::super::oi_generated::OpenInterest_v1;
        pub use super::super::features_generated::FeatureSnapshot_v1;
        pub use super::super::signal_generated::{RiskProfile_v1, Signal_v1};
        pub use super::super::order_generated::{OrderIntent_v1, OrderState_v1};
        pub use super::super::risk_generated::RiskApproval_v1;
        pub use super::super::latency_generated::LatencyRecord_v1;
    }
}

/// FlatBuffers `file_identifier` declared by every schema in this crate.
pub const FILE_IDENTIFIER: &[u8; 4] = b"HEDG";
