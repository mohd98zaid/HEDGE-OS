//! Runtime War_Mode profile tracker for the Feature_Extraction_Engine.
//!
//! On each `ops.warmode.start` the engine binary calls
//! [`WarModeProfile::activate`] with the configured `scan_multiplier`
//! and `min_confidence`; on `ops.warmode.end` it calls
//! [`WarModeProfile::deactivate`]. Reads on the steady-state hot loop go
//! through [`WarModeProfile::is_active`] / [`WarModeProfile::scan_multiplier`]
//! which both compile down to a single relaxed atomic load — cheap
//! enough to call per tick without lock contention (R26.2).
//!
//! ## Why this lives in `hedge-features`
//!
//! Design § Operating Modes — Market_Open_War_Mode requires Hot_Path
//! components to apply an "increased scan multiplier" while War_Mode is
//! active. The Feature_Extraction_Engine is a Hot_Path component
//! (design § Components § Feature_Extraction_Engine); it consumes
//! `ops.warmode.*` from the session controller and surfaces the
//! multiplier so any per-symbol scheduling caller (priority engine,
//! warm-cache scheduler) can boost its scan rate uniformly.
//!
//! ## Allocation discipline
//!
//! `WarModeProfile` stores its state in three atomics — no heap, no
//! locks, no `Vec`. Reads from the hot loop are wait-free.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Default scan multiplier when War_Mode is inactive — `1.0` (no
/// boost). Must match the design's Normal-mode behaviour.
pub const NORMAL_SCAN_MULTIPLIER: f32 = 1.0;

/// Default minimum confidence floor when War_Mode is inactive — `0.0`
/// (no gate). The Signal_Engine itself never sees this value while
/// War_Mode is off; it is published on the `WarModeEvent.min_confidence`
/// field for the symmetry the schema requires.
pub const NORMAL_MIN_CONFIDENCE: f32 = 0.0;

/// Wait-free War_Mode profile state shared between the binary's
/// `ops.warmode.*` subscriber and the engine's hot loop.
///
/// Stored as three atomics so the read path is a single relaxed load —
/// no `Mutex`/`RwLock`/`parking_lot` bounce on the per-tick path.
#[derive(Debug)]
pub struct WarModeProfile {
    /// `true` while `[ops.warmode.start, ops.warmode.end)` is active.
    active: AtomicBool,
    /// Scan-frequency multiplier from `WarModeConfig.scan_multiplier`.
    /// `f32::to_bits` gives us a `u32` we can store atomically without
    /// a lock; `from_bits` recovers the original on read.
    scan_multiplier_bits: AtomicU32,
    /// Confidence floor from `WarModeConfig.min_confidence`. Stored
    /// for parity with the wire schema; the Feature_Extraction_Engine
    /// itself does not gate on it (R26.3 places the gate in the
    /// Signal_Engine and UI gateway).
    min_confidence_bits: AtomicU32,
}

impl Default for WarModeProfile {
    fn default() -> Self {
        Self::inactive()
    }
}

impl WarModeProfile {
    /// Construct a fresh inactive profile (steady-state default).
    pub fn inactive() -> Self {
        Self {
            active: AtomicBool::new(false),
            scan_multiplier_bits: AtomicU32::new(NORMAL_SCAN_MULTIPLIER.to_bits()),
            min_confidence_bits: AtomicU32::new(NORMAL_MIN_CONFIDENCE.to_bits()),
        }
    }

    /// Activate the profile with the values published on the
    /// `ops.warmode.start` event. Idempotent — repeated activation
    /// with the same values is a no-op.
    pub fn activate(&self, scan_multiplier: f32, min_confidence: f32) {
        self.scan_multiplier_bits
            .store(scan_multiplier.to_bits(), Ordering::Relaxed);
        self.min_confidence_bits
            .store(min_confidence.to_bits(), Ordering::Relaxed);
        // Active flag last so a concurrent reader that observes
        // `is_active() == true` is guaranteed to see the new
        // multiplier and floor.
        self.active.store(true, Ordering::Release);
    }

    /// Deactivate the profile. Resets the multiplier and floor back to
    /// the Normal-mode defaults so any stale read returns the design's
    /// "no boost / no gate" baseline.
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        self.scan_multiplier_bits
            .store(NORMAL_SCAN_MULTIPLIER.to_bits(), Ordering::Relaxed);
        self.min_confidence_bits
            .store(NORMAL_MIN_CONFIDENCE.to_bits(), Ordering::Relaxed);
    }

    /// `true` while `[start, end)` is active.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Current scan multiplier. Returns [`NORMAL_SCAN_MULTIPLIER`] when
    /// the profile is inactive.
    #[inline]
    pub fn scan_multiplier(&self) -> f32 {
        f32::from_bits(self.scan_multiplier_bits.load(Ordering::Relaxed))
    }

    /// Current minimum confidence floor. Returns
    /// [`NORMAL_MIN_CONFIDENCE`] when the profile is inactive.
    #[inline]
    pub fn min_confidence(&self) -> f32 {
        f32::from_bits(self.min_confidence_bits.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_inactive_with_normal_baseline() {
        let p = WarModeProfile::default();
        assert!(!p.is_active());
        assert_eq!(p.scan_multiplier(), NORMAL_SCAN_MULTIPLIER);
        assert_eq!(p.min_confidence(), NORMAL_MIN_CONFIDENCE);
    }

    #[test]
    fn activate_sets_fields_and_flag() {
        let p = WarModeProfile::inactive();
        p.activate(2.0, 0.6);
        assert!(p.is_active());
        assert_eq!(p.scan_multiplier(), 2.0);
        assert_eq!(p.min_confidence(), 0.6);
    }

    #[test]
    fn deactivate_resets_to_baseline() {
        let p = WarModeProfile::inactive();
        p.activate(2.0, 0.6);
        p.deactivate();
        assert!(!p.is_active());
        assert_eq!(p.scan_multiplier(), NORMAL_SCAN_MULTIPLIER);
        assert_eq!(p.min_confidence(), NORMAL_MIN_CONFIDENCE);
    }

    #[test]
    fn repeat_activate_is_idempotent_for_same_values() {
        let p = WarModeProfile::inactive();
        p.activate(2.0, 0.6);
        p.activate(2.0, 0.6);
        assert!(p.is_active());
        assert_eq!(p.scan_multiplier(), 2.0);
        assert_eq!(p.min_confidence(), 0.6);
    }

    #[test]
    fn activate_with_new_values_overwrites_old_profile() {
        let p = WarModeProfile::inactive();
        p.activate(2.0, 0.6);
        p.activate(3.0, 0.7);
        assert!(p.is_active());
        assert_eq!(p.scan_multiplier(), 3.0);
        assert_eq!(p.min_confidence(), 0.7);
    }
}
