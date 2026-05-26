//! Runtime War_Mode profile tracker for the Orderflow_Engine.
//!
//! On `ops.warmode.start` the engine binary calls
//! [`WarModeProfile::activate`] with the configured `scan_multiplier`
//! and `min_confidence`; on `ops.warmode.end` it calls
//! [`WarModeProfile::deactivate`]. Reads on the steady-state hot loop
//! (`OrderflowEngine::ingest_book` / `ingest_tick`) go through wait-free
//! relaxed atomic loads — no lock bounce on the per-event path.
//!
//! ## Why this lives in `hedge-orderflow`
//!
//! Design § Operating Modes — Market_Open_War_Mode requires Hot_Path
//! components to apply "increased orderflow sensitivity" while War_Mode
//! is active (R26.2). The Orderflow_Engine hosts the absorption /
//! liquidity-gap detector logic; the multiplier scales the sensitivity
//! by lowering the detector's effective magnitude threshold (a higher
//! multiplier means the detector fires on smaller deviations, i.e.
//! "more sensitive").
//!
//! ## Allocation discipline
//!
//! Three atomics, no heap, no locks. The orderflow hot loop already
//! holds a `parking_lot::Mutex<HashMap<…>>` and a `Mutex<LiveBook>`;
//! adding another lock for War_Mode would compound contention. Atomics
//! sidestep the issue entirely.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Default scan multiplier when War_Mode is inactive — `1.0` (no
/// boost). Must match the design's Normal-mode behaviour.
pub const NORMAL_SCAN_MULTIPLIER: f32 = 1.0;

/// Default minimum confidence floor when War_Mode is inactive — `0.0`
/// (no gate). Mirrors the field on the wire schema for symmetry; the
/// Orderflow_Engine itself does not gate on confidence.
pub const NORMAL_MIN_CONFIDENCE: f32 = 0.0;

/// Wait-free War_Mode profile state shared between the binary's
/// `ops.warmode.*` subscriber and the orderflow hot loop.
///
/// Stored as three atomics so the read path is a single relaxed load —
/// no lock contention on the per-event path.
#[derive(Debug)]
pub struct WarModeProfile {
    /// `true` while `[ops.warmode.start, ops.warmode.end)` is active.
    active: AtomicBool,
    /// Scan-frequency multiplier from `WarModeConfig.scan_multiplier`.
    scan_multiplier_bits: AtomicU32,
    /// Confidence floor from `WarModeConfig.min_confidence`.
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

    /// Deactivate the profile and reset to Normal-mode defaults.
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        self.scan_multiplier_bits
            .store(NORMAL_SCAN_MULTIPLIER.to_bits(), Ordering::Relaxed);
        self.min_confidence_bits
            .store(NORMAL_MIN_CONFIDENCE.to_bits(), Ordering::Relaxed);
    }

    /// `true` while War_Mode is active.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Current scan multiplier. `1.0` when the profile is inactive.
    #[inline]
    pub fn scan_multiplier(&self) -> f32 {
        f32::from_bits(self.scan_multiplier_bits.load(Ordering::Relaxed))
    }

    /// Current minimum confidence floor. `0.0` when the profile is
    /// inactive.
    #[inline]
    pub fn min_confidence(&self) -> f32 {
        f32::from_bits(self.min_confidence_bits.load(Ordering::Relaxed))
    }

    /// Effective sensitivity scaling. Detectors that compare a measured
    /// magnitude against a fixed threshold should compare against
    /// `threshold / sensitivity_factor()` so a higher War_Mode
    /// multiplier yields a lower effective threshold ("more sensitive").
    /// Returns `1.0` when the profile is inactive — equivalent to
    /// "no scaling".
    #[inline]
    pub fn sensitivity_factor(&self) -> f32 {
        if self.is_active() {
            // Defensive against a misconfigured zero/negative
            // multiplier — fall back to the Normal-mode factor so the
            // detector does not divide by zero.
            let m = self.scan_multiplier();
            if m > 0.0 {
                m
            } else {
                NORMAL_SCAN_MULTIPLIER
            }
        } else {
            NORMAL_SCAN_MULTIPLIER
        }
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
        assert_eq!(p.sensitivity_factor(), NORMAL_SCAN_MULTIPLIER);
    }

    #[test]
    fn activate_sets_fields_and_flag() {
        let p = WarModeProfile::inactive();
        p.activate(2.0, 0.6);
        assert!(p.is_active());
        assert_eq!(p.scan_multiplier(), 2.0);
        assert_eq!(p.min_confidence(), 0.6);
        assert_eq!(p.sensitivity_factor(), 2.0);
    }

    #[test]
    fn deactivate_resets_to_baseline() {
        let p = WarModeProfile::inactive();
        p.activate(2.0, 0.6);
        p.deactivate();
        assert!(!p.is_active());
        assert_eq!(p.scan_multiplier(), NORMAL_SCAN_MULTIPLIER);
        assert_eq!(p.min_confidence(), NORMAL_MIN_CONFIDENCE);
        assert_eq!(p.sensitivity_factor(), NORMAL_SCAN_MULTIPLIER);
    }

    #[test]
    fn sensitivity_factor_handles_pathological_zero_multiplier() {
        // Defensive: if a misconfigured WarModeConfig publishes a
        // non-positive multiplier we fall back to 1.0 instead of
        // dividing by zero downstream.
        let p = WarModeProfile::inactive();
        p.activate(0.0, 0.6);
        assert_eq!(p.sensitivity_factor(), NORMAL_SCAN_MULTIPLIER);

        let p = WarModeProfile::inactive();
        p.activate(-1.0, 0.6);
        assert_eq!(p.sensitivity_factor(), NORMAL_SCAN_MULTIPLIER);
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
