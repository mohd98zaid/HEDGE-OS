// /warmode — Market_Open_War_Mode presentation state.
//
// Mirrors `WarModeConfig` (`crates/hedge-config/src/models.rs`) and the
// `ops.warmode.<phase>` JSON wire schema
// (`crates/hedge-schemas/json_schemas/ops_warmode.schema.json`):
//
//   * `active`           — true while the IST clock is inside
//                          `[start_ist, end_ist)` (default
//                          09:15:00 – 09:45:00 IST).
//   * `minConfidence`    — `war_mode.min_confidence` floor (default 0.6).
//                          The /signals reducer suppresses any
//                          `RankedSignal` whose `confidence` is below
//                          this floor while War_Mode is active (R26.3).
//   * `scanMultiplier`   — `war_mode.scan_multiplier` (default 2.0).
//                          Surfaced for the App-level reduced-clutter
//                          presentation profile and for instrumentation
//                          panels that show the active mode profile.
//
// The cockpit infers `active` from the local IST clock (mirroring the
// `WarModeController` server-side state machine in `hedge-session`)
// rather than waiting for an `ops.warmode.*` push. That keeps the UI in
// agreement with the canonical R26.1 IST-window membership rule even if
// the gateway has not yet relayed the latest transition.

/** R26 — Market_Open_War_Mode presentation state for the cockpit. */
export interface WarModeStatus {
  /** True while we are inside `[start_ist, end_ist)` in IST. */
  active: boolean;
  /** Floor below which signals are suppressed in the cockpit (R26.3). */
  minConfidence: number;
  /** Scan-frequency multiplier (R26.2). Informational on the UI side. */
  scanMultiplier: number;
}
