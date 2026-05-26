// useWarMode — drives the cockpit's Market_Open_War_Mode presentation
// state.
//
// R26 contract:
//   * R26.1 — War_Mode is active while the local IST clock is inside
//     `[start_ist, end_ist)` (defaults: 09:15:00–09:45:00).
//   * R26.2 — Hot_Path components apply the configured War_Mode profile
//     server-side (handled by `hedge-features` / `hedge-orderflow` /
//     `hedge-signals`); the cockpit surfaces the same window
//     deterministically from the IST clock so the trader's view is
//     never out of sync with the engine's gate.
//   * R26.3 — The Human_Control_UI applies a reduced-clutter
//     presentation profile and suppresses signals below
//     `war_mode.min_confidence`. The clutter side is exposed here as
//     `dimClass`; the suppression side is enforced inside the
//     `/signals` reducer (`reduceSignals` in `cockpitStore.ts`) which
//     reads from the same store slice this hook writes.
//   * R26.4 — Mode transitions are emitted to NATS by the
//     `WarModeController` task in `hedge-session::controller`; the
//     cockpit does not depend on receiving those events because the
//     IST window is the single source of truth for membership.
//
// Like `useHighVolMode`, this hook reads exclusively from the local
// process: `loadConfig()` for the IST window and a 1 Hz timer for the
// boundary check. There is no network call (R20.2).

import { useEffect, useMemo, useState } from "react";

import { loadConfig } from "../lib/config";
import { useCockpitStore } from "../store/cockpitStore";
import type { WarModeStatus } from "../types";

/**
 * Returned snapshot. Mirrors the [`WarModeStatus`] slice but adds a
 * `dimClass` Tailwind hint and pre-formatted summary so panels can
 * render without re-deriving the same computation per render.
 */
export interface WarModeView {
  /** R26 — War_Mode active right now. */
  active: boolean;
  /** Floor below which signals are suppressed (R26.3). */
  minConfidence: number;
  /** Scan-frequency multiplier (R26.2). */
  scanMultiplier: number;
  /**
   * Tailwind class to apply on non-critical panels while War_Mode is
   * active (R26.3 — reduced clutter). Empty when the mode is off so the
   * panel chrome is unmodified.
   */
  dimClass: string;
  /** Tailwind class for the App-level header pill. */
  pillClass: string;
}

/** Parse `"HH:MM:SS"` into seconds-since-midnight. Defensive against
 * malformed env values — falls back to `0` on parse failure. */
const parseHms = (s: string): number => {
  const parts = s.split(":");
  if (parts.length !== 3) return 0;
  const h = Number(parts[0]);
  const m = Number(parts[1]);
  const sec = Number(parts[2]);
  if (![h, m, sec].every(Number.isFinite)) return 0;
  return h * 3600 + m * 60 + sec;
};

/** Current IST seconds-since-midnight. IST is UTC+05:30 with no DST. */
const istSecondsSinceMidnight = (now: Date): number => {
  // `Date.getUTCHours()` etc. give us UTC components. IST = UTC + 5:30.
  const utcSec =
    now.getUTCHours() * 3600 +
    now.getUTCMinutes() * 60 +
    now.getUTCSeconds();
  const istSec = (utcSec + 5 * 3600 + 30 * 60) % (24 * 3600);
  return istSec;
};

/** Compute whether `now` falls inside `[start, end)` in IST. */
export const isWithinIstWindow = (now: Date, start: string, end: string): boolean => {
  const t = istSecondsSinceMidnight(now);
  const s = parseHms(start);
  const e = parseHms(end);
  if (s >= e) return false;
  return t >= s && t < e;
};

export function useWarMode(): WarModeView {
  const cfg = useMemo(loadConfig, []);
  const setWarMode = useCockpitStore((s) => s.setWarMode);
  const [, force] = useState(0);

  // Re-render once per second so the boundary crossing is reflected
  // promptly. The IST clock advances independently of any NATS push
  // so this is the canonical source for R26.1 in the cockpit.
  useEffect(() => {
    const id = setInterval(() => force((n) => n + 1), 1_000);
    return () => clearInterval(id);
  }, []);

  const active = isWithinIstWindow(new Date(), cfg.warModeStartIst, cfg.warModeEndIst);
  const status: WarModeStatus = useMemo(
    () => ({
      active,
      minConfidence: cfg.warModeMinConfidence,
      scanMultiplier: cfg.warModeScanMultiplier,
    }),
    [active, cfg.warModeMinConfidence, cfg.warModeScanMultiplier],
  );

  // Push the latest snapshot to the store so the /signals reducer can
  // gate ranked signals below the min-confidence floor (R26.3).
  useEffect(() => {
    setWarMode(status);
  }, [status, setWarMode]);

  return {
    active,
    minConfidence: cfg.warModeMinConfidence,
    scanMultiplier: cfg.warModeScanMultiplier,
    // Reduced clutter: dim non-critical panels while War_Mode is active.
    // Stays under the workspace's existing `opacity-50` Tailwind pattern
    // used by `useHighVolMode` so the two modes never fight each other
    // for the same visual state.
    dimClass: active ? "opacity-60" : "",
    pillClass: active
      ? "ml-1 rounded bg-hedge-accent/20 text-hedge-accent px-1"
      : "",
  };
}
