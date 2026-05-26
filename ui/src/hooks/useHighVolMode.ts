// useHighVolMode — drives the cockpit's high-volatility presentation mode.
//
// R20.4 contract:
//   * When `md.breadth.volatility` exceeds `ui.high_vol_threshold`, critical
//     panels (Live Market, Orderflow Heatmap, Risk, Execution, Latency)
//     refresh faster and secondary visual elements (tooltips, non-critical
//     panels) are dimmed.
//   * The threshold is configured server-side (default 0.05, see design.md
//     § Configuration Surface) and surfaced to the React build via the
//     `VITE_UI_HIGH_VOL_THRESHOLD` env var.
//
// The hook reads exclusively from the /market slice — there is no separate
// poll/REST source (R20.2).

import { useMemo } from "react";

import { loadConfig } from "../lib/config";
import { useCockpitStore } from "../store/cockpitStore";

export interface HighVolMode {
  /** True when realised-volatility breadth > configured threshold. */
  active: boolean;
  /** Latest breadth.volatility reading, undefined until first sample. */
  volatility: number | undefined;
  /** Configured threshold (default 0.05). */
  threshold: number;
  /** Refresh tick (ms) critical panels should use right now. */
  refreshMs: number;
  /** Tailwind class to dim secondary panels when active. */
  dimClass: string;
}

export function useHighVolMode(): HighVolMode {
  const cfg = useMemo(loadConfig, []);
  const reading = useCockpitStore((s) => s.market.breadthVolatility?.volatility);
  const active = reading != null && reading > cfg.highVolThreshold;
  return {
    active,
    volatility: reading,
    threshold: cfg.highVolThreshold,
    refreshMs: active ? cfg.refreshMsHighVol : cfg.refreshMsNormal,
    dimClass: active ? "opacity-50" : "",
  };
}
