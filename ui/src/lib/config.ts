// PROJECT HEDGE Human_Control_UI — config surface for the React cockpit.
//
// The single source of truth for `ui.high_vol_threshold` is the YAML config
// loaded by `hedge-config` (default 0.05, see `defaults::ui`). The gateway
// surfaces it to the React app via Vite environment variables so this build
// never reaches into Redis or NATS directly (R20.2).

const num = (v: string | undefined, fallback: number): number => {
  if (!v) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

export interface CockpitConfig {
  /** WebSocket base URL for the `ui-gateway` bridge. */
  gatewayUrl: string;
  /** `md.breadth.volatility` threshold above which the UI switches modes (R20.4). */
  highVolThreshold: number;
  /** Refresh tick (ms) used by the cockpit clock in normal mode. */
  refreshMsNormal: number;
  /** Refresh tick (ms) for critical panels under high-volatility mode. */
  refreshMsHighVol: number;
  /**
   * War_Mode start time, IST `HH:MM:SS`. Mirrors
   * `war_mode.start_ist` (default `09:15:00`, see
   * `crates/hedge-config/src/defaults.rs`). The cockpit infers
   * War_Mode membership from the local IST clock — see `useWarMode`.
   */
  warModeStartIst: string;
  /** War_Mode end time, IST `HH:MM:SS` (default `09:45:00`). */
  warModeEndIst: string;
  /** `war_mode.min_confidence` floor (R26.3, default 0.6). */
  warModeMinConfidence: number;
  /** `war_mode.scan_multiplier` (R26.2, default 2.0). */
  warModeScanMultiplier: number;
}

const defaultGateway = (): string => {
  // In `vite dev` we proxy `/ws` → `ws://hedge-ui-gateway:8080` (vite.config.ts).
  // In production the cockpit is served from the gateway, so a same-origin URL
  // works without configuration.
  if (typeof window !== "undefined") {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${window.location.host}/ws`;
  }
  return "ws://hedge-ui-gateway:8080/ws";
};

export const loadConfig = (): CockpitConfig => ({
  gatewayUrl: import.meta.env.VITE_HEDGE_GATEWAY_URL ?? defaultGateway(),
  highVolThreshold: num(import.meta.env.VITE_UI_HIGH_VOL_THRESHOLD, 0.05),
  refreshMsNormal: num(import.meta.env.VITE_UI_REFRESH_MS_NORMAL, 250),
  refreshMsHighVol: num(import.meta.env.VITE_UI_REFRESH_MS_HIGHVOL, 100),
  warModeStartIst:
    import.meta.env.VITE_UI_WAR_MODE_START_IST ?? "09:15:00",
  warModeEndIst:
    import.meta.env.VITE_UI_WAR_MODE_END_IST ?? "09:45:00",
  warModeMinConfidence: num(import.meta.env.VITE_UI_WAR_MODE_MIN_CONFIDENCE, 0.6),
  warModeScanMultiplier: num(import.meta.env.VITE_UI_WAR_MODE_SCAN_MULTIPLIER, 2.0),
});
