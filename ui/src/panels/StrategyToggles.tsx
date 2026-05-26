// Strategy Toggles — per-strategy enable/disable controls (R20.7).
//
// Each row publishes `trader.intent.strategy_toggle { strategy, enabled }` on
// the /control channel; the Risk_Engine and Signal_Engine apply the change
// under Authority_Hierarchy. The panel keeps an optimistic mirror in the
// signals slice so the checkbox responds instantly while the intent travels
// UI → gateway → Risk_Engine → Signal_Engine.
//
// `last emitted` shows when each strategy last fired a signal — sourced
// from `signals.lastEmittedAtByStrategy`, which is stamped by the cockpit
// store on every inbound `sig.emitted` event.

import { Panel } from "../components/Panel";
import { useCockpitStore } from "../store/cockpitStore";
import { tsAgo } from "../lib/format";
import {
  STRATEGIES,
  STRATEGY_LABELS,
  type StrategyName,
  type TraderIntent,
} from "../types";

export interface StrategyTogglesProps {
  sendIntent: (intent: TraderIntent) => boolean;
}

export function StrategyToggles({ sendIntent }: StrategyTogglesProps): JSX.Element {
  const enabledByStrategy = useCockpitStore((s) => s.signals.enabledByStrategy);
  const lastEmitted = useCockpitStore((s) => s.signals.lastEmittedAtByStrategy);
  const setStrategyEnabled = useCockpitStore((s) => s.setStrategyEnabled);

  const toggle = (s: StrategyName, next: boolean): void => {
    // Optimistic local update; the authoritative state lives in the
    // Signal_Engine and is implicit in whether new signals arrive.
    setStrategyEnabled(s, next);
    sendIntent({ kind: "strategy_toggle", strategy: s, enabled: next });
  };

  return (
    <Panel title="Strategies">
      <ul className="divide-y divide-slate-800/60">
        {STRATEGIES.map((s) => {
          // Default-enabled until the trader flips it off (R4.5).
          const enabled = enabledByStrategy[s] ?? true;
          const ts = lastEmitted[s];
          return (
            <li key={s} className="flex items-center justify-between py-2 gap-3">
              <label className="flex items-center gap-2 text-xs flex-1 min-w-0 cursor-pointer">
                <input
                  type="checkbox"
                  checked={enabled}
                  onChange={(e) => toggle(s, e.target.checked)}
                  className="h-4 w-4 cursor-pointer accent-hedge-accent"
                />
                <span className="truncate">
                  <span className="font-medium text-slate-200">
                    {STRATEGY_LABELS[s]}
                  </span>
                  <span className="ml-2 text-[10px] text-slate-500 font-mono">
                    {s}
                  </span>
                </span>
              </label>
              <span
                className={`text-[10px] font-mono whitespace-nowrap ${
                  enabled ? "text-slate-400" : "text-slate-600"
                }`}
              >
                {ts ? `last ${tsAgo(ts)}` : "—"}
              </span>
            </li>
          );
        })}
      </ul>
      <p className="mt-3 text-[10px] leading-snug text-slate-500">
        Toggles publish <code className="font-mono">trader.intent.strategy_toggle</code> and
        are reconciled by the Signal_Engine under Authority_Hierarchy.
      </p>
    </Panel>
  );
}
