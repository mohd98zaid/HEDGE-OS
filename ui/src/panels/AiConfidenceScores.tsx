// AI Confidence Scores — joins `sig.emitted` ⨝ `ai.rank.<correlation_id>`
// (already correlated server-side by ui-gateway, R20.3). Renders the
// trade_confidence_score plus the five-factor breakdown for the most recent
// non-shadow signals.
//
// Defensive shadow filter: ui-gateway already discards shadowed AI sources on
// /signals (task 36.1). This panel adds a redundant `payload.shadow` guard
// with a console.warn so a regression upstream is immediately visible.

import { useEffect } from "react";

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { tsAgo, pct } from "../lib/format";
import type { RankFactors, RankedSignal } from "../types";

const FACTOR_KEYS: (keyof RankFactors)[] = [
  "orderflow",
  "technical_strength",
  "news_sentiment",
  "market_regime",
  "trader_discipline",
];

const FACTOR_LABELS: Record<keyof RankFactors, string> = {
  orderflow: "Orderflow",
  technical_strength: "Tech",
  news_sentiment: "News",
  market_regime: "Regime",
  trader_discipline: "Discipline",
};

function FactorBar({ value }: { value: number }): JSX.Element {
  const w = `${Math.max(0, Math.min(1, value)) * 100}%`;
  return (
    <div className="h-1 w-full bg-slate-800 rounded">
      <div className="h-1 bg-hedge-accent rounded" style={{ width: w }} />
    </div>
  );
}

function shadowGuard(sig: RankedSignal): boolean {
  // The ui-gateway already filters shadowed AI sources, but mirror the guard
  // here so a regression surfaces without silent data corruption.
  if (sig.shadow) {
    // eslint-disable-next-line no-console
    console.warn(
      "[ai-confidence] shadowed signal leaked through ui-gateway",
      sig.correlation_id,
    );
    return true;
  }
  return false;
}

export function AiConfidenceScores(): JSX.Element {
  const recent = useCockpitStore((s) => s.signals.recent);

  useEffect(() => {
    // Sanity-check on mount in case the store was hydrated externally.
    for (const sig of recent) shadowGuard(sig);
  }, [recent]);

  const visible = recent.filter((s) => !shadowGuard(s)).slice(0, 8);

  return (
    <Panel
      title="AI Confidence Scores"
      status={<span>{visible.length} ranked</span>}
    >
      {visible.length === 0 ? (
        <p className="text-slate-500">Awaiting ranked signals …</p>
      ) : (
        <ul className="space-y-2 max-h-80 overflow-y-auto">
          {visible.map((s) => (
            <li
              key={s.correlation_id}
              className="rounded border border-slate-800/60 bg-slate-900/40 p-2"
            >
              <div className="flex items-baseline justify-between text-xs">
                <span className="font-semibold text-slate-200">
                  {s.symbol} · {s.strategy} · {s.side}
                </span>
                <span className="font-mono text-hedge-accent">
                  {s.trade_confidence_score != null ? pct(s.trade_confidence_score, 1) : "—"}
                </span>
              </div>
              <div className="mt-1 grid grid-cols-5 gap-1 text-[10px] text-slate-400">
                {FACTOR_KEYS.map((k) => {
                  const v = s.factors?.[k] ?? 0;
                  return (
                    <div key={k}>
                      <div className="flex justify-between">
                        <span>{FACTOR_LABELS[k]}</span>
                        <span className="font-mono">{(v * 100).toFixed(0)}</span>
                      </div>
                      <FactorBar value={v} />
                    </div>
                  );
                })}
              </div>
              <div className="mt-1 flex justify-between text-[10px] text-slate-500">
                <span>cid {s.correlation_id.slice(0, 8)}</span>
                <span>{tsAgo(s.ts_ns)}</span>
              </div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
