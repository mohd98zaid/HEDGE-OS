// AI Explanations — text rationales attached to ranked signals (R20.3).

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { EmptyState } from "../components/EmptyState";
import { pct, tsAgo } from "../lib/format";

export function AiExplanations(): JSX.Element {
  const recent = useCockpitStore((s) => s.signals.recent);
  const withText = recent.filter((s) => s.explanation && !s.shadow).slice(0, 12);

  return (
    <Panel title="AI Explanations" synthChannel="signals" status={<span>{withText.length} rationales</span>}>
      {withText.length === 0 ? (
        <EmptyState isEngineBacked />
      ) : (
        <ul className="space-y-3 max-h-80 overflow-y-auto">
          {withText.map((s) => (
            <li key={s.correlation_id} className="border-b border-slate-800/40 pb-2">
              <div className="flex items-baseline justify-between text-[11px]">
                <span className="font-semibold text-slate-200">
                  {s.symbol} · {s.strategy}
                </span>
                <span className="font-mono text-hedge-accent">
                  {s.trade_confidence_score != null ? pct(s.trade_confidence_score, 1) : "—"}
                </span>
              </div>
              <p className="mt-1 text-xs text-slate-300">{s.explanation}</p>
              <div className="mt-1 text-[10px] text-slate-500">{tsAgo(s.ts_ns)}</div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
