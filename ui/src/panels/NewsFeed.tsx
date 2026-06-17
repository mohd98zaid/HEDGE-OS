// News Feed — `ai.news.impact.*` entries, newest first (R20.3).

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { EmptyState } from "../components/EmptyState";
import { tsAgo } from "../lib/format";

export function NewsFeed(): JSX.Element {
  const recent = useCockpitStore((s) => s.news.recent);

  return (
    <Panel title="News" synthChannel="news" status={<span>{recent.length} items</span>}>
      {recent.length === 0 ? (
        <EmptyState isEngineBacked />
      ) : (
        <ul className="space-y-2 max-h-80 overflow-y-auto">
          {recent.slice(0, 30).map((n) => (
            <li
              key={`${n.headline_id}-${n.ts_ns ?? 0}`}
              className="border-b border-slate-800/40 pb-1"
            >
              <div className="flex items-baseline justify-between text-[11px]">
                <span className="font-semibold text-slate-200">{n.symbol}</span>
                <span className="font-mono text-slate-500">{tsAgo(n.ts_ns)}</span>
              </div>
              <div className="text-xs text-slate-300">
                {n.headline ?? n.headline_id}
              </div>
              <div className="mt-1 flex gap-3 font-mono text-[10px] text-slate-500">
                <span>
                  sent {n.sentiment > 0 ? "+" : ""}
                  {n.sentiment.toFixed(2)}
                </span>
                <span>impact {(n.impact_magnitude * 100).toFixed(0)}%</span>
                {n.fast_path ? <span className="text-hedge-warn">fast-path</span> : null}
                {n.slow_path_pending ? <span className="text-hedge-accent">slow-path …</span> : null}
              </div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
