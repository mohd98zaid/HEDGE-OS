// Alerts panel.
//
// Ordering contract (R20.5):
//   1. Severity rank: critical → high → medium → low → info
//   2. Within a severity bucket, newest first by `ts_ns`.
//
// The store applies the same comparator at insert time (see
// `sortAlerts` in cockpitStore.ts) so the list is rendered in O(n) here.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { tsAgo } from "../lib/format";
import type { AlertSeverity } from "../types";

const TONE: Record<AlertSeverity, string> = {
  critical: "border-hedge-danger/60 bg-hedge-danger/10 text-hedge-danger",
  high: "border-hedge-warn/60 bg-hedge-warn/10 text-hedge-warn",
  medium: "border-amber-500/40 bg-amber-500/5 text-amber-300",
  low: "border-slate-700 bg-slate-900 text-slate-300",
  info: "border-slate-700 bg-slate-900/60 text-slate-400",
};

export function Alerts(): JSX.Element {
  const list = useCockpitStore((s) => s.alerts.list);

  return (
    <Panel title="Alerts" status={<span>{list.length} active</span>}>
      {list.length === 0 ? (
        <p className="text-slate-500">No alerts.</p>
      ) : (
        <ul className="space-y-2 max-h-72 overflow-y-auto">
          {list.slice(0, 50).map((a) => (
            <li
              key={a.id}
              className={`rounded border px-2 py-1 text-xs ${TONE[a.severity]}`}
            >
              <div className="flex items-baseline justify-between">
                <span className="font-semibold uppercase tracking-wider text-[10px]">
                  {a.severity}
                </span>
                <span className="font-mono text-[10px] text-slate-500">{tsAgo(a.ts_ns)}</span>
              </div>
              <div>{a.title}</div>
              {a.body ? <div className="text-slate-400 text-[11px]">{a.body}</div> : null}
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
