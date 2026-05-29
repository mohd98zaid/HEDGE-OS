// Execution Panel — recent order/fill activity from /exec (R20.3). Critical.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { paiseToInr, tsAgo } from "../lib/format";

export function ExecutionPanel(): JSX.Element {
  const recent = useCockpitStore((s) => s.exec.recent);
  const failovers = useCockpitStore((s) => s.exec.failovers);

  return (
    <Panel
      title="Execution"
      synthChannel="exec"
      critical
      status={<span>{recent.length} recent</span>}
    >
      {failovers[0] ? (
        <div className="mb-2 rounded border border-hedge-warn/40 bg-hedge-warn/10 p-2 text-xs text-hedge-warn">
          broker failover: {failovers[0].from} → {failovers[0].to}
          {failovers[0].reason ? ` (${failovers[0].reason})` : ""}
        </div>
      ) : null}
      {recent.length === 0 ? (
        <p className="text-slate-500">No exec activity yet.</p>
      ) : (
        <ul className="font-mono text-[11px] space-y-[2px] max-h-64 overflow-y-auto">
          {recent.slice(0, 30).map((e, i) => {
            const cid = "data" in e ? (e.data as { correlation_id?: string }).correlation_id ?? "" : "";
            return (
              <li
                key={`${cid}-${i}`}
                className="flex justify-between border-b border-slate-800/30 py-1"
              >
                <span className="text-slate-300">{e.kind}</span>
                {e.kind === "order" || e.kind === "fill" ? (
                  <span className="text-slate-400">
                    {e.data.symbol ?? "—"} · {e.data.state} · {e.data.filled_qty}
                    {typeof e.data.avg_fill_paise === "number"
                      ? ` @ ${paiseToInr(e.data.avg_fill_paise)}`
                      : ""}
                  </span>
                ) : e.kind === "broker.failover" ? (
                  <span className="text-hedge-warn">
                    {e.data.from} → {e.data.to}
                  </span>
                ) : (
                  <span className="text-slate-400">
                    {e.data.symbol} · ₹{e.data.pnl_inr}
                  </span>
                )}
                <span className="text-slate-600">{tsAgo("data" in e ? (e.data as { ts_ns?: number }).ts_ns : undefined)}</span>
              </li>
            );
          })}
        </ul>
      )}
    </Panel>
  );
}
