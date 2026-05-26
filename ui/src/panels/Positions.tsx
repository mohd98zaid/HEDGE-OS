// Positions — open positions sourced from /risk `pos.update` (R20.3).
// Non-critical: dims in high-vol mode.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { paiseToInr, formatInr } from "../lib/format";

export function Positions(): JSX.Element {
  const positions = useCockpitStore((s) => s.risk.positions);
  const syms = Object.keys(positions).sort();

  return (
    <Panel title="Positions" status={<span>{syms.length} open</span>}>
      {syms.length === 0 ? (
        <p className="text-slate-500">No open positions.</p>
      ) : (
        <table className="w-full font-mono text-xs">
          <thead className="text-slate-500">
            <tr>
              <th className="text-left">Symbol</th>
              <th className="text-right">Qty</th>
              <th className="text-right">Avg</th>
              <th className="text-right">Realised</th>
              <th className="text-right">Unrealised</th>
            </tr>
          </thead>
          <tbody>
            {syms.map((sym) => {
              const p = positions[sym];
              const r = p.realised_pnl_inr ?? 0;
              const u = p.unrealised_pnl_inr ?? 0;
              return (
                <tr key={sym} className="border-t border-slate-800/50">
                  <td className="text-slate-200 py-1">{sym}</td>
                  <td className="text-right">{p.quantity}</td>
                  <td className="text-right">{paiseToInr(p.avg_price_paise)}</td>
                  <td className={`text-right ${r >= 0 ? "text-hedge-ok" : "text-hedge-danger"}`}>
                    {formatInr(r)}
                  </td>
                  <td className={`text-right ${u >= 0 ? "text-hedge-ok" : "text-hedge-danger"}`}>
                    {formatInr(u)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </Panel>
  );
}
