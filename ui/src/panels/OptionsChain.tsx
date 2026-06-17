// Options Chain — OI snapshot per strike, sourced from /market `oi` events.
// Non-critical panel: dims in high-vol mode (R20.4).

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { EmptyState } from "../components/EmptyState";
import { paiseToInr } from "../lib/format";

export function OptionsChain(): JSX.Element {
  const oi = useCockpitStore((s) => s.market.oi);
  const symbols = Object.keys(oi).sort();

  return (
    <Panel
      title="Options Chain"
      synthChannel="market"
      status={<span>{symbols.length} underlyings</span>}
    >
      {symbols.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="space-y-3 max-h-72 overflow-y-auto">
          {symbols.map((sym) => {
            const u = oi[sym];
            return (
              <div key={sym}>
                <div className="mb-1 flex items-baseline justify-between text-xs">
                  <span className="font-semibold text-slate-200">{sym}</span>
                  <span className="font-mono text-slate-500">
                    {u.expiry ?? "—"} · {u.strikes.length} strikes
                  </span>
                </div>
                <table className="w-full font-mono text-[11px]">
                  <thead className="text-slate-500">
                    <tr>
                      <th className="text-right">Call OI</th>
                      <th className="text-right">ΔCall</th>
                      <th className="text-center">Strike</th>
                      <th className="text-right">ΔPut</th>
                      <th className="text-right">Put OI</th>
                    </tr>
                  </thead>
                  <tbody>
                    {u.strikes.slice(0, 8).map((s) => (
                      <tr key={s.strike_paise} className="border-t border-slate-800/40">
                        <td className="text-right">{(s.call_oi ?? 0).toLocaleString()}</td>
                        <td className="text-right text-hedge-ok">
                          {(s.call_chg_oi ?? 0).toLocaleString()}
                        </td>
                        <td className="text-center text-slate-200">
                          {paiseToInr(s.strike_paise)}
                        </td>
                        <td className="text-right text-hedge-danger">
                          {(s.put_chg_oi ?? 0).toLocaleString()}
                        </td>
                        <td className="text-right">{(s.put_oi ?? 0).toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            );
          })}
        </div>
      )}
    </Panel>
  );
}
