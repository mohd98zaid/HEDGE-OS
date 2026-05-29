// Live Market — realtime ticker board fed by the /market channel
// (R20.3, R20.4). Critical panel: stays vivid in high-vol mode.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { paiseToInr, tsAgo } from "../lib/format";
import { useHighVolMode } from "../hooks/useHighVolMode";

export function LiveMarket(): JSX.Element {
  const ticks = useCockpitStore((s) => s.market.ticks);
  const breadth = useCockpitStore((s) => s.market.breadthVolatility);
  const { active, refreshMs } = useHighVolMode();
  const symbols = Object.keys(ticks).sort();

  return (
    <Panel
      title="Live Market"
      synthChannel="market"
      critical
      status={
        <span>
          {active ? "HIGH-VOL " : ""}
          {refreshMs} ms · {symbols.length} sym · σ{" "}
          {breadth ? (breadth.volatility * 100).toFixed(2) : "—"}%
        </span>
      }
    >
      {symbols.length === 0 ? (
        <p className="text-slate-500">Awaiting first md.tick.* frame …</p>
      ) : (
        <table className="w-full font-mono text-xs">
          <thead className="text-slate-500">
            <tr>
              <th className="text-left">Symbol</th>
              <th className="text-right">Bid</th>
              <th className="text-right">Ask</th>
              <th className="text-right">LTP</th>
              <th className="text-right">Recv</th>
            </tr>
          </thead>
          <tbody>
            {symbols.map((sym) => {
              const t = ticks[sym];
              return (
                <tr key={sym} className="border-t border-slate-800/50">
                  <td className="py-1 text-slate-200">{sym}</td>
                  <td className="text-right text-hedge-ok">{paiseToInr(t.bid_paise)}</td>
                  <td className="text-right text-hedge-danger">{paiseToInr(t.ask_paise)}</td>
                  <td className="text-right">{paiseToInr(t.ltp_paise)}</td>
                  <td className="text-right text-slate-500">{tsAgo(t.ts_recv_ns)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </Panel>
  );
}
