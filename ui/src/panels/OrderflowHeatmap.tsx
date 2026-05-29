// Orderflow Heatmap — buy/sell pressure stripes per price level (R2.4, R20.3).
// Critical panel: stays vivid in high-vol mode.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { paiseToInr } from "../lib/format";

export function OrderflowHeatmap(): JSX.Element {
  const heatmaps = useCockpitStore((s) => s.orderflow.heatmaps);
  const symbols = Object.keys(heatmaps);

  if (symbols.length === 0) {
    return (
      <Panel title="Orderflow Heatmap" synthChannel="orderflow" critical>
        <p className="text-slate-500">Awaiting first of.heatmap.* frame …</p>
      </Panel>
    );
  }

  return (
    <Panel
      title="Orderflow Heatmap"
      synthChannel="orderflow"
      critical
      status={<span>{symbols.length} symbols</span>}
    >
      <div className="space-y-3">
        {symbols.map((sym) => {
          const hm = heatmaps[sym];
          const total = hm.cells.reduce(
            (acc, c) => acc + c.buy_qty + c.sell_qty,
            0,
          );
          return (
            <div key={sym}>
              <div className="mb-1 flex items-center justify-between text-xs text-slate-400">
                <span className="font-semibold text-slate-200">{sym}</span>
                <span className="font-mono text-slate-500">
                  {hm.cells.length} levels · {total.toLocaleString()} qty
                </span>
              </div>
              <div className="space-y-[1px]">
                {hm.cells.slice(0, 12).map((cell) => {
                  const sum = cell.buy_qty + cell.sell_qty || 1;
                  const buyW = (cell.buy_qty / sum) * 100;
                  const sellW = (cell.sell_qty / sum) * 100;
                  return (
                    <div
                      key={cell.price_paise}
                      className="flex items-center font-mono text-[10px]"
                    >
                      <span className="w-20 text-right pr-2 text-slate-400">
                        {paiseToInr(cell.price_paise)}
                      </span>
                      <div className="flex-1 flex h-3">
                        <div
                          className="bg-hedge-ok/70"
                          style={{ width: `${buyW}%` }}
                          title={`buy ${cell.buy_qty}`}
                        />
                        <div
                          className="bg-hedge-danger/70"
                          style={{ width: `${sellW}%` }}
                          title={`sell ${cell.sell_qty}`}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </Panel>
  );
}
