// Risk Panel — kill-switch, drawdown, recent decisions, cooldowns. Critical.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { formatInr, tsAgo } from "../lib/format";

export function RiskPanel(): JSX.Element {
  const ks = useCockpitStore((s) => s.risk.killswitch);
  const portfolio = useCockpitStore((s) => s.risk.portfolio);
  const decisions = useCockpitStore((s) => s.risk.decisions);
  const cooldowns = useCockpitStore((s) => s.risk.cooldowns);

  return (
    <Panel
      title="Risk"
      synthChannel="risk"
      critical
      status={
        <span className={ks.active ? "text-hedge-danger" : "text-hedge-ok"}>
          kill-switch {ks.active ? "ENGAGED" : "armed"}
        </span>
      }
    >
      <div className="space-y-3">
        {portfolio ? (
          <dl className="grid grid-cols-3 gap-2 text-xs">
            <div>
              <dt className="text-slate-500">PnL</dt>
              <dd
                className={`font-mono ${
                  portfolio.portfolio_pnl_inr >= 0 ? "text-hedge-ok" : "text-hedge-danger"
                }`}
              >
                {formatInr(portfolio.portfolio_pnl_inr)}
              </dd>
            </div>
            <div>
              <dt className="text-slate-500">Exposure</dt>
              <dd className="font-mono">{formatInr(portfolio.gross_exposure_inr)}</dd>
            </div>
            <div>
              <dt className="text-slate-500">Drawdown</dt>
              <dd className="font-mono text-hedge-warn">{formatInr(portfolio.drawdown_inr)}</dd>
            </div>
          </dl>
        ) : null}

        {Object.keys(cooldowns).length > 0 ? (
          <div>
            <div className="text-[10px] uppercase tracking-wider text-slate-500">cooldowns</div>
            <ul className="font-mono text-xs">
              {Object.entries(cooldowns).map(([sym, until]) => (
                <li key={sym} className="flex justify-between">
                  <span>{sym}</span>
                  <span className="text-slate-400">{tsAgo(until)}</span>
                </li>
              ))}
            </ul>
          </div>
        ) : null}

        <div>
          <div className="text-[10px] uppercase tracking-wider text-slate-500">recent decisions</div>
          <ul className="font-mono text-[11px] max-h-32 overflow-y-auto">
            {decisions.slice(0, 8).map((d) => (
              <li
                key={d.correlation_id}
                className="flex justify-between border-b border-slate-800/30 py-1"
              >
                <span className={d.approved ? "text-hedge-ok" : "text-hedge-danger"}>
                  {d.approved ? "approved" : "denied"}
                </span>
                <span className="text-slate-400">
                  {d.rationale_code ?? "—"} · {d.sized_quantity ?? 0}
                </span>
                <span className="text-slate-600">{tsAgo(d.ts_ns)}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </Panel>
  );
}
