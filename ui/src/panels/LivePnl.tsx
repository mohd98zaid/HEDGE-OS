// Live PnL — portfolio summary fed by /risk `pos.risk_state`.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { EmptyState } from "../components/EmptyState";
import { formatInr } from "../lib/format";

export function LivePnl(): JSX.Element {
  const portfolio = useCockpitStore((s) => s.risk.portfolio);
  const target = useCockpitStore((s) => s.risk.daily_target_reached);

  return (
    <Panel
      title="Live PnL"
      synthChannel="risk"
      status={target ? <span className="text-hedge-ok">target reached</span> : null}
    >
      {!portfolio ? (
        <EmptyState isEngineBacked />
      ) : (
        <dl className="grid grid-cols-2 gap-2 text-sm">
          <dt className="text-slate-500">Portfolio PnL</dt>
          <dd
            className={`text-right font-mono ${
              portfolio.portfolio_pnl_inr >= 0 ? "text-hedge-ok" : "text-hedge-danger"
            }`}
          >
            {formatInr(portfolio.portfolio_pnl_inr)}
          </dd>
          <dt className="text-slate-500">Gross Exposure</dt>
          <dd className="text-right font-mono">{formatInr(portfolio.gross_exposure_inr)}</dd>
          <dt className="text-slate-500">Drawdown</dt>
          <dd className="text-right font-mono text-hedge-warn">
            {formatInr(portfolio.drawdown_inr)}
          </dd>
        </dl>
      )}
    </Panel>
  );
}
