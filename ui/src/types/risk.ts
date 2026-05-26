// /risk — `risk.*`, `pos.risk_state`, `pos.update.*` (R20.3).

export interface RiskDecision {
  correlation_id: string;
  approved: boolean;
  rationale_code?: number;
  rationale?: string;
  sized_quantity?: number;
  ts_ns?: number;
}

export interface KillSwitchState {
  active: boolean;
  reason?: string;
  ts_ns?: number;
}

export interface DailyTargetReached {
  ts_ns?: number;
  pnl_inr?: number;
}

export interface CooldownState {
  symbol: string;
  until_ts_ns?: number;
}

export interface PositionUpdate {
  symbol: string;
  quantity: number;
  avg_price_paise: number;
  realised_pnl_inr?: number;
  unrealised_pnl_inr?: number;
  ts_ns?: number;
}

export interface PortfolioRiskState {
  gross_exposure_inr: number;
  portfolio_pnl_inr: number;
  drawdown_inr: number;
  ts_ns?: number;
}

export type RiskEvent =
  | { kind: "decision"; data: RiskDecision }
  | { kind: "killswitch"; data: KillSwitchState }
  | { kind: "target.reached"; data: DailyTargetReached }
  | { kind: "cooldown"; data: CooldownState }
  | { kind: "pos.update"; data: PositionUpdate }
  | { kind: "pos.risk_state"; data: PortfolioRiskState };
