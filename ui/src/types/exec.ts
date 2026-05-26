// /exec — `exec.*` (R20.3).

export type OrderState =
  | "New"
  | "Submitted"
  | "PartiallyFilled"
  | "Filled"
  | "Cancelled"
  | "Rejected";

export interface ExecOrderUpdate {
  correlation_id: string;
  broker_order_id?: string;
  symbol?: string;
  state: OrderState;
  filled_qty: number;
  avg_fill_paise?: number;
  ts_ns?: number;
}

export interface ExecBrokerFailover {
  from: string;
  to: string;
  reason?: string;
  ts_ns?: number;
}

export interface TradeClosed {
  correlation_id: string;
  symbol: string;
  pnl_inr: number;
  ts_ns?: number;
}

export type ExecEvent =
  | { kind: "order"; data: ExecOrderUpdate }
  | { kind: "fill"; data: ExecOrderUpdate }
  | { kind: "broker.failover"; data: ExecBrokerFailover }
  | { kind: "trade.closed"; data: TradeClosed };
