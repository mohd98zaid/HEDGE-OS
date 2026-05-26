// /market — mirrors NATS `md.*` subjects forwarded by ui-gateway.
//
// Sources:
//   md.tick.<sym>          → Tick (FB Tick_v1, JSONified)
//   md.book.<sym>          → BookTopOfBook (top-of-book summary)
//   md.oi.<sym>            → OpenInterest (FB OpenInterest_v1)
//   md.breadth.sector      → BreadthSector
//   md.breadth.volatility  → BreadthVolatility (drives high-vol mode, R20.4)
//   md.connection.<source> → ConnectionStatus (R1.6)

export interface Tick {
  symbol: string;
  ltp_paise: number;
  bid_paise: number;
  ask_paise: number;
  ltq?: number;
  ts_recv_ns?: number;
}

export interface BookTopOfBook {
  symbol: string;
  bid_paise: number;
  bid_qty: number;
  ask_paise: number;
  ask_qty: number;
  ts_ns?: number;
}

/** Per-strike open interest snapshot, used by the Options Chain panel. */
export interface OpenInterestStrike {
  strike_paise: number;
  call_oi?: number;
  put_oi?: number;
  call_chg_oi?: number;
  put_chg_oi?: number;
}

export interface OpenInterest {
  symbol: string;
  expiry?: string;
  strikes: OpenInterestStrike[];
  ts_ns?: number;
}

export interface BreadthVolatility {
  /** Sector-aggregated realised volatility, ratio (e.g. 0.041). */
  volatility: number;
  ts_ns?: number;
}

export interface BreadthSector {
  sector: string;
  advancers: number;
  decliners: number;
  ts_ns?: number;
}

export interface ConnectionStatus {
  source: string;
  status: "ok" | "degraded" | "down";
  ts_ns?: number;
}

/** Discriminated union the gateway emits on the /market channel. */
export type MarketEvent =
  | { kind: "tick"; data: Tick }
  | { kind: "book"; data: BookTopOfBook }
  | { kind: "oi"; data: OpenInterest }
  | { kind: "breadth.volatility"; data: BreadthVolatility }
  | { kind: "breadth.sector"; data: BreadthSector }
  | { kind: "connection"; data: ConnectionStatus };
