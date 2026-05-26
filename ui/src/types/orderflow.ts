// /orderflow — `of.heatmap.<sym>` deltas (R2.4, R20.3).

export interface OrderflowHeatmapCell {
  price_paise: number;
  buy_qty: number;
  sell_qty: number;
}

export interface OrderflowHeatmap {
  symbol: string;
  cells: OrderflowHeatmapCell[];
  ts_ns?: number;
}

/** Optional named events surfaced alongside the heatmap (Spoofing, etc.). */
export interface OrderflowEvent {
  symbol: string;
  event:
    | "LiquidityGap"
    | "Absorption"
    | "HiddenLiquidity"
    | "Spoofing";
  ts_ns?: number;
  detail?: string;
}

export type OrderflowChannel =
  | { kind: "heatmap"; data: OrderflowHeatmap }
  | { kind: "event"; data: OrderflowEvent };
