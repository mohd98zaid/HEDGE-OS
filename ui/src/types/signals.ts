// /signals — `sig.emitted` joined with `ai.rank.<correlation_id>` by ui-gateway
// (design.md § WebSocket Channels, R20.3). The gateway has already filtered
// shadowed AI sources; the React side keeps a defensive guard for regression
// visibility (see panels/AiConfidenceScores.tsx).

export interface RankFactors {
  orderflow: number;
  technical_strength: number;
  news_sentiment: number;
  market_regime: number;
  trader_discipline: number;
}

/** Signal_v1 payload after AI ranking has been merged in. */
export interface RankedSignal {
  correlation_id: string;
  signal_id?: string;
  strategy: string;
  symbol: string;
  side: "buy" | "sell";
  /** From Signal_v1 (statistical edge prior to AI ranking). */
  base_probability: number;
  /** From Signal_v1 (Signal_Engine confidence prior to AI ranking). */
  confidence: number;
  /** From `ai.rank.*` — Trade_Confidence_Score, ∈ [0, 1]. */
  trade_confidence_score?: number;
  /** From `ai.rank.*` — five-factor breakdown. */
  factors?: RankFactors;
  /** Mirrored from `ai.rank.*`; gateway should already filter on shadow=true. */
  shadow?: boolean;
  /** Optional human-readable AI explanation (LLM, RAG-grounded). */
  explanation?: string;
  ts_ns?: number;
}
