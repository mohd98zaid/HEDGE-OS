// /news — `ai.news.impact.<symbol>`, mirrored from
// crates/hedge-schemas/json_schemas/ai_news_impact.schema.json.

export interface NewsImpact {
  correlation_id: string;
  symbol: string;
  headline_id: string;
  /** Optional human-readable headline if the gateway has joined Memory_RAG. */
  headline?: string;
  source?: string;
  /** [-1, 1] sentiment score from FinBERT. */
  sentiment: number;
  /** [0, 1] impact magnitude. */
  impact_magnitude: number;
  /** True when the heuristic Fast_Path fired and AI verdict is pending. */
  fast_path: boolean;
  /** True while the LLM/RAG slow-path is still computing. */
  slow_path_pending: boolean;
  ts_ns?: number;
}
