// /control — Trader → server intents (R20.6, R20.7, R20.8). Each intent is
// forwarded by ui-gateway onto the matching `trader.intent.*` NATS subject
// and is subject to Authority_Hierarchy enforcement at the Risk_Engine
// (design.md § Authority Hierarchy and Decision Flow).
//
// JSON shapes mirror crates/hedge-schemas/json_schemas/trader_intent_*.schema.json.
// `correlation_id`, `actor`, and `ts_ns` are stamped server-side by the gateway;
// the cockpit only declares trader-facing fields.
//
// Subject mapping for task 38.1:
//   killswitch       -> trader.intent.killswitch         { engaged }
//   strategy_toggle  -> trader.intent.strategy_toggle    { strategy, enabled }
//   priority         -> trader.intent.priority           { symbol, to }
//   order            -> trader.intent.order              (manual order; non-38.1)
//   replay.*         -> trader.intent.replay.{start,pause,scrub,step,...}
//
// The widgets never bypass the Risk_Engine — they publish the intent and
// react to the matching `risk.*` / `ai.priority.changed.<sym>` confirmation.

/** Display order matches design § Components → Signal_Engine strategies. */
export type StrategyName =
  | "OpeningRangeBreakout"
  | "VwapPullback"
  | "MomentumBreakout"
  | "LiquiditySweepReversal"
  | "OptionsOiExpansionBreakout"
  | "VolatilityCompressionBreakout";

/** Stable list of the six strategies the cockpit can toggle (R20.7). */
export const STRATEGIES: readonly StrategyName[] = [
  "OpeningRangeBreakout",
  "VwapPullback",
  "MomentumBreakout",
  "LiquiditySweepReversal",
  "OptionsOiExpansionBreakout",
  "VolatilityCompressionBreakout",
] as const;

/** Human-readable labels matching design § Components → Signal_Engine. */
export const STRATEGY_LABELS: Readonly<Record<StrategyName, string>> = {
  OpeningRangeBreakout: "Opening Range Breakout",
  VwapPullback: "VWAP Pullback",
  MomentumBreakout: "Momentum Breakout",
  LiquiditySweepReversal: "Liquidity Sweep Reversal",
  OptionsOiExpansionBreakout: "Options OI Expansion Breakout",
  VolatilityCompressionBreakout: "Volatility Compression Breakout",
};

export type PriorityTier = "P1" | "P2" | "P3" | "P4";

export const PRIORITY_TIERS: readonly PriorityTier[] = ["P1", "P2", "P3", "P4"] as const;

/** Replay op set forwarded by the gateway to `trader.intent.replay.*`. */
export type ReplayCommandIntent =
  | { op: "list" }
  | { op: "load"; session_id: string }
  /** Maps to `trader.intent.replay.start { session_id?, speed }`. */
  | { op: "play"; session_id?: string; speed: 1 | 10 | "max" }
  /** Maps to `trader.intent.replay.pause`. */
  | { op: "pause" }
  /** Maps to `trader.intent.replay.step`. */
  | { op: "step" }
  /** Maps to `trader.intent.replay.scrub { sequence_no }`. */
  | { op: "scrub"; sequence_no: number };

/**
 * Trader → server intent payload sent on the /control WebSocket channel.
 *
 * Each variant maps to one `trader.intent.*` NATS subject. Wire field names
 * (`engaged`, `strategy`, `enabled`, `to`) match the canonical JSON schemas
 * in `crates/hedge-schemas/json_schemas/trader_intent_*.schema.json` so the
 * gateway can forward the payload verbatim after stamping correlation_id /
 * actor / ts_ns.
 */
export type TraderIntent =
  | {
      kind: "killswitch";
      /** Wire field per `trader_intent_killswitch.schema.json`. */
      engaged: boolean;
      reason?: string;
    }
  | { kind: "strategy_toggle"; strategy: StrategyName; enabled: boolean }
  | { kind: "priority"; symbol: string; to: PriorityTier }
  | {
      kind: "order";
      symbol: string;
      side: "Buy" | "Sell";
      quantity: number;
      order_type: "Market" | "Limit";
      limit_paise?: number;
      exchange: "NSE" | "BSE";
    }
  | { kind: "replay"; command: ReplayCommandIntent }
  /** Live/paper execution mode toggle. `live: true` requests live broker
   *  submission; `false` (default) keeps paper mode. The Execution_Engine
   *  is the authority — it echoes the confirmed mode on `exec.mode.confirmed`. */
  | { kind: "trading_mode"; live: boolean };
