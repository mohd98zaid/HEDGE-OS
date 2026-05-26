// PROJECT HEDGE Human_Control_UI — Zustand store for the WebSocket cockpit.
//
// Each NATS / WebSocket channel owns its own slice of state. Inbound frames
// from `useUiGatewaySocket` are dispatched into the matching slice via the
// reducer-style `applyEnvelope` action below — there is no REST polling
// anywhere in this build (R20.2).
//
// Slices intentionally cap their history sizes to keep render budgets bounded
// during high-volatility presentation mode (R20.4).

import { create } from "zustand";

import {
  ALERT_SEVERITY_RANK,
  type Alert,
  type AlertSeverity,
  type BreadthSector,
  type BreadthVolatility,
  type ChannelId,
  type ConnectionStatus,
  type ExecEvent,
  type LatencyAggregate,
  type LatencyEvent,
  type LatencyRecord,
  type LatencyStage,
  type MarketEvent,
  type NewsImpact,
  type OpenInterest,
  type OrderflowChannel,
  type OrderflowHeatmap,
  type PortfolioRiskState,
  type PositionUpdate,
  type PriorityTier,
  type PsychEvent,
  type PsychIntervention,
  type PsychStability,
  type RankedSignal,
  type ReplayEvent,
  type ReplaySession,
  type ReplayStatus,
  type RiskDecision,
  type RiskEvent,
  type ServerEnvelope,
  type StrategyName,
  type Tick,
  type WarModeStatus,
} from "../types";

const MAX_NEWS = 200;
const MAX_ALERTS = 250;
const MAX_AI_EXPLANATIONS = 100;
const MAX_REPLAY_FRAMES = 200;
const MAX_RISK_DECISIONS = 200;
const MAX_EXEC_ORDERS = 250;
const MAX_BEHAVIORS = 32;

// ---------- per-channel slice shapes -----------------------------------------

export interface MarketSlice {
  ticks: Record<string, Tick>;
  oi: Record<string, OpenInterest>;
  breadthVolatility?: BreadthVolatility;
  breadthSectors: Record<string, BreadthSector>;
  connections: Record<string, ConnectionStatus>;
}

export interface OrderflowSlice {
  heatmaps: Record<string, OrderflowHeatmap>;
}

export interface SignalsSlice {
  /** Latest ranked signal per correlation_id; cap unbounded by capping ai-explain. */
  byCorrelation: Record<string, RankedSignal>;
  /** Most-recent first. */
  recent: RankedSignal[];
  /**
   * Last-known `ai.priority.changed.<sym>` tier per symbol (R20.8). Read by
   * `SymbolPriorityControls` so the cockpit reflects the engine's authoritative
   * tier, not the trader's optimistic intent.
   */
  priorities: Record<string, PriorityTier>;
  /**
   * Last-emitted timestamp (ns) per Signal_Engine strategy. Driven by inbound
   * `sig.emitted` events on /signals; surfaced by `StrategyToggles` so the
   * trader can see which strategies are firing.
   */
  lastEmittedAtByStrategy: Partial<Record<StrategyName, number>>;
  /**
   * Optimistic UI mirror of `trader.intent.strategy_toggle` (R20.7). Defaults
   * to `true` for every strategy; flipped immediately on click and reconciled
   * implicitly when signals stop / resume arriving for that strategy. The
   * authoritative state lives in the Signal_Engine; this map only drives the
   * checkbox so the trader gets responsive feedback while the intent travels
   * UI → gateway → Risk_Engine → Signal_Engine.
   */
  enabledByStrategy: Partial<Record<StrategyName, boolean>>;
}

export interface RiskSlice {
  decisions: RiskDecision[];
  killswitch: { active: boolean; reason?: string; ts_ns?: number };
  cooldowns: Record<string, number /* until_ts_ns */>;
  positions: Record<string, PositionUpdate>;
  portfolio?: PortfolioRiskState;
  daily_target_reached?: { ts_ns?: number; pnl_inr?: number };
}

export interface ExecSlice {
  orders: Record<string, ExecEvent>;
  recent: ExecEvent[];
  failovers: { from: string; to: string; reason?: string; ts_ns?: number }[];
}

export interface NewsSlice {
  recent: NewsImpact[];
}

export interface PsychSlice {
  stability?: PsychStability;
  recentBehaviors: string[];
  interventions: PsychIntervention[];
}

export interface AlertsSlice {
  /** Pre-sorted by severity then recency. */
  list: Alert[];
}

export interface ReplaySlice {
  status?: ReplayStatus;
  sessions: ReplaySession[];
  recentFrames: { sequence_no: number; record_kind: string; ts_ns?: number }[];
}

export interface LatencySlice {
  records: LatencyRecord[];
  aggregates: Record<LatencyStage, LatencyAggregate | undefined>;
}

export interface GatewayMeta {
  state: "connecting" | "open" | "reconnecting" | "closed";
  lastSeenByChannel: Partial<Record<ChannelId, number>>;
}

export interface CockpitState {
  meta: GatewayMeta;
  market: MarketSlice;
  orderflow: OrderflowSlice;
  signals: SignalsSlice;
  risk: RiskSlice;
  exec: ExecSlice;
  news: NewsSlice;
  psych: PsychSlice;
  alerts: AlertsSlice;
  replay: ReplaySlice;
  latency: LatencySlice;
  /**
   * Market_Open_War_Mode presentation state (R26.1–R26.4). Driven by
   * `useWarMode` from the local IST clock; consumed by the /signals
   * reducer so signals below `minConfidence` are suppressed while
   * `active === true` (R26.3) and by `App` / `Panel` to apply the
   * reduced-clutter presentation profile (R26.3).
   */
  warMode: WarModeStatus;

  setGatewayState: (state: GatewayMeta["state"]) => void;
  applyEnvelope: (env: ServerEnvelope) => void;
  pushAlert: (alert: Alert) => void;
  /**
   * Optimistic local update for a strategy enable/disable click. The
   * authoritative state still flows through the Risk_Engine; this only
   * keeps the checkbox in sync with the trader's last action.
   */
  setStrategyEnabled: (strategy: StrategyName, enabled: boolean) => void;
  /** Optimistic local tier update fired alongside the priority intent. */
  setSymbolPriority: (symbol: string, tier: PriorityTier) => void;
  /** Update the War_Mode presentation state (R26 — driven by `useWarMode`). */
  setWarMode: (next: WarModeStatus) => void;
  reset: () => void;
}

// ---------- empty/initial values ---------------------------------------------

const emptyMarket = (): MarketSlice => ({
  ticks: {},
  oi: {},
  breadthVolatility: undefined,
  breadthSectors: {},
  connections: {},
});

const emptyOrderflow = (): OrderflowSlice => ({ heatmaps: {} });
const emptySignals = (): SignalsSlice => ({
  byCorrelation: {},
  recent: [],
  priorities: {},
  lastEmittedAtByStrategy: {},
  enabledByStrategy: {},
});
const emptyRisk = (): RiskSlice => ({
  decisions: [],
  killswitch: { active: false },
  cooldowns: {},
  positions: {},
  portfolio: undefined,
  daily_target_reached: undefined,
});
const emptyExec = (): ExecSlice => ({ orders: {}, recent: [], failovers: [] });
const emptyNews = (): NewsSlice => ({ recent: [] });
const emptyPsych = (): PsychSlice => ({
  stability: undefined,
  recentBehaviors: [],
  interventions: [],
});
const emptyAlerts = (): AlertsSlice => ({ list: [] });
const emptyReplay = (): ReplaySlice => ({
  status: undefined,
  sessions: [],
  recentFrames: [],
});
const emptyLatency = (): LatencySlice => ({
  records: [],
  aggregates: {
    TickIngest: undefined,
    FeatureExtraction: undefined,
    AiScoringFetch: undefined,
    RiskCheck: undefined,
    ExecutionRouting: undefined,
    BrokerSubmit: undefined,
  },
});

/**
 * Initial War_Mode state — inactive at startup. The `useWarMode` hook
 * keeps this in sync with the local IST clock; the slice never starts
 * `active === true` so a panel that mounts mid-window will see one
 * tick at `false` before the hook's first effect run flips it.
 */
const emptyWarMode = (): WarModeStatus => ({
  active: false,
  minConfidence: 0.6,
  scanMultiplier: 2.0,
});

// ---------- helpers ----------------------------------------------------------

const sortAlerts = (xs: Alert[]): Alert[] => {
  // Critical above non-critical (R20.5); within a bucket newest-first.
  const ranked = xs.slice().sort((a, b) => {
    const ra =
      ALERT_SEVERITY_RANK[a.severity as AlertSeverity] ?? Number.MAX_SAFE_INTEGER;
    const rb =
      ALERT_SEVERITY_RANK[b.severity as AlertSeverity] ?? Number.MAX_SAFE_INTEGER;
    if (ra !== rb) return ra - rb;
    return (b.ts_ns ?? 0) - (a.ts_ns ?? 0);
  });
  return ranked.slice(0, MAX_ALERTS);
};

const cap = <T,>(xs: T[], n: number): T[] =>
  xs.length > n ? xs.slice(0, n) : xs;

const uniqLeft = (existing: string[], incoming: string[], cap_n: number): string[] => {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const s of [...incoming, ...existing]) {
    if (!seen.has(s)) {
      seen.add(s);
      out.push(s);
      if (out.length >= cap_n) break;
    }
  }
  return out;
};

// ---------- the store --------------------------------------------------------

export const useCockpitStore = create<CockpitState>((set) => ({
  meta: { state: "closed", lastSeenByChannel: {} },
  market: emptyMarket(),
  orderflow: emptyOrderflow(),
  signals: emptySignals(),
  risk: emptyRisk(),
  exec: emptyExec(),
  news: emptyNews(),
  psych: emptyPsych(),
  alerts: emptyAlerts(),
  replay: emptyReplay(),
  latency: emptyLatency(),
  warMode: emptyWarMode(),

  setGatewayState: (state) =>
    set((s) => ({ meta: { ...s.meta, state } })),

  pushAlert: (alert) =>
    set((s) => ({
      alerts: { list: sortAlerts([alert, ...s.alerts.list]) },
    })),

  setStrategyEnabled: (strategy, enabled) =>
    set((s) => ({
      signals: {
        ...s.signals,
        enabledByStrategy: { ...s.signals.enabledByStrategy, [strategy]: enabled },
      },
    })),

  setSymbolPriority: (symbol, tier) =>
    set((s) => ({
      signals: {
        ...s.signals,
        priorities: { ...s.signals.priorities, [symbol]: tier },
      },
    })),

  setWarMode: (next) => set(() => ({ warMode: next })),

  reset: () =>
    set(() => ({
      meta: { state: "closed", lastSeenByChannel: {} },
      market: emptyMarket(),
      orderflow: emptyOrderflow(),
      signals: emptySignals(),
      risk: emptyRisk(),
      exec: emptyExec(),
      news: emptyNews(),
      psych: emptyPsych(),
      alerts: emptyAlerts(),
      replay: emptyReplay(),
      latency: emptyLatency(),
      warMode: emptyWarMode(),
    })),

  applyEnvelope: (env) =>
    set((s) => {
      const stamped: GatewayMeta = {
        ...s.meta,
        lastSeenByChannel: {
          ...s.meta.lastSeenByChannel,
          [env.channel]: env.ts_ns ?? Date.now() * 1_000_000,
        },
      };

      switch (env.channel) {
        case "market":
          return { meta: stamped, market: reduceMarket(s.market, env.payload as MarketEvent) };
        case "orderflow":
          return {
            meta: stamped,
            orderflow: reduceOrderflow(s.orderflow, env.payload as OrderflowChannel),
          };
        case "signals":
          return {
            meta: stamped,
            signals: reduceSignalsChannel(s.signals, env, s.warMode),
          };
        case "risk":
          return { meta: stamped, risk: reduceRisk(s.risk, env.payload as RiskEvent) };
        case "exec":
          return { meta: stamped, exec: reduceExec(s.exec, env.payload as ExecEvent) };
        case "news":
          return { meta: stamped, news: reduceNews(s.news, env.payload as NewsImpact) };
        case "psych":
          return { meta: stamped, psych: reducePsych(s.psych, env.payload as PsychEvent) };
        case "alerts":
          return {
            meta: stamped,
            alerts: { list: sortAlerts([env.payload as Alert, ...s.alerts.list]) },
          };
        case "replay":
          return {
            meta: stamped,
            replay: reduceReplay(s.replay, env.payload as ReplayEvent),
          };
        case "latency":
          return {
            meta: stamped,
            latency: reduceLatency(s.latency, env.payload as LatencyEvent),
          };
        case "control":
          // /control is client → server only. Ignore inbound.
          return { meta: stamped };
        default:
          return { meta: stamped };
      }
    }),
}));

// ---------- per-channel reducers ---------------------------------------------

function reduceMarket(prev: MarketSlice, ev: MarketEvent): MarketSlice {
  switch (ev.kind) {
    case "tick":
      return { ...prev, ticks: { ...prev.ticks, [ev.data.symbol]: ev.data } };
    case "book":
      // Treat top-of-book as a tick-like update for the live market panel.
      return {
        ...prev,
        ticks: {
          ...prev.ticks,
          [ev.data.symbol]: {
            symbol: ev.data.symbol,
            ltp_paise: prev.ticks[ev.data.symbol]?.ltp_paise ?? ev.data.bid_paise,
            bid_paise: ev.data.bid_paise,
            ask_paise: ev.data.ask_paise,
            ts_recv_ns: ev.data.ts_ns,
          },
        },
      };
    case "oi":
      return { ...prev, oi: { ...prev.oi, [ev.data.symbol]: ev.data } };
    case "breadth.volatility":
      return { ...prev, breadthVolatility: ev.data };
    case "breadth.sector":
      return {
        ...prev,
        breadthSectors: { ...prev.breadthSectors, [ev.data.sector]: ev.data },
      };
    case "connection":
      return {
        ...prev,
        connections: { ...prev.connections, [ev.data.source]: ev.data },
      };
    default:
      return prev;
  }
}

function reduceOrderflow(
  prev: OrderflowSlice,
  ev: OrderflowChannel,
): OrderflowSlice {
  if (ev.kind === "heatmap") {
    return {
      ...prev,
      heatmaps: { ...prev.heatmaps, [ev.data.symbol]: ev.data },
    };
  }
  return prev;
}

function reduceSignals(
  prev: SignalsSlice,
  sig: RankedSignal,
  warMode: WarModeStatus,
): SignalsSlice {
  // Defensive: gateway already filters shadow=true on /signals (task 36.1),
  // but mirroring the guard here keeps regressions visible (task 37.1 spec).
  if (sig.shadow) return prev;
  // R26.3 — while Market_Open_War_Mode is active, suppress any signal
  // whose `confidence` is below `war_mode.min_confidence`. The
  // Signal_Engine itself enforces the same gate server-side
  // (`hedge_signals::gating::check_war_mode`); we mirror it here as
  // defence-in-depth so a stray signal that bypasses the engine gate
  // never reaches the cockpit's ranked-signal display.
  if (warMode.active && sig.confidence < warMode.minConfidence) {
    return prev;
  }
  const recent = cap([sig, ...prev.recent.filter((s) => s.correlation_id !== sig.correlation_id)], MAX_AI_EXPLANATIONS);
  // Stamp the strategy's last-emitted timestamp so StrategyToggles can show
  // when each strategy last fired (R20.7).
  const strategyKey = sig.strategy as StrategyName;
  const lastEmittedAtByStrategy = sig.ts_ns
    ? { ...prev.lastEmittedAtByStrategy, [strategyKey]: sig.ts_ns }
    : prev.lastEmittedAtByStrategy;
  return {
    ...prev,
    byCorrelation: { ...prev.byCorrelation, [sig.correlation_id]: sig },
    recent,
    lastEmittedAtByStrategy,
  };
}

/** Mirror of `ai.priority.changed.<sym>` JSON payload (R14.3 / R20.8). */
interface AiPriorityChangedPayload {
  symbol: string;
  to: PriorityTier;
  from?: PriorityTier;
  ts_ns?: number;
}

/**
 * Dispatch /signals envelopes between the Signal payload reducer and the
 * `ai.priority.changed.<sym>` priority-tier reducer. Subject-based routing
 * matches the gateway's NATS forwarding (design.md § Event Topics).
 */
function reduceSignalsChannel(
  prev: SignalsSlice,
  env: ServerEnvelope,
  warMode: WarModeStatus,
): SignalsSlice {
  if (env.subject && env.subject.startsWith("ai.priority.changed")) {
    const ev = env.payload as AiPriorityChangedPayload | null;
    if (!ev || !ev.symbol || !ev.to) return prev;
    return {
      ...prev,
      priorities: { ...prev.priorities, [ev.symbol]: ev.to },
    };
  }
  return reduceSignals(prev, env.payload as RankedSignal, warMode);
}

function reduceRisk(prev: RiskSlice, ev: RiskEvent): RiskSlice {
  switch (ev.kind) {
    case "decision":
      return {
        ...prev,
        decisions: cap([ev.data, ...prev.decisions], MAX_RISK_DECISIONS),
      };
    case "killswitch":
      return {
        ...prev,
        killswitch: { active: ev.data.active, reason: ev.data.reason, ts_ns: ev.data.ts_ns },
      };
    case "cooldown":
      return {
        ...prev,
        cooldowns: {
          ...prev.cooldowns,
          [ev.data.symbol]: ev.data.until_ts_ns ?? 0,
        },
      };
    case "target.reached":
      return { ...prev, daily_target_reached: ev.data };
    case "pos.update":
      return {
        ...prev,
        positions: { ...prev.positions, [ev.data.symbol]: ev.data },
      };
    case "pos.risk_state":
      return { ...prev, portfolio: ev.data };
    default:
      return prev;
  }
}

function reduceExec(prev: ExecSlice, ev: ExecEvent): ExecSlice {
  const recent = cap([ev, ...prev.recent], MAX_EXEC_ORDERS);
  switch (ev.kind) {
    case "order":
    case "fill":
      return {
        ...prev,
        orders: { ...prev.orders, [ev.data.correlation_id]: ev },
        recent,
      };
    case "broker.failover":
      return {
        ...prev,
        failovers: cap([ev.data, ...prev.failovers], 50),
        recent,
      };
    case "trade.closed":
      return { ...prev, recent };
    default:
      return prev;
  }
}

function reduceNews(prev: NewsSlice, n: NewsImpact): NewsSlice {
  return { recent: cap([n, ...prev.recent], MAX_NEWS) };
}

function reducePsych(prev: PsychSlice, ev: PsychEvent): PsychSlice {
  if (ev.kind === "stability") {
    return {
      ...prev,
      stability: ev.data,
      recentBehaviors: uniqLeft(prev.recentBehaviors, ev.data.behaviors, MAX_BEHAVIORS),
    };
  }
  return {
    ...prev,
    interventions: cap([ev.data, ...prev.interventions], 50),
  };
}

function reduceReplay(prev: ReplaySlice, ev: ReplayEvent): ReplaySlice {
  switch (ev.kind) {
    case "status":
      return { ...prev, status: ev.data };
    case "list":
      return { ...prev, sessions: ev.data.sessions };
    case "frame":
      return {
        ...prev,
        recentFrames: cap([ev.data, ...prev.recentFrames], MAX_REPLAY_FRAMES),
      };
    default:
      return prev;
  }
}

function reduceLatency(prev: LatencySlice, ev: LatencyEvent): LatencySlice {
  if (ev.kind === "record") {
    return { ...prev, records: cap([ev.data, ...prev.records], 500) };
  }
  return {
    ...prev,
    aggregates: { ...prev.aggregates, [ev.data.stage]: ev.data },
  };
}
