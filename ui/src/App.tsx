// PROJECT HEDGE Human_Control_UI — top-level cockpit shell.
//
// All 16 panels (R20.3) live in src/panels/ and read from the WebSocket-driven
// store (src/store/cockpitStore.ts). The single connection to `ui-gateway`
// is owned by `useUiGatewaySocket`, which subscribes to every channel the
// cockpit needs and dispatches inbound envelopes into the matching slice.
// There is no REST polling anywhere (R20.2).

import type { ReactNode } from "react";

import { useUiGatewaySocket } from "./hooks/useUiGatewaySocket";
import { useHighVolMode } from "./hooks/useHighVolMode";
import { useWarMode } from "./hooks/useWarMode";
import { useFeedStatusTicker } from "./hooks/useFeedStatusTicker";
import { TradingModeToggle } from "./components/TradingModeToggle";
import { ConnectionBanner } from "./components/ConnectionBanner";
import { ErrorBoundary } from "./components/ErrorBoundary";
import {
  AiConfidenceScores,
  AiExplanations,
  Alerts,
  ExecutionPanel,
  LatencyDashboard,
  LiveMarket,
  LivePnl,
  NewsFeed,
  OptionsChain,
  OrderflowHeatmap,
  Positions,
  ReplayControls,
  RiskPanel,
  StrategyToggles,
  SymbolPriorityControls,
  TraderStabilityScore,
} from "./panels";

/** Wrap a panel so a render crash inside it is isolated to that one tile
 *  instead of blanking the whole cockpit. */
function Guard({ name, children }: { name: string; children: ReactNode }): JSX.Element {
  return <ErrorBoundary name={name}>{children}</ErrorBoundary>;
}

export default function App(): JSX.Element {
  const { sendIntent } = useUiGatewaySocket();
  const { active, volatility, threshold } = useHighVolMode();
  const warMode = useWarMode();
  useFeedStatusTicker();

  return (
    <main
      className={`min-h-screen bg-hedge-bg text-slate-100 font-mono p-4 lg:p-6 ${
        warMode.active ? "war-mode" : ""
      }`}
    >
      <header className="mb-4 flex items-center justify-between border-b border-slate-800 pb-3">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">
            PROJECT <span className="text-hedge-accent">HEDGE</span>{" "}
            <span className="text-slate-500 text-xs">Human_Control_UI</span>
          </h1>
          <p className="text-[11px] text-slate-500">
            ws gateway:{" "}
            <ConnectionBanner />
            {" · "}
            breadth.σ {volatility != null ? `${(volatility * 100).toFixed(2)}%` : "—"}
            {" / "}
            threshold {(threshold * 100).toFixed(2)}%{" "}
            {active ? (
              <span className="ml-1 rounded bg-hedge-warn/20 text-hedge-warn px-1">
                HIGH-VOL
              </span>
            ) : null}
            {warMode.active ? (
              <span className={warMode.pillClass} title="Market_Open_War_Mode">
                WAR-MODE · floor {(warMode.minConfidence * 100).toFixed(0)}%
              </span>
            ) : null}
          </p>
        </div>
        <div className="flex items-center gap-4">
          <TradingModeToggle sendIntent={sendIntent} />
        </div>
      </header>

      {/* 4-column grid on lg+, collapses cleanly to 1-col on phones (R20.3).
          Each panel is wrapped in an ErrorBoundary so one bad frame can never
          blank the whole cockpit. */}
      <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
        {/* Row 1: critical market + orderflow + execution. */}
        <Guard name="Live Market"><LiveMarket /></Guard>
        <Guard name="Orderflow Heatmap"><OrderflowHeatmap /></Guard>
        <Guard name="Options Chain"><OptionsChain /></Guard>
        <Guard name="Execution"><ExecutionPanel /></Guard>

        {/* Row 2: positions + PnL + risk + latency (critical). */}
        <Guard name="Positions"><Positions /></Guard>
        <Guard name="Live PnL"><LivePnl /></Guard>
        <Guard name="Risk"><RiskPanel /></Guard>
        <Guard name="Latency"><LatencyDashboard /></Guard>

        {/* Row 3: AI surfaces + psychology. */}
        <Guard name="AI Confidence Scores"><AiConfidenceScores /></Guard>
        <Guard name="AI Explanations"><AiExplanations /></Guard>
        <Guard name="Trader Stability Score"><TraderStabilityScore /></Guard>
        <Guard name="News"><NewsFeed /></Guard>

        {/* Row 4: ops + replay + trader controls. */}
        <Guard name="Alerts"><Alerts /></Guard>
        <Guard name="Replay Controls"><ReplayControls sendIntent={sendIntent} /></Guard>
        <Guard name="Strategy Toggles"><StrategyToggles sendIntent={sendIntent} /></Guard>
        <Guard name="Symbol Priority"><SymbolPriorityControls sendIntent={sendIntent} /></Guard>
      </section>
    </main>
  );
}
