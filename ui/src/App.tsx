// PROJECT HEDGE Human_Control_UI — top-level cockpit shell.
//
// All 16 panels (R20.3) live in src/panels/ and read from the WebSocket-driven
// store (src/store/cockpitStore.ts). The single connection to `ui-gateway`
// is owned by `useUiGatewaySocket`, which subscribes to every channel the
// cockpit needs and dispatches inbound envelopes into the matching slice.
// There is no REST polling anywhere (R20.2).

import { useUiGatewaySocket } from "./hooks/useUiGatewaySocket";
import { useHighVolMode } from "./hooks/useHighVolMode";
import { useWarMode } from "./hooks/useWarMode";
import { useCockpitStore } from "./store/cockpitStore";
import { TradingModeToggle } from "./components/TradingModeToggle";
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

export default function App(): JSX.Element {
  const { sendIntent } = useUiGatewaySocket();
  const { active, volatility, threshold } = useHighVolMode();
  const warMode = useWarMode();
  const gatewayState = useCockpitStore((s) => s.meta.state);

  const stateTone =
    gatewayState === "open"
      ? "text-hedge-ok"
      : gatewayState === "connecting" || gatewayState === "reconnecting"
        ? "text-hedge-warn"
        : "text-hedge-danger";

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
            <span className={stateTone}>{gatewayState}</span>
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

      {/* 4-column grid on lg+, collapses cleanly to 1-col on phones (R20.3). */}
      <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
        {/* Row 1: critical market + orderflow + execution. */}
        <LiveMarket />
        <OrderflowHeatmap />
        <OptionsChain />
        <ExecutionPanel />

        {/* Row 2: positions + PnL + risk + latency (critical). */}
        <Positions />
        <LivePnl />
        <RiskPanel />
        <LatencyDashboard />

        {/* Row 3: AI surfaces + psychology. */}
        <AiConfidenceScores />
        <AiExplanations />
        <TraderStabilityScore />
        <NewsFeed />

        {/* Row 4: ops + replay + trader controls. */}
        <Alerts />
        <ReplayControls sendIntent={sendIntent} />
        <StrategyToggles sendIntent={sendIntent} />
        <SymbolPriorityControls sendIntent={sendIntent} />
      </section>
    </main>
  );
}
