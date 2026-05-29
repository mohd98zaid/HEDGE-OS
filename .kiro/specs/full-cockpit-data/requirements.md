# Requirements Document

_Feature: full-cockpit-data_

## Introduction

The cockpit dashboard at `http://localhost:5173` currently renders only one panel (`LiveMarket`) with real data; every other panel shows "Awaiting first …" because the engines that feed them are either scaffolding-only stubs (`hedge-risk`, `hedge-exec`, `hedge-position`) or expect a wire format the live publisher does not emit (`hedge-orderflow`, `hedge-features`, `hedge-signals` consume binary `Tick_v1` while `upstox-feed` publishes JSON only). The Warm_AI_Pipeline (Python) is not running at all, so the AI / news / psych panels have no publisher.

This feature delivers a fully populated cockpit dashboard in three phases:

- **Phase A** — a new Rust binary `hedge-demo-synth` injects deterministic, realistic JSON on every NATS subject the cockpit subscribes to. Every panel populates within 10 seconds of `start.bat` finishing. Synth defers to real publishers when present, so it remains harmless as later phases ship.
- **Phase B** — `upstox-feed` additionally publishes the binary `Tick_v1` FlatBuffer wire form on a parallel subject so `hedge-orderflow`, `hedge-features`, and `hedge-signals` start computing on real Upstox prices.
- **Phase C** — replace scaffolding stubs in `hedge-risk` / `hedge-exec` / `hedge-position` with real implementations, bring up the Warm_AI_Pipeline (Python + Ollama), and add the Upstox options-chain endpoint.

This is not an architectural rewrite. Every change rides on the existing NATS bus, the existing JSON-on-cockpit / FlatBuffers-on-Hot_Path split, and the existing `start.bat` process supervisor. The cockpit-side contracts owned by the `live-cockpit-data` spec (`MarketEvent` discriminator, `FeedStatus`, `EmptyState` reasons, IST timestamps, Connection_Banner) are preserved unchanged.

## Glossary

- **Cockpit_UI**: The React/TypeScript app at `http://localhost:5173`, fed by a single WebSocket to `hedge-ui-gateway`.
- **UI_Gateway**: The `hedge-ui-gateway` Rust binary that fans NATS messages out to per-connection WebSocket dispatchers.
- **Hot_Path_Engine**: One of `hedge-orderflow`, `hedge-features`, `hedge-signals`, `hedge-risk`, `hedge-exec`, `hedge-position`.
- **Demo_Synth**: The new `hedge-demo-synth.exe` binary introduced in Phase A.
- **Tick_v1**: The 93-byte fixed-layout FlatBuffer market-data tick consumed by Hot_Path_Engines (`hedge-schemas` crate, `tick.fbs`).
- **Binary_Tick_Subject**: `md.tick.bin.<SYMBOL>` — the parallel NATS subject Phase B introduces for `Tick_v1` publishes.
- **JSON_Tick_Subject**: `md.tick.<SYMBOL>` — the existing subject `upstox-feed` publishes JSON ticks on; consumed by the cockpit.
- **Synth_Tag**: The literal field `"_synth": true` injected into every JSON envelope `Demo_Synth` publishes.
- **Suppression_Window**: The 5-second period during which `Demo_Synth` does not publish on a subject after observing a non-Synth_Tagged payload on that subject.
- **Live_Tick_Staleness_Threshold**: The 5-second period without a non-Synth_Tagged tick on `md.tick.<SYM>` after which Demo_Synth treats live ticks as unavailable for that symbol.
- **NSE_Trading_Hours**: 09:15–15:30 Asia/Kolkata on NSE business days.
- **Cockpit_Channel**: One of `market`, `orderflow`, `signals`, `risk`, `exec`, `news`, `psych`, `alerts`, `replay`, `latency`, `control`.
- **Cockpit_Subscribed_Subject**: A NATS subject pattern that the UI_Gateway subscribes to for cockpit consumption — enumerated in the design's "Subject ownership matrix".
- **Warm_AI_Pipeline**: The Python `hedge_warm_ai` package set (`news`, `ranking`, `regime`, `psych`).
- **Authority_Hierarchy**: The contract that every `exec.order.submitted` event corresponds to exactly one prior `risk.decision.approved` event with the same `correlation_id`.

## Requirements

### Requirement 1: Demo_Synth Binary Emits On Every Cockpit_Subscribed_Subject

**User Story:** As a developer iterating on the cockpit UI outside trading hours, I want every panel to populate with synthetic data within 10 seconds of starting the system, so that I can validate UI changes without waiting for the market to open.

#### Acceptance Criteria

1. THE Demo_Synth SHALL publish at least one JSON envelope on every Cockpit_Subscribed_Subject within 10 seconds of NATS_Connected.
2. THE Demo_Synth SHALL emit on `md.oi.<SYM>`, `md.breadth.sector`, `md.breadth.volatility`, `of.event.<SYM>`, `of.heatmap.<SYM>`, `feat.update.<SYM>`, `sig.emitted`, `ai.rank.<corr_id>`, `risk.decision.approved`, `risk.decision.rejected`, `risk.killswitch.activated`, `risk.target.reached`, `risk.cooldown.<SYM>`, `pos.update.<SYM>`, `pos.risk_state`, `exec.order.<state>`, `exec.fill.<SYM>`, `exec.trade.closed`, `exec.broker.failover`, `ai.news.impact.<topic>`, `ai.psych.stability`, `ai.psych.intervention`, `obs.latency.<stage>`, `obs.budget.breach.<stage>`, and `ops.action.replay`.
3. THE Demo_Synth SHALL include the literal field `"_synth": true` at the top level of every JSON envelope it emits.
4. THE Demo_Synth SHALL emit envelopes whose payload structure deserialises into the matching cockpit reducer-side type (`MarketEvent`, `OrderflowChannel`, `RankedSignal`, `RiskDecision`, `ExecutionEvent`, `PositionUpdate`, `NewsImpact`, `TraderStability`, `LatencyRecord`, `ReplayEvent`) with no field falling back to a default.

### Requirement 2: Demo_Synth Defers To Real Publishers

**User Story:** As an operator running the system in any phase, I want Demo_Synth to back off automatically whenever a real publisher is producing on the same subject, so that I never see duplicate or contradictory events on a single subject.

#### Acceptance Criteria

1. THE Demo_Synth SHALL subscribe to every subject on which it publishes.
2. WHEN Demo_Synth observes a payload on a subscribed subject AND that payload does not contain `"_synth": true`, THE Demo_Synth SHALL set a Suppression_Window of 5 seconds for that subject.
3. WHILE a Suppression_Window is active for a subject, THE Demo_Synth SHALL NOT emit any new payload on that subject regardless of any other condition.
4. WHEN a Suppression_Window expires AND no new non-Synth_Tagged payload has been observed on that subject during the window, THE Demo_Synth SHALL resume publishing on that subject according to its configured cadence.
5. THE Demo_Synth SHALL ignore its own Synth_Tagged echoes when computing Suppression_Windows.
6. THE Demo_Synth SHALL NOT resume publishing on a subject before its Suppression_Window has expired, even if other gating conditions (e.g., cadence, downstream demand) would otherwise permit emission.

### Requirement 3: Demo_Synth Is Deterministic And Live-Aware

**User Story:** As a developer running visual regression tests, I want Demo_Synth output to be deterministic for a fixed seed, so that test runs produce comparable artifacts.

#### Acceptance Criteria

1. THE Demo_Synth SHALL seed its RNG from the constant `0x5EEDED`.
2. WHEN Demo_Synth runs twice with the same seed and same wall-clock duration, THE Demo_Synth SHALL produce byte-identical NATS publish sequences across both runs.
3. WHILE `upstox-feed` is publishing live ticks on `md.tick.<SYM>` within the Live_Tick_Staleness_Threshold, THE Demo_Synth SHALL derive its `feat.update.<SYM>`, `of.event.<SYM>`, `of.heatmap.<SYM>`, `sig.emitted`, `ai.rank.<corr_id>`, `risk.decision.*`, `exec.order.*`, `exec.fill.<SYM>`, `pos.update.<SYM>`, `pos.risk_state`, and `obs.latency.<stage>` payloads from the live LTPs in those ticks.
4. WHEN no non-Synth_Tagged tick has been observed on `md.tick.<SYM>` for longer than the Live_Tick_Staleness_Threshold (5 seconds), THE Demo_Synth SHALL switch derivation for that symbol to its deterministic random walk AND SHALL NOT use cached live ticks for derivation thereafter.
5. WHILE `upstox-feed` is NOT publishing live ticks for a given symbol, THE Demo_Synth SHALL drive its own deterministic random walk for tick prices for that symbol AND propagate downstream events from that walk.
6. WHEN live ticks resume on `md.tick.<SYM>` after a staleness gap, THE Demo_Synth SHALL switch derivation for that symbol back to live LTPs on the next observed live tick.

### Requirement 4: Demo_Synth Toggle And Default Behaviour

**User Story:** As an operator who wants the dashboard demonstrable when markets are closed but absolutely silent during real trading, I want a single environment variable to control Demo_Synth, so that production runs are predictable.

#### Acceptance Criteria

1. THE Start_Bat SHALL launch Demo_Synth only when the environment variable `HEDGE_DEMO_SYNTH` equals `on`.
2. THE Start_Bat SHALL default `HEDGE_DEMO_SYNTH` to `on`.
3. WHEN Demo_Synth is launched, THE Start_Bat SHALL set the title of its console window to `HEDGE-demo-synth`.
4. WHEN Demo_Synth receives Ctrl+C, THE Demo_Synth SHALL attempt graceful shutdown (drain in-flight publishes, close the NATS connection, flush state).
5. IF graceful shutdown completes successfully, THEN THE Demo_Synth SHALL exit with exit code 0.
6. IF graceful shutdown fails (e.g., NATS flush error, state-flush error), THEN THE Demo_Synth SHALL exit with a non-zero exit code reflecting the failure AND SHALL log the failing step to stderr.

### Requirement 5: Phase B Binary Tick Bridge

**User Story:** As a Hot_Path engineer, I want `upstox-feed` to publish a binary `Tick_v1` envelope alongside the existing JSON tick, so that `hedge-orderflow`, `hedge-features`, and `hedge-signals` can compute on real Upstox prices without forcing the cockpit to parse FlatBuffers.

#### Acceptance Criteria

1. WHEN `upstox-feed` resolves a new tick from the Upstox REST endpoint, THE `upstox-feed` SHALL publish a JSON envelope on JSON_Tick_Subject `md.tick.<SYMBOL>` AND a `Tick_v1` binary envelope on Binary_Tick_Subject `md.tick.bin.<SYMBOL>` within 1 millisecond of each other.
2. THE `Tick_v1` binary envelope SHALL conform to the 93-byte layout documented in the design's Phase B section (offsets 0–93 inclusive).
3. THE `hedge-features` binary SHALL subscribe to `md.tick.bin.>` and SHALL NOT subscribe to `md.tick.*` after Phase B ships.
4. THE `hedge-orderflow` binary SHALL subscribe to `md.tick.bin.>` and `md.book.>` after Phase B ships.
5. WHERE Phase B is active, THE `hedge-features` binary SHALL NOT log any "discarded malformed tick payload" warnings under normal operation.
6. THE `hedge-bus` crate SHALL expose a public `symbol_id_for(sym: &str) -> u32` and `symbol_for_id(id: u32) -> Option<&'static str>` round-tripping for at least the symbols `RELIANCE`, `INFY`, `SBIN`, `HDFCBANK`, `ICICIBANK`.

### Requirement 6: Real Risk_Engine Implementation

**User Story:** As a trader, I want `hedge-risk` to make real risk decisions on signals from `hedge-signals`, so that the RiskPanel shows real approval/rejection events instead of synthetic ones.

#### Acceptance Criteria

1. THE `hedge-risk` binary SHALL subscribe to `sig.emitted`, `feat.update.*`, `pos.update.*`, `pos.risk_state`, `trader.intent.killswitch`, `trader.intent.priority`, and `md.connection.upstox`.
2. WHEN `hedge-risk` receives a `sig.emitted` event AND the kill-switch is engaged, THE `hedge-risk` SHALL publish a `risk.decision.rejected` event with reason `killswitch_engaged`.
3. WHEN `hedge-risk` receives a `sig.emitted` event AND a per-symbol cooldown is active, THE `hedge-risk` SHALL publish a `risk.decision.rejected` event with reason `cooldown_active`.
4. WHEN `hedge-risk` receives a `sig.emitted` event AND the AI priority for the signal's symbol is below the configured floor for the strategy, THE `hedge-risk` SHALL publish a `risk.decision.rejected` event with reason `below_priority_floor`.
5. WHEN `hedge-risk` receives a `sig.emitted` event AND configured sizing returns zero quantity, THE `hedge-risk` SHALL publish a `risk.decision.rejected` event with reason `size_zero`.
6. WHEN `hedge-risk` approves a signal, THE `hedge-risk` SHALL publish a `risk.decision.approved` event AND SHALL set a per-symbol cooldown of the configured duration.
7. THE `hedge-risk` SHALL NOT set a per-symbol cooldown on any rejection path (`killswitch_engaged`, `cooldown_active`, `below_priority_floor`, `size_zero`, or any other rejection reason).
8. WHEN `hedge-risk` approves a signal, THE `hedge-risk` SHALL persist the new cooldown to Redis warm cache before publishing `risk.decision.approved`; IF the cooldown persistence fails, THEN THE `hedge-risk` SHALL NOT publish `risk.decision.approved` AND SHALL publish `risk.decision.rejected` with reason `cooldown_persist_failed`.
9. THE `hedge-risk` SHALL persist active cooldowns and cumulative daily P&L in Redis warm cache so that a process restart preserves them.

### Requirement 7: Real Execution_Engine Implementation

**User Story:** As a trader, I want `hedge-exec` to route approved orders to Upstox primary with Angel One backup, so that the ExecutionPanel shows real broker activity.

#### Acceptance Criteria

1. THE `hedge-exec` binary SHALL subscribe to `risk.decision.approved` and broker fill streams from Upstox and Angel One.
2. WHEN `hedge-exec` receives a `risk.decision.approved` event, THE `hedge-exec` SHALL submit an order to the active broker AND publish an `exec.order.submitted` event with the same `correlation_id` as the approval.
3. WHEN the active broker returns a fill, THE `hedge-exec` SHALL publish an `exec.fill.<SYM>` event with the fill quantity, average price, and `correlation_id`.
4. WHEN the active broker returns an HTTP 5xx error or times out on order submit, THE `hedge-exec` SHALL fail over to the backup broker AND publish an `exec.broker.failover` event.
5. WHEN the active broker returns an HTTP 4xx error on order submit, THE `hedge-exec` SHALL publish an `exec.order.rejected` event AND SHALL NOT retry the submit on the backup broker; THE `hedge-exec` SHALL NOT publish `exec.broker.failover` for 4xx errors.
6. WHEN a position closes via cumulative offsetting fills, THE `hedge-exec` SHALL publish an `exec.trade.closed` event with realised P&L.
7. THE `hedge-exec` SHALL persist pending orders in Redis warm cache so that a process restart can reconcile them against broker state.

### Requirement 8: Real Position_Engine Implementation

**User Story:** As a trader, I want `hedge-position` to maintain per-symbol position state and live P&L from fills and ticks, so that the Positions and LivePnl panels are accurate.

#### Acceptance Criteria

1. THE `hedge-position` binary SHALL subscribe to `exec.fill.*` and Binary_Tick_Subject `md.tick.bin.>`.
2. WHEN `hedge-position` receives an `exec.fill.<SYM>` event, THE `hedge-position` SHALL update the per-symbol quantity AND average cost basis AND realised P&L for that symbol.
3. WHEN `hedge-position` receives a tick on `md.tick.bin.<SYM>` AND a non-zero position exists for that symbol, THE `hedge-position` SHALL update the unrealised P&L for that symbol using the tick's `ltp_paise`.
4. THE `hedge-position` SHALL publish a `pos.update.<SYM>` event for each fill within 100 milliseconds.
5. THE `hedge-position` SHALL publish a `pos.risk_state` event aggregating the entire portfolio at least once per second.
6. THE `hedge-position` SHALL satisfy the conservation-of-cash property: aggregate `pos.risk_state.total_realized_pnl + total_unrealized_pnl` shall equal the sum of `exec.fill.*.realized_pnl` modulo broker fees, after any sequence of fills and ticks.

### Requirement 9: Warm_AI_Pipeline Bring-Up

**User Story:** As a trader, I want the AI panels (AiConfidenceScores, AiExplanations, NewsFeed, TraderStabilityScore) to show real model output, so that I can read the system's interpretation of current conditions.

#### Acceptance Criteria

1. THE Start_Bat SHALL launch `python -m hedge_warm_ai.news.engine`, `python -m hedge_warm_ai.ranking.engine`, `python -m hedge_warm_ai.regime.engine`, and `python -m hedge_warm_ai.psych.engine` after Phase C completes.
2. THE `hedge_warm_ai.ranking.engine` SHALL subscribe to `sig.emitted` AND publish `ai.rank.<correlation_id>` events for each signal within 800 milliseconds, where `correlation_id` matches the originating signal's `correlation_id`.
3. THE `hedge_warm_ai.news.engine` SHALL publish `ai.news.impact.<topic>` events containing model-generated impact scores from configured news fetchers (HTTP, Twitter bearer token, Telegram bot token per `.env`).
4. THE `hedge_warm_ai.regime.engine` SHALL replace Demo_Synth as the publisher of `md.breadth.sector` and `md.breadth.volatility` once running.
5. THE `hedge_warm_ai.psych.engine` SHALL publish `ai.psych.stability` events at least once every 5 seconds.
6. WHEN the active Ollama model is unresponsive for longer than 5 seconds, THE Warm_AI_Pipeline SHALL publish `ai.ollama.degraded` AND switch to the configured fallback model.
7. IF the fallback model switch itself fails, THEN THE Warm_AI_Pipeline SHALL continue serving the most recent successful response path AND SHALL re-publish `ai.ollama.degraded` with reason `switch_failed` AND SHALL retry the switch on a bounded exponential backoff (initial 5s, max 60s).
8. THE Warm_AI_Pipeline SHALL NOT halt AI processing entirely on switch failure; it SHALL continue producing best-effort output until the switch succeeds or the operator intervenes.

### Requirement 10: Upstox Options-Chain Endpoint

**User Story:** As a trader, I want the OptionsChain panel to show live OI ladders, so that I can see strike-level participation for the underlying.

#### Acceptance Criteria

1. THE `upstox-feed` binary SHALL poll `https://api.upstox.com/v2/option/chain` for each underlying in `HEDGE_UPSTOX_OI_UNDERLYINGS` at a 5-second cadence.
2. THE `upstox-feed` SHALL default `HEDGE_UPSTOX_OI_UNDERLYINGS` to `NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank`.
3. WHILE option-chain polling is active, THE `upstox-feed` SHALL publish each polled chain on `md.oi.<UNDERLYING>` as a JSON envelope of `kind: "oi"` matching the `OpenInterest` type in `ui/src/types/market.ts`.
4. WHILE option-chain polling is stopped (for any reason, including a prior 401 response), THE `upstox-feed` SHALL NOT publish on `md.oi.<UNDERLYING>` AND SHALL leave the OptionsChain panel in a degraded state driven by `md.connection.upstox`.
5. WHEN the current expiry is less than 1 day away OR has already passed (negative days until expiry), THE `upstox-feed` SHALL auto-rotate the queried `expiry_date` to the next weekly expiry.
6. WHEN the Upstox option-chain endpoint returns an HTTP 401 response, THE `upstox-feed` SHALL stop polling for option chains AND publish `md.connection.upstox` with `status="down"` and `reason` containing `401`.
7. WHILE option-chain polling is stopped due to a 401, THE `upstox-feed` SHALL NOT resume polling until a successful re-authentication is observed (next successful auth refresh) AND THE `upstox-feed` SHALL publish `md.connection.upstox` with `status="up"` upon resumption.

### Requirement 11: Authority_Hierarchy Preservation Across Phase C

**User Story:** As a compliance-minded operator, I want every executed order to be traceable to a risk approval, so that no order escapes the risk gate.

#### Acceptance Criteria

1. WHEN `hedge-exec` publishes an `exec.order.submitted` event, THE `hedge-exec` SHALL include a `correlation_id` field whose value matches a `risk.decision.approved` event published earlier by `hedge-risk`.
2. THE `hedge-exec` SHALL NOT publish any `exec.order.submitted` event whose `correlation_id` has not appeared in a prior `risk.decision.approved` event.
3. WHEN `hedge-position` publishes a `pos.update.<SYM>` event derived from an `exec.fill.<SYM>` event, THE `hedge-position` SHALL include the same `correlation_id` as the fill.

### Requirement 12: Phase A / B / C Coexistence

**User Story:** As an operator running the system at any point in the build, I want each phase to remain functional as later phases ship, so that I can roll forward without regressions.

#### Acceptance Criteria

1. WHILE Phase A is running AND no Phase B or C publisher is active, THE Cockpit_UI SHALL render every panel populated with Synth_Tagged data.
2. WHILE Phase A and Phase B are both running, THE Cockpit_UI SHALL render `OrderflowHeatmap`, `Latency` (`TickIngest`, `FeatureExtraction`), `AiConfidenceScores`, and `AiExplanations` panels with non-Synth_Tagged data AND every other panel with Synth_Tagged data.
3. WHILE Phase A, Phase B, and Phase C are all running with `HEDGE_DEMO_SYNTH=off`, THE Cockpit_UI SHALL render every panel with non-Synth_Tagged data.
4. WHILE Phase A, Phase B, and Phase C are all running with `HEDGE_DEMO_SYNTH=on`, THE Demo_Synth SHALL be in Suppression_Window for every subject that has a real publisher AND emit only on subjects without a real publisher.
5. WHEN `HEDGE_DEMO_SYNTH=off`, THE Demo_Synth SHALL NOT be launched by Start_Bat; suppression behaviour is therefore inapplicable.
6. THE Suppression_Window mechanism SHALL apply only while Demo_Synth is running; no suppression behaviour applies when Demo_Synth is not running.

### Requirement 13: Synth Badge On Synth-Driven Panels

**User Story:** As a trader, I want each panel to clearly show whether its data is synthetic, so that I never confuse demo data for live data.

#### Acceptance Criteria

1. WHEN the most recent envelope applied to a panel's underlying store slice contains `"_synth": true`, THE Cockpit_UI SHALL render a compact `synth` badge in that panel's header.
2. WHEN the most recent envelope applied to a panel's underlying store slice does NOT contain `"_synth": true`, THE Cockpit_UI SHALL NOT render a `synth` badge in that panel's header.
3. THE Cockpit_UI SHALL update the `synth` badge state within 1 second of the most recent envelope being applied.

### Requirement 14: Build And Launch Surface

**User Story:** As an operator, I want one command to build everything and one command to launch everything, so that I can run the full system without remembering invocation details.

#### Acceptance Criteria

1. THE workspace SHALL accept `cargo build --release --workspace` at any point in the build.
2. WHEN `cargo build --release --workspace` is invoked AND Phase A has shipped, THE workspace SHALL produce `target/release/hedge-demo-synth.exe`.
3. WHEN `HEDGE_DEMO_SYNTH=on`, THE Start_Bat SHALL launch `target\release\hedge-demo-synth.exe` after launching `upstox-feed` AND before launching `hedge-ui-gateway`.
4. THE Start_Bat SHALL launch `hedge-ui-gateway` after `hedge-demo-synth.exe` has been started; THE Start_Bat SHALL NOT block `hedge-ui-gateway` startup on Demo_Synth's readiness.
5. WHEN every service in the launch order has been started, THE Start_Bat SHALL print a single summary block listing each window title with a one-word lifetime label (`long-running` or `one-shot`).
