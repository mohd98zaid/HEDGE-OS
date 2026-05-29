# Requirements Document

## Introduction

The PROJECT HEDGE React cockpit at `http://localhost:5173` is the trader's only window into the live state of the Hot_Path pipeline (market-data → orderflow → features → signals → risk → exec → position) and the Warm_AI_Pipeline. Today the cockpit successfully opens its WebSocket to `hedge-ui-gateway` (`ws gateway: open`) and the `upstox-feed` binary publishes JSON-shaped `md.tick.<SYMBOL>` and `md.book.<SYMBOL>` frames on NATS, but every data panel renders a generic "Awaiting first md.tick.* frame…" placeholder. The end-to-end pipeline has been verified at the WebSocket boundary using a raw client, so the gap is in the cockpit's subscription, dispatch, and presentation paths.

This feature delivers a working live dashboard: every panel that can render real data does so within seconds of `start.bat` completing during NSE_Trading_Hours, and every panel whose backing engine has not yet produced data shows a precise, panel-specific empty state that distinguishes "feed offline", "market closed", "engine not implemented yet", and "no events yet today" from each other. It also covers the three operational concerns that make the dashboard reliable in daily use: launch ordering in `start.bat`, IST log timestamps, broker token expiry alerting, and a Demo_Mode simulator that keeps the dashboard demonstrable when the market is closed.

This is a presentation- and integration-layer feature. It does not introduce new alpha logic, new broker adapters, or new Hot_Path engines; it surfaces the data those subsystems already produce and clearly labels the gaps where a subsystem is silent.

## Glossary

- **Cockpit_UI**: The React/TypeScript Single-Page-Application served from `ui/` at `http://localhost:5173`, fed entirely by the WebSocket connection to `hedge-ui-gateway` (no REST polling).
- **UI_Gateway**: The `hedge-ui-gateway` Rust binary that subscribes to curated NATS subjects, fan-outs each delivered message into per-connection Dispatchers, and forwards each event as a typed `ServerEnvelope` (`{channel, payload, ts_ns, subject?}`) on the cockpit WebSocket at `ws://127.0.0.1:8088/ws`.
- **Upstox_Feed**: The `upstox-feed.exe` binary in `crates/hedge-market-data/src/bin/upstox_feed.rs` that polls Upstox V2 REST endpoints (`/v2/market-quote/ltp` every 500 ms, `/v2/market-quote/quotes` every 2 s) and publishes `md.tick.<SYMBOL>`, `md.book.<SYMBOL>`, and `md.connection.upstox` events as `MarketEvent`-shaped JSON.
- **Market_Event**: The discriminated-union JSON shape consumed by the Cockpit_UI's `/market` reducer: `{kind: "tick"|"book"|"oi"|"breadth.volatility"|"breadth.sector"|"connection", data: {...}}`.
- **Server_Envelope**: The outer JSON frame the UI_Gateway sends to the Cockpit_UI: `{channel: ChannelId, payload: any, ts_ns?: number, subject?: string}`.
- **Cockpit_Channel**: One of the ten WebSocket channel identifiers consumed by the Cockpit_UI store: `market`, `orderflow`, `signals`, `risk`, `exec`, `news`, `psych`, `alerts`, `replay`, `latency`.
- **Cockpit_Panel**: A discrete React panel rendered inside the cockpit. The current set is: `LiveMarket`, `OrderflowHeatmap`, `OptionsChain`, `LivePnl`, `Positions`, `RiskPanel`, `ExecutionPanel`, `LatencyDashboard`, `AiConfidenceScores`, `AiExplanations`, `NewsFeed`, `TraderStabilityScore`, `Alerts`, `KillSwitchControl`, `StrategyToggles`, `SymbolPriorityControls`, `ReplayControls`.
- **Live_Tick_Render**: The end-to-end behavior of a `md.tick.*` payload appearing as a row in the LiveMarket panel.
- **Feed_Status**: The Cockpit_UI's banner-level summary of broker feed health derived from `md.connection.<source>` events and the age of the most-recent `md.tick.*` frame; valid values are `open`, `degraded`, `offline`, `token_expired`, `market_closed`, `demo_mode`.
- **NSE_Trading_Hours**: The interval `09:15:00`–`15:30:00` Asia/Kolkata (UTC+05:30) on NSE business days. Outside this window the broker feed publishes no live ticks.
- **IST_Timestamp**: A wall-clock timestamp formatted in Asia/Kolkata (UTC+05:30), suitable for display in panels and logs (e.g. `2025-12-04T10:14:33+05:30`).
- **Demo_Mode**: A trader-toggleable Cockpit_UI mode that consumes a deterministic in-process simulator instead of (or as a fallback to) the live broker feed, so the dashboard is never empty during development or after market hours.
- **Empty_State**: The text and visual treatment a Cockpit_Panel renders when it has no data to display, parameterised by reason: `feed_offline`, `market_closed`, `token_expired`, `engine_not_implemented`, `no_events_yet`, `demo_mode`.
- **Token_Expired_State**: The Feed_Status the Cockpit_UI enters when the UI_Gateway reports an Upstox 401 response or an `md.connection.upstox` event with `status="down"` and `reason` containing `401`/`unauthorized`.
- **Start_Bat**: The Windows orchestrator script `start.bat` that brings up Docker infra (NATS, Redis, Postgres, Qdrant), then session controller, supervisor, every Hot_Path engine, the UI_Gateway, and the Cockpit_UI in dependency order.
- **Engine_Not_Implemented_State**: The Empty_State a Cockpit_Panel renders when its backing Hot_Path or Warm_AI_Pipeline subject group (e.g. `of.heatmap.*`, `feat.*`, `sig.emitted`, `obs.latency.*`) has not produced any events since the cockpit connected and is known not to publish in the current build.
- **Connection_Banner**: The Cockpit_UI element near the top of the layout that displays the current Feed_Status and a human-readable detail string.
- **Live_Data_Panels**: The Cockpit_Panels that depend on a live broker feed: `LiveMarket`, `OrderflowHeatmap`, `OptionsChain`, `LivePnl`, `Positions`, `RiskPanel`, `ExecutionPanel`.
- **Engine_Backed_Panels**: The Cockpit_Panels that depend on Hot_Path or Warm_AI_Pipeline outputs other than raw market data: `OrderflowHeatmap`, `LatencyDashboard`, `AiConfidenceScores`, `AiExplanations`, `NewsFeed`, `TraderStabilityScore`, `StrategyToggles` (last-emitted timestamp).

## Requirements

### Requirement 1: Live Tick Rendering In LiveMarket Panel

**User Story:** As a trader, I want the LiveMarket panel to display ticking LTP and best bid/ask for every configured symbol within seconds of starting the system, so that I can confirm the live data pipeline is healthy before I trade.

#### Acceptance Criteria

1. WHEN the Cockpit_UI receives a Server_Envelope with `channel="market"` and a Market_Event of `kind="tick"`, THE Cockpit_UI SHALL update the LiveMarket panel row for `data.symbol` with `data.ltp_paise`, `data.bid_paise`, `data.ask_paise`, and `data.ts_recv_ns` within 100 milliseconds of envelope receipt.
2. WHEN the Cockpit_UI receives a Server_Envelope with `channel="market"` and a Market_Event of `kind="book"`, THE Cockpit_UI SHALL update the LiveMarket panel row for `data.symbol` with `data.bid_paise` and `data.ask_paise` within 100 milliseconds of envelope receipt.
3. WHILE Upstox_Feed is publishing `md.tick.*` events on NATS during NSE_Trading_Hours, THE Cockpit_UI SHALL display at least one populated symbol row in the LiveMarket panel within 10 seconds of the cockpit WebSocket reaching `state="open"`.
4. THE Cockpit_UI SHALL render the `ts_recv_ns` field in each LiveMarket row as an IST_Timestamp-formatted age (relative `Xs ago`) using the trader's local clock.
5. WHEN the LiveMarket panel has at least one populated symbol row, THE Cockpit_UI SHALL hide the `Awaiting first md.tick.* frame …` placeholder and display the symbol table.
6. IF a Server_Envelope is received with `channel="market"` and a payload that fails Market_Event schema validation, THEN THE Cockpit_UI SHALL log a single warning naming the violating field and SHALL leave existing tick rows unchanged.
7. THE Cockpit_UI SHALL bind the `/market` channel subscription on every reconnect of the cockpit WebSocket without requiring a page reload.

### Requirement 2: Broker Feed Lifecycle And Connection Banner

**User Story:** As a trader, I want a single banner that tells me at a glance whether the broker feed is producing live data, so that I never confuse a stale display with a live market.

#### Acceptance Criteria

1. THE Cockpit_UI SHALL render a Connection_Banner that displays exactly one Feed_Status value at any time.
2. WHEN the cockpit WebSocket transitions to `state="open"` AND a `md.tick.*` Market_Event has been received within the last 5 seconds, THE Cockpit_UI SHALL set Feed_Status to `open`.
3. WHEN no `md.tick.*` Market_Event has been received for at least 5 seconds AND the most recent `md.connection.upstox` event reports `status="ok"`, THE Cockpit_UI SHALL set Feed_Status to `degraded`.
4. WHEN the most recent `md.connection.upstox` event reports `status="down"` OR no `md.tick.*` Market_Event has been received for at least 30 seconds AND the local IST clock is inside NSE_Trading_Hours, THE Cockpit_UI SHALL set Feed_Status to `offline`.
5. WHEN the most recent `md.connection.upstox` event reports `status="down"` AND its `reason` field contains the substring `401` or `unauthorized` (case-insensitive), THE Cockpit_UI SHALL set Feed_Status to `token_expired`.
6. WHEN the local IST clock is outside NSE_Trading_Hours AND Feed_Status is not `demo_mode`, THE Cockpit_UI SHALL set Feed_Status to `market_closed`.
7. WHILE Demo_Mode is active, THE Cockpit_UI SHALL set Feed_Status to `demo_mode` regardless of broker feed events.
8. THE Cockpit_UI SHALL render the Feed_Status with a distinct color and a one-line human-readable detail string.
9. WHEN Feed_Status is `token_expired`, THE Cockpit_UI SHALL display a detail string instructing the trader to refresh `HEDGE_UPSTOX_ACCESS_TOKEN` in `.env` and re-run Start_Bat.

### Requirement 3: Empty And Error States Per Panel

**User Story:** As a trader, I want each cockpit panel to tell me precisely why it has no data, so that I can distinguish a broken feed from an unimplemented engine from an idle market.

#### Acceptance Criteria

1. THE Cockpit_UI SHALL render an Empty_State in every Cockpit_Panel that has no data to display.
2. THE Cockpit_UI SHALL render an Empty_State whose reason is one of `feed_offline`, `market_closed`, `token_expired`, `engine_not_implemented`, `no_events_yet`, `demo_mode`.
3. WHEN a Live_Data_Panel has no data AND Feed_Status is `offline`, THE Cockpit_UI SHALL render that panel's Empty_State with reason `feed_offline`.
4. WHEN a Live_Data_Panel has no data AND Feed_Status is `market_closed`, THE Cockpit_UI SHALL render that panel's Empty_State with reason `market_closed`.
5. WHEN a Live_Data_Panel has no data AND Feed_Status is `token_expired`, THE Cockpit_UI SHALL render that panel's Empty_State with reason `token_expired`.
6. WHEN an Engine_Backed_Panel has no data AND its backing NATS subject group is known to have no active publisher in the running build, THE Cockpit_UI SHALL render that panel's Empty_State with reason `engine_not_implemented` AND SHALL display the panel-specific subject group (e.g. `of.heatmap.*`, `obs.latency.*`, `sig.emitted`).
7. WHEN a panel has no data AND none of the conditions in 3–6 apply, THE Cockpit_UI SHALL render that panel's Empty_State with reason `no_events_yet`.
8. THE Cockpit_UI SHALL NOT render two distinct panels with the identical placeholder string `Awaiting first md.tick.* frame …`.
9. WHEN Demo_Mode is active AND a panel is being driven by the simulator, THE Cockpit_UI SHALL display a `demo_mode` indicator inside that panel.
10. WHEN a Cockpit_Panel is an Engine_Backed_Panel AND has no data, THE Cockpit_UI SHALL NOT use the `feed_offline` reason.

### Requirement 4: Order Book Depth In LiveMarket And RiskPanel Top-Of-Book

**User Story:** As a trader, I want best bid/ask updated from the depth feed within seconds, so that I can see the live spread before placing an order.

#### Acceptance Criteria

1. WHEN the Cockpit_UI receives a Server_Envelope with `channel="market"` and a Market_Event of `kind="book"` for a symbol, THE Cockpit_UI SHALL display the symbol's `bid_paise` and `ask_paise` in the LiveMarket panel within 100 milliseconds of envelope receipt.
2. WHILE Upstox_Feed is publishing `md.book.*` events on NATS during NSE_Trading_Hours, THE Cockpit_UI SHALL display non-zero `bid_paise` and `ask_paise` values for at least one symbol in the LiveMarket panel within 10 seconds of the cockpit WebSocket reaching `state="open"`.
3. WHERE the running build has no Hot_Path publisher for full L5 depth heatmaps, THE Cockpit_UI SHALL render the OrderflowHeatmap panel with reason `engine_not_implemented` referencing the `of.heatmap.*` subject group.
4. IF a Market_Event of `kind="book"` arrives with `bid_paise=0`, THEN THE Cockpit_UI SHALL leave any previously-displayed bid for that symbol unchanged. IF a Market_Event of `kind="book"` arrives with `ask_paise=0`, THEN THE Cockpit_UI SHALL leave any previously-displayed ask for that symbol unchanged.

### Requirement 5: Engine_Backed_Panel Coverage And Differentiated Empty States

**User Story:** As a trader, I want every engine-backed panel to either show real data or clearly tell me the engine is silent in this build, so that I do not mistake a missing engine for a broken UI.

#### Acceptance Criteria

1. THE Cockpit_UI SHALL classify each Engine_Backed_Panel against its primary backing subject group: OrderflowHeatmap → `of.heatmap.*`, LatencyDashboard → `obs.latency.*` and `obs.budget.breach.*`, AiConfidenceScores → `sig.emitted` joined with `ai.rank.*`, AiExplanations → `sig.emitted` joined with `ai.rank.*`, NewsFeed → `ai.news.impact.*`, TraderStabilityScore → `ai.psych.stability`.
2. WHEN an Engine_Backed_Panel has received at least one matching Server_Envelope on its backing channel, THE Cockpit_UI SHALL render the panel's data view.
3. WHEN an Engine_Backed_Panel has received zero matching Server_Envelopes since the cockpit WebSocket reached `state="open"` AND the backing subject group has no active publisher in the running build, THE Cockpit_UI SHALL render the panel's Empty_State with reason `engine_not_implemented`.
4. THE Cockpit_UI SHALL display the backing subject group (e.g. `of.heatmap.*`) inside the Engine_Not_Implemented_State.
5. WHEN the Cockpit_UI is in Engine_Not_Implemented_State for a panel, THE Cockpit_UI SHALL NOT log a recurring warning every render frame.

### Requirement 6: IST Log Timestamps

**User Story:** As an operator, I want every log line emitted by every native binary launched from `start.bat` to use IST timestamps, so that I can correlate cockpit events with broker activity without doing UTC↔IST math.

#### Acceptance Criteria

1. THE UI_Gateway SHALL emit every log line with an IST_Timestamp.
2. THE Upstox_Feed SHALL emit every log line with an IST_Timestamp.
3. THE Cockpit_UI SHALL emit every browser-console log line with an IST_Timestamp.
4. WHEN Start_Bat launches a Hot_Path engine binary, THE Start_Bat SHALL set the `TZ` environment variable for that process to `Asia/Kolkata`.
5. WHERE a binary uses the `tracing-subscriber` formatter, THE binary SHALL configure its time formatter to render in `Asia/Kolkata` with a `+05:30` offset suffix.
6. THE IST_Timestamp format used in logs SHALL include date, time to millisecond precision, and the `+05:30` offset (e.g. `2025-12-04T10:14:33.482+05:30`).

### Requirement 7: Start_Bat Launch Ordering And No-Op Service Suppression

**User Story:** As an operator, I want `start.bat` to launch only services that actually run, in the right order, so that the cockpit becomes live in one click without me having to triage immediately-exiting windows.

#### Acceptance Criteria

1. WHEN Start_Bat is invoked, THE Start_Bat SHALL launch services in the order: Docker_Infra → NATS_Ready_Probe → session controller → supervisor → Upstox_Feed → Hot_Path engines (orderflow, features, signals, risk, exec, position) → UI_Gateway → Cockpit_UI.
2. THE Start_Bat SHALL NOT launch the `hedge-replay` binary as a long-running service.
3. WHERE the `hedge-replay` binary is an inspector CLI, THE Start_Bat SHALL document its CLI usage in a comment within the script and SHALL omit it from the auto-launched service set.
4. IF `HEDGE_UPSTOX_ACCESS_TOKEN` is not set in the merged environment, THEN THE Start_Bat SHALL print a warning naming the missing variable and SHALL NOT launch Upstox_Feed.
5. WHEN Start_Bat launches a Hot_Path engine binary that does not yet publish on its expected NATS subject group, THE Start_Bat SHALL log an informational note that the engine is running in placeholder mode AND SHALL still launch the binary so that the supervisor can observe its lifecycle.
6. WHEN Start_Bat launches the Cockpit_UI dev server, THE Start_Bat SHALL set `VITE_HEDGE_GATEWAY_URL` to `ws://127.0.0.1:8088/ws`.
7. WHEN every service in the ordered launch list has been started, THE Start_Bat SHALL print a summary block listing each service window title, its dashboard URL where applicable, and its expected lifetime (`long-running` or `one-shot`).
8. IF a launched binary exits within 3 seconds of being started, THEN THE Start_Bat SHALL print a warning naming the binary and pointing the operator to its window for the error message.

### Requirement 8: Demo_Mode Simulator Fallback

**User Story:** As a trader or developer, I want a demo mode that drives the cockpit with simulated ticks when the market is closed, so that I can validate UI changes without waiting for `09:15` IST.

#### Acceptance Criteria

1. THE Cockpit_UI SHALL expose a Demo_Mode toggle accessible from the Connection_Banner.
2. WHEN the trader activates the Demo_Mode toggle, THE Cockpit_UI SHALL start an in-process Demo_Mode simulator that produces Market_Event payloads of `kind="tick"` and `kind="book"` for at least 5 symbols at a cadence of at least 1 event per second per symbol.
3. WHILE Demo_Mode is active, THE Cockpit_UI SHALL NOT apply any Server_Envelope received from the UI_Gateway to the `/market` slice of the cockpit store.
4. WHEN Demo_Mode is deactivated, THE Cockpit_UI SHALL resume applying live Server_Envelopes from the UI_Gateway within 1 second.
5. THE Cockpit_UI SHALL persist the Demo_Mode toggle state in `localStorage` so it survives a page reload.
6. WHERE the local IST clock is outside NSE_Trading_Hours AND no live `md.tick.*` events have been received within the last 60 seconds AND Demo_Mode has not been explicitly deactivated by the trader in the current browser session, THE Cockpit_UI SHALL display an unobtrusive prompt offering to enable Demo_Mode.
7. WHILE Demo_Mode is active, THE Cockpit_UI SHALL render the `demo_mode` indicator inside every panel driven by the simulator.
8. THE Demo_Mode simulator SHALL produce deterministic price walks for a fixed seed so that visual regression tests can drive the cockpit reproducibly.
9. WHILE Demo_Mode is active, THE Cockpit_UI SHALL discard incoming Server_Envelopes received on data channels (market, orderflow, signals, risk, exec, news, psych, latency, replay) and SHALL NOT buffer them for later replay.

### Requirement 9: Token_Expired_State Surfacing And Recovery Hint

**User Story:** As a trader, I want the cockpit to tell me clearly when my Upstox access token has expired and how to fix it, so that I do not stare at stale prices for hours.

#### Acceptance Criteria

1. WHEN the Upstox_Feed startup probe receives an HTTP 401 response, THE Upstox_Feed SHALL publish an `md.connection.upstox` event with `status="down"` and `reason` containing the substring `401 unauthorized`.
2. WHEN the Cockpit_UI sets Feed_Status to `token_expired`, THE Cockpit_UI SHALL render an alert banner in the Alerts panel containing the literal string `Upstox access token expired` and the literal string `HEDGE_UPSTOX_ACCESS_TOKEN`.
3. WHEN Feed_Status is `token_expired`, THE Cockpit_UI SHALL link the Connection_Banner detail string to the `.env` filename (display only; no URL).
4. WHEN Feed_Status transitions from `token_expired` back to `open` after a successful token refresh and Upstox_Feed restart, THE Cockpit_UI SHALL clear the `Upstox access token expired` alert from the Alerts panel within 5 seconds.
5. THE Cockpit_UI SHALL NOT enter Token_Expired_State based solely on the absence of `md.tick.*` events; only an explicit `md.connection.upstox` event with a 401-bearing `reason` SHALL trigger Token_Expired_State.

### Requirement 10: Subscription Resilience Across Reconnect

**User Story:** As a trader, I want the cockpit to recover its subscriptions automatically after a transient WebSocket drop, so that my dashboard does not need a manual page reload to come back.

#### Acceptance Criteria

1. WHEN the cockpit WebSocket transitions from `state="open"` to `state="reconnecting"`, THE Cockpit_UI SHALL preserve every cockpit-store slice's data unchanged.
2. WHEN the cockpit WebSocket transitions from `state="reconnecting"` to `state="open"`, THE Cockpit_UI SHALL re-issue subscribe frames for every Cockpit_Channel within 1 second.
3. WHILE the cockpit WebSocket is in `state="reconnecting"`, THE Cockpit_UI SHALL display a Connection_Banner detail string indicating reconnect-in-progress.
4. IF the cockpit WebSocket has been in `state="reconnecting"` continuously for at least 30 seconds, THEN THE Cockpit_UI SHALL set Feed_Status to `offline`.
5. WHEN a `md.tick.*` Market_Event arrives after a successful reconnect, THE Cockpit_UI SHALL set Feed_Status to `open` within 1 second.

### Requirement 11: Panel-Level Data Freshness Indicators

**User Story:** As a trader, I want each panel to show how fresh its data is, so that I can spot a stalled stream even when the broker feed banner says `open`.

#### Acceptance Criteria

1. THE Cockpit_UI SHALL display a `last update` indicator on every Live_Data_Panel and Engine_Backed_Panel, showing the IST_Timestamp-formatted age of the most-recent Server_Envelope applied to that panel.
2. WHEN the most-recent Server_Envelope applied to a Live_Data_Panel is older than 10 seconds (i.e. age strictly greater than 10000 ms) AND Feed_Status is `open`, THE Cockpit_UI SHALL render that panel's `last update` indicator with a warning color.
3. WHEN the most-recent Server_Envelope applied to a Live_Data_Panel is older than 60 seconds (age strictly greater than 60000 ms) AND Feed_Status is `open`, THE Cockpit_UI SHALL render that panel's `last update` indicator with a danger color.
4. THE `last update` indicator SHALL update at least once per second while the panel is mounted.
