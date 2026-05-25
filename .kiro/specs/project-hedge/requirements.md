# Requirements Document

## Introduction

PROJECT HEDGE is a production-grade, ultra-low latency AI-assisted intraday trading operating system for the Indian stock market (NSE/BSE). The system functions as a professional trading cockpit that combines deterministic hot-path execution (Rust) with asynchronous AI-assisted decision support (local Ollama LLMs, classical ML), under the final authority of a Risk Engine and a human trader.

The system targets an Indian retail intraday trader with a capital base of ₹20,000 and a daily profit target of ₹300-₹1,000, operating during NSE/BSE trading hours (09:15 AM - 03:30 PM IST). It is explicitly NOT an autonomous trading bot, prediction engine, or strategy backtester; it is a human-in-the-loop assistant with disciplined risk governance.

The architecture is organized into four layers:
1. **Hot Path** - Deterministic Rust services for market data, orderflow, features, signals, risk, execution, and positions (tick-to-trade < 50ms).
2. **Warm AI Pipeline** - Asynchronous AI services for context, news, regime, psychology, ranking, and journaling.
3. **Memory + RAG** - Vector and time-series storage for trades, market memory, and psychology history.
4. **Human Control UI** - React/TypeScript cockpit driven by WebSockets.

This document captures the functional and quality requirements that the system must satisfy. Technical design (specific data structures, API shapes, deployment topology) is deferred to the design phase.

## Glossary

- **Hot_Path**: The synchronous Rust execution pipeline from market tick to broker order, with a strict tick-to-trade latency budget of 50 milliseconds.
- **Warm_AI_Pipeline**: The asynchronous AI inference and reasoning subsystem that provides context, ranking, and explanations but is not on the execution path.
- **Tick_To_Trade_Latency**: The time elapsed from receipt of a market tick by the Market_Data_Engine to dispatch of a broker order by the Execution_Engine.
- **Market_Data_Engine**: The Hot_Path component that ingests, parses, and routes NSE/BSE tick data, orderbook updates, and options chain data.
- **Orderflow_Engine**: The Hot_Path component that analyzes bid/ask imbalance, aggression, absorption, spoofing, and liquidity pressure from the live orderbook.
- **Feature_Extraction_Engine**: The Hot_Path component that computes technical features (VWAP, ATR, EMA, momentum, volatility, etc.) incrementally and in-memory.
- **Signal_Engine**: The Hot_Path component that emits base trade signals from configured strategies (ORB, VWAP Pullback, Momentum Breakout, Liquidity Sweep Reversal, OI Expansion Breakout, Volatility Compression Breakout).
- **Risk_Engine**: The Hot_Path component that holds final authority over all order dispatch decisions and enforces risk limits.
- **Execution_Engine**: The Hot_Path component responsible for low-latency order routing, lifecycle management, retries, and broker failover.
- **Position_Engine**: The Hot_Path component that tracks live positions, realized and unrealized PnL, exposure, and margin.
- **Ollama_Infrastructure**: The local LLM inference subsystem hosting Qwen2.5:14B, Mistral:7B, DeepSeek-R1, and Phi models as independent microservices.
- **News_Intelligence_Engine**: The Warm_AI_Pipeline component that ingests, parses, and scores news from configured sources (Reuters, Moneycontrol, NSE filings, RBI, Twitter/X, Telegram, Economic Times, broker feeds).
- **Market_Regime_Engine**: The Warm_AI_Pipeline component that classifies the current market regime (trending, sideways, panic, high volatility, news-driven, liquidity crisis, low participation).
- **Symbol_Priority_Engine**: The Warm_AI_Pipeline component that assigns symbols to priority tiers P1, P2, P3, or P4 with corresponding resource allocation.
- **Previous_Day_Memory_Engine**: The Warm_AI_Pipeline component that retains and exposes prior-session structural data (highs, lows, failed breakouts, gap reactions, delivery volume).
- **Trader_Psychology_Engine**: The Warm_AI_Pipeline component that monitors trader behavior and computes Trader_Stability_Score.
- **Trader_Stability_Score**: A scalar in [0.0, 1.0] computed as 0.35×Discipline + 0.25×EmotionalControl + 0.20×RiskConsistency + 0.20×Patience.
- **AI_Trade_Ranking_Engine**: The Warm_AI_Pipeline component that ranks candidate signals using Trade_Confidence_Score.
- **Trade_Confidence_Score**: A scalar in [0.0, 1.0] computed as 0.30×Orderflow + 0.25×TechnicalStrength + 0.20×NewsSentiment + 0.15×MarketRegime + 0.10×TraderDiscipline.
- **AI_Trade_Journal_Engine**: The Warm_AI_Pipeline component that produces post-trade explanations covering outcome, emotional state, regime, missed opportunities, and execution quality.
- **Adaptive_Risk**: A scalar derived as BaseRisk × MarketStability × SignalConfidence × TraderDiscipline, used to size positions and gate signals.
- **Memory_RAG_Layer**: The persistence subsystem combining Qdrant (vector), PostgreSQL with TimescaleDB (time-series and relational), and Redis (cache).
- **Human_Control_UI**: The React/TypeScript trader cockpit communicating with backend services via WebSockets.
- **Replay_Engine**: The subsystem that records and re-plays ticks, orders, news, trader actions, AI decisions, and market conditions.
- **AI_Shadow_Mode**: An operating mode in which Warm_AI_Pipeline outputs are recorded and scored against actual outcomes but do not influence trade ranking surfaced to the trader.
- **AI_Governance_Engine**: The component that monitors model drift, confidence stability, and prediction quality, and reduces AI influence when degradation is detected.
- **Self_Healing_Infrastructure**: The supervisory subsystem that detects and recovers from websocket disconnects, Redis failures, broker failures, and VPS restarts.
- **Market_Open_War_Mode**: The operating mode active from 09:15:00 IST to 09:45:00 IST that increases scan frequency and suppresses weak signals.
- **Kill_Switch**: A trader- or Risk_Engine-triggered control that halts all new order entry and optionally flattens open positions.
- **Authority_Hierarchy**: The decision precedence order: Risk_Engine > Execution_Engine > Signal_Engine (Quant Core) > Warm_AI_Pipeline > Trader_Input.
- **Broker_Adapter**: A pluggable Hot_Path module that translates internal order intents to broker-specific APIs for Zerodha, Dhan, Shoonya, or Angel One.
- **Trading_Session**: The interval from 09:15:00 IST to 15:30:00 IST on an NSE/BSE trading day.
- **NATS_Bus**: The NATS messaging system used for inter-service event distribution.
- **Redis_Stream**: A Redis Streams channel used for ordered event distribution within the Hot_Path.

## Requirements

### Requirement 1: Market Data Ingestion

**User Story:** As a trader, I want the system to ingest NSE/BSE market data with minimal latency, so that all downstream analysis reflects the current market state.

#### Acceptance Criteria

1. THE Market_Data_Engine SHALL ingest NSE and BSE tick data, orderbook updates, options chain data, and open interest data via WebSocket connections.
2. WHEN a market tick is received, THE Market_Data_Engine SHALL parse the tick and emit a normalized event to the NATS_Bus within 1 millisecond.
3. WHEN a market tick is parsed, THE Market_Data_Engine SHALL complete tick processing and downstream routing within 2 milliseconds.
4. THE Market_Data_Engine SHALL distribute tick events using lock-free, zero-copy data structures.
5. WHERE FlatBuffers serialization is configured, THE Market_Data_Engine SHALL serialize outbound tick events using FlatBuffers.
6. IF a WebSocket connection to a market data source disconnects, THEN THE Market_Data_Engine SHALL attempt reconnection and emit a connection-status event to the NATS_Bus.
7. THE Market_Data_Engine SHALL compute and publish sector breadth and volatility breadth metrics on each tick batch.
8. THE Market_Data_Engine SHALL route tick events per symbol to subscribed Hot_Path consumers without polling.

### Requirement 2: Orderflow Analysis (Primary Alpha Source)

**User Story:** As a trader, I want real-time orderflow analytics, so that I can detect aggression, absorption, and liquidity pressure as the primary alpha source.

#### Acceptance Criteria

1. THE Orderflow_Engine SHALL compute bid/ask imbalance, aggressive-buyer volume, aggressive-seller volume, and rolling delta on each orderbook update.
2. THE Orderflow_Engine SHALL detect liquidity gaps, absorption events, and hidden liquidity, and emit a typed event for each detection.
3. WHEN spoofing-pattern criteria are met on the live orderbook, THE Orderflow_Engine SHALL emit a spoofing-alert event.
4. THE Orderflow_Engine SHALL maintain a live orderflow heatmap data structure accessible to the Human_Control_UI via WebSocket.
5. THE Orderflow_Engine SHALL compute a liquidity pressure score in the range [-1.0, 1.0] for each tracked symbol on each orderbook update.
6. THE Orderflow_Engine SHALL process each orderbook update without allocating heap memory in the steady-state path.

### Requirement 3: Feature Extraction

**User Story:** As a trader, I want technical features computed incrementally on every tick, so that strategies receive up-to-date inputs without batch latency.

#### Acceptance Criteria

1. THE Feature_Extraction_Engine SHALL compute VWAP, ATR, EMA, EMA slope, realized volatility, momentum, and rolling delta incrementally per symbol.
2. THE Feature_Extraction_Engine SHALL compute liquidity imbalance, orderflow strength, candle structure, breakout pressure, compression-zone indicators, and liquidity-sweep indicators per symbol.
3. WHEN a tick or orderbook update is received, THE Feature_Extraction_Engine SHALL update all dependent features within 3 milliseconds.
4. THE Feature_Extraction_Engine SHALL hold all live feature state in-memory.
5. THE Feature_Extraction_Engine SHALL stream feature updates to the Signal_Engine via in-process channels or NATS_Bus subjects.
6. THE Feature_Extraction_Engine SHALL NOT depend on pandas, NumPy, or any Python runtime in the Hot_Path.

### Requirement 4: Signal Generation Strategies

**User Story:** As a trader, I want a set of well-defined intraday strategies, so that the system surfaces selective high-probability trade candidates.

#### Acceptance Criteria

1. THE Signal_Engine SHALL implement the following strategies: Opening_Range_Breakout, VWAP_Pullback, Momentum_Breakout, Liquidity_Sweep_Reversal, Options_OI_Expansion_Breakout, and Volatility_Compression_Breakout.
2. WHEN a strategy's preconditions are satisfied, THE Signal_Engine SHALL emit a signal event containing strategy identifier, symbol, side, base probability score, confidence score, and risk profile.
3. THE Signal_Engine SHALL constrain each signal's base probability score and confidence score to the range [0.0, 1.0].
4. THE Signal_Engine SHALL evaluate strategies on each feature update without polling.
5. WHERE a strategy is disabled by trader configuration, THE Signal_Engine SHALL suppress emission of signals from that strategy.
6. WHEN the Market_Regime_Engine reports a regime in which a strategy is configured to be disabled, THE Signal_Engine SHALL suppress emission of signals from that strategy.

### Requirement 5: Risk Engine Authority and Limits

**User Story:** As a trader, I want a Risk Engine with final authority over all orders, so that capital is preserved and discipline is enforced regardless of AI or trader impulse.

#### Acceptance Criteria

1. THE Risk_Engine SHALL hold final authority over order dispatch and SHALL be the only component permitted to grant the Execution_Engine permission to send an order.
2. THE Risk_Engine SHALL enforce a configurable maximum daily loss limit, and WHEN realized plus unrealized loss for the Trading_Session reaches the limit, THE Risk_Engine SHALL block all new order entries for the remainder of the Trading_Session.
3. THE Risk_Engine SHALL enforce a configurable maximum position size per symbol and per portfolio.
4. THE Risk_Engine SHALL enforce configurable leverage limits per symbol and per account.
5. THE Risk_Engine SHALL enforce a configurable maximum drawdown limit, and IF the limit is breached, THEN THE Risk_Engine SHALL activate the Kill_Switch.
6. THE Risk_Engine SHALL enforce a configurable maximum trade frequency per minute, per hour, and per Trading_Session.
7. THE Risk_Engine SHALL enforce configurable maximum exposure per symbol and per sector.
8. WHEN observed slippage on a recent fill exceeds a configurable threshold, THE Risk_Engine SHALL apply a configurable cooldown during which new orders for the affected symbol are blocked.
9. WHEN the Kill_Switch is activated, THE Risk_Engine SHALL block all new order entries and SHALL emit a Kill_Switch_Activated event.
10. WHEN realized volatility for a symbol exceeds a configurable threshold, THE Risk_Engine SHALL block new entries for that symbol until volatility returns below the threshold.
11. WHEN measured broker round-trip latency exceeds a configurable threshold, THE Risk_Engine SHALL block new order entries to that broker.
12. THE Risk_Engine SHALL evaluate any single order request and produce an approve-or-reject decision within 2 milliseconds.
13. THE Risk_Engine SHALL compute Adaptive_Risk as BaseRisk × MarketStability × SignalConfidence × TraderDiscipline and SHALL use Adaptive_Risk to scale position size for approved orders.
14. THE Risk_Engine SHALL override conflicting requests from the Warm_AI_Pipeline, the Execution_Engine, the Signal_Engine, and trader inputs in accordance with the Authority_Hierarchy.

### Requirement 6: Execution Engine

**User Story:** As a trader, I want low-latency order execution with adaptive routing and broker failover, so that approved trades are placed quickly and reliably.

#### Acceptance Criteria

1. THE Execution_Engine SHALL submit a Risk_Engine-approved order to the active Broker_Adapter and complete routing within 5 milliseconds of approval.
2. THE Execution_Engine SHALL support market and limit order types.
3. WHEN an order receives a partial fill, THE Execution_Engine SHALL update the Position_Engine and continue managing the remaining quantity per the order's configured policy.
4. WHEN a broker order request fails with a retryable error, THE Execution_Engine SHALL retry the request up to a configurable maximum number of attempts.
5. WHEN measured broker latency or error rate breaches a configurable failover threshold, THE Execution_Engine SHALL switch to a configured backup Broker_Adapter and emit a broker-failover event.
6. THE Execution_Engine SHALL track the lifecycle state of each order through New, Submitted, Partially_Filled, Filled, Cancelled, and Rejected, and SHALL publish state-transition events.
7. THE Execution_Engine SHALL adapt order type and aggressiveness based on Risk_Engine-approved execution parameters.
8. THE Execution_Engine SHALL NOT submit any order without a current approval from the Risk_Engine.

### Requirement 7: Broker Integrations

**User Story:** As a trader, I want pluggable broker integrations with Zerodha, Dhan, Shoonya, and Angel One, so that the system can connect to my chosen broker and fail over when needed.

#### Acceptance Criteria

1. THE Execution_Engine SHALL support Broker_Adapter implementations for Zerodha, Dhan, Shoonya, and Angel One.
2. THE Broker_Adapter SHALL expose a uniform internal interface for order placement, modification, cancellation, and status queries.
3. WHEN a Broker_Adapter receives an internal order intent, THE Broker_Adapter SHALL translate the intent into the broker-specific API call.
4. THE Broker_Adapter SHALL emit broker latency and error metrics on each request.
5. IF broker authentication credentials are missing or invalid at startup, THEN THE Broker_Adapter SHALL emit a configuration-error event and SHALL refuse to accept order requests.

### Requirement 8: Position and PnL Tracking

**User Story:** As a trader, I want live position and PnL tracking, so that I can see exposure and performance in real time.

#### Acceptance Criteria

1. THE Position_Engine SHALL maintain live positions per symbol with quantity, average entry price, realized PnL, and unrealized PnL.
2. WHEN a fill event is received, THE Position_Engine SHALL update the affected position and recompute realized and unrealized PnL within 5 milliseconds.
3. WHEN a market tick is received for a held symbol, THE Position_Engine SHALL update unrealized PnL for that symbol.
4. THE Position_Engine SHALL expose current exposure, used margin, and per-strategy capital allocation.
5. THE Position_Engine SHALL publish a trader risk state event containing aggregate exposure, drawdown, and available margin to the Risk_Engine and the Human_Control_UI.

### Requirement 9: Tick-to-Trade Latency Budget

**User Story:** As a trader, I want a strict end-to-end latency budget, so that the system remains competitive on intraday execution.

#### Acceptance Criteria

1. THE Hot_Path SHALL achieve Tick_To_Trade_Latency of less than 50 milliseconds at the 99th percentile during a Trading_Session under nominal load.
2. THE Hot_Path SHALL be implemented in Rust with Tokio asynchronous runtime.
3. THE Hot_Path SHALL be event-driven across NATS_Bus subjects and Redis_Streams, with no polling loops in steady-state operation.
4. THE Hot_Path SHALL NOT invoke any LLM inference call.
5. THE Hot_Path SHALL NOT invoke any blocking external HTTP API on the per-tick path.
6. THE Hot_Path SHALL NOT depend on cloud-hosted services for execution decisions.
7. THE Hot_Path SHALL emit per-stage latency measurements (tick ingest, feature extraction, signal evaluation, AI scoring fetch, risk check, execution routing) for every order request.

### Requirement 10: Ollama AI Infrastructure

**User Story:** As a trader, I want all LLM inference to run locally via Ollama, so that the system has no cloud dependency for reasoning.

#### Acceptance Criteria

1. THE Ollama_Infrastructure SHALL host Qwen2.5:14B as the primary reasoning service.
2. THE Ollama_Infrastructure SHALL host Mistral:7B as the fast assistant service.
3. THE Ollama_Infrastructure SHALL host DeepSeek-R1 as the deep reasoning service.
4. THE Ollama_Infrastructure SHALL host a Phi model as the lightweight service.
5. THE Ollama_Infrastructure SHALL run each model as an independent microservice.
6. THE Ollama_Infrastructure SHALL load models in GGUF format with Q4_K_M quantization on GPU.
7. THE Ollama_Infrastructure SHALL expose streaming inference endpoints to the Warm_AI_Pipeline.
8. THE Ollama_Infrastructure SHALL NOT make outbound calls to any cloud LLM provider.
9. IF a model service becomes unresponsive, THEN THE Ollama_Infrastructure SHALL emit a service-degraded event and SHALL route requests to a configured fallback model.

### Requirement 11: Classical Machine Learning and Fast NLP

**User Story:** As a trader, I want fast classical ML and NLP models for low-latency scoring, so that quantitative and news scoring is not blocked by LLM latency.

#### Acceptance Criteria

1. THE Warm_AI_Pipeline SHALL host XGBoost, LightGBM, Isolation Forest, and a Tiny LSTM model for quantitative scoring.
2. THE Warm_AI_Pipeline SHALL host FinBERT and DistilBERT for fast NLP scoring.
3. THE Warm_AI_Pipeline SHALL execute classical ML and fast NLP inference via ONNX Runtime.
4. WHEN a fast NLP scoring request is received, THE Warm_AI_Pipeline SHALL return a result within 10 milliseconds at the 95th percentile.

### Requirement 12: News Intelligence

**User Story:** As a trader, I want news ingested and scored in real time, so that the system can adapt strategies and risk to breaking events.

#### Acceptance Criteria

1. THE News_Intelligence_Engine SHALL ingest content from Reuters, Moneycontrol, NSE filings, RBI announcements, Twitter/X, Telegram, Economic Times, and configured broker feeds.
2. WHEN a news item is ingested, THE News_Intelligence_Engine SHALL execute a fast-path pipeline of NLP, entity extraction, sentiment scoring, impact estimation, and symbol mapping using FinBERT on ONNX Runtime within 10 milliseconds at the 95th percentile.
3. WHEN a news item requires reasoning beyond the fast path, THE News_Intelligence_Engine SHALL dispatch a slow-path reasoning request to the Ollama_Infrastructure asynchronously.
4. WHEN a news item is mapped to a tracked symbol, THE News_Intelligence_Engine SHALL emit a news-impact event to the NATS_Bus tagged with symbol, sentiment, and impact magnitude.
5. WHEN a news-impact event is emitted, THE Risk_Engine SHALL incorporate the event into Adaptive_Risk computation.
6. WHEN a news-impact event is emitted, THE Signal_Engine SHALL incorporate the event into strategy gating per its configuration.

### Requirement 13: Market Regime Classification

**User Story:** As a trader, I want the current market regime classified continuously, so that strategies adapt to conditions instead of running blindly.

#### Acceptance Criteria

1. THE Market_Regime_Engine SHALL classify the current market regime as one of Trending, Sideways, Panic, High_Volatility, News_Driven, Liquidity_Crisis, or Low_Participation.
2. THE Market_Regime_Engine SHALL recompute the regime classification on each configured evaluation interval.
3. WHEN the classified regime changes, THE Market_Regime_Engine SHALL emit a regime-change event to the NATS_Bus.
4. WHEN a regime-change event is received, THE Signal_Engine SHALL apply the regime-specific strategy configuration.
5. WHEN a regime-change event is received, THE Risk_Engine SHALL update the MarketStability factor used in Adaptive_Risk.

### Requirement 14: Symbol Priority Allocation

**User Story:** As a trader, I want symbols assigned to priority tiers, so that scarce CPU, AI, and alert resources are spent on the most important names.

#### Acceptance Criteria

1. THE Symbol_Priority_Engine SHALL assign each tracked symbol to exactly one of priority tiers P1, P2, P3, or P4.
2. THE Symbol_Priority_Engine SHALL allocate CPU budget, AI inference budget, scan frequency, and alert frequency per tier per a configurable allocation table.
3. WHEN trader, regime, or news inputs change a symbol's priority, THE Symbol_Priority_Engine SHALL emit a priority-change event to the NATS_Bus.
4. WHEN a priority-change event is received, THE Hot_Path components SHALL apply the new resource allocation for that symbol.

### Requirement 15: Previous-Day Memory

**User Story:** As a trader, I want previous-day structural data available, so that intraday decisions consider context from prior sessions.

#### Acceptance Criteria

1. THE Previous_Day_Memory_Engine SHALL persist for each tracked symbol the previous Trading_Session's high, low, close, failed-breakout markers, gap reactions, delivery volume, trend continuation indicators, institutional behavior indicators, and significant news reactions.
2. THE Previous_Day_Memory_Engine SHALL expose this data to the Signal_Engine, Risk_Engine, and Human_Control_UI via query and event subscription.
3. WHEN a Trading_Session ends, THE Previous_Day_Memory_Engine SHALL compute and persist the next-session memory dataset before the next Trading_Session begins.

### Requirement 16: Trader Psychology Monitoring

**User Story:** As a trader, I want the system to detect and intervene on revenge trading, FOMO, tilt, and discipline deviations, so that I am protected from my own behavioral mistakes.

#### Acceptance Criteria

1. THE Trader_Psychology_Engine SHALL monitor trader actions for revenge trading, FOMO entries, overconfidence, tilt, impulsive trading, rapid re-entry, stop-loss removal, and discipline deviation.
2. THE Trader_Psychology_Engine SHALL compute Trader_Stability_Score as 0.35×Discipline + 0.25×EmotionalControl + 0.20×RiskConsistency + 0.20×Patience and SHALL constrain the score to the range [0.0, 1.0].
3. THE Trader_Psychology_Engine SHALL emit Trader_Stability_Score updates to the Risk_Engine and Human_Control_UI on each behavioral event.
4. WHEN Trader_Stability_Score falls below a configurable warning threshold, THE Trader_Psychology_Engine SHALL emit a warning intervention to the Human_Control_UI.
5. WHEN Trader_Stability_Score falls below a configurable cooldown threshold, THE Trader_Psychology_Engine SHALL request the Risk_Engine apply a configurable cooldown blocking new entries.
6. WHEN Trader_Stability_Score falls below a configurable suppression threshold, THE Trader_Psychology_Engine SHALL request the Risk_Engine reduce position sizing per the configured reduction factor.
7. WHEN Trader_Stability_Score falls below a configurable critical threshold, THE Trader_Psychology_Engine SHALL request the Risk_Engine activate the Kill_Switch.

### Requirement 17: AI Trade Ranking

**User Story:** As a trader, I want AI to rank candidate signals, so that I see the highest-quality opportunities first.

#### Acceptance Criteria

1. WHEN a signal event is emitted by the Signal_Engine, THE AI_Trade_Ranking_Engine SHALL compute Trade_Confidence_Score as 0.30×Orderflow + 0.25×TechnicalStrength + 0.20×NewsSentiment + 0.15×MarketRegime + 0.10×TraderDiscipline.
2. THE AI_Trade_Ranking_Engine SHALL constrain Trade_Confidence_Score to the range [0.0, 1.0].
3. THE AI_Trade_Ranking_Engine SHALL emit a ranked-signal event containing the original signal identifier and Trade_Confidence_Score.
4. THE AI_Trade_Ranking_Engine SHALL execute asynchronously and SHALL NOT block the Hot_Path.
5. THE AI_Trade_Ranking_Engine SHALL produce a ranking decision within 5 milliseconds at the 95th percentile.

### Requirement 18: AI Trade Journal

**User Story:** As a trader, I want every trade explained after the fact, so that I can learn from outcomes and behavior.

#### Acceptance Criteria

1. WHEN a trade is closed, THE AI_Trade_Journal_Engine SHALL produce a journal entry containing the outcome, the contributing strategy and signal, the trader emotional state at entry and exit, the prevailing market regime, identified missed opportunities, and execution-quality metrics.
2. THE AI_Trade_Journal_Engine SHALL persist each journal entry to the Memory_RAG_Layer.
3. THE AI_Trade_Journal_Engine SHALL expose journal entries to the Human_Control_UI via query and subscription.

### Requirement 19: Memory and RAG Layer

**User Story:** As a trader, I want trades, market memory, news, and psychology history retained and searchable, so that AI reasoning can use prior context.

#### Acceptance Criteria

1. THE Memory_RAG_Layer SHALL persist trades, market memory, psychology history, news history, symbol behavior, strategy outcomes, and execution statistics.
2. THE Memory_RAG_Layer SHALL store vector embeddings in Qdrant.
3. THE Memory_RAG_Layer SHALL store time-series data in PostgreSQL with the TimescaleDB extension.
4. THE Memory_RAG_Layer SHALL cache hot read paths in Redis.
5. WHEN a trader event occurs that triggers reasoning, THE Memory_RAG_Layer SHALL execute a retrieval pipeline of trader-event lookup, memory retrieval, context assembly, Ollama_Infrastructure reasoning, and recommendation generation.
6. THE Memory_RAG_Layer SHALL expose retrieval queries to the Warm_AI_Pipeline.
7. THE Memory_RAG_Layer SHALL be reachable from the Warm_AI_Pipeline only and SHALL NOT be invoked synchronously by the Hot_Path.

### Requirement 20: Human Control UI

**User Story:** As a trader, I want a real-time cockpit, so that I can observe the market and the system and exercise final command.

#### Acceptance Criteria

1. THE Human_Control_UI SHALL be implemented in React with TypeScript and Tailwind CSS.
2. THE Human_Control_UI SHALL receive live data from backend services exclusively via WebSocket connections.
3. THE Human_Control_UI SHALL display the live market feed, orderflow heatmap, options chain, current positions, live PnL, an execution panel, a risk panel, AI confidence scores, the current Trader_Stability_Score, the news feed, alerts, the Replay_Engine controls, AI explanations, symbol priority controls, strategy toggles, and a latency dashboard.
4. WHEN volatility breadth exceeds a configurable threshold, THE Human_Control_UI SHALL switch to a high-volatility presentation mode that increases refresh rate for critical panels and reduces secondary visual elements.
5. THE Human_Control_UI SHALL surface critical alerts above non-critical alerts.
6. THE Human_Control_UI SHALL provide controls that allow the trader to activate and deactivate the Kill_Switch.
7. THE Human_Control_UI SHALL provide controls that allow the trader to enable and disable individual strategies.
8. THE Human_Control_UI SHALL provide controls that allow the trader to change a symbol's priority tier.

### Requirement 21: Authority Hierarchy

**User Story:** As a trader, I want a clearly enforced authority hierarchy across all components, so that no AI or strategy can override risk limits.

#### Acceptance Criteria

1. THE system SHALL enforce the Authority_Hierarchy of Risk_Engine over Execution_Engine over Signal_Engine over Warm_AI_Pipeline over Trader_Input.
2. WHEN any component issues a request that conflicts with a higher-authority component's decision, THE higher-authority component's decision SHALL prevail.
3. THE Warm_AI_Pipeline SHALL NOT issue order requests directly to the Execution_Engine.
4. THE Warm_AI_Pipeline SHALL surface recommendations only to the trader, the Risk_Engine, and the Signal_Engine.

### Requirement 22: Replay Engine

**User Story:** As a trader and developer, I want full replay of sessions, so that I can debug issues, train AI, and backtest strategies on real data.

#### Acceptance Criteria

1. THE Replay_Engine SHALL record ticks, orderbook updates, orders, news events, trader actions, AI decisions, and market condition snapshots during each Trading_Session.
2. THE Replay_Engine SHALL re-play a recorded session deterministically into the Hot_Path and Warm_AI_Pipeline at configurable speed multipliers.
3. THE Replay_Engine SHALL expose controls in the Human_Control_UI for selecting, scrubbing, and stepping through recorded sessions.
4. WHEN a replay is active, THE Execution_Engine SHALL be configured to route orders to a simulated broker rather than a live broker.

### Requirement 23: AI Shadow Mode

**User Story:** As a trader, I want a shadow mode for AI components, so that I can validate AI quality before letting it influence ranking shown to me.

#### Acceptance Criteria

1. WHERE AI_Shadow_Mode is enabled for an AI component, THE component SHALL produce its outputs and persist them with timestamps.
2. WHERE AI_Shadow_Mode is enabled, THE Human_Control_UI SHALL NOT use the shadowed component's outputs to influence the ranked-signal display shown to the trader.
3. THE AI_Governance_Engine SHALL compare shadowed AI outputs against actual subsequent market outcomes and SHALL produce accuracy metrics per shadowed component.

### Requirement 24: AI Governance

**User Story:** As a trader, I want AI components monitored for drift and degradation, so that the system can reduce their influence when they become unreliable.

#### Acceptance Criteria

1. THE AI_Governance_Engine SHALL track model drift, confidence stability, hallucination indicators, and prediction quality per AI component.
2. WHEN a tracked metric breaches a configurable degradation threshold, THE AI_Governance_Engine SHALL reduce that component's influence weight in Trade_Confidence_Score and Adaptive_Risk per the configured policy.
3. WHEN a tracked metric breaches a configurable critical threshold, THE AI_Governance_Engine SHALL place the affected component into AI_Shadow_Mode.
4. THE AI_Governance_Engine SHALL emit governance-action events to the Human_Control_UI describing each influence change.

### Requirement 25: Self-Healing Infrastructure

**User Story:** As a trader, I want the system to recover automatically from common failures, so that operational issues do not interrupt trading.

#### Acceptance Criteria

1. WHEN a WebSocket connection disconnects, THE Self_Healing_Infrastructure SHALL trigger reconnection with exponential backoff bounded by a configurable maximum delay.
2. IF Redis becomes unavailable, THEN THE Self_Healing_Infrastructure SHALL attempt reconnection and SHALL emit a degraded-state event to the Human_Control_UI.
3. WHEN a Broker_Adapter reports persistent failure, THE Self_Healing_Infrastructure SHALL invoke the Execution_Engine's broker failover.
4. WHEN the VPS or a host is restarted, THE Self_Healing_Infrastructure SHALL bring services back to their last-known-healthy configuration.
5. WHEN an external API exhibits latency above a configurable threshold, THE Self_Healing_Infrastructure SHALL emit a latency-spike event and SHALL apply a configured mitigation per the affected component.

### Requirement 26: Market Open War Mode

**User Story:** As a trader, I want a special operating mode during the market open, so that the system focuses on the highest-impact period.

#### Acceptance Criteria

1. WHILE the current IST time is between 09:15:00 and 09:45:00 on a Trading_Session, THE system SHALL operate in Market_Open_War_Mode.
2. WHILE Market_Open_War_Mode is active, THE Hot_Path SHALL apply increased processing frequency, increased orderflow sensitivity, and increased breakout detection sensitivity per the configured War_Mode profile.
3. WHILE Market_Open_War_Mode is active, THE Human_Control_UI SHALL apply a reduced-clutter presentation profile and SHALL suppress signals below a configurable War_Mode confidence threshold.
4. WHEN Market_Open_War_Mode begins or ends, THE system SHALL emit a mode-transition event to the NATS_Bus.

### Requirement 27: Observability and Telemetry

**User Story:** As a trader and operator, I want comprehensive observability, so that I can debug, monitor, and improve the system.

#### Acceptance Criteria

1. THE system SHALL export tick latency, AI latency, execution latency, broker latency, slippage, fill quality, websocket-drop counts, risk anomaly events, trader emotional risk, and AI model drift as metrics to Prometheus.
2. THE system SHALL ship logs to Loki and traces to Jaeger via OpenTelemetry instrumentation.
3. THE system SHALL provide Grafana dashboards covering Hot_Path latency budgets, Warm_AI_Pipeline performance, broker performance, risk events, and trader psychology metrics.
4. THE system SHALL emit a per-stage latency record for every order request including tick ingest, feature extraction, AI scoring, risk check, and execution routing stages.

### Requirement 28: Latency Budget Compliance

**User Story:** As a trader, I want explicit latency budgets enforced per stage, so that the overall tick-to-trade target is achievable and observable.

#### Acceptance Criteria

1. THE Market_Data_Engine SHALL complete tick ingestion within 2 milliseconds at the 99th percentile.
2. THE Feature_Extraction_Engine SHALL complete feature extraction within 3 milliseconds at the 99th percentile.
3. THE AI_Trade_Ranking_Engine SHALL produce a ranking decision within 5 milliseconds at the 95th percentile.
4. THE Risk_Engine SHALL complete a risk check within 2 milliseconds at the 99th percentile.
5. THE Execution_Engine SHALL complete execution routing within 5 milliseconds at the 99th percentile.
6. WHEN a per-stage latency budget is breached, THE component SHALL emit a budget-breach event to Prometheus and the NATS_Bus.

### Requirement 29: Service-Oriented Architecture and Messaging

**User Story:** As an operator, I want a service-oriented event-driven architecture, so that the system is scalable, fault-tolerant, and operable.

#### Acceptance Criteria

1. THE system SHALL be packaged as a set of microservices and SHALL NOT be deployed as a monolith.
2. THE system SHALL use the NATS_Bus as the primary inter-service messaging system.
3. THE system SHALL use Redis_Streams for ordered intra-Hot_Path event distribution where ordering is required.
4. THE system SHALL be deployable via Docker on Ubuntu hosts.
5. THE system SHALL run on a Mumbai VPS for the Hot_Path and SHALL support an optional local GPU node for the Warm_AI_Pipeline.
6. WHERE a service fails, THE system SHALL continue operating other services and SHALL surface the failure via observability.

### Requirement 30: Architectural Prohibitions

**User Story:** As a trader, I want explicit architectural prohibitions enforced, so that the system stays deterministic, fast, and safe.

#### Acceptance Criteria

1. THE Hot_Path SHALL NOT include Pine Script execution.
2. THE system SHALL NOT depend on TradingView for any execution decision.
3. THE Hot_Path SHALL NOT include any polling loop in steady-state operation.
4. THE Hot_Path SHALL NOT invoke any LLM inference call.
5. THE Hot_Path SHALL NOT execute any per-tick AI inference of the size of Qwen2.5:14B, DeepSeek-R1, or comparable models.
6. THE Warm_AI_Pipeline SHALL NOT submit orders directly to the Execution_Engine.
7. THE Hot_Path SHALL NOT invoke any blocking external API on the per-tick path.
8. THE Hot_Path SHALL NOT depend on pandas, NumPy, or any Python runtime.

### Requirement 31: Trading Session Time Boundaries

**User Story:** As a trader, I want trading-session boundaries enforced, so that the system only trades during market hours.

#### Acceptance Criteria

1. WHILE the current IST time is outside the interval 09:15:00 to 15:30:00 on a Trading_Session, THE Risk_Engine SHALL block all new order entries.
2. WHEN the Trading_Session begins at 09:15:00 IST, THE system SHALL emit a session-start event to the NATS_Bus.
3. WHEN the Trading_Session ends at 15:30:00 IST, THE system SHALL emit a session-end event to the NATS_Bus.
4. WHEN the Trading_Session ends, THE Risk_Engine SHALL request the Execution_Engine cancel all open orders not configured to persist.

### Requirement 32: Configuration of Capital and Profit Targets

**User Story:** As a trader, I want capital base and profit targets configurable, so that the system reflects my account size and goals.

#### Acceptance Criteria

1. THE system SHALL accept a configurable capital base in Indian Rupees and SHALL default to ₹20,000.
2. THE system SHALL accept a configurable daily profit target range in Indian Rupees and SHALL default to a range of ₹300 to ₹1,000.
3. WHEN the daily profit target upper bound is reached during a Trading_Session, THE Risk_Engine SHALL emit a target-reached event and SHALL apply the configured post-target policy.
4. THE Risk_Engine SHALL size positions consistent with the configured capital base.
