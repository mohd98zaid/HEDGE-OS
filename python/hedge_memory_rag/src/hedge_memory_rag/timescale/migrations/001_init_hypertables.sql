-- ============================================================================
-- Memory_RAG_Layer · Timescale hypertables (task 32.1, R19.1, R19.3)
-- ============================================================================
--
-- Idempotent migration that:
--   * makes sure the timescaledb extension is loaded (no-op if the
--     init-timescaledb.sql in docker/postgres already created it),
--   * creates the eight underlying tables for sampled ticks, fills, orders,
--     AI scores, regime history, psychology timeline, broker metrics, and
--     journal entries,
--   * promotes each to a hypertable via `create_hypertable(..., if_not_exists =>
--     true, migrate_data => true)` with a `chunk_time_interval` matched to the
--     producer cadence,
--   * registers BRIN/B-tree indexes on the access patterns the retrieval
--     pipeline (task 34.1) reads through (`(symbol, ts)`, `(broker, ts)`,
--     `(correlation_id, ts)` etc.).
--
-- Every statement is `IF NOT EXISTS` so the runner can re-apply on every
-- service start without producing errors. The runner records the file name
-- in `hedge_memory_rag.schema_migrations`; this file is also defensible
-- against partial re-runs because of the IF NOT EXISTS guards.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ----------------------------------------------------------------------------
-- 1. tick_samples — sampled `Tick_v1` rows. 1-hour chunks (high cadence,
--    NSE/BSE tape volumes).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tick_samples (
    ts              TIMESTAMPTZ NOT NULL,
    symbol_id       BIGINT      NOT NULL,
    exchange        SMALLINT    NOT NULL,        -- 0 = NSE, 1 = BSE
    ltp_paise       BIGINT      NOT NULL,
    bid_paise       BIGINT      NOT NULL,
    ask_paise       BIGINT      NOT NULL,
    ltq             BIGINT      NOT NULL,
    total_buy_qty   BIGINT      NOT NULL,
    total_sell_qty  BIGINT      NOT NULL,
    correlation_id  BYTEA       NOT NULL
);

SELECT create_hypertable(
    'tick_samples',
    'ts',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_tick_samples_symbol_ts
    ON tick_samples (symbol_id, ts DESC);

-- ----------------------------------------------------------------------------
-- 2. orders — `OrderState_v1` lifecycle rows. 6-hour chunks (one trading day
--    spans multiple chunks but still gives Timescale plenty of compression
--    headroom).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS orders (
    ts                TIMESTAMPTZ NOT NULL,
    correlation_id    BYTEA       NOT NULL,
    broker_order_id   TEXT        NOT NULL,
    state             TEXT        NOT NULL,    -- New / Submitted / ... / Rejected
    symbol_id         BIGINT      NOT NULL,
    side              TEXT        NOT NULL,    -- Buy / Sell
    order_type        TEXT        NOT NULL,    -- Market / Limit
    quantity          BIGINT      NOT NULL,
    limit_paise       BIGINT,
    filled_qty        BIGINT      NOT NULL,
    avg_fill_paise    BIGINT      NOT NULL
);

SELECT create_hypertable(
    'orders',
    'ts',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_orders_correlation_ts
    ON orders (correlation_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_orders_broker_order_ts
    ON orders (broker_order_id, ts DESC);

-- ----------------------------------------------------------------------------
-- 3. fills — one row per partial or final fill (projection of OrderState_v1).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fills (
    ts                TIMESTAMPTZ NOT NULL,
    correlation_id    BYTEA       NOT NULL,
    broker_order_id   TEXT        NOT NULL,
    symbol_id         BIGINT      NOT NULL,
    side              TEXT        NOT NULL,
    fill_qty          BIGINT      NOT NULL,
    fill_paise        BIGINT      NOT NULL,
    cumulative_qty    BIGINT      NOT NULL,
    avg_fill_paise    BIGINT      NOT NULL
);

SELECT create_hypertable(
    'fills',
    'ts',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_fills_symbol_ts
    ON fills (symbol_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_fills_correlation_ts
    ON fills (correlation_id, ts DESC);

-- ----------------------------------------------------------------------------
-- 4. ai_scores — `ai.rank.<cid>` rows.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ai_scores (
    ts                          TIMESTAMPTZ NOT NULL,
    correlation_id              TEXT        NOT NULL,
    signal_id                   TEXT        NOT NULL,
    trade_confidence_score      DOUBLE PRECISION NOT NULL,
    factor_orderflow            DOUBLE PRECISION NOT NULL,
    factor_technical_strength   DOUBLE PRECISION NOT NULL,
    factor_news_sentiment       DOUBLE PRECISION NOT NULL,
    factor_market_regime        DOUBLE PRECISION NOT NULL,
    factor_trader_discipline    DOUBLE PRECISION NOT NULL,
    shadow                      BOOLEAN     NOT NULL
);

SELECT create_hypertable(
    'ai_scores',
    'ts',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_ai_scores_correlation_ts
    ON ai_scores (correlation_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_ai_scores_signal_ts
    ON ai_scores (signal_id, ts DESC);

-- ----------------------------------------------------------------------------
-- 5. regime_history — `ai.regime.changed` (edge-triggered, low cardinality).
--    1-day chunks suffice.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS regime_history (
    ts            TIMESTAMPTZ NOT NULL,
    from_regime   TEXT        NOT NULL,
    to_regime     TEXT        NOT NULL
);

SELECT create_hypertable(
    'regime_history',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- ----------------------------------------------------------------------------
-- 6. psychology_timeline — Trader_Stability_Score samples.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS psychology_timeline (
    ts                  TIMESTAMPTZ NOT NULL,
    score               DOUBLE PRECISION NOT NULL,
    discipline          DOUBLE PRECISION NOT NULL,
    emotional_control   DOUBLE PRECISION NOT NULL,
    risk_consistency    DOUBLE PRECISION NOT NULL,
    patience            DOUBLE PRECISION NOT NULL,
    behaviors           TEXT[]      NOT NULL DEFAULT '{}'
);

SELECT create_hypertable(
    'psychology_timeline',
    'ts',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- ----------------------------------------------------------------------------
-- 7. broker_metrics — broker latency / error / connectivity samples.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS broker_metrics (
    ts            TIMESTAMPTZ NOT NULL,
    broker        TEXT        NOT NULL,
    latency_ms    DOUBLE PRECISION NOT NULL,
    error_rate    DOUBLE PRECISION NOT NULL,
    connected     BOOLEAN     NOT NULL,
    last_error    TEXT
);

SELECT create_hypertable(
    'broker_metrics',
    'ts',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_broker_metrics_broker_ts
    ON broker_metrics (broker, ts DESC);

-- ----------------------------------------------------------------------------
-- 8. journal_entries — `ai.journal.entry` (R18.2). 1-day chunks because the
--    Journal_Engine emits at most a few hundred entries per session.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS journal_entries (
    ts                TIMESTAMPTZ NOT NULL,
    correlation_id    TEXT        NOT NULL,
    trade_id          TEXT        NOT NULL,
    symbol            TEXT        NOT NULL,
    side              TEXT        NOT NULL,
    quantity          BIGINT      NOT NULL,
    entry_paise       BIGINT      NOT NULL,
    exit_paise        BIGINT      NOT NULL,
    pnl_inr           DOUBLE PRECISION NOT NULL,
    narrative         TEXT        NOT NULL
);

SELECT create_hypertable(
    'journal_entries',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_journal_entries_symbol_ts
    ON journal_entries (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_journal_entries_trade_ts
    ON journal_entries (trade_id, ts DESC);
