-- ============================================================================
-- Memory_RAG_Layer · Previous_Day_Memory_Engine hypertable (task 24.1, R15)
-- ============================================================================
--
-- Idempotent migration that creates the ``prev_day_memory`` hypertable
-- backing the ``Previous_Day_Memory_Engine`` (R15.1, R15.3). One row
-- per (symbol_id, session_date) carries:
--
--   * The previous Trading_Session OHLCV / VWAP fix points (open, high,
--     low, close, vwap, total_volume).
--   * ``delivery_volume`` separated as a column because the Risk_Engine
--     gates trade size on this aggregate.
--   * ``key_levels`` JSONB so the canonical ``mem.prev_day.<sym>`` event
--     payload (R15.2) round-trips verbatim on read.
--   * Structural behaviour markers as JSONB so the schema can grow
--     without further migrations:
--       - failed_breakouts        — list of {price_paise, ts_ns, side}
--       - gap_reactions           — {gap_paise, fill_ratio, retraced, ...}
--       - trend_continuation      — {direction, strength, ...}
--       - institutional_behavior  — {delivery_pct, large_order_ratio, ...}
--       - news_reactions          — list of {headline_id, magnitude, ts_ns}
--   * ``embedding_point_id`` — the Qdrant ``market_memory`` point id
--     associated with this session's narrative summary (NULL until the
--     embedder runs).
--
-- A composite ``UNIQUE (symbol_id, ts)`` index enforces "exactly one
-- persisted next-session record per symbol per day" (task 24.2 /
-- Property 5). ``ts`` is set to ``session_date::timestamptz`` by the
-- writer so the unique constraint reduces to one row per (symbol_id,
-- session_date). The next-session compute job uses
-- ``INSERT ... ON CONFLICT (symbol_id, ts) DO UPDATE`` so repeated runs
-- (e.g. after a Self_Healing_Supervisor restart between session end
-- and session start) remain idempotent.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ----------------------------------------------------------------------------
-- 1. prev_day_memory — one row per (symbol_id, session_date).
--    7-day chunks: low cardinality (a few hundred symbols × 1 row per day).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS prev_day_memory (
    ts                     TIMESTAMPTZ NOT NULL,    -- session_date at 00:00:00 UTC
    session_date           DATE        NOT NULL,
    symbol_id              BIGINT      NOT NULL,
    symbol                 TEXT        NOT NULL,
    open_paise             BIGINT      NOT NULL,
    high_paise             BIGINT      NOT NULL,
    low_paise              BIGINT      NOT NULL,
    close_paise            BIGINT      NOT NULL,
    vwap_paise             BIGINT      NOT NULL,
    total_volume           BIGINT      NOT NULL,
    delivery_volume        BIGINT      NOT NULL,
    key_levels             JSONB       NOT NULL DEFAULT '[]'::JSONB,
    failed_breakouts       JSONB       NOT NULL DEFAULT '[]'::JSONB,
    gap_reactions          JSONB       NOT NULL DEFAULT '{}'::JSONB,
    trend_continuation     JSONB       NOT NULL DEFAULT '{}'::JSONB,
    institutional_behavior JSONB       NOT NULL DEFAULT '{}'::JSONB,
    news_reactions         JSONB       NOT NULL DEFAULT '[]'::JSONB,
    embedding_point_id     TEXT,
    computed_ts_ns         BIGINT      NOT NULL
);

SELECT create_hypertable(
    'prev_day_memory',
    'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- One row per symbol per session_date. Timescale requires the partition
-- column (``ts``) to participate in any UNIQUE index, so we key on
-- ``(symbol_id, ts)`` — given ``ts == session_date::timestamptz`` this
-- reduces to "one row per (symbol_id, session_date)".
CREATE UNIQUE INDEX IF NOT EXISTS idx_prev_day_memory_symbol_session
    ON prev_day_memory (symbol_id, ts);

CREATE INDEX IF NOT EXISTS idx_prev_day_memory_symbol_ts
    ON prev_day_memory (symbol_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_prev_day_memory_session_date
    ON prev_day_memory (session_date DESC);
