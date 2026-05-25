-- ============================================================================
-- Memory_RAG_Layer · AI_Governance_Engine hypertable (task 28.1, R23.3, R24.1)
-- ============================================================================
--
-- Idempotent migration that creates the ``governance_metrics`` hypertable
-- backing the ``AI_Governance_Engine`` (R23.3, R24.1–R24.4). One row per
-- governance sample per (component, metric, ts) carries:
--
--   * ``component`` — the Warm_AI_Pipeline component that produced the
--     governed output (one of the canonical seven: ``news``, ``regime``,
--     ``priority``, ``prev_day``, ``psychology``, ``ranking``, ``journal``).
--   * ``metric`` — the canonical ``ai.gov.action`` metric enum value
--     (``drift``, ``accuracy``, ``latency``, ``error_rate``). Two of the
--     four engine-tracked metrics map to ``error_rate``
--     (``confidence_stability`` and ``hallucination_indicators``); the
--     ``metric_kind`` column carries the precise engine-internal name so
--     readers can distinguish them without losing the wire-schema mapping.
--   * ``value`` and ``threshold`` — the live metric value and the threshold
--     it crossed (``degradation_threshold`` or ``critical_threshold``).
--   * ``level`` — the per-component governance level
--     (``none`` / ``degraded`` / ``critical``) at the moment of write.
--   * ``action`` — the canonical ``ai.gov.action`` action string when an
--     edge transition was emitted (``reduce_influence``, ``shadow_mode``,
--     ``rollback``); ``NULL`` when the row records a non-edge sample
--     (e.g. continued metric tracking with the level unchanged).
--   * ``correlation_id`` — when the row is correlated with a closed-trade
--     outcome (R23.3, prediction-quality measurement); ``NULL`` for plain
--     metric snapshots.
--
-- This file is idempotent (uses ``CREATE TABLE IF NOT EXISTS`` and
-- ``create_hypertable(..., if_not_exists => TRUE, migrate_data => TRUE)``)
-- so re-running the migration on a partially migrated database is safe.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ----------------------------------------------------------------------------
-- governance_metrics — one row per governance sample / edge emission.
--    1-day chunks: low-to-medium cardinality (a handful of components ×
--    four metrics × per-emission frequency).
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS governance_metrics (
    ts             TIMESTAMPTZ NOT NULL,
    component      TEXT        NOT NULL,
    metric         TEXT        NOT NULL,    -- canonical wire enum: drift|accuracy|latency|error_rate
    metric_kind    TEXT        NOT NULL,    -- engine-internal: drift|confidence_stability|hallucination_indicators|prediction_quality
    value          DOUBLE PRECISION NOT NULL,
    threshold      DOUBLE PRECISION NOT NULL,
    level          TEXT        NOT NULL,    -- none|degraded|critical
    action         TEXT,                    -- reduce_influence|shadow_mode|rollback (NULL on non-edge samples)
    correlation_id TEXT,                    -- non-NULL when correlated with a closed-trade outcome (R23.3)
    sample_count   BIGINT      NOT NULL DEFAULT 0
);

SELECT create_hypertable(
    'governance_metrics',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_governance_metrics_component_ts
    ON governance_metrics (component, ts DESC);

CREATE INDEX IF NOT EXISTS idx_governance_metrics_component_metric_ts
    ON governance_metrics (component, metric_kind, ts DESC);

CREATE INDEX IF NOT EXISTS idx_governance_metrics_correlation_ts
    ON governance_metrics (correlation_id, ts DESC)
    WHERE correlation_id IS NOT NULL;
