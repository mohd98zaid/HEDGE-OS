-- Enable TimescaleDB so the Memory_RAG_Layer can create hypertables for
-- ticks (sampled), fills, orders, AI scores, regime history, psychology
-- timeline, and broker metrics. Schema lands in tasks D2.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- A single application database. Concrete schemas (`hedge_md`, `hedge_exec`,
-- `hedge_ai`, `hedge_psych`, `hedge_journal`, `hedge_obs`) are migrated in
-- by hedge-memory-rag's loader.
