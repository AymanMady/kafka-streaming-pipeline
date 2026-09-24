-- =============================================================================
--  PostgreSQL - initialisation of the Kafka Streaming Pipeline project
--
--  WARNING - rule worth knowing:
--  This script runs ONLY ONCE, when the ksp-pgdata volume is first created.
--  A "docker compose restart" or "down/up" does NOT replay it.
--  To replay it: make clean (destructive) then make up.
--  Schema changes in later phases go through sql/ + make db-migrate, with
--  idempotent DDL (CREATE ... IF NOT EXISTS).
-- =============================================================================

-- raw       : data exactly as received from Kafka (order by order)
-- analytics : aggregated results produced by Spark Structured Streaming
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Witness table: its only job is to prove, from phase 1 on, that the database
-- is reachable, initialised, and able to write and read.
CREATE TABLE IF NOT EXISTS analytics.pipeline_health (
    id           SERIAL PRIMARY KEY,
    component    TEXT        NOT NULL,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    note         TEXT
);

INSERT INTO analytics.pipeline_health (component, note)
VALUES ('postgres', 'init.sql ran when the volume was created - phase 1');

COMMENT ON SCHEMA raw       IS 'Raw events consumed from Kafka';
COMMENT ON SCHEMA analytics IS 'Real-time aggregates produced by Spark Structured Streaming';
