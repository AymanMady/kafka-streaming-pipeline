-- =============================================================================
--  Tables of the streaming pipeline (phases 15 and 17).
--
--  This file is a MIGRATION, not an init script: it is idempotent
--  (CREATE ... IF NOT EXISTS) and can be replayed on an existing database.
--  Run it with: make db-migrate
--
--  docker/postgres/init.sql only ever runs when the volume is created, which
--  makes it useless for a schema that keeps growing phase after phase.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------------------------------------------------------
--  raw.orders - what the Python consumer writes, one row per event (phase 15)
--
--  THE PRIMARY KEY IS event_id, NOT order_id.
--  order_id is a business identifier: one order can emit several events.
--  event_id identifies THIS message. A consumer that restarts re-reads
--  messages it had already processed (at-least-once delivery); the re-inserts
--  then hit ON CONFLICT DO NOTHING and are ignored. The end state is the same
--  as if every message had been processed exactly once.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.orders (
    event_id          TEXT        PRIMARY KEY,
    event_type        TEXT,
    order_id          BIGINT,
    customer_id       BIGINT,
    product_id        BIGINT,
    quantity          INTEGER,
    amount            NUMERIC(12, 2),
    currency          TEXT,
    country           TEXT,
    event_timestamp   TEXT,
    kafka_partition   INTEGER     NOT NULL,
    kafka_offset      BIGINT      NOT NULL,
    is_valid          BOOLEAN     NOT NULL,
    rejection_reasons TEXT[],
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Two questions the operators ask constantly: "what came in recently?" and
-- "what is being rejected?".
CREATE INDEX IF NOT EXISTS idx_raw_orders_ingested  ON raw.orders (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_orders_invalid   ON raw.orders (is_valid) WHERE NOT is_valid;
CREATE INDEX IF NOT EXISTS idx_raw_orders_order_id  ON raw.orders (order_id);

-- -----------------------------------------------------------------------------
--  analytics.revenue_by_country_window - windowed aggregate (phases 12 to 15)
--
--  The key is (window_start, window_end, country): that is what a window IS,
--  a bucket identified by its bounds.
--
--  Why an UPSERT and not an INSERT? Because of the WATERMARK. As long as a
--  window stays open, a late event can still land in it, and Spark re-emits
--  the window with an updated total. The row must therefore be REPLACED, not
--  added. Without the upsert you would get one row per re-emission and count
--  the same revenue several times.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.revenue_by_country_window (
    window_start  TIMESTAMPTZ    NOT NULL,
    window_end    TIMESTAMPTZ    NOT NULL,
    country       TEXT           NOT NULL,
    orders_count  BIGINT         NOT NULL,
    total_amount  NUMERIC(14, 2) NOT NULL,
    avg_amount    NUMERIC(12, 2),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end, country)
);

CREATE INDEX IF NOT EXISTS idx_revenue_window_start
    ON analytics.revenue_by_country_window (window_start DESC);

-- -----------------------------------------------------------------------------
--  analytics.orders_per_minute - the global pulse of the stream (phase 12)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.orders_per_minute (
    window_start       TIMESTAMPTZ    NOT NULL,
    window_end         TIMESTAMPTZ    NOT NULL,
    orders_count       BIGINT         NOT NULL,
    total_amount       NUMERIC(14, 2) NOT NULL,
    avg_amount         NUMERIC(12, 2),
    distinct_customers BIGINT,
    updated_at         TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end)
);

-- -----------------------------------------------------------------------------
--  analytics.stream_metrics - one row per micro-batch (phase 17)
--
--  Spark exposes these numbers in its StreamingQueryProgress, but they vanish
--  when the query stops. Persisting them is what lets you answer, the morning
--  after an incident: "at what time did the throughput collapse?".
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.stream_metrics (
    id                    BIGSERIAL   PRIMARY KEY,
    query_name            TEXT        NOT NULL,
    batch_id              BIGINT      NOT NULL,
    recorded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    input_rows            BIGINT,
    input_rows_per_second DOUBLE PRECISION,
    processed_rows_per_second DOUBLE PRECISION,
    batch_duration_ms     BIGINT,
    state_rows            BIGINT,
    UNIQUE (query_name, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_stream_metrics_recorded
    ON analytics.stream_metrics (query_name, recorded_at DESC);

-- -----------------------------------------------------------------------------
--  analytics.consumer_lag - lag history, sampled by scripts/monitor_lag.py
--  (phase 17)
--
--  Lag = (last offset produced) - (last offset committed by the group).
--  It is THE health metric of a streaming pipeline: a lag that grows without
--  stopping means consumption is slower than production, and no amount of
--  waiting will fix it.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.consumer_lag (
    id            BIGSERIAL   PRIMARY KEY,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumer_group TEXT       NOT NULL,
    topic         TEXT        NOT NULL,
    partition     INTEGER     NOT NULL,
    current_offset BIGINT,
    end_offset    BIGINT,
    lag           BIGINT
);

CREATE INDEX IF NOT EXISTS idx_consumer_lag_recorded
    ON analytics.consumer_lag (consumer_group, recorded_at DESC);

COMMENT ON TABLE raw.orders IS
    'One row per Kafka event. Primary key event_id: makes the ingestion idempotent.';
COMMENT ON TABLE analytics.revenue_by_country_window IS
    'Windowed revenue by country. Upserted: a window can be re-emitted after a late event.';
COMMENT ON TABLE analytics.stream_metrics IS
    'One row per micro-batch, from StreamingQueryProgress (phase 17).';
