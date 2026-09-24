"""
PostgreSQL sinks for the streaming jobs (phases 15 to 17).

Structured Streaming has no native "upsert to a relational database" sink. The
built-in JDBC sink only knows append and overwrite, and neither is what a
windowed aggregate needs: as long as a window stays open, a late event can
change its total and Spark re-emits the row.

So we go through foreachBatch, which hands us a normal batch DataFrame and
gives us back the full SQL vocabulary - including ON CONFLICT DO UPDATE.
"""

from __future__ import annotations

from typing import Any

from src.common import config

# ---------------------------------------------------------------------------
#  Upsert
#
#  The rows are brought back to the driver before being written. That is a
#  deliberate choice, and it has a limit worth stating: it only holds because
#  an AGGREGATE is small (a few windows x a few countries per micro-batch),
#  not because bringing data back to the driver is a good habit. For a raw
#  stream of millions of rows, you would write to a staging table through the
#  distributed JDBC sink, then run a single server-side MERGE.
# ---------------------------------------------------------------------------


def _connect():
    import psycopg2
    return psycopg2.connect(config.postgres_dsn())


def upsert_rows(rows: list[tuple], table: str, columns: list[str], keys: list[str]) -> int:
    """INSERT ... ON CONFLICT (keys) DO UPDATE. Returns the number of rows sent."""
    if not rows:
        return 0

    from psycopg2.extras import execute_values

    updatable = [c for c in columns if c not in keys]
    assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {assignments}, updated_at = now()"
    )

    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows)
    finally:
        conn.close()
    return len(rows)


def upsert_batch(batch_df, table: str, keys: list[str]) -> int:
    """Upsert a whole micro-batch DataFrame into `table`.

    The DataFrame's column names must match the table's. Collecting here is
    safe for the reason given above: this is only ever called on aggregates.
    """
    columns = list(batch_df.columns)
    rows = [tuple(r) for r in batch_df.collect()]
    return upsert_rows(rows, table, columns, keys)


# ---------------------------------------------------------------------------
#  PHASE 17 - persisting the query metrics
#
#  Spark exposes these numbers live in StreamingQueryProgress, but they die
#  with the query. Writing them down is what lets you answer, the morning
#  after an incident: "at what time did the throughput collapse?".
# ---------------------------------------------------------------------------

METRICS_TABLE = "analytics.stream_metrics"
METRICS_COLUMNS = [
    "query_name", "batch_id", "input_rows", "input_rows_per_second",
    "processed_rows_per_second", "batch_duration_ms", "state_rows",
]


def record_progress(progress: dict[str, Any]) -> None:
    """Persist one StreamingQueryProgress. Never raises: monitoring must not
    be able to bring down the pipeline it is watching."""
    try:
        state = progress.get("stateOperators") or []
        row = (
            progress.get("name") or "unnamed",
            progress.get("batchId"),
            progress.get("numInputRows"),
            progress.get("inputRowsPerSecond"),
            progress.get("processedRowsPerSecond"),
            (progress.get("durationMs") or {}).get("triggerExecution"),
            sum(op.get("numRowsTotal", 0) for op in state) or None,
        )
        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {METRICS_TABLE} ({', '.join(METRICS_COLUMNS)}) "
                    f"VALUES ({', '.join(['%s'] * len(METRICS_COLUMNS))}) "
                    f"ON CONFLICT (query_name, batch_id) DO NOTHING",
                    row,
                )
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - monitoring must never be fatal
        print(f"  [metrics] could not be written: {exc}")


def attach_metrics_listener(spark):
    """Register a listener that persists every micro-batch's progress.

    A listener rather than a print inside foreachBatch: it sees EVERY query in
    the session, including the ones that do not go through foreachBatch, and
    it also catches termination with an exception.

    Returns the listener so the caller can detach it BEFORE stopping the
    queries. Without that, onQueryTerminated fires while the py4j callback
    server is already shutting down, and the driver prints a long
    Py4JException that has nothing to do with the pipeline.
    """
    import json

    from pyspark.sql.streaming import StreamingQueryListener

    class _Listener(StreamingQueryListener):
        def onQueryStarted(self, event):
            print(f"  [listener] query started: {event.name}")

        def onQueryProgress(self, event):
            record_progress(json.loads(event.progress.json))

        def onQueryTerminated(self, event):
            if event.exception:
                print(f"  [listener] query terminated on error: {event.exception}")

    listener = _Listener()
    spark.streams.addListener(listener)
    return listener
