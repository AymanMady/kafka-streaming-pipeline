"""
PHASES 11 to 17 - Real-time analytics: windows, watermarks, checkpoints,
idempotent writes to PostgreSQL.

    make stream-analytics
    make stream-analytics ARGS="--sink console --window 30s --watermark 1m"

What this job answers, continuously:
  - revenue and order count per country, per time window;
  - the global pulse of the stream (orders per minute, average basket,
    distinct customers).

The four ideas stacked here, in the order they become necessary:
  11. aggregate a stream           -> a running total that keeps changing
  12. window it                    -> "per minute", not "since the beginning"
  13. watermark it                 -> accept late events, but not forever
  14. checkpoint it                -> survive a restart without recounting
  15/16. write it idempotently     -> a replayed batch must not double the total
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common import config
from src.streaming.sinks import attach_metrics_listener, upsert_batch
from src.streaming.spark_session import describe, get_spark, kafka_source
from src.streaming.transform import parse_events, split

REVENUE_TABLE = "analytics.revenue_by_country_window"
PULSE_TABLE = "analytics.orders_per_minute"


# ---------------------------------------------------------------------------
#  PHASES 11 to 13 - the aggregations
# ---------------------------------------------------------------------------

def revenue_by_country(valid: DataFrame, window: str, watermark: str) -> DataFrame:
    """Revenue per country, per tumbling window.

    EVENT TIME, not processing time. We aggregate on `event_time`, the moment
    the order was PLACED, not the moment Spark saw it. That distinction is the
    whole point: an event delayed by the network belongs in the window of when
    it happened, otherwise a five-minute outage would move a whole afternoon's
    revenue into one minute.

    THE WATERMARK is the answer to the question event time forces on you: how
    long do we keep a window open, waiting for stragglers? Forever would mean
    keeping every window in memory forever. The watermark says "beyond this
    lateness, we drop". It is a memory/completeness trade-off, and there is no
    right answer - only a decision the business has to make.
    """
    return (
        valid
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window), F.col("country"))
        .agg(
            F.count("*").alias("orders_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "country", "orders_count", "total_amount", "avg_amount",
        )
    )


def orders_pulse(valid: DataFrame, window: str, watermark: str) -> DataFrame:
    """The global pulse: volume, revenue, average basket, distinct customers."""
    return (
        valid
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window))
        .agg(
            F.count("*").alias("orders_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            # approx_count_distinct, not countDistinct: an exact distinct count
            # on a stream means keeping every id seen in the window in state.
            # HyperLogLog gives ~2% error for a fixed, tiny memory footprint.
            F.approx_count_distinct("customer_id").alias("distinct_customers"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "orders_count", "total_amount", "avg_amount", "distinct_customers",
        )
    )


# ---------------------------------------------------------------------------
#  PHASES 15 and 16 - the sinks
# ---------------------------------------------------------------------------

def _to_postgres(table: str, keys: list[str], quiet: bool):
    """foreachBatch callback: upsert the aggregate.

    IDEMPOTENCY. Structured Streaming guarantees at-least-once on the sink
    side: after a crash, a micro-batch can be REPLAYED. With a plain INSERT
    the revenue of that window would be counted twice. The upsert makes the
    write idempotent - replaying it changes nothing - which turns
    at-least-once delivery into an exactly-once RESULT.

    This is the practical lesson: exactly-once is far more often obtained by
    making the write idempotent than by a distributed transaction.
    """
    def _write(batch: DataFrame, batch_id: int) -> None:
        n = upsert_batch(batch, table, keys)
        if n and not quiet:
            print(f"  batch {batch_id:>4} -> {table}: {n} window(s) upserted")
    return _write


def _start(agg: DataFrame, name: str, args, table: str, keys: list[str]):
    writer = agg.writeStream.queryName(name).outputMode(args.output_mode)

    if args.sink == "console":
        writer = writer.format("console").option("truncate", "false").option("numRows", 20)
    else:
        writer = writer.foreachBatch(_to_postgres(table, keys, args.quiet))

    # THE CHECKPOINT (phase 14) holds two things: the Kafka offsets already
    # consumed, and the STATE of the open windows. Without it, a restart
    # re-reads from startingOffsets and loses every partial window.
    # One checkpoint PER QUERY: sharing one between two queries corrupts both.
    return (
        writer
        .option("checkpointLocation", str(config.CHECKPOINT_DIR / args.checkpoint_prefix / name))
        .trigger(processingTime=args.trigger)
        .start()
    )


def _shutdown(spark, queries, listener) -> None:
    """Detach the listener, then stop the queries - in that order.

    The reverse order makes the driver print a Py4JException: Spark notifies
    onQueryTerminated while the py4j callback server is already closing.
    """
    if listener is not None:
        spark.streams.removeListener(listener)
    for q in queries:
        if q.isActive:
            q.stop()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phases 11-17 - real-time analytics")
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--window", default="1 minute", help="window size, e.g. '30 seconds'")
    p.add_argument("--watermark", default="2 minutes", help="accepted lateness")
    p.add_argument("--trigger", default="10 seconds", help="micro-batch interval")
    p.add_argument("--sink", choices=["postgres", "console"], default="postgres")
    p.add_argument("--output-mode", choices=["update", "append", "complete"], default="update",
                   help="update = re-emit a window whenever it changes")
    p.add_argument("--from-beginning", action="store_true")
    p.add_argument("--duration", type=int, default=0, help="stop after N seconds")
    p.add_argument("--only", choices=["revenue", "pulse", "both"], default="both")
    p.add_argument("--checkpoint-prefix", default="analytics")
    p.add_argument("--no-metrics", action="store_true", help="do not persist the metrics")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args(argv)

    spark = get_spark("phases11-17-analytics")
    print(f"  {describe(spark)}")
    print(f"  window={args.window} watermark={args.watermark} "
          f"trigger={args.trigger} sink={args.sink} mode={args.output_mode}")

    listener = None
    if not args.no_metrics and args.sink == "postgres":
        listener = attach_metrics_listener(spark)

    valid, _ = split(parse_events(
        kafka_source(spark, args.topic, "earliest" if args.from_beginning else "latest")
    ))

    queries = []
    if args.only in ("revenue", "both"):
        queries.append(_start(
            revenue_by_country(valid, args.window, args.watermark),
            "revenue_by_country", args, REVENUE_TABLE,
            ["window_start", "window_end", "country"],
        ))
    if args.only in ("pulse", "both"):
        queries.append(_start(
            orders_pulse(valid, args.window, args.watermark),
            "orders_per_minute", args, PULSE_TABLE,
            ["window_start", "window_end"],
        ))

    print(f"\n  {len(queries)} query(ies) running. Ctrl+C to stop.\n")

    if args.duration:
        # A shared deadline: awaiting each query for `duration` in turn would
        # wait `duration x number of queries`, since they run concurrently.
        deadline = time.monotonic() + args.duration
        for q in queries:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                q.awaitTermination(remaining)
        _shutdown(spark, queries, listener)
    else:
        try:
            for q in queries:
                q.awaitTermination()
        finally:
            _shutdown(spark, queries, listener)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
