"""
PHASE 13 - What a watermark accepts, and what it drops.

The watermark is the hardest idea in Structured Streaming, because it answers
a question batch processing never has to ask: how long do we keep a window
open, waiting for events that have not arrived yet?

  - no watermark -> every window stays open forever -> the state grows without
    bound, and the job eventually dies of memory;
  - a watermark  -> "beyond this lateness we drop" -> bounded memory, but data
    that is genuinely lost.

There is no right answer, only a trade-off the business has to make.

This demo makes it concrete, and deterministic:
  1. it produces ON-TIME events, which push the watermark forward;
  2. it then produces two LATE events, one inside the watermark and one
     outside it;
  3. it shows which one landed in its window, and reads the number of rows
     Spark itself reports as dropped.

Run it:
    make demo-watermark
    make demo-watermark ARGS="--watermark '5 minutes'"
"""

from __future__ import annotations

import argparse
import contextlib
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from src.common import config
from src.common.schemas import make_order_event, serialize
from src.streaming.spark_session import get_spark
from src.streaming.transform import parse_events

DEMO_TOPIC = "orders-watermark-demo"


def _recreate_topic(bootstrap: str, topic: str, partitions: int = 1) -> None:
    """A single partition, recreated from scratch: the demo must be reproducible.

    With several partitions the watermark would depend on the interleaving of
    the partitions, and the result would change from one run to the next.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        for f in admin.delete_topics([topic]).values():
            # Already gone is exactly what we want.
            with contextlib.suppress(Exception):
                f.result(timeout=30)
        time.sleep(3)
    for f in admin.create_topics([NewTopic(topic, num_partitions=partitions,
                                           replication_factor=1)]).values():
        f.result(timeout=30)
    time.sleep(2)


def _produce(bootstrap: str, topic: str, events: list[dict]) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap})
    for event in events:
        producer.produce(topic, key=str(event["order_id"]), value=serialize(event))
    producer.flush(30)


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _event(order_id: int, moment: datetime) -> dict:
    return make_order_event(
        order_id=order_id, customer_id=order_id, product_id=700,
        quantity=1, amount=100.0, country="Mauritania", timestamp=_iso(moment),
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 13 - watermark demo")
    p.add_argument("--watermark", default="2 minutes")
    p.add_argument("--window", default="1 minute")
    p.add_argument("--topic", default=DEMO_TOPIC)
    args = p.parse_args(argv)

    bootstrap = config.KAFKA_BOOTSTRAP
    print(f"\n  Preparing the topic {args.topic} on {bootstrap}...")
    _recreate_topic(bootstrap, args.topic)

    # A fixed base time. The watermark is computed from the EVENT times seen,
    # never from the wall clock, so any base works - as long as it is stable.
    base = (datetime.now(timezone.utc) - timedelta(minutes=30)).replace(
        second=0, microsecond=0)

    spark = get_spark("phase13-watermark-demo", master="local[2]")
    collected: list[dict] = []

    def _collect(batch, batch_id):  # noqa: ARG001
        for row in batch.collect():
            collected.append(row.asDict())

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    windowed = (
        parse_events(stream)
        .withWatermark("event_time", args.watermark)
        .groupBy(F.window("event_time", args.window))
        .agg(F.count("*").alias("orders_count"),
             F.collect_list("order_id").alias("order_ids"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "orders_count", "order_ids")
    )
    query = (
        windowed.writeStream.outputMode("update")
        .foreachBatch(_collect)
        .trigger(processingTime="5 seconds")
        .start()
    )

    def _wait_for_one_batch(previous: int) -> int:
        """Block until Spark has finished a micro-batch that saw some input."""
        deadline = time.time() + 90
        while time.time() < deadline:
            progresses = query.recentProgress
            done = sum(1 for pr in progresses if pr.get("numInputRows", 0) > 0)
            if done > previous:
                time.sleep(2)
                return done
            time.sleep(1)
        raise TimeoutError("no micro-batch processed in time")

    # --- Step 1: on-time events. They are what pushes the watermark forward.
    on_time = [_event(i + 1, base + timedelta(seconds=offset))
               for i, offset in enumerate([0, 70, 130, 190, 250])]
    print("  Step 1 - 5 on-time events, from T+0s to T+250s")
    _produce(bootstrap, args.topic, on_time)
    seen = _wait_for_one_batch(0)

    watermark_at = base + timedelta(seconds=250) - _to_timedelta(args.watermark)
    print("    max event time seen : T+250s")
    print(f"    watermark now at    : T+{int((watermark_at - base).total_seconds())}s "
          f"(max seen - {args.watermark})")

    # --- Step 2: two late events, one on each side of the watermark.
    inside = _event(101, base + timedelta(seconds=140))   # after the watermark
    outside = _event(102, base + timedelta(seconds=10))   # before it
    print("\n  Step 2 - 2 late events:")
    print("    order 101 at T+140s -> AFTER the watermark  -> should be accepted")
    print("    order 102 at T+10s  -> BEFORE the watermark -> should be dropped")
    _produce(bootstrap, args.topic, [inside, outside])
    _wait_for_one_batch(seen)

    query.stop()

    # --- Result
    dropped = sum(
        op.get("numRowsDroppedByWatermark", 0)
        for pr in query.recentProgress for op in (pr.get("stateOperators") or [])
    )

    final: dict[tuple, dict] = {}
    for row in collected:
        final[(row["window_start"], row["window_end"])] = row

    print("\n  " + "=" * 62)
    print("  Windows emitted (last state of each)")
    print("  " + "=" * 62)
    for (start, _end), row in sorted(final.items()):
        offset = int((start.replace(tzinfo=timezone.utc) - base).total_seconds())
        ids = sorted(row["order_ids"])
        print(f"    T+{offset:>3}s -> T+{offset + 60:>3}s | "
              f"{row['orders_count']} event(s) | orders {ids}")

    accepted = any(101 in sorted(r["order_ids"]) for r in final.values())
    print("\n  Order 101 (inside the watermark) : "
          f"{'ACCEPTED, its window was re-emitted' if accepted else 'MISSING'}")
    print(f"  Rows dropped by the watermark    : {dropped}")
    print("\n  Read it this way: order 102 never appears anywhere. It was not")
    print("  rejected for being invalid - it was simply too late. Spark counts")
    print("  it in numRowsDroppedByWatermark, and that counter is the one to")
    print("  put on a dashboard: a watermark silently eating data is a bug you")
    print("  only notice in the numbers.\n")

    spark.stop()
    return 0


def _to_timedelta(spec: str) -> timedelta:
    """'2 minutes' -> timedelta(minutes=2). Accepts the Spark spellings."""
    amount, unit = spec.strip().split()
    unit = unit.rstrip("s")
    factor = {"second": 1, "minute": 60, "hour": 3600}[unit]
    return timedelta(seconds=int(amount) * factor)


if __name__ == "__main__":
    raise SystemExit(main())
