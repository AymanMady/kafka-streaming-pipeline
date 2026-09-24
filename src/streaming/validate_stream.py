"""
PHASE 10 - Streaming transformations: validate, and route the rejects.

Reads the stream, applies the quality rules, sends the invalid events to the
orders-invalid topic (the dead letter queue) and reports on the valid ones.

Run it:
    make stream-validate
    make stream-validate ARGS="--from-beginning --duration 60"

Why a DLQ rather than a .filter() that drops the bad rows?
Because a filter is silent. Three months later, nobody can say what was
dropped or why. A DLQ is a topic like any other: you read it, you count it,
you alert on it, and you can replay it once the producer is fixed.
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from src.common import config
from src.streaming.spark_session import describe, get_spark, kafka_source
from src.streaming.transform import ERROR_COL, annotate, parse_events, split, to_dlq


def _process_batch(batch, batch_id: int, dlq_topic: str, quiet: bool) -> None:
    """Called once per micro-batch.

    foreachBatch hands us a normal (batch) DataFrame: every ordinary Spark
    operation is available again, including writing to several destinations
    from a single read of the stream. Two separate writeStream queries would
    read Kafka twice.
    """
    # The batch DataFrame is consumed several times below. Without cache, each
    # action re-runs the whole parse + validate chain.
    batch.persist()
    try:
        valid, invalid = split(batch)
        n_valid, n_invalid = valid.count(), invalid.count()

        if n_invalid:
            (to_dlq(invalid).write.format("kafka")
             .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
             .option("topic", dlq_topic)
             .save())

        total = n_valid + n_invalid
        ratio = (100.0 * n_valid / total) if total else 100.0
        print(f"  batch {batch_id:>4} | {total:>6} events | valid {n_valid:>6} "
              f"({ratio:5.1f}%) | rejected {n_invalid:>4} -> {dlq_topic}")

        if n_invalid and not quiet:
            (invalid.select(F.explode(ERROR_COL).alias("rule"))
             .groupBy("rule").count().orderBy(F.desc("count"))
             .show(10, truncate=False))
    finally:
        batch.unpersist()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 10 - validate the stream, route the rejects")
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--dlq", default=config.TOPIC_ORDERS_INVALID)
    p.add_argument("--from-beginning", action="store_true")
    p.add_argument("--duration", type=int, default=0, help="stop after N seconds")
    p.add_argument("--quiet", action="store_true", help="do not detail the violated rules")
    p.add_argument("--checkpoint", default=None, help="checkpoint directory")
    args = p.parse_args(argv)

    spark = get_spark("phase10-validate-stream")
    print(f"  {describe(spark)}")

    checkpoint = args.checkpoint or str(config.CHECKPOINT_DIR / "validate")
    parsed = annotate(parse_events(
        kafka_source(spark, args.topic, "earliest" if args.from_beginning else "latest")
    ))

    query = (
        parsed.writeStream
        .foreachBatch(lambda df, bid: _process_batch(df, bid, args.dlq, args.quiet))
        .option("checkpointLocation", checkpoint)
        .start()
    )

    print(f"\n  Validating. Rejects -> {args.dlq}. Checkpoint: {checkpoint}")
    print("  Read the DLQ with: make dlq-tail\n")

    if args.duration:
        query.awaitTermination(args.duration)
        query.stop()
    else:
        query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
