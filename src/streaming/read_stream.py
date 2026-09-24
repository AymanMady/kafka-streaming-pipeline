"""
PHASE 9 - Kafka -> Spark: read the stream, and nothing else.

The point of this job is to show what Kafka actually hands Spark, BEFORE any
parsing. It answers one question: what is a message, physically?

Run it:
    make stream-raw
    make stream-raw ARGS="--from-beginning --show-payload"
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from src.common import config
from src.streaming.spark_session import describe, get_spark, kafka_source


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 9 - raw read of the Kafka stream")
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--from-beginning", action="store_true",
                   help="read the topic from offset 0 rather than from the end")
    p.add_argument("--show-payload", action="store_true",
                   help="also print the JSON body of the message")
    p.add_argument("--duration", type=int, default=0,
                   help="stop after N seconds (0 = until Ctrl+C)")
    args = p.parse_args(argv)

    spark = get_spark("phase09-read-stream")
    print(f"  {describe(spark)}")
    print(f"  config: {config.describe()}")

    raw = kafka_source(spark, args.topic, "earliest" if args.from_beginning else "latest")

    # The Kafka envelope. key and value are BINARY: Kafka carries bytes and
    # knows nothing about JSON, Avro or anything else. Interpreting them is
    # entirely the consumer's job - that is phase 10.
    columns = [
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("key"),
        F.length("value").alias("value_bytes"),
    ]
    if args.show_payload:
        columns.append(F.col("value").cast("string").alias("payload"))

    query = (
        raw.select(*columns)
        .writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 10)
        # No checkpoint here on purpose: this job is an observation tool, it
        # must be restartable from scratch without leaving any state behind.
        .start()
    )

    print("\n  Reading the stream. Ctrl+C to stop.")
    print("  What to notice: the key decides the partition, and the offset only")
    print("  ever grows WITHIN one partition - never across the topic.\n")

    if args.duration:
        query.awaitTermination(args.duration)
        query.stop()
    else:
        query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
