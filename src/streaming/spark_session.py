"""
SparkSession factory for the streaming jobs (phase 8).

Centralised for the same reason as everywhere else: a hardcoded master URL or
a forgotten checkpoint option is the kind of thing that only breaks once the
job is in production.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.common import config


def get_spark(app_name: str, master: str | None = None, extra_conf: dict[str, str] | None = None):
    builder = SparkSession.builder.appName(app_name)

    chosen = master or os.getenv("SPARK_MASTER_URL")
    if chosen:
        builder = builder.master(chosen)

    defaults = {
        # Everything in UTC. A streaming pipeline whose windows depend on the
        # machine's time zone produces results that differ between the laptop
        # and the cluster, and the bug only surfaces when clocks change.
        "spark.sql.session.timeZone": "UTC",
        # 4 partitions in the topic -> more shuffle partitions than that just
        # creates empty tasks on every micro-batch. The default of 200 would
        # mean 200 tasks to aggregate a few hundred rows.
        "spark.sql.shuffle.partitions": "4",
        # Reject ambiguous dates instead of converting them wrongly.
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    }
    for key, value in {**defaults, **(extra_conf or {})}.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_source(spark: SparkSession, topic: str | None = None, starting: str = "latest"):
    """The Kafka source, with the options that matter.

    startingOffsets only applies the FIRST time a query runs. As soon as a
    checkpoint exists, Spark resumes from the committed offsets and ignores
    this option entirely (phase 14). Deleting the checkpoint is what makes it
    apply again.

    failOnDataLoss=false: when a segment is deleted by retention while the job
    was down, Spark would otherwise abort the query. Here we prefer to keep
    going and lose the gap. In production this is a decision to take
    consciously, not a default to copy.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", topic or config.TOPIC_ORDERS)
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        # Caps the size of a micro-batch. Without it, a job restarting after an
        # outage swallows the whole backlog in one batch and runs out of memory.
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000"))
        .load()
    )


def describe(spark: SparkSession) -> str:
    sc = spark.sparkContext
    return (
        f"spark={spark.version} master={sc.master} app={sc.applicationId} "
        f"shuffle_partitions={spark.conf.get('spark.sql.shuffle.partitions')}"
    )
