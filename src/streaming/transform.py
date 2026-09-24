"""
PHASE 10 - Streaming transformations: parse, validate, split.

The rules here mirror validate_event() in src/common/schemas.py. Same contract,
two implementations: one in plain Python (fast unit tests, no cluster), one in
Spark (runs on the stream). The tests check that the two agree.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.schemas import SUPPORTED_CURRENCIES, spark_order_schema

ERROR_COL = "_errors"

# ISO 8601 with a trailing Z. XXX accepts "Z" as the zero offset.
# Anything that does not match becomes NULL rather than raising: in streaming,
# one unparseable event must not bring the whole query down.
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka envelope -> typed columns + event_time.

    The Kafka value is kept as `payload`: an event rejected for being
    unparseable is worthless without its original bytes.
    """
    return (
        raw.select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload"),
        )
        # from_json in its default PERMISSIVE mode returns a struct full of
        # NULLs for a malformed body, instead of failing. The required-field
        # rules below are what turns those NULLs into an explicit rejection.
        .withColumn("event", F.from_json("payload", spark_order_schema()))
        .select("kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_key",
                "payload", "event.*")
        .withColumn("event_time", F.to_timestamp("timestamp", TIMESTAMP_FORMAT))
    )


def _safe(condition):
    """NULL -> False.

    In SQL, `NULL > 0` is not False, it is NULL. Without this coalesce a
    missing quantity would trigger NO rule at all and would pass as valid.
    It is the single most common way broken data reaches a dashboard.
    """
    return F.coalesce(condition, F.lit(False))


def _rules() -> list[tuple[str, object]]:
    """(name, "the event is valid on this point") pairs."""
    required = [
        "event_id", "event_type", "order_id", "customer_id", "product_id",
        "quantity", "amount", "currency", "country", "timestamp",
    ]
    rules: list[tuple[str, object]] = [
        (f"missing_{field}", F.col(field).isNotNull()) for field in required
    ]
    rules += [
        ("quantity_not_positive", F.col("quantity") > 0),
        ("amount_negative", F.col("amount") >= 0),
        ("currency_unsupported", F.col("currency").isin(list(SUPPORTED_CURRENCIES))),
        # The field was present but to_timestamp could not read it.
        ("timestamp_invalid", F.col("event_time").isNotNull()),
    ]
    return rules


def annotate(df: DataFrame) -> DataFrame:
    """Add ERROR_COL: the array of violated rules, empty when the event is valid.

    One pass over the data, whatever the number of rules. Counting rule by
    rule would mean as many aggregations as there are rules.
    """
    flags = [
        F.when(~_safe(condition), F.lit(name)).otherwise(F.lit(None))
        for name, condition in _rules()
    ]
    return df.withColumn(ERROR_COL, F.array_compact(F.array(*flags)))


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid, invalid). The technical column does not follow the valid rows."""
    annotated = df if ERROR_COL in df.columns else annotate(df)
    valid = annotated.filter(F.size(ERROR_COL) == 0).drop(ERROR_COL)
    invalid = annotated.filter(F.size(ERROR_COL) > 0)
    return valid, invalid


def to_dlq(invalid: DataFrame) -> DataFrame:
    """Shape the rejects for the orders-invalid topic.

    The rejected event keeps its original payload AND gains the reason for the
    rejection. A dead letter queue that only stores the payload tells you that
    something failed, never what.
    """
    return invalid.select(
        F.col("kafka_key").alias("key"),
        F.to_json(
            F.struct(
                F.col("payload"),
                F.col(ERROR_COL).alias("rejection_reasons"),
                F.col("kafka_partition").alias("source_partition"),
                F.col("kafka_offset").alias("source_offset"),
                F.current_timestamp().alias("rejected_at"),
            )
        ).alias("value"),
    )
