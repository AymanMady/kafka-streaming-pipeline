"""
Tests of the streaming transformations (phase 19).

These run on static DataFrames: Structured Streaming applies the SAME
operators to a stream and to a batch, so testing the logic does not require a
stream. That property is what makes streaming code testable at all.
"""

from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from src.common.schemas import make_order_event, serialize, validate_event
from src.streaming.transform import ERROR_COL, annotate, parse_events, split, to_dlq


def _envelope(spark, payloads: list[bytes | str]):
    """Rebuild the Kafka envelope parse_events() expects."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    rows = [
        (0, i, now, str(i).encode(), p if isinstance(p, bytes) else p.encode())
        for i, p in enumerate(payloads)
    ]
    return spark.createDataFrame(rows, "partition int, offset long, timestamp timestamp, "
                                       "key binary, value binary")


def _event(**overrides):
    base = {"order_id": 1, "customer_id": 10, "product_id": 700,
            "quantity": 2, "amount": 99.5, "country": "Mauritania"}
    base.update(overrides)
    return make_order_event(**base)


def _errors(spark, event_or_payload) -> list[str]:
    payload = (serialize(event_or_payload) if isinstance(event_or_payload, dict)
               else event_or_payload)
    annotated = annotate(parse_events(_envelope(spark, [payload])))
    return sorted(annotated.collect()[0][ERROR_COL])


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def test_parse_extracts_the_typed_fields_and_the_event_time(spark):
    df = parse_events(_envelope(spark, [serialize(
        _event(timestamp="2026-03-15T10:30:00.000Z"))]))
    row = df.collect()[0]
    assert row.order_id == 1
    assert row.amount == pytest.approx(99.5)
    assert row.event_time is not None
    assert row.event_time.strftime("%Y-%m-%d %H:%M:%S") == "2026-03-15 10:30:00"


def test_the_original_payload_is_kept(spark):
    """A rejected event is worthless without the bytes that were received."""
    event = _event()
    row = parse_events(_envelope(spark, [serialize(event)])).collect()[0]
    assert json.loads(row.payload)["event_id"] == event["event_id"]


def test_malformed_json_does_not_crash_the_query(spark):
    """In streaming, one unreadable event must not bring the job down."""
    errors = _errors(spark, b"{not json at all")
    assert errors, "a malformed body must be rejected"
    assert "missing_event_id" in errors


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def test_a_valid_event_has_no_error(spark):
    assert _errors(spark, _event()) == []


@pytest.mark.parametrize("overrides,expected", [
    ({"quantity": 0}, "quantity_not_positive"),
    ({"quantity": -3}, "quantity_not_positive"),
    ({"amount": -1.0}, "amount_negative"),
    ({"currency": "XXX"}, "currency_unsupported"),
    ({"timestamp": "not-a-date"}, "timestamp_invalid"),
])
def test_each_corruption_triggers_its_rule(spark, overrides, expected):
    assert expected in _errors(spark, _event(**overrides))


def test_a_missing_field_is_reported(spark):
    event = _event()
    del event["country"]
    assert "missing_country" in _errors(spark, event)


def test_a_null_quantity_counts_as_invalid(spark):
    """THE test of this file.

    In SQL, NULL > 0 is not False, it is NULL. Without the coalesce in
    _safe(), an event with a null quantity would trigger NO rule at all and
    would pass as valid. It is the most common way broken data reaches a
    dashboard.
    """
    event = _event()
    event["quantity"] = None
    errors = _errors(spark, event)
    assert "missing_quantity" in errors
    assert "quantity_not_positive" in errors, "the NULL must not escape the comparison"


def test_one_event_can_violate_several_rules(spark):
    errors = _errors(spark, _event(quantity=0, amount=-5, currency="XXX"))
    assert {"quantity_not_positive", "amount_negative", "currency_unsupported"} <= set(errors)


# ---------------------------------------------------------------------------
# Split and dead letter queue
# ---------------------------------------------------------------------------

def test_split_separates_and_drops_the_technical_column(spark):
    df = parse_events(_envelope(spark, [
        serialize(_event(order_id=1)),
        serialize(_event(order_id=2, quantity=0)),
        serialize(_event(order_id=3)),
    ]))
    valid, invalid = split(df)
    assert valid.count() == 2
    assert invalid.count() == 1
    assert ERROR_COL not in valid.columns, "the technical column does not follow valid rows"
    assert ERROR_COL in invalid.columns, "the DLQ keeps the reason for the rejection"


def test_the_dlq_keeps_the_payload_the_reasons_and_the_source(spark):
    df = parse_events(_envelope(spark, [serialize(_event(currency="XXX"))]))
    _, invalid = split(df)
    row = to_dlq(invalid).collect()[0]
    body = json.loads(row.value)
    assert body["rejection_reasons"] == ["currency_unsupported"]
    assert json.loads(body["payload"])["currency"] == "XXX"
    assert body["source_partition"] == 0
    assert "rejected_at" in body


def test_an_empty_dataframe_does_not_break_the_split(spark):
    """A stream has quiet minutes: an empty micro-batch must be a no-op."""
    df = parse_events(_envelope(spark, []))
    valid, invalid = split(df)
    assert valid.count() == 0
    assert invalid.count() == 0


# ---------------------------------------------------------------------------
# THE CONTRACT: the two implementations must agree
# ---------------------------------------------------------------------------

CORPUS = [
    _event(),
    _event(quantity=0),
    _event(quantity=-2),
    _event(amount=-10.0),
    _event(currency="XXX"),
    _event(currency="EUR"),
    _event(timestamp="not-a-date"),
    _event(quantity=1, amount=0.0),
]


def test_python_and_spark_reach_the_same_verdict(spark):
    """The rules exist twice: in plain Python and in Spark. This test is what
    keeps them from drifting apart.

    It compares the VERDICT, not the reason names. The names legitimately
    differ on one point: from_json casts a badly typed field to NULL, so Spark
    can no longer tell "absent" from "present but of the wrong type", and
    reports missing_x where Python reports x_not_numeric. The verdict, which
    is what decides whether the event is processed, must be identical.
    """
    payloads = [serialize(e) for e in CORPUS]
    annotated = annotate(parse_events(_envelope(spark, payloads)))
    spark_valid = [
        len(r[ERROR_COL]) == 0
        for r in annotated.orderBy("kafka_offset").collect()
    ]
    python_valid = [validate_event(e)[0] for e in CORPUS]
    assert spark_valid == python_valid


def test_the_split_matches_the_python_validator(spark):
    payloads = [serialize(e) for e in CORPUS]
    valid, invalid = split(parse_events(_envelope(spark, payloads)))
    expected_valid = sum(1 for e in CORPUS if validate_event(e)[0])
    assert valid.count() == expected_valid
    assert invalid.count() == len(CORPUS) - expected_valid


def test_the_rules_run_in_a_single_pass(spark):
    """The error column is an array built in one projection.

    Counting rule by rule would mean as many aggregations as there are rules.
    Here one explode is enough to count everything.
    """
    payloads = [serialize(e) for e in CORPUS]
    counts = (annotate(parse_events(_envelope(spark, payloads)))
              .select(F.explode(ERROR_COL).alias("rule"))
              .groupBy("rule").count().collect())
    by_rule = {r["rule"]: r["count"] for r in counts}
    assert by_rule["quantity_not_positive"] == 2
    assert by_rule["amount_negative"] == 1
    assert by_rule["currency_unsupported"] == 1
    assert by_rule["timestamp_invalid"] == 1
