"""
Event schema + quality rules.

This is THE contract between the producer and every consumer. It is written
here once, in plain Python (testable without Kafka or Spark), and the Spark
version is derived from it further down.

------------------------------------------------------------------------------
Why does event_id matter so much?
------------------------------------------------------------------------------
order_id is a BUSINESS identifier: "order number 123".
event_id is a TECHNICAL identifier: "this message, produced at this instant".

They do not serve the same purpose:
  - one order can produce SEVERAL events
    (order_created, order_paid, order_shipped -> same order_id, different
     event_ids);
  - the SAME event can arrive TWICE in Kafka
    (the producer never got its acknowledgement and retried, or Spark replays a
     micro-batch after a crash).

In the second case both copies are identical, event_id included. That is
exactly what makes them deduplicable: "I already processed evt_abc, skip it".
Without event_id there would be no way to tell a technical duplicate (drop it)
from a genuine second business event (keep it).

This is the foundation of idempotency, covered in phase 16.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------
EVENT_TYPE_ORDER_CREATED = "order_created"

# Mandatory fields: without them the event cannot be used.
REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "amount",
    "currency",
    "country",
    "timestamp",
)

SUPPORTED_CURRENCIES: frozenset[str] = frozenset({"MRU", "EUR", "USD", "MAD", "XOF"})


def new_event_id() -> str:
    """Unique technical id of a message. uuid4 = random, needs no coordination."""
    return f"evt_{uuid.uuid4().hex[:16]}"


def utc_now_iso() -> str:
    """ISO 8601 timestamp in UTC.

    Always UTC, never local time: a pipeline that mixes time zones produces
    wrong time windows, and the bug only shows up when the clocks change. The
    trailing 'Z' says UTC explicitly.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def make_order_event(
    *,
    order_id: int,
    customer_id: int,
    product_id: int,
    quantity: int,
    amount: float,
    country: str,
    currency: str = "MRU",
    timestamp: str | None = None,
    event_id: str | None = None,
    event_type: str = EVENT_TYPE_ORDER_CREATED,
) -> dict[str, Any]:
    """Build an event that satisfies the contract.

    event_id and timestamp can be injected: that is what makes it possible to
    reproduce a duplicate (same event_id) or a late event (timestamp in the
    past) in the phase 13 and phase 16 demos.
    """
    return {
        "event_id": event_id or new_event_id(),
        "event_type": event_type,
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "amount": round(float(amount), 2),
        "currency": currency,
        "country": country,
        "timestamp": timestamp or utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# Data quality: the same rules will be applied on the Spark side (phase 10)
# ---------------------------------------------------------------------------
def validate_event(event: Any) -> tuple[bool, list[str]]:
    """Return (valid, list of rejection reasons).

    It returns EVERY reason, not just the first one: when investigating a dirty
    stream, knowing that one event has three problems at once is far more
    useful than a single isolated error message.
    """
    reasons: list[str] = []

    if not isinstance(event, dict):
        return False, ["not_a_json_object"]

    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            reasons.append(f"missing_{field}")

    def _num(field: str) -> float | None:
        value = event.get(field)
        if isinstance(value, bool) or not isinstance(value, int | float):
            return None
        return float(value)

    # A present but non-numeric field is NOT the same problem as an absent
    # one, and it must not slip through: a quantity of "3" as text is one of
    # the most common corruptions there is (a CSV export, a JSON built by
    # string concatenation). Without this check it would pass as valid, since
    # no comparison rule can fire on it.
    qty = _num("quantity")
    if event.get("quantity") is not None and qty is None:
        reasons.append("quantity_not_numeric")
    elif qty is not None and qty <= 0:
        reasons.append("quantity_not_positive")

    amount = _num("amount")
    if event.get("amount") is not None and amount is None:
        reasons.append("amount_not_numeric")
    elif amount is not None and amount < 0:
        reasons.append("amount_negative")

    order_id = event.get("order_id")
    if order_id is not None and (isinstance(order_id, bool) or not isinstance(order_id, int)):
        reasons.append("order_id_not_integer")

    currency = event.get("currency")
    if currency is not None and currency not in SUPPORTED_CURRENCIES:
        reasons.append("currency_unsupported")

    ts = event.get("timestamp")
    if ts is not None and (not isinstance(ts, str) or not _is_iso8601(ts)):
        reasons.append("timestamp_invalid")

    return (len(reasons) == 0), reasons


def _is_iso8601(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except (ValueError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# (De)serialisation
# ---------------------------------------------------------------------------
def serialize(event: dict[str, Any]) -> bytes:
    """JSON -> bytes. Kafka only ever carries bytes, never objects."""
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def deserialize(payload: bytes) -> Any:
    """bytes -> Python object. May raise json.JSONDecodeError: it is up to the
    consumer to decide what to do with an unreadable message (phase 4)."""
    return json.loads(payload.decode("utf-8"))


# ---------------------------------------------------------------------------
# Spark version of the same contract
# ---------------------------------------------------------------------------
def spark_order_schema():
    """StructType describing an order event.

    The pyspark import is DELIBERATELY deferred into the function: this module
    must stay importable from the host (unit tests, producer) without PySpark
    being installed.

    Why an EXPLICIT schema rather than automatic inference?
      1. In streaming, inference is simply impossible: Spark would have to read
         data to guess the types, but the stream is infinite and empty at
         startup.
      2. Inference is unstable: a micro-batch where every amount is a round
         number would give LongType, the next one DoubleType. The schema would
         change mid-flight.
      3. An explicit schema is a CONTRACT: if the producer starts sending
         amount as text, the column becomes null and we notice, instead of
         silently propagating wrong data.
      4. Performance: no extra read pass.
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType([
        StructField("event_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("order_id", LongType(), nullable=True),
        StructField("customer_id", LongType(), nullable=True),
        StructField("product_id", LongType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
    ])
