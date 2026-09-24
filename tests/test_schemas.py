"""Tests of the event contract (phase 19). Pure Python: no Kafka, no Spark."""

from __future__ import annotations

import json

import pytest

from src.common.schemas import (
    SUPPORTED_CURRENCIES,
    deserialize,
    make_order_event,
    serialize,
    validate_event,
)


def _event(**overrides):
    base = {"order_id": 1, "customer_id": 10, "product_id": 700,
            "quantity": 2, "amount": 99.5, "country": "Mauritania"}
    base.update(overrides)
    return make_order_event(**base)


def test_a_well_formed_event_is_valid():
    valid, reasons = validate_event(_event())
    assert valid, reasons


def test_every_generated_event_carries_a_unique_event_id():
    """Two events built identically must still be distinguishable."""
    a, b = _event(), _event()
    assert a["event_id"] != b["event_id"]


def test_an_injected_event_id_is_kept():
    """Reproducing a duplicate requires being able to force the id."""
    assert _event(event_id="evt_fixed")["event_id"] == "evt_fixed"


def test_the_timestamp_is_utc_and_says_so():
    assert _event()["timestamp"].endswith("Z")


@pytest.mark.parametrize("field", [
    "event_id", "event_type", "order_id", "customer_id", "product_id",
    "quantity", "amount", "currency", "country", "timestamp",
])
def test_every_required_field_is_required(field):
    event = _event()
    del event[field]
    valid, reasons = validate_event(event)
    assert not valid
    assert f"missing_{field}" in reasons


def test_a_null_field_counts_as_missing():
    """None is not a value: an explicit null is as unusable as an absence."""
    valid, reasons = validate_event(_event(order_id=None) | {"order_id": None})
    assert not valid
    assert "missing_order_id" in reasons


def test_quantity_must_be_positive():
    valid, reasons = validate_event(_event(quantity=0))
    assert not valid
    assert "quantity_not_positive" in reasons


def test_amount_cannot_be_negative():
    valid, reasons = validate_event(_event(amount=-1))
    assert not valid
    assert "amount_negative" in reasons


def test_a_numeric_field_sent_as_text_is_rejected():
    """The regression that motivated the rule.

    Without an explicit type check, "3" triggers no comparison rule at all
    (it is present, so not missing; it cannot be compared, so no threshold
    fires) and the event passes as valid. A quantity exported as text is one
    of the most common corruptions there is.
    """
    event = _event()
    event["quantity"] = "3"
    valid, reasons = validate_event(event)
    assert not valid
    assert "quantity_not_numeric" in reasons


def test_an_unsupported_currency_is_rejected():
    valid, reasons = validate_event(_event(currency="XXX"))
    assert not valid
    assert "currency_unsupported" in reasons
    assert "XXX" not in SUPPORTED_CURRENCIES


def test_an_unreadable_timestamp_is_rejected():
    valid, reasons = validate_event(_event(timestamp="not-a-date"))
    assert not valid
    assert "timestamp_invalid" in reasons


def test_every_reason_is_reported_not_just_the_first():
    """Investigating a dirty stream needs the whole list, not one error."""
    event = _event(quantity=0, amount=-5, currency="XXX")
    valid, reasons = validate_event(event)
    assert not valid
    assert {"quantity_not_positive", "amount_negative", "currency_unsupported"} <= set(reasons)


def test_something_that_is_not_an_object_is_rejected_without_crashing():
    assert validate_event([1, 2, 3]) == (False, ["not_a_json_object"])
    assert validate_event("hello") == (False, ["not_a_json_object"])


def test_serialise_then_deserialise_is_the_identity():
    event = _event()
    assert deserialize(serialize(event)) == event


def test_serialisation_produces_utf8_bytes():
    payload = serialize(_event(country="Côte d'Ivoire"))
    assert isinstance(payload, bytes)
    assert json.loads(payload.decode("utf-8"))["country"] == "Côte d'Ivoire"


def test_a_boolean_is_not_a_number():
    """In Python, True == 1. Without an explicit check, a boolean quantity
    would sail through every numeric rule."""
    event = _event()
    event["quantity"] = True
    valid, reasons = validate_event(event)
    assert not valid
    assert "quantity_not_numeric" in reasons
