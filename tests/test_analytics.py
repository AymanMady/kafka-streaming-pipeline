"""
Tests of the windowed aggregations (phase 19).

Structured Streaming applies the same operators to a batch and to a stream, so
the window logic is tested here on a static DataFrame with hand-picked event
times - and the expected result is computable in your head.
"""

from __future__ import annotations

import datetime as dt

import pytest

from src.streaming.analytics_stream import orders_pulse, revenue_by_country

T0 = dt.datetime(2026, 3, 15, 10, 0, 0)


def _events(spark, rows):
    """rows = (seconds after T0, country, amount, customer_id)."""
    data = [
        (T0 + dt.timedelta(seconds=offset), country, float(amount), int(customer))
        for offset, country, amount, customer in rows
    ]
    return spark.createDataFrame(
        data, "event_time timestamp, country string, amount double, customer_id long")


def test_the_windows_are_cut_on_event_time(spark):
    """Two events 70 s apart land in two different one-minute windows.

    The cut is on EVENT time, the moment the order was placed - not on the
    moment Spark saw it. That is the whole point: a five-minute network delay
    must not move an afternoon's revenue into one minute.
    """
    df = _events(spark, [(0, "Senegal", 100, 1), (70, "Senegal", 200, 2)])
    rows = revenue_by_country(df, "1 minute", "2 minutes").orderBy("window_start").collect()
    assert len(rows) == 2
    assert rows[0].window_start == T0
    assert rows[0].window_end == T0 + dt.timedelta(minutes=1)
    assert rows[0].total_amount == pytest.approx(100.0)
    assert rows[1].total_amount == pytest.approx(200.0)


def test_the_boundary_belongs_to_the_next_window(spark):
    """A window is [start, end): the upper bound is excluded.

    Getting this wrong double-counts every event that lands exactly on a
    boundary - rare in tests, constant in production.
    """
    df = _events(spark, [(59, "Mali", 10, 1), (60, "Mali", 20, 2)])
    rows = revenue_by_country(df, "1 minute", "2 minutes").orderBy("window_start").collect()
    assert len(rows) == 2
    assert rows[0].total_amount == pytest.approx(10.0)
    assert rows[1].window_start == T0 + dt.timedelta(minutes=1)


def test_countries_are_aggregated_separately(spark):
    df = _events(spark, [
        (0, "Senegal", 100, 1), (10, "Senegal", 50, 2), (20, "Morocco", 70, 3)])
    rows = {r.country: r for r in revenue_by_country(df, "1 minute", "2 minutes").collect()}
    assert rows["Senegal"].orders_count == 2
    assert rows["Senegal"].total_amount == pytest.approx(150.0)
    assert rows["Senegal"].avg_amount == pytest.approx(75.0)
    assert rows["Morocco"].orders_count == 1


def test_the_average_is_computed_on_the_raw_sum(spark):
    """Rounding the total and then dividing gives a different answer.

    Round once, at the end. An intermediate rounding is a loss of information
    that propagates - the one-cent gap nobody can explain three months later.
    """
    df = _events(spark, [(0, "Mali", 0.335, 1), (1, "Mali", 0.335, 2), (2, "Mali", 0.335, 3)])
    row = revenue_by_country(df, "1 minute", "2 minutes").collect()[0]
    assert row.avg_amount == pytest.approx(round(0.335, 2), abs=0.005)


def test_the_pulse_counts_distinct_customers(spark):
    df = _events(spark, [
        (0, "Senegal", 10, 1), (1, "Senegal", 10, 1), (2, "Morocco", 10, 2)])
    row = orders_pulse(df, "1 minute", "2 minutes").collect()[0]
    assert row.orders_count == 3
    # approx_count_distinct: HyperLogLog, exact on such small cardinalities.
    assert row.distinct_customers == 2


def test_the_pulse_ignores_the_country(spark):
    """One row per window, whatever the number of countries."""
    df = _events(spark, [(0, "Senegal", 10, 1), (1, "Morocco", 20, 2), (2, "Mali", 30, 3)])
    rows = orders_pulse(df, "1 minute", "2 minutes").collect()
    assert len(rows) == 1
    assert rows[0].total_amount == pytest.approx(60.0)


def test_a_thirty_second_window_cuts_twice_as_often(spark):
    df = _events(spark, [(0, "Mali", 10, 1), (40, "Mali", 20, 2)])
    one_minute = revenue_by_country(df, "1 minute", "2 minutes").count()
    thirty_seconds = revenue_by_country(df, "30 seconds", "2 minutes").count()
    assert one_minute == 1
    assert thirty_seconds == 2


def test_an_empty_dataframe_produces_no_window(spark):
    """A stream has quiet minutes. They must produce nothing, not crash."""
    df = _events(spark, [])
    assert revenue_by_country(df, "1 minute", "2 minutes").count() == 0
    assert orders_pulse(df, "1 minute", "2 minutes").count() == 0


def test_the_output_columns_match_the_postgres_tables(spark):
    """The sink upserts by column name: a rename here breaks the write at
    runtime, in the first micro-batch, not at build time."""
    df = _events(spark, [(0, "Mali", 10, 1)])
    assert revenue_by_country(df, "1 minute", "2 minutes").columns == [
        "window_start", "window_end", "country", "orders_count",
        "total_amount", "avg_amount"]
    assert orders_pulse(df, "1 minute", "2 minutes").columns == [
        "window_start", "window_end", "orders_count", "total_amount",
        "avg_amount", "distinct_customers"]
