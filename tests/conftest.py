"""
Shared test configuration (phase 19).

Principle: the tests need NEITHER Kafka NOR a cluster. They run on a local
SparkSession with hand-written DataFrames of a few rows. A test has to be fast
and deterministic, otherwise it stops being run.
"""

from __future__ import annotations

import logging

import pytest

# py4j writes one last message after pytest has closed stdout, which pollutes
# the output with a "Logging error" unrelated to the tests.
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession, shared by the whole test session.

    scope="session" matters: starting a JVM costs several seconds. Recreating
    one per test would make the suite unbearably slow.

    local[2]: two partitions are enough to exercise the parallel code paths
    without saturating the machine.
    """
    from src.streaming.spark_session import get_spark

    session = get_spark(
        "pytest-suite",
        master="local[2]",
        extra_conf={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
    )
    yield session
    session.stop()
