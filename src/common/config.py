"""
Central configuration for the project.

Why a config module rather than os.environ calls scattered everywhere?
Because a hardcoded broker address is the first thing that breaks when you move
from your laptop to a container, and then to production. Here a single place
decides, and it documents its defaults.

One important subtlety in this project: the broker address DEPENDS ON WHERE YOU
RUN.
  - from your machine (venv)    -> localhost:9092   (EXTERNAL listener)
  - from a docker container     -> kafka:9092       (INTERNAL listener)
So KAFKA_BOOTSTRAP stays overridable through an environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Project root = two levels above this file (src/common/config.py)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# load_dotenv never REPLACES a variable already present in the environment, so
# `export KAFKA_BOOTSTRAP=kafka:9092` wins over the .env file.
load_dotenv(PROJECT_ROOT / ".env", override=False)


def _env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value.strip().strip('"').strip("'")


# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------
KAFKA_HOST_PORT: str = _env("KAFKA_HOST_PORT", "9092")

# Default address: the one seen from the host. Override with KAFKA_BOOTSTRAP
# (used by the Spark containers: "kafka:9092").
KAFKA_BOOTSTRAP: str = _env("KAFKA_BOOTSTRAP", f"localhost:{KAFKA_HOST_PORT}")

TOPIC_ORDERS: str = _env("TOPIC_ORDERS", "orders")
TOPIC_ORDERS_INVALID: str = _env("TOPIC_ORDERS_INVALID", "orders-invalid")

# Consumer group of the Python consumer. Spark gets its own (phase 9): two
# distinct groups read the same topic without interfering.
CONSUMER_GROUP: str = _env("CONSUMER_GROUP", "order-processing-group")

# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------
POSTGRES_USER: str = _env("POSTGRES_USER", "stream_user")
POSTGRES_PASSWORD: str = _env("POSTGRES_PASSWORD", "change_me_in_local_env")
POSTGRES_DB: str = _env("POSTGRES_DB", "streaming_analytics")

# Same logic as Kafka: "postgres" from a container, "localhost" from the host.
POSTGRES_HOST: str = _env("PGHOST_OVERRIDE", "localhost")
POSTGRES_PORT: str = _env("PGPORT_OVERRIDE", _env("POSTGRES_HOST_PORT", "5441"))


def postgres_dsn() -> str:
    """psycopg2 connection string (Python consumer, check scripts)."""
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )


def postgres_jdbc_url() -> str:
    """JDBC URL (Spark). From the Spark containers: host=postgres, port=5432."""
    host = _env("PGHOST_OVERRIDE", "postgres")
    port = _env("PGPORT_OVERRIDE", "5432")
    return f"jdbc:postgresql://{host}:{port}/{POSTGRES_DB}"


# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
OUTPUT_DIR = DATA_DIR / "output"


def describe() -> str:
    """Readable summary of the active configuration (printed when a job starts)."""
    return (
        f"kafka={KAFKA_BOOTSTRAP} topic={TOPIC_ORDERS} "
        f"dlq={TOPIC_ORDERS_INVALID} postgres={POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
