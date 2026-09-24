#!/usr/bin/env python3
"""
Kafka producer: generates e-commerce orders and publishes them to the `orders` topic.

------------------------------------------------------------------------------
What a producer is
------------------------------------------------------------------------------
An application that PUBLISHES events into Kafka. It does not know, and does not
need to know, who will read them: zero, one or ten consumers. That decoupling
is the whole point.

------------------------------------------------------------------------------
The message key: the most important thing in this file
------------------------------------------------------------------------------
When the producer sends a message it picks a key. Kafka derives the destination
partition from it:

        partition = murmur2(key) % number_of_partitions

Direct consequences:
  - same key -> ALWAYS the same partition (as long as the partition count does
    not change);
  - no key (None) -> balanced spread across partitions ("sticky" partitioner:
    in batches, for network efficiency).

And since Kafka only guarantees ordering WITHIN a partition, choosing the key
means choosing WHAT WILL BE ORDERED.

Here the default key is customer_id: every event of a given customer lands in
the same partition, so they are read in the order they were produced.
"Customer 45: order created, then cancelled" can never be read the other way
round. Nothing is guaranteed BETWEEN two different customers -- and that is
fine, because no business rule depends on the relative order of two distinct
customers' orders.

Classic trap: using order_id as the key gives a perfect spread (every order is
unique) but loses per-customer ordering. Using country as the key would pile
all Mauritanian traffic onto a single partition: a hot partition that caps
throughput.

------------------------------------------------------------------------------
Usage
------------------------------------------------------------------------------
    python src/producer/order_producer.py --rate 10
    python src/producer/order_producer.py --rate 100 --duration 30
    python src/producer/order_producer.py --rate 5 --invalid-rate 0.2     # phase 10
    python src/producer/order_producer.py --rate 5 --duplicate-rate 0.3   # phase 16
    python src/producer/order_producer.py --rate 5 --late-rate 0.2        # phase 13
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Makes "python src/producer/order_producer.py" work from the project root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import KafkaException, Producer  # noqa: E402

from src.common import config  # noqa: E402
from src.common.schemas import make_order_event, serialize  # noqa: E402

LOG = logging.getLogger("producer")

# ---------------------------------------------------------------------------
# Reference data: enough to generate believable orders
# ---------------------------------------------------------------------------
COUNTRIES: list[tuple[str, str, float]] = [
    # (country, currency, relative share of traffic)
    ("Mauritania", "MRU", 0.45),
    ("Senegal", "XOF", 0.20),
    ("Mali", "XOF", 0.12),
    ("Morocco", "MAD", 0.13),
    ("France", "EUR", 0.10),
]

# (product_id, unit price)
PRODUCTS: list[tuple[int, float]] = [
    (701, 125.00), (702, 250.00), (703, 49.90), (704, 1200.00),
    (705, 15.50), (706, 780.00), (707, 320.00), (708, 95.00),
]


@dataclass
class Stats:
    """Observability counters. These are what answer 'is it actually working?'."""

    produced: int = 0          # handed to the client library
    delivered: int = 0         # ACKNOWLEDGED by the broker (this is what counts)
    failed: int = 0
    invalid_injected: int = 0
    duplicates_injected: int = 0
    late_injected: int = 0
    started_at: float = field(default_factory=time.perf_counter)
    per_partition: dict[int, int] = field(default_factory=dict)

    def elapsed(self) -> float:
        return max(time.perf_counter() - self.started_at, 1e-9)

    def rate(self) -> float:
        return self.delivered / self.elapsed()


class OrderGenerator:
    """Generates the events. Kept separate from Kafka so it is testable without
    a broker (phase 19)."""

    def __init__(
        self,
        seed: int | None = None,
        invalid_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        late_rate: float = 0.0,
        late_seconds: int = 900,
        first_order_id: int = 1,
        n_customers: int = 200,
    ) -> None:
        self.rng = random.Random(seed)
        self.invalid_rate = invalid_rate
        self.duplicate_rate = duplicate_rate
        self.late_rate = late_rate
        self.late_seconds = late_seconds
        self.next_order_id = first_order_id
        self.n_customers = n_customers
        self._last_event: dict[str, Any] | None = None
        self._countries = [c[0] for c in COUNTRIES]
        self._currencies = {c[0]: c[1] for c in COUNTRIES}
        self._weights = [c[2] for c in COUNTRIES]

    def next_event(self) -> tuple[dict[str, Any], str]:
        """Return (event, kind) where kind = valid | invalid | duplicate | late."""
        # --- duplicate: republish the PREVIOUS event byte for byte, event_id
        # included. That is exactly what a producer that never got its ACK and
        # retried does. The consumer will have to recognise it (phase 16).
        if self._last_event is not None and self.rng.random() < self.duplicate_rate:
            self._last_event = dict(self._last_event)
            return self._last_event, "duplicate"

        country = self.rng.choices(self._countries, weights=self._weights, k=1)[0]
        product_id, unit_price = self.rng.choice(PRODUCTS)
        quantity = self.rng.randint(1, 5)

        kind = "valid"
        timestamp = None

        # --- late event: BUSINESS timestamp in the past, while Kafka receives
        # it now. Demo material for watermarks.
        if self.rng.random() < self.late_rate:
            delay = self.rng.randint(60, max(61, self.late_seconds))
            timestamp = (
                datetime.now(timezone.utc) - timedelta(seconds=delay)
            ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            kind = "late"

        event = make_order_event(
            order_id=self.next_order_id,
            customer_id=self.rng.randint(1, self.n_customers),
            product_id=product_id,
            quantity=quantity,
            amount=round(unit_price * quantity, 2),
            country=country,
            currency=self._currencies[country],
            timestamp=timestamp,
        )
        self.next_order_id += 1

        # --- invalid event: the contract is broken on purpose.
        if self.rng.random() < self.invalid_rate:
            event = self._corrupt(event)
            kind = "invalid"

        self._last_event = event
        return event, kind

    def _corrupt(self, event: dict[str, Any]) -> dict[str, Any]:
        """Apply ONE realistic corruption, picked from those seen in production."""
        broken = dict(event)
        choice = self.rng.choice(
            ["null_order_id", "negative_amount", "zero_quantity",
             "missing_country", "bad_currency", "bad_timestamp"]
        )
        if choice == "null_order_id":
            broken["order_id"] = None
        elif choice == "negative_amount":
            broken["amount"] = -abs(broken["amount"])
        elif choice == "zero_quantity":
            broken["quantity"] = 0
        elif choice == "missing_country":
            broken.pop("country", None)
        elif choice == "bad_currency":
            broken["currency"] = "XXX"
        elif choice == "bad_timestamp":
            broken["timestamp"] = "not-a-date"
        broken["_corruption"] = choice  # trace, handy to verify phase 10
        return broken


def build_producer(bootstrap: str, extra: dict[str, Any] | None = None) -> Producer:
    """Create the producer with explicit, commented settings."""
    conf: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "client.id": "order-producer",

        # acks=all: the broker only confirms AFTER the write has reached every
        # replica in sync with the leader (ISR). With acks=1 we would lose the
        # unreplicated messages during a leader failover; with acks=0 there
        # would not even be an acknowledgement -> at-most-once.
        "acks": "all",

        # enable.idempotence: the producer numbers its messages per partition.
        # If an ACK is lost and it retries, the broker recognises the sequence
        # number and does NOT write the message twice. This is exactly-once ON
        # THE PRODUCER SIDE, and it is free. Not to be confused with end-to-end
        # exactly-once (phase 16).
        "enable.idempotence": True,
        "retries": 5,
        "max.in.flight.requests.per.connection": 5,

        # ---------------------------------------------------------------
        # partitioner: CRITICAL AND LITTLE-KNOWN DETAIL
        # librdkafka's default partitioner (so confluent-kafka Python's) is
        # "consistent_random", based on CRC32. JAVA clients -- Spark included
        # -- use murmur2. With the defaults, a Python producer and a Spark
        # producer sending THE SAME key write to DIFFERENT partitions: ordering
        # by key breaks as soon as the two clients are mixed.
        # So we force Java compatibility.
        # ---------------------------------------------------------------
        "partitioner": "murmur2_random",

        # linger.ms: wait a few ms to group messages into one batch. 5 ms of
        # latency in exchange for much higher throughput.
        "linger.ms": 5,
        "batch.size": 64 * 1024,
        "compression.type": "snappy",
        "delivery.timeout.ms": 30000,
    }
    if extra:
        conf.update(extra)
    return Producer(conf)


def make_delivery_callback(stats: Stats, verbose: bool):
    """Callback librdkafka runs once the broker has ACKNOWLEDGED (or rejected).

    Crucial point: produce() is ASYNCHRONOUS. It drops the message into a local
    queue and returns immediately. Until this callback has run, we do NOT know
    whether the message made it into Kafka. That is why `produced` and
    `delivered` are counted separately.
    """

    def _on_delivery(err, msg):
        if err is not None:
            stats.failed += 1
            LOG.error("delivery FAILED: %s", err)
            return
        stats.delivered += 1
        part = msg.partition()
        stats.per_partition[part] = stats.per_partition.get(part, 0) + 1
        if verbose:
            key = msg.key().decode() if msg.key() else "<none>"
            LOG.info(
                "delivered  topic=%s partition=%s offset=%-6s key=%s",
                msg.topic(), part, msg.offset(), key,
            )

    return _on_delivery


def run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    generator = OrderGenerator(
        seed=args.seed,
        invalid_rate=args.invalid_rate,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        late_seconds=args.late_seconds,
        first_order_id=args.first_order_id,
    )
    stats = Stats()
    producer = build_producer(args.bootstrap)
    on_delivery = make_delivery_callback(stats, verbose=args.verbose)

    running = True

    def _stop(signum, frame):  # noqa: ARG001
        nonlocal running
        running = False
        LOG.info("stop requested: draining the send queue...")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    LOG.info(
        "starting | bootstrap=%s topic=%s rate=%s/s key=%s",
        args.bootstrap, args.topic, args.rate, args.key_field,
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    next_send = time.perf_counter()
    last_report = time.perf_counter()
    deadline = time.perf_counter() + args.duration if args.duration > 0 else None

    try:
        while running:
            if args.count > 0 and stats.produced >= args.count:
                break
            if deadline is not None and time.perf_counter() >= deadline:
                break

            event, kind = generator.next_event()

            # --- CHOOSING THE KEY --------------------------------------------
            if args.key_field == "none":
                key = None
            else:
                raw = event.get(args.key_field)
                key = str(raw).encode("utf-8") if raw is not None else None
            # ------------------------------------------------------------------

            try:
                producer.produce(
                    topic=args.topic,
                    key=key,
                    value=serialize(event),
                    on_delivery=on_delivery,
                    # Kafka timestamp = now (ingestion time). The BUSINESS
                    # timestamp stays in the payload: these are two different
                    # notions of time, and that is precisely what the watermark
                    # exploits (phase 13).
                )
                stats.produced += 1
                if kind == "invalid":
                    stats.invalid_injected += 1
                elif kind == "duplicate":
                    stats.duplicates_injected += 1
                elif kind == "late":
                    stats.late_injected += 1
            except BufferError:
                # Local queue full: the broker is not absorbing fast enough.
                # We do not drop the message, we wait for the queue to drain.
                LOG.warning("local queue full (backpressure): waiting...")
                producer.poll(0.5)
                continue
            except KafkaException as exc:
                LOG.error("Kafka error while producing: %s", exc)
                stats.failed += 1

            # poll() drives the internal machinery and fires the callbacks.
            # Without regular poll() calls, no callback ever runs.
            producer.poll(0)

            now = time.perf_counter()
            if now - last_report >= args.stats_every:
                _report(stats)
                last_report = now

            if interval > 0:
                next_send += interval
                sleep_for = next_send - time.perf_counter()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    # We are behind the requested rate: rather than catching up
                    # in a burst, we realign. The measured throughput will say
                    # whether the machine can take the load (phase 18).
                    next_send = time.perf_counter()
    finally:
        LOG.info("flush: waiting for the pending acknowledgements...")
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            LOG.error("%s message(s) NOT confirmed after 30s", remaining)
        _report(stats, final=True)

    return 0 if stats.failed == 0 else 1


def _report(stats: Stats, final: bool = False) -> None:
    parts = " ".join(f"P{p}={n}" for p, n in sorted(stats.per_partition.items()))
    prefix = "TOTAL " if final else "stats "
    LOG.info(
        "%s| produced=%d delivered=%d failed=%d | %.1f ev/s | %.1fs "
        "| invalid=%d duplicates=%d late=%d | %s",
        prefix, stats.produced, stats.delivered, stats.failed, stats.rate(),
        stats.elapsed(), stats.invalid_injected, stats.duplicates_injected,
        stats.late_injected, parts or "(no partition yet)",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="E-commerce order producer for Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP, help="broker address")
    p.add_argument("--topic", default=config.TOPIC_ORDERS, help="destination topic")
    p.add_argument("--rate", type=float, default=1.0,
                   help="events per second (0 = as fast as possible)")
    p.add_argument("--count", type=int, default=0, help="total number of events (0 = unlimited)")
    p.add_argument("--duration", type=float, default=0, help="duration in seconds (0 = unlimited)")
    p.add_argument("--key-field", choices=["customer_id", "order_id", "country", "none"],
                   default="customer_id", help="field used as the Kafka key")
    p.add_argument("--invalid-rate", type=float, default=0.0, help="share of invalid events")
    p.add_argument("--duplicate-rate", type=float, default=0.0, help="share of exact duplicates")
    p.add_argument("--late-rate", type=float, default=0.0, help="share of late events")
    p.add_argument("--late-seconds", type=int, default=900, help="maximum injected lateness (s)")
    p.add_argument("--first-order-id", type=int, default=1, help="first generated order_id")
    p.add_argument("--seed", type=int, default=None, help="random seed (reproducibility)")
    p.add_argument("--stats-every", type=float, default=5.0, help="stats interval (s)")
    p.add_argument("--verbose", action="store_true", help="log every delivery")
    p.add_argument("--quiet", action="store_true", help="only log errors")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
