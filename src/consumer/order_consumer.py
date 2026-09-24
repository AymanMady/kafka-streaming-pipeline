#!/usr/bin/env python3
"""
Kafka consumer: reads the `orders` topic, deserialises, validates, prints (or persists).

This file backs several phases:
  phase 4  : reading messages
  phase 6  : consumer groups and rebalancing (run several instances)
  phase 7  : offsets, restart, replay
  phase 15 : writing to PostgreSQL (--sink postgres)
  phase 17 : consumer lag

------------------------------------------------------------------------------
Consumer group: the central idea
------------------------------------------------------------------------------
A consumer never subscribes alone: it declares a GROUP (`group.id`). Kafka then
guarantees that, WITHIN THAT GROUP, each partition is assigned to exactly ONE
consumer.

    Topic orders (4 partitions)
        P0  P1  P2  P3
         |   |   |   |
         +---+---+---+
               |
      group "order-processing-group"
         /     |     \\
    consumer consumer consumer      <- Kafka spreads the 4 partitions
       A        B        C             across A, B and C

Two consequences worth remembering:
  1. Adding consumers beyond the partition count buys you NOTHING: the 5th
     consumer of a group on a 4-partition topic stays idle. A group's maximum
     parallelism = the number of partitions.
  2. Two DIFFERENT groups each receive ALL the messages, with their own
     offsets. That is how Spark and this consumer will read the same topic
     without interfering.

------------------------------------------------------------------------------
Offsets and commits: at-least-once by default
------------------------------------------------------------------------------
The offset is a message's position within a partition (0, 1, 2, ...). Kafka
remembers, for each (group, partition), the last COMMITTED offset in an
internal topic: __consumer_offsets. That is what lets a consumer resume exactly
where it stopped after a restart.

Here auto-commit is disabled and we commit AFTER processing:

    read -> process -> commit

If the process dies between "process" and "commit", the message is read again
on restart: it gets processed TWICE. That is AT-LEAST-ONCE: nothing is lost,
things may be duplicated. The opposite (commit before processing) gives
at-most-once: nothing is duplicated, things may be lost.

There is no magic third option on the consumer side: exactly-once is obtained
by making the PROCESSING idempotent (phase 16).

------------------------------------------------------------------------------
Usage
------------------------------------------------------------------------------
    python src/consumer/order_consumer.py
    python src/consumer/order_consumer.py --name B          # 2nd instance of the group
    python src/consumer/order_consumer.py --group audit --from-beginning
    python src/consumer/order_consumer.py --sink postgres   # phase 15
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition  # noqa: E402

from src.common import config  # noqa: E402
from src.common.schemas import deserialize, validate_event  # noqa: E402

LOG = logging.getLogger("consumer")


@dataclass
class Stats:
    received: int = 0
    valid: int = 0
    invalid: int = 0
    undecodable: int = 0
    committed: int = 0
    errors: int = 0
    started_at: float = field(default_factory=time.perf_counter)
    per_partition: dict[int, int] = field(default_factory=dict)

    def elapsed(self) -> float:
        return max(time.perf_counter() - self.started_at, 1e-9)


# ---------------------------------------------------------------------------
# Sinks: where the data goes once it has been read
# ---------------------------------------------------------------------------
class StdoutSink:
    """Prints the event. The simplest sink: useful to understand what happens."""

    def __init__(self, verbose: bool = True) -> None:
        self.verbose = verbose

    def write(self, event: dict[str, Any], meta: dict[str, Any],
              valid: bool, reasons: list[str]) -> None:
        if not self.verbose:
            return
        flag = "OK " if valid else "BAD"
        LOG.info(
            "%s p%-2s off=%-7s key=%-6s order=%-7s %-10s %8.2f %-4s %s",
            flag, meta["partition"], meta["offset"], meta["key"],
            event.get("order_id"), event.get("country"),
            event.get("amount") if isinstance(event.get("amount"), int | float) else 0.0,
            event.get("currency"),
            ("rejected=" + ",".join(reasons)) if reasons else "",
        )

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class PostgresSink:
    """Writes the raw orders into raw.orders (phase 15).

    Idempotency: the primary key is event_id and the insert uses
    ON CONFLICT DO NOTHING. If the consumer restarts and re-reads messages it
    already processed (at-least-once), the re-inserts are silently ignored. The
    end result is the same as if every message had been processed exactly once:
    the EXACTLY-ONCE effect, obtained without exactly-once.
    """

    INSERT = """
        INSERT INTO raw.orders (
            event_id, event_type, order_id, customer_id, product_id,
            quantity, amount, currency, country, event_timestamp,
            kafka_partition, kafka_offset, is_valid, rejection_reasons
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (event_id) DO NOTHING
    """

    def __init__(self, batch_size: int = 100) -> None:
        import psycopg2  # local import: psycopg2 is only needed by this sink

        self.conn = psycopg2.connect(config.postgres_dsn())
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.batch_size = batch_size
        self.buffer: list[tuple] = []
        self.written = 0
        self.ignored_duplicates = 0

    def write(self, event: dict[str, Any], meta: dict[str, Any],
              valid: bool, reasons: list[str]) -> None:
        self.buffer.append((
            event.get("event_id"), event.get("event_type"), event.get("order_id"),
            event.get("customer_id"), event.get("product_id"), event.get("quantity"),
            event.get("amount"), event.get("currency"), event.get("country"),
            event.get("timestamp"), meta["partition"], meta["offset"],
            valid, reasons or None,
        ))
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        for row in self.buffer:
            # Missing event_id (invalid event) -> deduplication is impossible,
            # so we build a technical key to keep the trace.
            if row[0] is None:
                row = (f"noid_p{row[10]}_o{row[11]}",) + row[1:]
            try:
                self.cur.execute(self.INSERT, row)
                if self.cur.rowcount == 0:
                    self.ignored_duplicates += 1
                else:
                    self.written += 1
            except Exception as exc:  # poison row: log it without killing the stream
                LOG.error("insert failed (%s): %s", row[0], exc)
                self.conn.rollback()
        self.conn.commit()
        self.buffer.clear()

    def close(self) -> None:
        self.flush()
        self.cur.close()
        self.conn.close()


def build_consumer(args: argparse.Namespace) -> Consumer:
    conf: dict[str, Any] = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "client.id": f"order-consumer-{args.name}",

        # auto.offset.reset: what to do when the GROUP has NO committed offset
        #   - "latest"   : only read what arrives after connecting
        #   - "earliest" : start from the very beginning of the topic (full replay)
        # This setting only applies the FIRST time: afterwards the group's
        # committed offset wins.
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",

        # Manual commit: we decide ourselves WHEN an offset becomes valid
        "enable.auto.commit": args.auto_commit,

        # Failure detection: if the consumer goes quiet for session.timeout.ms,
        # the group declares it dead and redistributes its partitions
        # (rebalancing). Too short -> false positives under load.
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,

        # max.poll.interval.ms: maximum time allowed BETWEEN two poll() calls.
        # If processing a batch takes longer than this, Kafka kicks the
        # consumer out. This is the number one cause of spurious rebalancings
        # in production.
        "max.poll.interval.ms": 300000,

        # Cooperative sticky: during a rebalance, only the partitions that
        # actually move are revoked. The older "eager" strategy stopped EVERY
        # consumer in the group on every change.
        "partition.assignment.strategy": "cooperative-sticky",
    }
    return Consumer(conf)


def run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)-7s [{args.name}] | %(message)s",
        datefmt="%H:%M:%S",
    )

    stats = Stats()
    consumer = build_consumer(args)
    sink = PostgresSink() if args.sink == "postgres" else StdoutSink(verbose=not args.quiet)

    # --- Rebalance callbacks: this is where you SEE Kafka move partitions
    def on_assign(_consumer, partitions: list[TopicPartition]) -> None:
        LOG.warning(
            "REBALANCE - partitions ASSIGNED: %s",
            sorted(p.partition for p in partitions) or "(none)",
        )

    def on_revoke(_consumer, partitions: list[TopicPartition]) -> None:
        LOG.warning(
            "REBALANCE - partitions REVOKED : %s",
            sorted(p.partition for p in partitions) or "(none)",
        )
        # Commit before letting the partitions go: otherwise the consumer that
        # picks them up would re-read messages that were already processed.
        if not args.auto_commit:
            # Nothing to commit, or the group already rebalanced away.
            with contextlib.suppress(KafkaException):
                _consumer.commit(asynchronous=False)

    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)

    running = True

    def _stop(signum, frame):  # noqa: ARG001
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    LOG.info(
        "starting | group=%s topic=%s from=%s sink=%s",
        args.group, args.topic,
        "beginning" if args.from_beginning else "end", args.sink,
    )

    uncommitted = 0
    last_report = time.perf_counter()

    try:
        while running:
            if args.max_messages > 0 and stats.received >= args.max_messages:
                LOG.info("limit of %d messages reached", args.max_messages)
                break

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                LOG.error("Kafka error: %s", msg.error())
                stats.errors += 1
                continue

            stats.received += 1
            stats.per_partition[msg.partition()] = stats.per_partition.get(msg.partition(), 0) + 1

            meta = {
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8", "replace") if msg.key() else "<none>",
                "kafka_ts": msg.timestamp()[1],
            }

            # --- Deserialisation: an unreadable message must NEVER kill the stream
            try:
                event = deserialize(msg.value())
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                stats.undecodable += 1
                LOG.error("unreadable message p%s off=%s: %s",
                          meta["partition"], meta["offset"], exc)
                uncommitted += 1
                continue

            valid, reasons = validate_event(event)
            stats.valid += valid
            stats.invalid += (not valid)

            if args.slow_ms > 0:
                time.sleep(args.slow_ms / 1000.0)

            sink.write(event, meta, valid, reasons)
            uncommitted += 1

            # --- COMMIT: after processing -> at-least-once
            if not args.auto_commit and uncommitted >= args.commit_every:
                sink.flush()
                consumer.commit(asynchronous=False)
                stats.committed += uncommitted
                uncommitted = 0

            if time.perf_counter() - last_report >= args.stats_every:
                _report(stats)
                last_report = time.perf_counter()
    except KafkaException as exc:
        LOG.error("Kafka exception: %s", exc)
        return 1
    finally:
        try:
            sink.flush()
            if not args.auto_commit and uncommitted > 0:
                consumer.commit(asynchronous=False)
                stats.committed += uncommitted
        except KafkaException as exc:
            LOG.error("final commit failed: %s", exc)
        _report(stats, final=True)
        if isinstance(sink, PostgresSink):
            LOG.info("postgres: %d rows written, %d duplicates ignored",
                     sink.written, sink.ignored_duplicates)
        sink.close()
        # close() leaves the group cleanly -> immediate rebalance instead of
        # waiting for session.timeout.ms to expire.
        consumer.close()

    return 0


def _report(stats: Stats, final: bool = False) -> None:
    parts = " ".join(f"P{p}={n}" for p, n in sorted(stats.per_partition.items()))
    LOG.info(
        "%s| received=%d valid=%d invalid=%d unreadable=%d committed=%d | %.1f msg/s | %s",
        "TOTAL " if final else "stats ",
        stats.received, stats.valid, stats.invalid, stats.undecodable,
        stats.committed, stats.received / stats.elapsed(), parts or "(none)",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Order consumer reading from Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--group", default=config.CONSUMER_GROUP, help="consumer group")
    p.add_argument("--name", default="A", help="instance name (used in the logs)")
    p.add_argument("--from-beginning", action="store_true",
                   help="if the group has no offset: start from the beginning")
    p.add_argument("--auto-commit", action="store_true",
                   help="let Kafka commit periodically (less safe)")
    p.add_argument("--commit-every", type=int, default=10, help="commit every N messages")
    p.add_argument("--max-messages", type=int, default=0,
                   help="stop after N messages (0 = unlimited)")
    p.add_argument("--slow-ms", type=float, default=0, help="simulate slow processing (ms/message)")
    p.add_argument("--sink", choices=["stdout", "postgres"], default="stdout")
    p.add_argument("--stats-every", type=float, default=10.0)
    p.add_argument("--quiet", action="store_true", help="do not print every message")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
