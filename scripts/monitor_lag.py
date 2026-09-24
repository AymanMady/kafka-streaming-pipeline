"""
PHASE 17 - Consumer lag: the health metric of a streaming pipeline.

    lag = (last offset produced) - (last offset committed by the group)

It is the only number that answers "are we keeping up?". Throughput on its own
does not: consuming 10 000 events/s while 12 000 are produced still means
falling behind, and no amount of waiting fixes it.

How to read it:
    lag stable near 0   -> healthy, consumption keeps up
    lag stable but high -> keeping up, but with a permanent delay (a backlog
                           that was never caught up)
    lag growing         -> THE alert. Consumption is slower than production.
    lag falling         -> catching up after an incident

    make lag
    make lag ARGS="--watch 5 --persist"

A caveat worth knowing: a Spark Structured Streaming query does NOT commit its
offsets to Kafka. It tracks them in its own checkpoint, which is exactly what
makes it independent of the broker. So it does not appear here unless it was
given a kafka.group.id. Spark's own lag is read from its Structured Streaming
tab, or from analytics.stream_metrics (this project persists it there).
"""

from __future__ import annotations

import argparse
import time

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from src.common import config


def _groups(admin: AdminClient) -> list[str]:
    result = admin.list_consumer_groups(request_timeout=10).result(timeout=15)
    return sorted(g.group_id for g in result.valid)


def _lag_for_group(bootstrap: str, group: str, topic_filter: str | None) -> list[dict]:
    """Committed offsets vs end offsets, one row per partition."""
    admin = AdminClient({"bootstrap.servers": bootstrap})

    futures = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group)])
    committed = futures[group].result(timeout=15).topic_partitions

    # A consumer is needed to read the high watermark: that information lives
    # on the partition, not in the group.
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{group}-lagprobe"})
    rows = []
    try:
        for tp in committed:
            if topic_filter and tp.topic != topic_filter:
                continue
            low, high = consumer.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition), timeout=10)
            # offset < 0 means "this group has never committed this partition".
            current = tp.offset if tp.offset >= 0 else None
            lag = (high - current) if current is not None else None
            rows.append({
                "group": group, "topic": tp.topic, "partition": tp.partition,
                "current": current, "end": high, "lag": lag,
            })
    finally:
        consumer.close()
    return sorted(rows, key=lambda r: (r["topic"], r["partition"]))


def _persist(rows: list[dict]) -> None:
    import psycopg2

    conn = psycopg2.connect(config.postgres_dsn())
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO analytics.consumer_lag "
                "(consumer_group, topic, partition, current_offset, end_offset, lag) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                [(r["group"], r["topic"], r["partition"], r["current"], r["end"], r["lag"])
                 for r in rows],
            )
    finally:
        conn.close()


def _show(rows: list[dict]) -> None:
    if not rows:
        print("  (no committed offset for this group)")
        return
    print(f"  {'group':<28} {'topic':<16} {'part':>4} {'current':>10} "
          f"{'end':>10} {'lag':>8}")
    print("  " + "-" * 80)
    total = 0
    for r in rows:
        lag = r["lag"]
        total += lag or 0
        print(f"  {r['group']:<28} {r['topic']:<16} {r['partition']:>4} "
              f"{str(r['current'] if r['current'] is not None else '-'):>10} "
              f"{r['end']:>10} {str(lag if lag is not None else '-'):>8}")
    print(f"  {'':<28} {'':<16} {'':>4} {'':>10} {'TOTAL':>10} {total:>8}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 17 - consumer lag")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--group", default=None, help="a single group (default: all of them)")
    p.add_argument("--topic", default=None, help="restrict to one topic")
    p.add_argument("--watch", type=float, default=0, help="refresh every N seconds")
    p.add_argument("--persist", action="store_true",
                   help="also write into analytics.consumer_lag")
    args = p.parse_args(argv)

    admin = AdminClient({"bootstrap.servers": args.bootstrap})

    while True:
        groups = [args.group] if args.group else _groups(admin)
        # The probe consumers this script creates would otherwise show up in
        # its own output at the next refresh.
        groups = [g for g in groups if not g.endswith("-lagprobe")]

        rows: list[dict] = []
        for group in groups:
            try:
                rows.extend(_lag_for_group(args.bootstrap, group, args.topic))
            except Exception as exc:  # noqa: BLE001
                print(f"  {group}: unreadable ({exc})")

        print(f"\n  --- consumer lag @ {time.strftime('%H:%M:%S')} ---")
        _show(rows)
        if args.persist and rows:
            _persist(rows)
            print(f"  ({len(rows)} row(s) written to analytics.consumer_lag)")

        if not args.watch:
            return 0
        time.sleep(args.watch)


if __name__ == "__main__":
    raise SystemExit(main())
