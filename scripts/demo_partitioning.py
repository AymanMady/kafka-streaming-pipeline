#!/usr/bin/env python3
"""
PHASE 5 - Demo: message key -> partition.

This script does not explain, it COMPUTES the expected partition with Kafka's
own algorithm, then actually PRODUCES the messages and compares against the
partition the broker really assigned.

    python scripts/demo_partitioning.py
"""

from __future__ import annotations

import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common import config
from src.common.schemas import make_order_event, serialize
from src.producer.order_producer import build_producer

TOPIC = config.TOPIC_ORDERS
N_PARTITIONS = 4


# ---------------------------------------------------------------------------
# Kafka's exact algorithm (org.apache.kafka.common.utils.Utils.murmur2)
# ---------------------------------------------------------------------------
def murmur2(data: bytes) -> int:
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        i4 = i * 4
        k = (data[i4] | (data[i4 + 1] << 8) | (data[i4 + 2] << 16)
             | (data[i4 + 3] << 24)) & 0xFFFFFFFF
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = length & ~3
    rem = length % 4
    if rem == 3:
        h ^= (data[tail + 2] & 0xFF) << 16
    if rem >= 2:
        h ^= (data[tail + 1] & 0xFF) << 8
    if rem >= 1:
        h ^= data[tail] & 0xFF
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h & 0xFFFFFFFF


def partition_for(key: str, n_partitions: int = N_PARTITIONS) -> int:
    """partition = toPositive(murmur2(key)) % number_of_partitions"""
    return (murmur2(key.encode()) & 0x7FFFFFFF) % n_partitions


def section(title: str) -> None:
    print(f"\n\033[0;36m{'=' * 74}\n  {title}\n{'=' * 74}\033[0m")


def main() -> int:
    # ---------------------------------------------------------------------
    section("1. PREDICTION: which partition for which key?")
    # ---------------------------------------------------------------------
    print("  key (customer_id)  ->  computed partition")
    for customer in [1, 2, 3, 42, 45, 100, 165, 999]:
        print(f"      {str(customer):<6}         ->  P{partition_for(str(customer))}")
    print("\n  The same key ALWAYS gives the same partition: it is deterministic,")
    print("  needs no coordination and no network call. The producer computes it alone.")

    # ---------------------------------------------------------------------
    section("2. VERIFICATION: does the broker really put the messages there?")
    # ---------------------------------------------------------------------
    producer = build_producer(config.KAFKA_BOOTSTRAP)
    observed: dict[str, int] = {}
    mismatches = 0

    def on_delivery(err, msg):
        nonlocal mismatches
        if err:
            print(f"  FAILED: {err}")
            return
        key = msg.key().decode()
        observed[key] = msg.partition()
        expected = partition_for(key)
        status = "OK" if expected == msg.partition() else "MISMATCH"
        if expected != msg.partition():
            mismatches += 1
        print(f"      key={key:<6} predicted=P{expected}  actual=P{msg.partition()}"
              f"  offset={msg.offset():<5} {status}")

    test_keys = [1, 2, 3, 42, 45, 100, 165, 999]
    for customer in test_keys:
        event = make_order_event(
            order_id=900000 + customer, customer_id=customer, product_id=701,
            quantity=1, amount=100.0, country="Mauritania",
        )
        producer.produce(TOPIC, key=str(customer).encode(), value=serialize(event),
                         on_delivery=on_delivery)
    producer.flush(15)
    print(f"\n  Mismatches: {mismatches}/{len(test_keys)}")

    # ---------------------------------------------------------------------
    section("3. SAME KEY, SEVERAL MESSAGES: is ordering guaranteed?")
    # ---------------------------------------------------------------------
    trace: list[tuple[int, int]] = []

    def on_delivery_seq(err, msg):
        if not err:
            trace.append((msg.partition(), msg.offset()))

    print("  Sending 6 orders from the SAME customer (customer_id=45):")
    for i in range(6):
        event = make_order_event(
            order_id=910000 + i, customer_id=45, product_id=702,
            quantity=1, amount=50.0 + i, country="Mauritania",
        )
        producer.produce(TOPIC, key=b"45", value=serialize(event), on_delivery=on_delivery_seq)
    producer.flush(15)
    partitions = {p for p, _ in trace}
    offsets = [o for _, o in trace]
    print(f"      partitions used    : {sorted(partitions)}")
    print(f"      offsets            : {offsets}")
    print(f"      offsets increasing : {offsets == sorted(offsets)}")
    print("\n  A single partition -> the offsets follow each other -> the production")
    print("  order is preserved on read. That is THE Kafka guarantee.")

    # ---------------------------------------------------------------------
    section("4. DISTRIBUTION depending on the key you pick (300 messages)")
    # ---------------------------------------------------------------------
    scenarios = {
        "customer_id (200 customers)": lambda i: str(i % 200),
        "order_id (all unique)": lambda i: str(1_000_000 + i),
        "country (5 values)": lambda i: ["Mauritania"] * 45 + ["Senegal"] * 20 +
                                        ["Mali"] * 12 + ["Morocco"] * 13 + ["France"] * 10,
        "no key - burst": None,
        "no key - spaced out": "SPACED",
    }

    for label, keyfn in scenarios.items():
        counts: Counter[int] = Counter()
        pending = {"n": 0}

        # counts/pending are bound as defaults, not captured: a callback
        # firing after the loop has moved on would otherwise credit the
        # NEXT scenario's counter. The flush below makes that impossible
        # today, but the binding should not depend on that.
        def cb(err, msg, counts=counts, pending=pending):
            if not err:
                counts[msg.partition()] += 1
            pending["n"] += 1

        n = 300 if keyfn != "SPACED" else 40
        for i in range(n):
            event = make_order_event(
                order_id=920000 + i, customer_id=i % 200, product_id=703,
                quantity=1, amount=10.0, country="Mauritania",
            )
            if keyfn is None or keyfn == "SPACED":
                key = None
            elif label.startswith("country"):
                pool = keyfn(i)
                key = pool[i % len(pool)].encode()
            else:
                key = keyfn(i).encode()
            producer.produce(TOPIC, key=key, value=serialize(event), on_delivery=cb)
            if keyfn == "SPACED":
                # Force an immediate send and let sticky.partitioning.linger.ms
                # (10 ms by default) expire: the "sticky" partition is then
                # picked again at random.
                producer.flush(5)
                time.sleep(0.015)
        producer.flush(20)

        total = sum(counts.values()) or 1
        bars = "  ".join(
            f"P{p}={counts.get(p,0):>3} ({100*counts.get(p,0)/total:4.1f}%)"
            for p in range(N_PARTITIONS)
        )
        print(f"  {label:<28} {bars}")

    print("""
  How to read this table:
    - customer_id : correct spread AND per-customer ordering guaranteed.
    - order_id    : perfect spread, but two orders from the same customer land
                    in different partitions -> no ordering at all.
    - country     : Mauritania is 45% of the traffic and always falls into the
                    same partition -> HOT PARTITION. Adding consumers will not
                    help: one partition = one consumer.
    - no key (burst)   : EVERYTHING goes to a SINGLE partition. This is not a
                    bug: it is STICKY PARTITIONING (KIP-480). Without a key the
                    client sticks messages to one partition for
                    sticky.partitioning.linger.ms (10 ms) in order to build
                    large batches -- one large batch is far cheaper than four
                    small ones. The partition changes afterwards.
    - no key (spaced)  : by letting that delay expire between two sends, the
                    spread across the four partitions comes back.
                    Conclusion: "no key = evenly spread" is only true at the
                    scale of a MINUTE, not of a millisecond.

  Rule: choosing the key means choosing WHAT IS ORDERED
        and WHAT CAN BE PARALLELISED. It is a business decision.
""")
    return 0 if mismatches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
