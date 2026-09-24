"""
PHASE 18 - Load test: where does this pipeline actually saturate?

A streaming pipeline is not characterised by "it works" but by two numbers:
the throughput it sustains, and the latency at which it sustains it. This
script measures both, on the machine that runs it. No number here is an
estimate.

What it measures:
  - ACHIEVED throughput vs the target rate (they diverge at saturation);
  - the produce -> broker acknowledgement latency, as percentiles;
  - the failure count.

Why percentiles and not an average: an average hides the tail. A pipeline with
a 5 ms average and a 3 s p99 drops one request in a hundred into a timeout,
and the average will never tell you.

    make load-test
    make load-test COUNT=200000 ARGS="--steps 1000,5000,20000,0"
"""

from __future__ import annotations

import argparse
import statistics
import time

from confluent_kafka import Producer

from src.common import config
from src.common.schemas import make_order_event, serialize

COUNTRIES = ["Mauritania", "Senegal", "Morocco", "Mali", "France"]


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(int(round(pct / 100.0 * len(ordered) + 0.5)) - 1, len(ordered) - 1)
    return ordered[max(index, 0)]


def _run_step(bootstrap: str, topic: str, count: int, rate: float) -> dict:
    """Produce `count` events at `rate`/s (0 = as fast as possible)."""
    latencies: list[float] = []
    failures = 0

    def _callback_for(sent_at: float):
        """One closure per message: it is what carries the send time.

        confluent-kafka's produce() takes no correlation token, and the
        delivery callback only receives (err, msg). The closure is therefore
        the simplest way to time an individual acknowledgement.
        """
        def _on_delivery(err, _msg):
            nonlocal failures
            if err is not None:
                failures += 1
            else:
                latencies.append((time.perf_counter() - sent_at) * 1000.0)
        return _on_delivery

    producer = Producer({
        "bootstrap.servers": bootstrap,
        # A load test must push the client, not throttle it artificially.
        "linger.ms": 5,
        "batch.size": 65536,
        "compression.type": "lz4",
        "acks": "1",
        "queue.buffering.max.messages": 500_000,
    })

    interval = (1.0 / rate) if rate > 0 else 0.0
    started = time.perf_counter()
    next_at = started

    for i in range(count):
        event = make_order_event(
            order_id=i + 1, customer_id=(i % 500) + 1, product_id=(i % 200) + 700,
            quantity=(i % 5) + 1, amount=round(10 + (i % 1000) * 1.5, 2),
            country=COUNTRIES[i % len(COUNTRIES)],
        )
        on_delivery = _callback_for(time.perf_counter())
        while True:
            try:
                producer.produce(topic, key=str(event["customer_id"]),
                                 value=serialize(event), on_delivery=on_delivery)
                break
            except BufferError:
                # The local queue is full: the client is producing faster than
                # the broker accepts. Serving the callbacks drains it.
                producer.poll(0.1)

        producer.poll(0)
        if interval:
            next_at += interval
            sleep_for = next_at - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)

    producer.flush(120)
    elapsed = time.perf_counter() - started

    return {
        "target_rate": rate,
        "count": count,
        "elapsed": elapsed,
        "achieved": count / elapsed if elapsed else 0.0,
        "failures": failures,
        "p50": _percentile(latencies, 50),
        "p95": _percentile(latencies, 95),
        "p99": _percentile(latencies, 99),
        "mean": statistics.fmean(latencies) if latencies else 0.0,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Phase 18 - load test")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=config.TOPIC_ORDERS)
    p.add_argument("--count", type=int, default=50_000, help="events per step")
    p.add_argument("--rate", type=float, default=0, help="single target rate (0 = max)")
    p.add_argument("--steps", default=None,
                   help="comma-separated rates, e.g. '1000,5000,20000,0'")
    args = p.parse_args(argv)

    rates = ([float(x) for x in args.steps.split(",")] if args.steps
             else [args.rate])

    print(f"\n  Load test on {args.bootstrap}, topic {args.topic}")
    print(f"  {args.count:,} events per step | 0 = as fast as possible\n")

    results = []
    for rate in rates:
        label = "max" if rate == 0 else f"{rate:,.0f}/s"
        print(f"  step {label} ...", flush=True)
        results.append(_run_step(args.bootstrap, args.topic, args.count, rate))
        # Let the broker breathe between steps, otherwise a step inherits the
        # queueing of the previous one and the measurement is meaningless.
        time.sleep(3)

    print(f"\n  {'target':>10} {'achieved':>12} {'elapsed':>9} {'p50':>8} "
          f"{'p95':>8} {'p99':>8} {'failed':>7}")
    print("  " + "-" * 70)
    for r in results:
        target = "max" if r["target_rate"] == 0 else f"{r['target_rate']:,.0f}/s"
        print(f"  {target:>10} {r['achieved']:>10,.0f}/s {r['elapsed']:>8.1f}s "
              f"{r['p50']:>7.1f}ms {r['p95']:>7.1f}ms {r['p99']:>7.1f}ms "
              f"{r['failures']:>7}")

    print("\n  How to read it: as long as the achieved rate tracks the target,")
    print("  the pipeline is keeping up. The step where they diverge - and")
    print("  where p99 climbs - is the saturation point. Check `make lag`")
    print("  right after: the producer can saturate long before the consumer.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
