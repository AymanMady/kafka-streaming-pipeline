#!/usr/bin/env python3
"""
PHASE 6 - Consumer groups: a real experiment, not an explanation.

It starts REAL consumers as subprocesses, reads what Kafka assigns to them, and
counts what each one receives.

Scenarios:
    1 partition  + 1 consumer
    4 partitions + 1 consumer
    4 partitions + 4 consumers
    4 partitions + 6 consumers      <- the point to understand
    rebalancing: 2 consumers, then a 3rd joins

    python scripts/demo_consumer_groups.py
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Prefer the project venv, fall back to the interpreter running this script so
# the demo also works on a fresh clone without a .venv.
_VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
PYTHON = str(_VENV_PYTHON) if _VENV_PYTHON.exists() else sys.executable
CONSUMER = str(ROOT / "src" / "consumer" / "order_consumer.py")
PRODUCER = str(ROOT / "src" / "producer" / "order_producer.py")

# These must stay in sync with the consumer's rebalance log lines.
ASSIGN_RE = re.compile(r"partitions ASSIGNED\s*:\s*\[([^\]]*)\]")
REVOKE_RE = re.compile(r"partitions REVOKED\s*:\s*\[([^\]]*)\]")
TOTAL_RE = re.compile(r"received=(\d+)")


def section(title: str) -> None:
    print(f"\n\033[0;36m{'=' * 76}\n  {title}\n{'=' * 76}\033[0m")


def start_consumer(name: str, group: str, topic: str) -> subprocess.Popen:
    return subprocess.Popen(
        [PYTHON, CONSUMER, "--name", name, "--group", group, "--topic", topic,
         "--from-beginning", "--quiet", "--stats-every", "3600"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(ROOT),
    )


def stop(proc: subprocess.Popen) -> str:
    proc.terminate()
    try:
        out, _ = proc.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    return out or ""


def summarize(name: str, output: str) -> tuple[str, str, int]:
    # cooperative-sticky: assignments are INCREMENTAL, so they accumulate
    owned: set[str] = set()
    for chunk in ASSIGN_RE.findall(output):
        owned |= {c.strip() for c in chunk.split(",") if c.strip()}
    for chunk in REVOKE_RE.findall(output):
        owned -= {c.strip() for c in chunk.split(",") if c.strip()}
    received = TOTAL_RE.findall(output)
    n = int(received[-1]) if received else 0
    return name, ",".join(sorted(owned, key=lambda x: int(x))) or "(none)", n


def scenario(title: str, topic: str, n_consumers: int, produce: int, wait: float = 12.0) -> None:
    section(title)
    group = f"demo-{uuid.uuid4().hex[:8]}"
    print(f"  group: {group}   topic: {topic}   consumers: {n_consumers}")

    if produce:
        subprocess.run(
            [PYTHON, PRODUCER, "--topic", topic, "--rate", "0", "--count", str(produce), "--quiet"],
            cwd=str(ROOT), check=False, capture_output=True, timeout=120,
        )
        print(f"  {produce} messages produced into {topic}")

    procs = [(chr(65 + i), start_consumer(chr(65 + i), group, topic)) for i in range(n_consumers)]
    time.sleep(wait)

    print(f"\n  {'consumer':<10}{'partitions assigned':<26}{'messages received'}")
    print(f"  {'-'*10}{'-'*26}{'-'*17}")
    idle = 0
    for name, proc in procs:
        label, parts, count = summarize(name, stop(proc))
        if parts == "(none)":
            idle += 1
        print(f"  {label:<10}{parts:<26}{count}")
    if idle:
        print(f"\n  -> {idle} consumer(s) with NO PARTITION AT ALL: they are idle.")


def rebalancing_demo() -> None:
    section("5. REBALANCING: a consumer joins the group mid-flight")
    group = f"demo-{uuid.uuid4().hex[:8]}"
    print(f"  group: {group}   topic: orders   (4 partitions)\n")

    print("  t+0s  : starting A and B")
    a = start_consumer("A", group, "orders")
    b = start_consumer("B", group, "orders")
    time.sleep(10)

    print("  t+10s : C joins the group -> Kafka MUST redistribute")
    c = start_consumer("C", group, "orders")
    time.sleep(12)

    print("  t+22s : stopping B -> another redistribution\n")
    out_b = stop(b)
    time.sleep(10)

    out_a, out_c = stop(a), stop(c)

    for name, out in (("A", out_a), ("B", out_b), ("C", out_c)):
        print(f"  --- consumer {name}: timeline of the assignments ---")
        events = []
        for line in out.splitlines():
            if "ASSIGNED" in line:
                events.append("    + assigned: [" + (ASSIGN_RE.search(line).group(1) or "") + "]")
            elif "REVOKED" in line:
                events.append("    - revoked : [" + (REVOKE_RE.search(line).group(1) or "") + "]")
        print("\n".join(events) if events else "    (no event)")
        print()

    print("""  What to remember about rebalancing:
    - it is triggered by ANY change in the group's membership: a join, a clean
      leave, a crash (detected after session.timeout.ms), or a processing step
      slow enough to blow past max.poll.interval.ms;
    - during a rebalance, consumption of the affected partitions is SUSPENDED:
      that is a processing pause, so lag builds up;
    - with the historical "eager" strategy, the WHOLE group stopped on every
      change. With "cooperative-sticky" (used here), only the partitions that
      actually move are revoked;
    - a group that rebalances in a loop is almost always the symptom of
      processing that is too slow between two poll() calls, not of a network
      problem.""")


def main() -> int:
    scenario("1. ONE partition, ONE consumer", "orders-invalid", 1, produce=50)
    scenario("2. FOUR partitions, ONE consumer", "orders", 1, produce=200)
    scenario("3. FOUR partitions, FOUR consumers", "orders", 4, produce=200)
    scenario("4. FOUR partitions, SIX consumers", "orders", 6, produce=200)
    rebalancing_demo()

    print("""
\033[0;36m==============================================================================
  CONCLUSION OF PHASE 6
==============================================================================\033[0m
  Inside a consumer group, a partition is assigned to EXACTLY ONE consumer. A
  group's maximum parallelism is therefore capped by the topic's partition
  count. The extra consumers are not "a bit slower": they receive NOTHING.

  They are not useless for all that: they are STANDBY consumers. If an active
  consumer dies, rebalancing hands them its partitions within seconds.

  To genuinely increase parallelism you have to increase the number of
  PARTITIONS -- a decision to take when the topic is created, because adding
  partitions later breaks the key -> partition mapping, and therefore ordering.
""")
    return 0


if __name__ == "__main__":
    sys.exit(main())
