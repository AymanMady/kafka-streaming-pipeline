#!/usr/bin/env bash
# =============================================================================
#  Topic creation - PHASE 2
#
#  IDEMPOTENT script: it can be re-run as often as you like, it never recreates
#  an existing topic and never loses data.
#
#  Why a script rather than auto.create.topics.enable=true?
#  Because an auto-created topic takes the broker's defaults. The partition
#  count is FROZEN at creation (you can raise it, never lower it, and raising
#  it breaks the key -> partition mapping). This is an architecture decision:
#  it has to be explicit and versioned in git.
#
#  Usage:  make topics
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

KCMD="docker compose exec -T kafka /opt/kafka/bin"
BOOTSTRAP="localhost:9092"

GREEN='\033[0;32m'; YELLOW='\033[0;33m'; BLUE='\033[0;36m'; NC='\033[0m'

# -----------------------------------------------------------------------------
#  Topic declarations: name | partitions | replication | retention (ms)
# -----------------------------------------------------------------------------
#  orders          : the main stream. 4 partitions -> up to 4 consumers running
#                    in parallel inside one group (phase 6).
#  orders-invalid  : the dead letter topic. Events rejected by Spark (negative
#                    amount, null order_id...) are routed here. 1 partition is
#                    enough: the volume is low and we want to be able to replay
#                    them in order while investigating.
#
#  Why NO "orders-valid" topic?
#  Because it would be a copy of "orders" minus the invalid rows: we would pay
#  a full network + disk round trip for information Spark computes in memory
#  with a single filter(). Only create a topic when ANOTHER application has to
#  consume the result independently.
#
#  Why NO "orders-analytics" topic?
#  Aggregates are a serving destination (SQL queries, dashboards), not an event
#  stream: they belong in PostgreSQL. We would add an analytics topic only if
#  another system had to react to those aggregates.
# -----------------------------------------------------------------------------
TOPICS=(
  "orders|4|1|604800000"
  "orders-invalid|1|1|604800000"
)

echo -e "${BLUE}--- Creating the topics ---${NC}"
for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name parts repl retention <<< "$spec"
  if $KCMD/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list 2>/dev/null | tr -d '\r' | grep -qx "$name"; then
    echo -e "  ${YELLOW}[SKIP]${NC} '$name' already exists (no data touched)"
  else
    $KCMD/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create \
      --topic "$name" \
      --partitions "$parts" \
      --replication-factor "$repl" \
      --config "retention.ms=$retention" >/dev/null 2>&1 \
      && echo -e "  ${GREEN}[OK]${NC}   '$name' created (${parts} partitions, replication ${repl})" \
      || echo -e "  [FAIL] could not create '$name'"
  fi
done

echo ""
echo -e "${BLUE}--- Topic anatomy ---${NC}"
for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name _ _ _ <<< "$spec"
  $KCMD/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic "$name" 2>/dev/null | tr -d '\r' | sed 's/^/  /'
  echo ""
done
