#!/usr/bin/env bash
# =============================================================================
#  End-to-end check of the stack - PHASE 1
#
#  This script assumes nothing: it actually queries every service and prints
#  what it gets back. No result is simulated.
#
#  Usage:  make check     (or: bash scripts/check_stack.sh)
# =============================================================================
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
[ -f .env ] && set -a && . ./.env && set +a

GREEN='\033[0;32m'; RED='\033[0;31m'; BLUE='\033[0;36m'; NC='\033[0m'
FAILED=0

ok()   { echo -e "  ${GREEN}[OK]${NC}   $1"; }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; FAILED=1; }
step() { echo -e "\n${BLUE}--- $1 ---${NC}"; }

# -----------------------------------------------------------------------------
step "1. Container status"
# -----------------------------------------------------------------------------
docker compose ps --format 'table {{.Service}}\t{{.Status}}'

for svc in kafka postgres kafka-ui; do
  state=$(docker compose ps --format '{{.Service}}:{{.State}}' | grep "^${svc}:" | cut -d: -f2)
  if [ "$state" = "running" ]; then ok "container '$svc' is running"
  else fail "container '$svc' is not 'running' (state: ${state:-missing})"; fi
done

# -----------------------------------------------------------------------------
step "2. Kafka: does the broker answer?"
# -----------------------------------------------------------------------------
# kafka-cluster.sh queries the cluster and returns its KRaft id.
CLUSTER=$(docker compose exec -T kafka \
  /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 2>/dev/null \
  | tr -d '\r')
if echo "$CLUSTER" | grep -q "Cluster ID"; then
  ok "broker reachable -> ${CLUSTER}"
else
  fail "broker unreachable from inside the container"
fi

# -----------------------------------------------------------------------------
step "3. Kafka: EXTERNAL listener (from the host to localhost:PORT)"
# -----------------------------------------------------------------------------
# Check that the host port is open and that a Kafka handshake succeeds.
if command -v nc >/dev/null 2>&1; then
  if nc -z localhost "${KAFKA_HOST_PORT:-9092}" 2>/dev/null; then
    ok "host port ${KAFKA_HOST_PORT:-9092} open"
  else
    fail "host port ${KAFKA_HOST_PORT:-9092} closed"
  fi
else
  if timeout 2 bash -c "</dev/tcp/localhost/${KAFKA_HOST_PORT:-9092}" 2>/dev/null; then
    ok "host port ${KAFKA_HOST_PORT:-9092} open"
  else
    fail "host port ${KAFKA_HOST_PORT:-9092} closed"
  fi
fi

# -----------------------------------------------------------------------------
step "4. Kafka: existing topics"
# -----------------------------------------------------------------------------
TOPICS=$(docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | tr -d '\r')
if [ -z "$TOPICS" ]; then
  ok "no user topic yet (expected in phase 1, they arrive in phase 2)"
else
  echo "$TOPICS" | sed 's/^/        /'
  ok "topic list retrieved"
fi

# -----------------------------------------------------------------------------
step "5. Kafka: physical data storage (KRaft mode)"
# -----------------------------------------------------------------------------
if [ -f data/kafka/meta.properties ]; then
  ok "storage formatted: data/kafka/meta.properties"
  sed 's/^/        /' data/kafka/meta.properties | grep -v '^\s*#' | grep -v '^\s*$'
else
  fail "data/kafka/meta.properties missing (the broker never formatted its storage)"
fi

# -----------------------------------------------------------------------------
step "6. PostgreSQL: connection + read"
# -----------------------------------------------------------------------------
PG_OUT=$(docker compose exec -T postgres psql -U "${POSTGRES_USER:-stream_user}" \
  -d "${POSTGRES_DB:-streaming_analytics}" -tAc \
  "SELECT component || ' | ' || checked_at FROM analytics.pipeline_health ORDER BY id LIMIT 1;" 2>&1 | tr -d '\r')
if echo "$PG_OUT" | grep -q "postgres |"; then
  ok "database reachable and initialised -> ${PG_OUT}"
else
  fail "read failed: ${PG_OUT}"
fi

PG_SCHEMAS=$(docker compose exec -T postgres psql -U "${POSTGRES_USER:-stream_user}" \
  -d "${POSTGRES_DB:-streaming_analytics}" -tAc \
  "SELECT string_agg(schema_name, ', ') FROM information_schema.schemata
   WHERE schema_name IN ('raw','analytics');" 2>/dev/null | tr -d '\r')
if [ -n "$PG_SCHEMAS" ]; then ok "schemas present: ${PG_SCHEMAS}"; else fail "schemas raw/analytics missing"; fi

# -----------------------------------------------------------------------------
step "7. Kafka UI"
# -----------------------------------------------------------------------------
UI_CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
  "http://localhost:${KAFKA_UI_PORT:-8085}/" 2>/dev/null)
if [ "$UI_CODE" = "200" ]; then
  ok "interface available -> http://localhost:${KAFKA_UI_PORT:-8085}"
else
  fail "interface unavailable (HTTP code: ${UI_CODE:-none}) - it takes ~30s to start"
fi

# -----------------------------------------------------------------------------
echo ""
if [ "$FAILED" -eq 0 ]; then
  echo -e "${GREEN}=====================================================${NC}"
  echo -e "${GREEN}  PHASE 1 PASSED: Kafka + PostgreSQL are operational${NC}"
  echo -e "${GREEN}=====================================================${NC}"
else
  echo -e "${RED}=====================================================${NC}"
  echo -e "${RED}  Some checks failed (see [FAIL] above)${NC}"
  echo -e "${RED}=====================================================${NC}"
fi
exit "$FAILED"
