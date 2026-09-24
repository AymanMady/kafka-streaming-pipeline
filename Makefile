# =============================================================================
#  Kafka Streaming Pipeline - control shortcuts
#  Just type "make" to see every available command.
# =============================================================================

# Load .env if it exists (the "-" avoids an error when the file is missing).
-include .env
export

COMPOSE := docker compose
KAFKA   := $(COMPOSE) exec -T kafka /opt/kafka/bin
SPARK   := $(COMPOSE) exec -T spark-app
BOOTSTRAP_IN := localhost:9092

# Extra arguments for the streaming jobs:
#     make stream-analytics ARGS="--window 30s --duration 120"
ARGS ?=

.DEFAULT_GOAL := help
.PHONY: help setup build up up-core down restart ps logs logs-kafka logs-postgres \
        logs-spark check kafka-shell kafka-info topics topics-describe psql ui \
        spark-ui spark-shell db-migrate dlq-tail reset-checkpoints \
        stream-raw stream-validate stream-analytics demo-watermark \
        lag load-test test clean

help: ## Show this help
	@echo ""
	@echo "  Kafka Streaming Pipeline - available commands"
	@echo "  ---------------------------------------------"
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""

setup: ## Create .env from .env.example (never overwrites it)
	@if [ -f .env ]; then \
		echo "  .env already exists: nothing to do."; \
	else \
		cp .env.example .env; \
		echo "  .env created. Change POSTGRES_PASSWORD before going further."; \
	fi

build: ## Build the Spark image (phase 8, ~3 min the first time)
	$(COMPOSE) build

up: ## Start the whole stack (kafka + ui + postgres + spark)
	$(COMPOSE) up -d
	@echo ""
	@echo "  Kafka (from the host) : localhost:$(KAFKA_HOST_PORT)"
	@echo "  Kafka UI              : http://localhost:$(KAFKA_UI_PORT)"
	@echo "  PostgreSQL            : localhost:$(POSTGRES_HOST_PORT)"
	@echo ""
	@echo "  The broker takes ~30s to become 'healthy'. Then: make check"
	@echo ""

down: ## Stop the containers (Kafka and PostgreSQL data is kept)
	$(COMPOSE) down

restart: down up ## Restart the stack

ps: ## Container status
	$(COMPOSE) ps

logs: ## Logs of every service (Ctrl+C to exit)
	$(COMPOSE) logs -f --tail=50

logs-kafka: ## Logs of the Kafka broker
	$(COMPOSE) logs -f --tail=80 kafka

logs-postgres: ## Logs of PostgreSQL
	$(COMPOSE) logs -f --tail=80 postgres

check: ## Check end to end that the stack works (phase 1)
	@bash scripts/check_stack.sh

topics: ## Create the project topics (idempotent) and show their anatomy
	@bash scripts/create_topics.sh

topics-describe: ## Topic details: partitions, leader, replicas, ISR
	@$(KAFKA)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP_IN) --describe | sed 's/^/  /'

kafka-info: ## Show the Kafka cluster identity and its key settings
	@echo "  --- Cluster ---"
	@$(KAFKA)/kafka-cluster.sh cluster-id --bootstrap-server $(BOOTSTRAP_IN)
	@echo "  --- Broker: effective configuration (excerpt) ---"
	@$(KAFKA)/kafka-configs.sh --bootstrap-server $(BOOTSTRAP_IN) \
		--entity-type brokers --entity-name 1 --describe --all 2>/dev/null \
		| grep -E 'num.partitions|log.retention.hours|auto.create.topics.enable|default.replication.factor' \
		| sed 's/^/  /' || true

kafka-shell: ## Open a shell inside the Kafka container (CLI tools in /opt/kafka/bin)
	$(COMPOSE) exec kafka bash

psql: ## Open a SQL console on PostgreSQL
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

ui: ## Open Kafka UI in the browser
	@xdg-open http://localhost:$(KAFKA_UI_PORT) >/dev/null 2>&1 || \
		echo "  Open it manually: http://localhost:$(KAFKA_UI_PORT)"

clean: ## Remove containers + PostgreSQL volume + Kafka segments (DESTRUCTIVE)
	@printf "  This will delete the PostgreSQL database AND the Kafka messages. Continue? [y/N] " && read ans && [ "$$ans" = "y" ]
	$(COMPOSE) down -v
	rm -rf data/kafka/*
	@echo "  Done."

up-core: ## Start only kafka + kafka-ui + postgres (no Spark, lighter)
	$(COMPOSE) up -d kafka kafka-ui postgres

logs-spark: ## Logs of the Spark master and worker
	$(COMPOSE) logs -f --tail=80 spark-master spark-worker

spark-ui: ## Open the Spark interfaces (master, then the app during a job)
	@xdg-open http://localhost:$(SPARK_MASTER_UI_PORT) >/dev/null 2>&1 || \
		echo "  Master: http://localhost:$(SPARK_MASTER_UI_PORT)  App: http://localhost:$(SPARK_APP_UI_PORT)"

spark-shell: ## Open a shell in the Spark client container (the driver)
	$(COMPOSE) exec spark-app bash

# =============================================================================
#  DATABASE
# =============================================================================

db-migrate: ## Apply sql/*.sql (idempotent: replayable on an existing database)
	@for f in sql/*.sql; do \
		echo "  applying $$f"; \
		$(COMPOSE) exec -T postgres psql -q -v ON_ERROR_STOP=1 \
			-U $(POSTGRES_USER) -d $(POSTGRES_DB) -f - < $$f || exit 1; \
	done
	@echo "  Schema up to date."

# =============================================================================
#  STREAMING (phases 9 to 17)
# =============================================================================

stream-raw: ## Phase 9: read the Kafka stream as-is (envelope, no parsing)
	$(SPARK) spark-submit src/streaming/read_stream.py $(ARGS)

stream-validate: ## Phase 10: validate the stream, reject to the DLQ
	$(SPARK) spark-submit src/streaming/validate_stream.py $(ARGS)

stream-analytics: ## Phases 11-17: windows, watermarks, PostgreSQL
	$(SPARK) spark-submit src/streaming/analytics_stream.py $(ARGS)

demo-watermark: ## Phase 13: what a watermark accepts, and what it drops
	$(SPARK) python3 scripts/demo_watermark.py $(ARGS)

dlq-tail: ## Read the last rejected events (orders-invalid topic)
	@$(KAFKA)/kafka-console-consumer.sh --bootstrap-server $(BOOTSTRAP_IN) \
		--topic $(TOPIC_ORDERS_INVALID) --max-messages 10 --timeout-ms 8000 \
		--property print.partition=true 2>/dev/null || \
		echo "  (no recent message in the DLQ)"

reset-checkpoints: ## Delete the checkpoints (the next run replays the topic)
	@printf "  This deletes the offsets AND the state of the open windows. Continue? [y/N] " \
		&& read ans && [ "$$ans" = "y" ]
	rm -rf data/checkpoints/*
	@echo "  Done. The next run starts again from startingOffsets."

# =============================================================================
#  OBSERVABILITY AND TESTS (phases 17 to 19)
# =============================================================================

lag: ## Phase 17: consumer lag per group and per partition
	$(SPARK) python3 scripts/monitor_lag.py $(ARGS)

load-test: ## Phase 18: throughput test (RATE=5000 COUNT=200000)
	$(SPARK) python3 scripts/load_test.py --rate $(or $(RATE),0) --count $(or $(COUNT),50000) $(ARGS)

test: ## Phase 19: the pytest suite
	$(SPARK) python3 -m pytest tests/ -q $(ARGS)
