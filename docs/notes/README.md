# Engineering notes

Short reference notes on the data-engineering practices applied (or worth
applying) in this project. Each note is self-contained.

- [Window functions cheat sheet](window-functions.md) — common analytical patterns
- [Secrets management](secrets-management.md) — keeping credentials out of the repository
- [Late-arriving data](late-arriving-data.md) — event time, watermarks and corrections
- [Change data capture](change-data-capture.md) — streaming database changes instead of polling
- [PostgreSQL indexing](postgres-indexing.md) — when indexes help analytical workloads
- [Kafka partition keys](kafka-partition-keys.md) — choosing the message key decides ordering and load balance
- [Backfills](backfills.md) — how to reprocess a historical date range safely
