# Testing data pipelines

## Layers

1. **Unit tests** on pure transformation functions with tiny in-memory inputs.
   Fast, run on every commit.
2. **Integration tests** against real services (PostgreSQL, Kafka) started in
   Docker. Slower, run in CI.
3. **Data tests** on the produced tables (dbt tests, row counts, freshness).
   Run after every pipeline execution.

## Tips

- Build fixtures from a handful of rows that each exercise one edge case:
  nulls, duplicates, time-zone boundaries, empty input.
- Test the idempotency explicitly: run twice, compare.
- Keep a small, versioned sample dataset for end-to-end runs.
- Assert on schemas, not only on values.
