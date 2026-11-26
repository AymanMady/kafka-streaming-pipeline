# Backfills

A backfill reprocesses past data, after a bug fix or when a new column is added.

## Procedure

1. Make sure the job is idempotent (see `idempotent-loads.md`).
2. Parameterise the job by logical date; never read "today" implicitly.
3. Start with a single day, compare with the current output, then widen.
4. Run in chronological order when a day depends on the previous one
   (running totals, SCD history).
5. Limit concurrency so the source and the warehouse are not overloaded.

## With Airflow

```bash
airflow dags backfill <dag_id> --start-date 2026-01-01 --end-date 2026-01-31
```

Set `max_active_runs` on the DAG to bound parallelism.

## After the run

Record the range, the reason and the commit hash in the changelog or the
incident ticket, and re-run the data-quality checks on the whole range.
