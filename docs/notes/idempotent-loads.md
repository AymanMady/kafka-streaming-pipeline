# Idempotent loads

A load is idempotent when running it twice with the same input leaves the target
in the same state as running it once. It is what makes retries and backfills safe.

## Techniques

- **Delete-then-insert per partition**: wipe the slice being loaded (e.g. one
  day) inside the same transaction, then insert it again.
- **Upsert on a natural key**: `INSERT ... ON CONFLICT (key) DO UPDATE` in
  PostgreSQL, `MERGE` in Delta Lake / most warehouses.
- **Overwrite by partition**: `mode("overwrite")` with
  `partitionOverwriteMode=dynamic` in Spark.

## Anti-patterns

- Plain `INSERT` of a batch with no key: a retry duplicates rows.
- Using `now()` inside the transformation: the output depends on run time,
  not on the data. Pass the logical date as a parameter instead.

## Check

Run the same job twice on the same input and compare row counts and a checksum
of the target table. They must be identical.
