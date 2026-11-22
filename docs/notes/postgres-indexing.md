# PostgreSQL indexing

## B-tree

Default index; good for equality and range filters on selective columns, and
for foreign keys used in joins.

## Composite indexes

Order columns from equality filters to range filters:
`(customer_id, order_date)` serves `where customer_id = ? and order_date >= ?`.

## BRIN

Tiny index for very large, naturally ordered tables (append-only events by
timestamp). Great for time-range scans.

## Partial indexes

`create index on orders (created_at) where status = 'pending';` keeps the
index small when queries target a subset.

## Measure

Use `explain (analyze, buffers)` before and after. Every index slows writes and
must be maintained; drop unused ones (`pg_stat_user_indexes.idx_scan = 0`).
