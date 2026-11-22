# Change data capture

CDC reads the database transaction log to capture inserts, updates and
deletes as events.

## Compared with polling

| | Polling on `updated_at` | Log-based CDC |
|---|---|---|
| Deletes | not visible | captured |
| Load on source | queries | reads the WAL |
| Latency | interval | seconds |
| Setup | trivial | replication slot, connector |

## PostgreSQL

Logical replication (`wal_level = logical`) with Debezium publishes one Kafka
topic per table. Monitor replication slot lag: a stuck consumer makes the WAL
grow until the disk is full.

## Applying changes

Order events per key, apply them as upserts, and handle the delete tombstones.
