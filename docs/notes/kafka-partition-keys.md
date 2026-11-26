# Kafka partition keys

Kafka only guarantees ordering **inside a partition**. The producer picks the
partition by hashing the message key, so the key decides which events stay in
order relative to each other.

## Choosing a key

- Use the entity whose events must stay ordered: `order_id`, `customer_id`,
  `device_id`.
- A `null` key spreads messages across partitions (sticky partitioner): good
  throughput, no ordering.
- Avoid low-cardinality keys (`country`, `status`): a few partitions get all
  the traffic while the others stay idle.

## Hot keys

One very active key (a bot, a large customer) overloads one partition and its
consumer. Options: add a suffix to split it (`customer_42#3`) and re-aggregate
downstream, or route it to a dedicated topic.

## Changing the partition count

Adding partitions changes `hash(key) % partitions`, so a key can move to a new
partition and lose ordering with its older events. Size the topic for the
expected peak up front, or plan a migration to a new topic.
