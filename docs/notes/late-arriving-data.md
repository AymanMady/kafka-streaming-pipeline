# Late-arriving data

Events often arrive after the window they belong to has been computed.

## Event time vs processing time

Aggregate on the time the event happened, not the time it was received, or the
numbers change with network delays.

## Streaming: watermarks

```python
events.withWatermark("event_time", "10 minutes") \
      .groupBy(window("event_time", "5 minutes")).count()
```

Events older than the watermark are dropped; pick the delay from the observed
lateness distribution (e.g. p99).

## Batch: reprocessing window

Recompute the last N days on every run so late rows are included, and make the
load idempotent so the recomputation is safe.

## Monitor it

Track the share of events arriving after the watermark. A sudden rise usually
means an upstream outage.
