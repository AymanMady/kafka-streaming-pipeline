# Retries and backoff

## Retry only what is transient

Timeouts, connection resets, HTTP 429/503: retry. Authentication errors,
schema mismatches, HTTP 400: fail fast, a retry will not help.

## Exponential backoff with jitter

```python
delay = min(cap, base * 2 ** attempt) * random.uniform(0.5, 1.0)
```

Jitter prevents all workers from retrying at the same instant.

## Limits

Cap the number of attempts and the total time. In Airflow use `retries`,
`retry_delay` and `retry_exponential_backoff=True` on the task.

## Prerequisite

Retries are only safe when the operation is idempotent.
