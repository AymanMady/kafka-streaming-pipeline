# Data contracts

A data contract is an explicit agreement between the producer of a dataset and
its consumers.

## What it states

- Schema: column names, types, nullability.
- Semantics: units, currency, time zone, meaning of each status value.
- Quality: uniqueness of keys, accepted ranges, freshness.
- Change policy: how breaking changes are announced and versioned.

## Enforcing it

- Validate at the boundary (ingestion) and reject or quarantine bad records.
- Encode the rules as tests (dbt tests, Great Expectations, pydantic models).
- Version event schemas (Avro / JSON Schema with a registry for Kafka).

## Breaking vs non-breaking

Adding a nullable column is non-breaking. Renaming, removing, changing a type
or the meaning of a value is breaking and needs a new version.
