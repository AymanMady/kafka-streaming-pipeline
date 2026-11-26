# dbt tests

## Generic tests

Declared in YAML on a column or model: `unique`, `not_null`,
`accepted_values`, `relationships`. Custom ones are macros declared with
`{% test name(model, column_name) %}` in `macros/` or `tests/generic/`.

## Singular tests

A `.sql` file in `tests/` that returns the **failing rows**. The test passes
when the query returns nothing. Good for business rules spanning several
columns ("delivered orders must have a delivery date").

## Severity

```yaml
tests:
  - not_null:
      config:
        severity: warn
        error_if: ">100"
```

Use `warn` for known data issues that should not block a deployment, with a
threshold so that a sudden increase still fails.

## What to test first

1. Primary keys: `unique` + `not_null` on every model.
2. Foreign keys of facts: `relationships` to their dimensions.
3. Enumerations: `accepted_values` on status columns.

Run `dbt build` rather than `run` then `test`: it tests each model before
building the models that depend on it.
