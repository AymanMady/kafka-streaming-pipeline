# Secrets management

## Rules

- No credential in code, notebooks or committed config files.
- `.env` is git-ignored; `.env.example` documents the variables with dummy values.
- Use different credentials per environment.
- Grant the minimal privileges a job needs (read-only users for extraction).

## Where to store them

- Local: `.env` loaded by Docker Compose.
- CI: repository or environment secrets.
- Airflow: connections and variables backed by a secrets backend.
- Production: a vault or the cloud provider's secret manager.

## If a secret leaks

Rotate it first, then remove it from history. Deleting the commit alone is not
enough: it may already be cloned or cached.
