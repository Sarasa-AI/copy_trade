# Database schema

**Source of truth:** Alembic migrations in `migrations/versions/`.

| Artifact | Role |
|---|---|
| `migrations/` + `alembic.ini` | Versioned schema (apply with `alembic upgrade head`) |
| `db/schema.sql` | Human-readable mirror of head revision — **not** used for initdb |

## Apply migrations

```bash
# from copy-trading/
export $(grep -v '^#' .env | xargs)
# on host, POSTGRES_HOST=postgres is rewritten to localhost by migrations/env.py
alembic upgrade head
```

## Docker

`docker-compose.yml` runs a one-shot `migrate` service before `python-agent`.
Postgres data volumes are no longer initialized via `schema.sql`.

## Downgrade

```bash
alembic downgrade -1
```

UUID `wallet_id` values are converted back to TEXT addresses. Empty DBs may
drop tables; DBs with rows keep tables in legacy TEXT form.
