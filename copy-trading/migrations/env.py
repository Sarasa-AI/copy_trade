"""Alembic environment — DATABASE_URL / POSTGRES_* from environment."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def database_url() -> str:
    """Build a sync SQLAlchemy URL for Alembic migrations."""
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        # asyncpg URLs are not valid for Alembic's sync engine
        if url.startswith("postgresql+asyncpg://"):
            return "postgresql://" + url.removeprefix("postgresql+asyncpg://")
        if url.startswith("postgres://"):
            return "postgresql://" + url.removeprefix("postgres://")
        return url

    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "copytrading")
    if not user or not password:
        raise RuntimeError(
            "DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD required for migrations"
        )
    # Local/dev: docker service hostname "postgres" is not resolvable on host
    if host == "postgres" and os.environ.get("MIGRATE_ON_HOST", "1") == "1":
        host = "localhost"
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def run_migrations_offline() -> None:
    url = database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = database_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
