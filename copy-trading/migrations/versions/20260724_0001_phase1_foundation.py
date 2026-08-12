"""Phase 1 foundation: baseline schema + wallet_id UUID FKs.

Revision ID: 20260724_0001
Revises:
Create Date: 2026-07-24

Alembic is the source of truth for schema. This revision:
- Creates core tables on a fresh database (UUID wallet FKs from the start)
- Upgrades legacy TEXT wallet_id columns to UUID FK while preserving rows
- Adds wallet_id / closed_at indexes required for integrity and lookups

Downgrade converts UUID wallet_id back to TEXT address (data preserved).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text

revision: str = "20260724_0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _tables(conn) -> set[str]:
    return set(inspect(conn).get_table_names())


def _wallet_id_data_type(conn, table: str) -> str | None:
    row = conn.execute(
        text(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table
              AND column_name = 'wallet_id'
            """
        ),
        {"table": table},
    ).fetchone()
    return row[0] if row else None


def _create_fresh_schema() -> None:
    op.create_table(
        "wallets",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("address", sa.Text(), nullable=False),
        sa.Column(
            "win_rate",
            sa.Numeric(7, 4),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "total_trades",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "last_updated",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.UniqueConstraint("address", name="wallets_address_key"),
    )

    op.create_table(
        "paper_orders",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("qty", sa.Numeric(18, 8), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("wallet_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "side IN ('BUY', 'SELL')", name="paper_orders_side_check"
        ),
        sa.ForeignKeyConstraint(
            ["wallet_id"],
            ["wallets.id"],
            name="paper_orders_wallet_id_fkey",
        ),
    )

    op.create_table(
        "positions",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("entry_price", sa.Numeric(18, 8), nullable=False),
        sa.Column("qty", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "pnl",
            sa.Numeric(18, 8),
            nullable=False,
            server_default="0",
        ),
        sa.Column("wallet_id", sa.Uuid(), nullable=False),
        sa.Column(
            "opened_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("closed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["wallet_id"],
            ["wallets.id"],
            name="positions_wallet_id_fkey",
        ),
    )

    op.create_table(
        "daily_stats",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column(
            "total_pnl",
            sa.Numeric(18, 8),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "trade_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "max_drawdown",
            sa.Numeric(18, 8),
            nullable=False,
            server_default="0",
        ),
        sa.UniqueConstraint("date", name="daily_stats_date_key"),
    )

    _create_indexes()


def _create_indexes() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_orders_symbol ON paper_orders (symbol)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_orders_created_at "
        "ON paper_orders (created_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_orders_wallet_id "
        "ON paper_orders (wallet_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions (symbol)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions (opened_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_positions_closed_at ON positions (closed_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_positions_wallet_id ON positions (wallet_id)"
    )


def _ensure_legacy_base_tables() -> None:
    """Create pre-Phase-1 tables if somehow missing on a partial DB."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS wallets (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address TEXT NOT NULL UNIQUE,
            win_rate NUMERIC(7, 4) NOT NULL DEFAULT 0,
            total_trades INTEGER NOT NULL DEFAULT 0,
            last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_orders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
            qty NUMERIC(18, 8) NOT NULL,
            price NUMERIC(18, 8) NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            wallet_id TEXT NOT NULL DEFAULT 'unknown',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol TEXT NOT NULL,
            entry_price NUMERIC(18, 8) NOT NULL,
            qty NUMERIC(18, 8) NOT NULL,
            pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
            wallet_id TEXT NOT NULL DEFAULT 'unknown',
            opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            closed_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_stats (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            date DATE NOT NULL UNIQUE,
            total_pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
            trade_count INTEGER NOT NULL DEFAULT 0,
            max_drawdown NUMERIC(18, 8) NOT NULL DEFAULT 0
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_orders_symbol ON paper_orders (symbol)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_orders_created_at "
        "ON paper_orders (created_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions (symbol)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions (opened_at)"
    )


def _migrate_text_wallet_id_to_uuid(conn) -> None:
    """Preserve rows: map TEXT wallet_id → wallets.address → wallets.id."""
    # Collect every legacy text token used as wallet_id
    op.execute(
        """
        INSERT INTO wallets (address)
        SELECT DISTINCT wallet_id
        FROM (
            SELECT wallet_id FROM positions
            UNION
            SELECT wallet_id FROM paper_orders
            UNION
            SELECT 'unknown'
        ) AS tokens(wallet_id)
        WHERE wallet_id IS NOT NULL
          AND wallet_id <> ''
          AND NOT EXISTS (
              SELECT 1 FROM wallets w WHERE w.address = tokens.wallet_id
          )
        """
    )

    for table in ("positions", "paper_orders"):
        data_type = _wallet_id_data_type(conn, table)
        # Already UUID → only ensure FK/indexes below
        if data_type == "uuid":
            continue

        op.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS wallet_id_uuid UUID"
        )
        op.execute(
            f"""
            UPDATE {table} AS t
            SET wallet_id_uuid = w.id
            FROM wallets AS w
            WHERE w.address = t.wallet_id
              AND t.wallet_id_uuid IS NULL
            """
        )
        orphan = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {table} WHERE wallet_id_uuid IS NULL"
            )
        ).scalar()
        if orphan:
            raise RuntimeError(
                f"{table}: {orphan} row(s) could not be mapped to wallets.id"
            )

        op.execute(f"ALTER TABLE {table} DROP COLUMN wallet_id")
        op.execute(
            f"ALTER TABLE {table} RENAME COLUMN wallet_id_uuid TO wallet_id"
        )
        op.execute(f"ALTER TABLE {table} ALTER COLUMN wallet_id SET NOT NULL")

    # Drop legacy text default if somehow still present — N/A after rename
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'positions_wallet_id_fkey'
            ) THEN
                ALTER TABLE positions
                ADD CONSTRAINT positions_wallet_id_fkey
                FOREIGN KEY (wallet_id) REFERENCES wallets(id);
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'paper_orders_wallet_id_fkey'
            ) THEN
                ALTER TABLE paper_orders
                ADD CONSTRAINT paper_orders_wallet_id_fkey
                FOREIGN KEY (wallet_id) REFERENCES wallets(id);
            END IF;
        END $$;
        """
    )
    _create_indexes()


def upgrade() -> None:
    conn = op.get_bind()
    tables = _tables(conn)

    if not tables.intersection({"wallets", "positions", "paper_orders", "daily_stats"}):
        _create_fresh_schema()
        return

    _ensure_legacy_base_tables()
    conn = op.get_bind()
    _migrate_text_wallet_id_to_uuid(conn)


def downgrade() -> None:
    """Convert UUID FKs back to TEXT addresses (best-effort rollback)."""
    conn = op.get_bind()
    tables = _tables(conn)
    if "wallets" not in tables:
        return

    for table in ("positions", "paper_orders"):
        if table not in tables:
            continue

        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = '{table}_wallet_id_fkey'
                ) THEN
                    ALTER TABLE {table} DROP CONSTRAINT {table}_wallet_id_fkey;
                END IF;
            END $$;
            """
        )
        op.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS wallet_id_text TEXT"
        )
        op.execute(
            f"""
            UPDATE {table} AS t
            SET wallet_id_text = w.address
            FROM wallets AS w
            WHERE w.id = t.wallet_id
              AND t.wallet_id_text IS NULL
            """
        )
        op.execute(
            f"""
            UPDATE {table}
            SET wallet_id_text = 'unknown'
            WHERE wallet_id_text IS NULL
            """
        )
        op.execute(f"ALTER TABLE {table} DROP COLUMN wallet_id")
        op.execute(
            f"ALTER TABLE {table} RENAME COLUMN wallet_id_text TO wallet_id"
        )
        op.execute(f"ALTER TABLE {table} ALTER COLUMN wallet_id SET NOT NULL")
        op.execute(
            f"ALTER TABLE {table} ALTER COLUMN wallet_id SET DEFAULT 'unknown'"
        )

    op.execute("DROP INDEX IF EXISTS idx_positions_wallet_id")
    op.execute("DROP INDEX IF EXISTS idx_paper_orders_wallet_id")
    op.execute("DROP INDEX IF EXISTS idx_positions_closed_at")

    # Full table drop only if this revision created a fresh DB and caller
    # wants hard rollback — keep tables to avoid destroying paper history.
    # Hard drop path for empty fresh DBs:
    counts = conn.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM positions)
            + (SELECT COUNT(*) FROM paper_orders)
            + (SELECT COUNT(*) FROM daily_stats)
            + (SELECT COUNT(*) FROM wallets)
            """
        )
    ).scalar()
    if counts == 0:
        op.drop_table("positions")
        op.drop_table("paper_orders")
        op.drop_table("daily_stats")
        op.drop_table("wallets")
