"""Phase 7: ledger provenance, append-only, reconciliation history, critical events.

Revision ID: 20260804_0009
Revises: 20260802_0008
Create Date: 2026-08-04

Pre-testnet auditability only. No exchange / fee / client-order identifiers.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260804_0009"
down_revision: Union[str, Sequence[str], None] = "20260802_0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "capital_ledger",
        sa.Column("actor", sa.Text(), nullable=True),
    )
    op.add_column(
        "capital_ledger",
        sa.Column("source", sa.Text(), nullable=True),
    )
    op.add_column(
        "capital_ledger",
        sa.Column("reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "capital_ledger",
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "capital_ledger",
        sa.Column("order_id", sa.Uuid(), nullable=True),
    )
    op.create_index(
        "idx_capital_ledger_correlation_id",
        "capital_ledger",
        ["correlation_id"],
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION capital_ledger_append_only()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'capital_ledger is append-only: % not allowed',
                TG_OP;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_capital_ledger_append_only
        BEFORE UPDATE OR DELETE ON capital_ledger
        FOR EACH ROW
        EXECUTE FUNCTION capital_ledger_append_only();
        """
    )

    op.create_table(
        "reconciliation_runs",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("ok", sa.Boolean(), nullable=False),
        sa.Column(
            "strict",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("mismatch_count", sa.Integer(), nullable=False),
        sa.Column(
            "mismatches",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("notes", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_reconciliation_runs_started_at",
        "reconciliation_runs",
        ["started_at"],
    )
    op.create_index(
        "idx_reconciliation_runs_correlation_id",
        "reconciliation_runs",
        ["correlation_id"],
    )

    op.create_table(
        "critical_events",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("wallet_id", sa.Uuid(), nullable=True),
        sa.Column("order_id", sa.Uuid(), nullable=True),
        sa.Column("position_id", sa.Uuid(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column(
            "detail",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index(
        "idx_critical_events_ts", "critical_events", ["ts"]
    )
    op.create_index(
        "idx_critical_events_type", "critical_events", ["event_type"]
    )
    op.create_index(
        "idx_critical_events_correlation_id",
        "critical_events",
        ["correlation_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_critical_events_correlation_id", table_name="critical_events"
    )
    op.drop_index("idx_critical_events_type", table_name="critical_events")
    op.drop_index("idx_critical_events_ts", table_name="critical_events")
    op.drop_table("critical_events")

    op.drop_index(
        "idx_reconciliation_runs_correlation_id",
        table_name="reconciliation_runs",
    )
    op.drop_index(
        "idx_reconciliation_runs_started_at",
        table_name="reconciliation_runs",
    )
    op.drop_table("reconciliation_runs")

    op.execute("DROP TRIGGER IF EXISTS trg_capital_ledger_append_only ON capital_ledger")
    op.execute("DROP FUNCTION IF EXISTS capital_ledger_append_only()")

    op.drop_index(
        "idx_capital_ledger_correlation_id", table_name="capital_ledger"
    )
    op.drop_column("capital_ledger", "order_id")
    op.drop_column("capital_ledger", "correlation_id")
    op.drop_column("capital_ledger", "reason")
    op.drop_column("capital_ledger", "source")
    op.drop_column("capital_ledger", "actor")
