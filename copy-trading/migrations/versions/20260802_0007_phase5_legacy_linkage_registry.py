"""Phase 5.1 follow-up: registry of legacy unlinked FILLED orders.

Revision ID: 20260802_0007
Revises: 20260802_0006
Create Date: 2026-08-02

Snapshots FILLED paper_orders that have no positions.order_id link at upgrade
time. Reconciliation treats those as non-fatal legacy notes; any later FILLED
orphan is a critical failure.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "20260802_0007"
down_revision: Union[str, Sequence[str], None] = "20260802_0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "phase5_legacy_unlinked_orders",
        sa.Column("order_id", sa.Uuid(), primary_key=True),
        sa.ForeignKeyConstraint(
            ["order_id"], ["paper_orders.id"], name="phase5_legacy_orders_fkey"
        ),
    )
    conn = op.get_bind()
    conn.execute(
        text(
            """
            INSERT INTO phase5_legacy_unlinked_orders (order_id)
            SELECT o.id
            FROM paper_orders o
            WHERE o.status = 'FILLED'
              AND NOT EXISTS (
                  SELECT 1 FROM positions p WHERE p.order_id = o.id
              )
            ON CONFLICT DO NOTHING
            """
        )
    )

    op.create_table(
        "phase5_legacy_unlinked_positions",
        sa.Column("position_id", sa.Uuid(), primary_key=True),
        sa.ForeignKeyConstraint(
            ["position_id"],
            ["positions.id"],
            name="phase5_legacy_positions_fkey",
        ),
    )
    conn.execute(
        text(
            """
            INSERT INTO phase5_legacy_unlinked_positions (position_id)
            SELECT p.id
            FROM positions p
            WHERE p.order_id IS NULL
              AND EXISTS (
                  SELECT 1 FROM capital_ledger cl
                  WHERE cl.position_id = p.id AND cl.entry_type = 'RESERVE'
              )
            ON CONFLICT DO NOTHING
            """
        )
    )


def downgrade() -> None:
    op.drop_table("phase5_legacy_unlinked_positions")
    op.drop_table("phase5_legacy_unlinked_orders")
