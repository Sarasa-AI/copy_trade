"""Phase 8A-1: persist stop_loss_pct / take_profit_pct on exchange_orders.

Revision ID: 20260807_0011
Revises: 20260806_0010
Create Date: 2026-08-07

recover_pending_orders() previously hard-coded 0.03 / 0.06 when reconstructing
a crash-orphaned OPEN position because the original SL/TP percentages were not
stored on the intent row.  This migration adds the two columns so that recovery
always uses the caller's original values.

Both columns are nullable so that existing PENDING rows (if any exist at
upgrade time) are not broken; the recovery code falls back to the old defaults
when the columns are NULL.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260807_0011"
down_revision: Union[str, Sequence[str], None] = "20260806_0010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "exchange_orders",
        sa.Column("stop_loss_pct", sa.Numeric(9, 6), nullable=True),
    )
    op.add_column(
        "exchange_orders",
        sa.Column("take_profit_pct", sa.Numeric(9, 6), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("exchange_orders", "take_profit_pct")
    op.drop_column("exchange_orders", "stop_loss_pct")
