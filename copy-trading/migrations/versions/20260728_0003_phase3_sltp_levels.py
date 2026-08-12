"""Phase 3 SL/TP price levels on positions.

Revision ID: 20260728_0003
Revises: 20260728_0002
Create Date: 2026-07-28

Adds nullable stop_loss_price and take_profit_price columns for Position Manager
monitoring. No backfill — legacy open positions keep NULL levels per policy.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260728_0003"
down_revision: Union[str, Sequence[str], None] = "20260728_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "positions",
        sa.Column("stop_loss_price", sa.Numeric(18, 8), nullable=True),
    )
    op.add_column(
        "positions",
        sa.Column("take_profit_price", sa.Numeric(18, 8), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("positions", "take_profit_price")
    op.drop_column("positions", "stop_loss_price")
