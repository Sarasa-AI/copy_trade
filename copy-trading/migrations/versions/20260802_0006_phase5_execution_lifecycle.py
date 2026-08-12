"""Phase 5 execution lifecycle: order↔position link + order status CHECK.

Revision ID: 20260802_0006
Revises: 20260729_0005
Create Date: 2026-08-02

Adds:
- positions.order_id UUID NULL UNIQUE FK → paper_orders(id)
- Deterministic best-effort backfill for uniquely provable pairs only
- paper_orders_status_check: PENDING | FILLED | REJECTED | FAILED

Ambiguous historical pairs are left with order_id NULL (legacy exception).
New paper-trading opens must set order_id.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "20260802_0006"
down_revision: Union[str, Sequence[str], None] = "20260729_0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _backfill_order_id(conn) -> None:
    """Greedy 1:1 assignment when wallet/symbol/price/qty/FILLED uniquely match."""
    positions = conn.execute(
        text(
            """
            SELECT id, wallet_id, symbol, entry_price, qty, opened_at
            FROM positions
            WHERE order_id IS NULL
            ORDER BY opened_at, id
            """
        )
    ).fetchall()

    used_order_ids: set[str] = set()

    for position in positions:
        pos_id, wallet_id, symbol, entry_price, qty, opened_at = position
        order = conn.execute(
            text(
                """
                SELECT o.id
                FROM paper_orders o
                WHERE o.wallet_id = :wallet_id
                  AND o.symbol = :symbol
                  AND o.price = :entry_price
                  AND o.qty = :qty
                  AND o.status = 'FILLED'
                  AND o.id::text <> ALL(:used_ids)
                  AND NOT EXISTS (
                      SELECT 1 FROM positions p
                      WHERE p.order_id = o.id
                  )
                ORDER BY
                    ABS(EXTRACT(EPOCH FROM (:opened_at - o.created_at))),
                    o.created_at,
                    o.id
                LIMIT 1
                """
            ),
            {
                "wallet_id": wallet_id,
                "symbol": symbol,
                "entry_price": entry_price,
                "qty": qty,
                "opened_at": opened_at,
                "used_ids": list(used_order_ids) or [""],
            },
        ).fetchone()

        if order is None:
            # Ambiguous or unmatched — leave NULL (legacy exception).
            continue

        order_id = order[0]
        conn.execute(
            text("UPDATE positions SET order_id = :oid WHERE id = :pid"),
            {"oid": order_id, "pid": pos_id},
        )
        used_order_ids.add(str(order_id))


def upgrade() -> None:
    conn = op.get_bind()

    invalid_status = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM paper_orders
            WHERE status NOT IN ('PENDING', 'FILLED', 'REJECTED', 'FAILED')
            """
        )
    ).scalar()
    if invalid_status:
        raise RuntimeError(
            f"Phase 5 upgrade blocked: {invalid_status} paper_orders row(s) "
            "have status outside PENDING|FILLED|REJECTED|FAILED"
        )

    op.add_column(
        "positions",
        sa.Column("order_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "positions_order_id_fkey",
        "positions",
        "paper_orders",
        ["order_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "positions_order_id_key",
        "positions",
        ["order_id"],
    )

    _backfill_order_id(conn)

    op.create_check_constraint(
        "paper_orders_status_check",
        "paper_orders",
        "status IN ('PENDING', 'FILLED', 'REJECTED', 'FAILED')",
    )


def downgrade() -> None:
    op.drop_constraint("paper_orders_status_check", "paper_orders", type_="check")
    op.drop_constraint("positions_order_id_key", "positions", type_="unique")
    op.drop_constraint("positions_order_id_fkey", "positions", type_="foreignkey")
    op.drop_column("positions", "order_id")
