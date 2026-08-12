"""Phase 3 position lifecycle: side, exit_price, close_reason.

Revision ID: 20260728_0002
Revises: 20260724_0001
Create Date: 2026-07-28

Adds position direction and close metadata required by the Paper Close Policy.
Side backfill joins positions to paper_orders on wallet_id, symbol, price,
qty, and FILLED status. When multiple orders match, each position is paired
to the temporally closest unused FILLED order (deterministic greedy assignment).
Positions with no matching order fail the migration — BUY/SELL is never guessed.
Legacy closed rows receive exit_price derived from historical long-only PnL
formula and close_reason='ADMIN' (no recorded reason at close time).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "20260728_0002"
down_revision: Union[str, Sequence[str], None] = "20260724_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _assert_side_backfill_complete(conn) -> None:
    """Fail migration if any position lacks a deterministically mapped side."""
    missing = conn.execute(
        text(
            """
            SELECT id::text
            FROM positions
            WHERE side IS NULL
            ORDER BY id
            LIMIT 20
            """
        )
    ).fetchall()

    if not missing:
        return

    samples = ", ".join(row[0] for row in missing)
    total = conn.execute(
        text("SELECT COUNT(*) FROM positions WHERE side IS NULL")
    ).scalar()
    raise RuntimeError(
        f"Phase 3 migration: {total} position(s) could not be mapped to a "
        f"FILLED paper_order for side backfill. Samples: {samples}. "
        "Resolve manually before upgrading; do not guess BUY/SELL."
    )


def _backfill_side(conn) -> None:
    """Greedy 1:1 assignment by opened_at/created_at proximity."""
    positions = conn.execute(
        text(
            """
            SELECT id, wallet_id, symbol, entry_price, qty, opened_at
            FROM positions
            WHERE side IS NULL
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
                SELECT o.id, o.side
                FROM paper_orders o
                WHERE o.wallet_id = :wallet_id
                  AND o.symbol = :symbol
                  AND o.price = :entry_price
                  AND o.qty = :qty
                  AND o.status = 'FILLED'
                  AND o.id::text <> ALL(:used_ids)
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
            continue

        order_id, side = order
        conn.execute(
            text("UPDATE positions SET side = :side WHERE id = :pos_id"),
            {"side": side, "pos_id": pos_id},
        )
        used_order_ids.add(str(order_id))

    _assert_side_backfill_complete(conn)


def _backfill_closed_legacy_metadata(conn) -> None:
    invalid = conn.execute(
        text(
            """
            SELECT id::text
            FROM positions
            WHERE closed_at IS NOT NULL
              AND (
                  qty <= 0
                  OR (entry_price + (pnl / qty)) <= 0
              )
            LIMIT 20
            """
        )
    ).fetchall()
    if invalid:
        samples = ", ".join(row[0] for row in invalid)
        raise RuntimeError(
            "Phase 3 migration: closed legacy position(s) cannot derive a "
            f"positive exit_price from stored pnl/qty. Samples: {samples}"
        )

    conn.execute(
        text(
            """
            UPDATE positions
            SET exit_price = entry_price + (pnl / qty),
                close_reason = 'ADMIN'
            WHERE closed_at IS NOT NULL
              AND exit_price IS NULL
            """
        )
    )

    still_open_with_close_fields = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM positions
            WHERE closed_at IS NULL
              AND (exit_price IS NOT NULL OR close_reason IS NOT NULL)
            """
        )
    ).scalar()
    if still_open_with_close_fields:
        raise RuntimeError(
            "Phase 3 migration: open positions must not have exit_price or "
            "close_reason set before lifecycle constraints are applied."
        )

    closed_incomplete = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM positions
            WHERE closed_at IS NOT NULL
              AND (exit_price IS NULL OR close_reason IS NULL)
            """
        )
    ).scalar()
    if closed_incomplete:
        raise RuntimeError(
            "Phase 3 migration: closed positions missing exit_price or "
            "close_reason after legacy backfill."
        )


def upgrade() -> None:
    conn = op.get_bind()

    op.add_column("positions", sa.Column("side", sa.Text(), nullable=True))
    op.add_column(
        "positions",
        sa.Column("exit_price", sa.Numeric(18, 8), nullable=True),
    )
    op.add_column(
        "positions",
        sa.Column("close_reason", sa.Text(), nullable=True),
    )

    row_count = conn.execute(text("SELECT COUNT(*) FROM positions")).scalar()
    if row_count and row_count > 0:
        _backfill_side(conn)
        _backfill_closed_legacy_metadata(conn)

    op.alter_column("positions", "side", nullable=False)

    op.create_check_constraint(
        "positions_side_check",
        "positions",
        "side IN ('BUY', 'SELL')",
    )
    op.create_check_constraint(
        "positions_close_reason_check",
        "positions",
        "close_reason IS NULL OR close_reason IN ("
        "'STOP_LOSS', 'TAKE_PROFIT', 'ADMIN', "
        "'RISK', 'KILL_SWITCH', 'LEAD_CLOSE'"
        ")",
    )
    op.create_check_constraint(
        "positions_lifecycle_check",
        "positions",
        "(closed_at IS NULL AND exit_price IS NULL AND close_reason IS NULL) "
        "OR (closed_at IS NOT NULL AND exit_price IS NOT NULL "
        "AND close_reason IS NOT NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("positions_lifecycle_check", "positions", type_="check")
    op.drop_constraint(
        "positions_close_reason_check", "positions", type_="check"
    )
    op.drop_constraint("positions_side_check", "positions", type_="check")
    op.drop_column("positions", "close_reason")
    op.drop_column("positions", "exit_price")
    op.drop_column("positions", "side")
