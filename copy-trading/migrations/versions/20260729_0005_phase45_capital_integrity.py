"""Phase 4.5 capital integrity: ledger uniqueness + wallet conservation CHECKs.

Revision ID: 20260729_0005
Revises: 20260728_0004
Create Date: 2026-07-29

Adds:
- Partial unique indexes so each position has at most one RESERVE / RELEASE /
  REALIZE_PNL ledger row.
- Wallet conservation CHECK:
  available_balance + reserved_margin = initial_capital + realized_pnl
- Wallet equity CHECK:
  current_equity = available_balance + reserved_margin + unrealized_pnl

Upgrade fails loudly if existing rows violate the new CHECKs.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "20260729_0005"
down_revision: Union[str, Sequence[str], None] = "20260728_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    bad_conservation = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM wallet_balances
            WHERE available_balance + reserved_margin
                  <> initial_capital + realized_pnl
            """
        )
    ).scalar()
    if bad_conservation:
        raise RuntimeError(
            f"phase4.5 upgrade blocked: {bad_conservation} wallet_balances "
            "row(s) violate available+reserved = initial+realized"
        )

    bad_equity = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM wallet_balances
            WHERE current_equity
                  <> available_balance + reserved_margin + unrealized_pnl
            """
        )
    ).scalar()
    if bad_equity:
        raise RuntimeError(
            f"phase4.5 upgrade blocked: {bad_equity} wallet_balances "
            "row(s) violate equity = available+reserved+unrealized"
        )

    dup_settlement = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM (
                SELECT position_id, entry_type
                FROM capital_ledger
                WHERE position_id IS NOT NULL
                  AND entry_type IN ('RESERVE', 'RELEASE', 'REALIZE_PNL')
                GROUP BY position_id, entry_type
                HAVING COUNT(*) > 1
            ) dups
            """
        )
    ).scalar()
    if dup_settlement:
        raise RuntimeError(
            f"phase4.5 upgrade blocked: {dup_settlement} duplicate "
            "RESERVE/RELEASE/REALIZE_PNL ledger group(s) for position_id"
        )

    op.create_index(
        "uq_capital_ledger_reserve_position",
        "capital_ledger",
        ["position_id"],
        unique=True,
        postgresql_where=text(
            "entry_type = 'RESERVE' AND position_id IS NOT NULL"
        ),
    )
    op.create_index(
        "uq_capital_ledger_release_position",
        "capital_ledger",
        ["position_id"],
        unique=True,
        postgresql_where=text(
            "entry_type = 'RELEASE' AND position_id IS NOT NULL"
        ),
    )
    op.create_index(
        "uq_capital_ledger_realize_position",
        "capital_ledger",
        ["position_id"],
        unique=True,
        postgresql_where=text(
            "entry_type = 'REALIZE_PNL' AND position_id IS NOT NULL"
        ),
    )

    op.create_check_constraint(
        "wallet_balances_conservation",
        "wallet_balances",
        "available_balance + reserved_margin = initial_capital + realized_pnl",
    )
    op.create_check_constraint(
        "wallet_balances_equity",
        "wallet_balances",
        "current_equity = available_balance + reserved_margin + unrealized_pnl",
    )


def downgrade() -> None:
    op.drop_constraint(
        "wallet_balances_equity", "wallet_balances", type_="check"
    )
    op.drop_constraint(
        "wallet_balances_conservation", "wallet_balances", type_="check"
    )
    op.drop_index(
        "uq_capital_ledger_realize_position", table_name="capital_ledger"
    )
    op.drop_index(
        "uq_capital_ledger_release_position", table_name="capital_ledger"
    )
    op.drop_index(
        "uq_capital_ledger_reserve_position", table_name="capital_ledger"
    )
