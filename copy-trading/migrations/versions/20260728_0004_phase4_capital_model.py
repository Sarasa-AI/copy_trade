"""Phase 4 capital model: master_pool, wallet_balances, capital_ledger.

Revision ID: 20260728_0004
Revises: 20260728_0003
Create Date: 2026-07-28

Resolves Open Decision #4 with separate capital tables (not columns on wallets).
Seeds singleton master_pool and allocates AGENT_ALLOCATION_USDT (default 10000)
to each existing wallet, capped by MASTER_POOL_USDT (default 100000).
Legacy open positions do not receive reserved_margin backfill.
"""

from __future__ import annotations

import os
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "20260728_0004"
down_revision: Union[str, Sequence[str], None] = "20260728_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _master_total() -> float:
    return float(os.getenv("MASTER_POOL_USDT", "100000"))


def _agent_allocation() -> float:
    return float(os.getenv("AGENT_ALLOCATION_USDT", "10000"))


def upgrade() -> None:
    op.create_table(
        "master_pool",
        sa.Column("id", sa.SmallInteger(), primary_key=True),
        sa.Column("total_capital", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "allocated_capital",
            sa.Numeric(18, 8),
            nullable=False,
            server_default="0",
        ),
        sa.Column("available_capital", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint("id = 1", name="master_pool_singleton_check"),
        sa.CheckConstraint(
            "total_capital >= 0", name="master_pool_total_nonneg"
        ),
        sa.CheckConstraint(
            "allocated_capital >= 0", name="master_pool_allocated_nonneg"
        ),
        sa.CheckConstraint(
            "available_capital >= 0", name="master_pool_available_nonneg"
        ),
        sa.CheckConstraint(
            "allocated_capital + available_capital = total_capital",
            name="master_pool_conservation",
        ),
    )

    op.create_table(
        "wallet_balances",
        sa.Column(
            "wallet_id",
            sa.Uuid(),
            sa.ForeignKey("wallets.id"),
            primary_key=True,
        ),
        sa.Column("initial_capital", sa.Numeric(18, 8), nullable=False),
        sa.Column("current_equity", sa.Numeric(18, 8), nullable=False),
        sa.Column("available_balance", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "reserved_margin",
            sa.Numeric(18, 8),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "unrealized_pnl",
            sa.Numeric(18, 8),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "realized_pnl",
            sa.Numeric(18, 8),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "initial_capital >= 0", name="wallet_balances_initial_nonneg"
        ),
        sa.CheckConstraint(
            "available_balance >= 0", name="wallet_balances_available_nonneg"
        ),
        sa.CheckConstraint(
            "reserved_margin >= 0", name="wallet_balances_reserved_nonneg"
        ),
    )

    op.create_table(
        "capital_ledger",
        sa.Column(
            "id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "wallet_id", sa.Uuid(), sa.ForeignKey("wallets.id"), nullable=True
        ),
        sa.Column("entry_type", sa.Text(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 8), nullable=False),
        sa.Column("balance_after_available", sa.Numeric(18, 8), nullable=True),
        sa.Column("balance_after_reserved", sa.Numeric(18, 8), nullable=True),
        sa.Column(
            "position_id",
            sa.Uuid(),
            sa.ForeignKey("positions.id"),
            nullable=True,
        ),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "entry_type IN ("
            "'MASTER_INIT', 'ALLOCATE', 'RESERVE', 'RELEASE', "
            "'REALIZE_PNL', 'MARK_UNREALIZED')",
            name="capital_ledger_entry_type_check",
        ),
    )
    op.create_index(
        "idx_capital_ledger_wallet_id", "capital_ledger", ["wallet_id"]
    )
    op.create_index(
        "idx_capital_ledger_created_at", "capital_ledger", ["created_at"]
    )

    op.add_column(
        "positions",
        sa.Column("reserved_margin", sa.Numeric(18, 8), nullable=True),
    )

    conn = op.get_bind()
    total = _master_total()
    per_agent = _agent_allocation()

    conn.execute(
        text(
            """
            INSERT INTO master_pool (
                id, total_capital, allocated_capital, available_capital
            )
            VALUES (1, :total, 0, :total)
            """
        ),
        {"total": total},
    )
    conn.execute(
        text(
            """
            INSERT INTO capital_ledger (wallet_id, entry_type, amount, note)
            VALUES (NULL, 'MASTER_INIT', :total, 'Phase 4 master pool seed')
            """
        ),
        {"total": total},
    )

    wallets = conn.execute(
        text("SELECT id FROM wallets ORDER BY last_updated, id")
    ).fetchall()

    n = len(wallets)
    remaining = total
    if n > 0:
        # Equal split capped by AGENT_ALLOCATION_USDT so every existing wallet
        # receives a balance when possible (avoids unfunded legacy agents).
        equal_share = total / n
        per = min(per_agent, equal_share) if equal_share > 0 else 0.0
    else:
        per = 0.0

    for (wallet_id,) in wallets:
        amount = min(per, remaining) if per > 0 else 0.0
        if amount <= 0:
            break
        conn.execute(
            text(
                """
                INSERT INTO wallet_balances (
                    wallet_id, initial_capital, current_equity,
                    available_balance, reserved_margin,
                    unrealized_pnl, realized_pnl
                )
                VALUES (
                    :wallet_id, :amount, :amount, :amount, 0, 0, 0
                )
                """
            ),
            {"wallet_id": wallet_id, "amount": amount},
        )
        conn.execute(
            text(
                """
                INSERT INTO capital_ledger (
                    wallet_id, entry_type, amount,
                    balance_after_available, balance_after_reserved, note
                )
                VALUES (
                    :wallet_id, 'ALLOCATE', :amount, :amount, 0,
                    'Phase 4 migration seed allocation'
                )
                """
            ),
            {"wallet_id": wallet_id, "amount": amount},
        )
        remaining -= amount

    allocated = total - remaining
    conn.execute(
        text(
            """
            UPDATE master_pool
            SET allocated_capital = :allocated,
                available_capital = :remaining,
                updated_at = NOW()
            WHERE id = 1
            """
        ),
        {"allocated": allocated, "remaining": remaining},
    )


def downgrade() -> None:
    op.drop_column("positions", "reserved_margin")
    op.drop_index("idx_capital_ledger_created_at", table_name="capital_ledger")
    op.drop_index("idx_capital_ledger_wallet_id", table_name="capital_ledger")
    op.drop_table("capital_ledger")
    op.drop_table("wallet_balances")
    op.drop_table("master_pool")
