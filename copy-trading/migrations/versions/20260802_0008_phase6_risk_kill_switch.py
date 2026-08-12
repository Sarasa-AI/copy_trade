"""Phase 6: equity SoD snapshots, kill switch, risk denials.

Revision ID: 20260802_0008
Revises: 20260802_0007
Create Date: 2026-08-02

Adds persistent global kill switch, UTC start-of-day equity snapshots,
and deterministic risk-denial audit rows. Does not change capital
authority (scripts/capital.py).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260802_0008"
down_revision: Union[str, Sequence[str], None] = "20260802_0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "risk_control_lock",
        sa.Column("id", sa.SmallInteger(), primary_key=True),
        sa.CheckConstraint("id = 1", name="risk_control_lock_singleton"),
    )
    op.execute("INSERT INTO risk_control_lock (id) VALUES (1)")

    op.create_table(
        "equity_sod_snapshots",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("wallet_id", sa.Uuid(), sa.ForeignKey("wallets.id"), nullable=True),
        sa.Column("equity", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "scope IN ('GLOBAL', 'WALLET')",
            name="equity_sod_scope_check",
        ),
        sa.CheckConstraint(
            "(scope = 'GLOBAL' AND wallet_id IS NULL) OR "
            "(scope = 'WALLET' AND wallet_id IS NOT NULL)",
            name="equity_sod_scope_wallet_check",
        ),
    )
    op.create_index(
        "uq_equity_sod_global_date",
        "equity_sod_snapshots",
        ["as_of_date"],
        unique=True,
        postgresql_where=sa.text("scope = 'GLOBAL'"),
    )
    op.create_index(
        "uq_equity_sod_wallet_date",
        "equity_sod_snapshots",
        ["as_of_date", "wallet_id"],
        unique=True,
        postgresql_where=sa.text("scope = 'WALLET'"),
    )

    op.create_table(
        "kill_switch_state",
        sa.Column("id", sa.SmallInteger(), primary_key=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=True),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint("id = 1", name="kill_switch_state_singleton"),
    )
    op.execute(
        """
        INSERT INTO kill_switch_state (id, active)
        VALUES (1, false)
        """
    )

    op.create_table(
        "kill_switch_events",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("active_after", sa.Boolean(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=True),
        sa.Column("equity_at_event", sa.Numeric(18, 8), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "event_type IN ('ACTIVATE', 'DEACTIVATE')",
            name="kill_switch_events_type_check",
        ),
    )
    op.create_index(
        "idx_kill_switch_events_created_at",
        "kill_switch_events",
        ["created_at"],
    )

    op.create_table(
        "risk_denials",
        sa.Column("id", sa.Uuid(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("reason_code", sa.Text(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("wallet_id", sa.Uuid(), sa.ForeignKey("wallets.id"), nullable=True),
        sa.Column("sod_equity", sa.Numeric(18, 8), nullable=True),
        sa.Column("current_equity", sa.Numeric(18, 8), nullable=True),
        sa.Column("loss_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column("limit_pct", sa.Numeric(18, 8), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index(
        "idx_risk_denials_created_at", "risk_denials", ["created_at"]
    )


def downgrade() -> None:
    op.drop_index("idx_risk_denials_created_at", table_name="risk_denials")
    op.drop_table("risk_denials")
    op.drop_index(
        "idx_kill_switch_events_created_at", table_name="kill_switch_events"
    )
    op.drop_table("kill_switch_events")
    op.drop_table("kill_switch_state")
    op.drop_index(
        "uq_equity_sod_wallet_date", table_name="equity_sod_snapshots"
    )
    op.drop_index(
        "uq_equity_sod_global_date", table_name="equity_sod_snapshots"
    )
    op.drop_table("equity_sod_snapshots")
    op.drop_table("risk_control_lock")
