"""Phase 6 — Persistent global Kill Switch.

Enforced inside the authoritative open transaction (via risk_engine /
place_order). Direct ``place_order`` calls cannot bypass an active switch.
"""

from __future__ import annotations

from typing import Any

import asyncpg

from risk_engine import current_global_equity, lock_risk_controls
from observability import emit_event_async, inc_kill_activation, new_correlation_id


class KillSwitchActive(PermissionError):
    """Raised when the global kill switch blocks new entries."""

    def __init__(self, reason: str | None = None, actor: str | None = None) -> None:
        self.reason = reason
        self.actor = actor
        super().__init__(reason or "kill_switch_active")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "kill_switch_active",
            "reason": self.reason,
            "actor": self.actor,
        }


async def get_kill_switch(conn: asyncpg.Connection) -> asyncpg.Record:
    row = await conn.fetchrow("SELECT * FROM kill_switch_state WHERE id = 1")
    if row is None:
        raise RuntimeError("kill_switch_state missing; run migrations")
    return row


async def assert_kill_switch_inactive(conn: asyncpg.Connection) -> None:
    """Hard deny when active. Prefer calling under risk_control_lock."""
    state = await get_kill_switch(conn)
    if state["active"]:
        raise KillSwitchActive(state["reason"], state["actor"])


async def activate_kill_switch(
    conn: asyncpg.Connection,
    *,
    reason: str,
    actor: str,
) -> asyncpg.Record:
    """Persist global halt. Blocks new opens; does not auto-flatten."""
    if not reason or not str(reason).strip():
        raise ValueError("reason is required")
    if not actor or not str(actor).strip():
        raise ValueError("actor is required")

    async with conn.transaction():
        await lock_risk_controls(conn)
        equity = await current_global_equity(conn)
        await conn.execute(
            """
            UPDATE kill_switch_state
            SET active = true,
                reason = $1,
                actor = $2,
                activated_at = NOW(),
                deactivated_at = NULL,
                updated_at = NOW()
            WHERE id = 1
            """,
            str(reason).strip(),
            str(actor).strip(),
        )
        await conn.execute(
            """
            INSERT INTO kill_switch_events (
                event_type, active_after, reason, actor, equity_at_event
            )
            VALUES ('ACTIVATE', true, $1, $2, $3)
            """,
            str(reason).strip(),
            str(actor).strip(),
            equity,
        )
        corr = new_correlation_id()
        inc_kill_activation()
        await emit_event_async(
            conn,
            "kill_activate",
            severity="critical",
            correlation_id=corr,
            actor=str(actor).strip(),
            source="kill_switch",
            reason=str(reason).strip(),
            detail={"equity_at_event": equity},
        )
        return await get_kill_switch(conn)


async def deactivate_kill_switch(
    conn: asyncpg.Connection,
    *,
    reason: str,
    actor: str,
) -> asyncpg.Record:
    """Clear global halt (audited). Does not imply risk limits are healthy."""
    if not reason or not str(reason).strip():
        raise ValueError("reason is required")
    if not actor or not str(actor).strip():
        raise ValueError("actor is required")

    async with conn.transaction():
        await lock_risk_controls(conn)
        equity = await current_global_equity(conn)
        await conn.execute(
            """
            UPDATE kill_switch_state
            SET active = false,
                reason = $1,
                actor = $2,
                deactivated_at = NOW(),
                updated_at = NOW()
            WHERE id = 1
            """,
            str(reason).strip(),
            str(actor).strip(),
        )
        await conn.execute(
            """
            INSERT INTO kill_switch_events (
                event_type, active_after, reason, actor, equity_at_event
            )
            VALUES ('DEACTIVATE', false, $1, $2, $3)
            """,
            str(reason).strip(),
            str(actor).strip(),
            equity,
        )
        corr = new_correlation_id()
        await emit_event_async(
            conn,
            "kill_deactivate",
            severity="info",
            correlation_id=corr,
            actor=str(actor).strip(),
            source="kill_switch",
            reason=str(reason).strip(),
            detail={"equity_at_event": equity},
        )
        return await get_kill_switch(conn)
