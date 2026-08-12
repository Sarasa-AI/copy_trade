"""Phase 6 — Minimal emergency flatten (manual / administrative).

HALT NEW ENTRIES (kill switch) + authorized close of open positions through
the existing ``close_position`` path. Never bypasses ``capital.py``.

P7-013: every position yields an explicit outcome; residual → NOT_SAFE.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from kill_switch import activate_kill_switch, get_kill_switch
from mock_exchange import PositionAlreadyClosed, close_position
from observability import (
    emit_event_async,
    new_correlation_id,
    set_flatten_residual,
    set_open_exposure,
)


async def list_open_positions(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    return list(
        await conn.fetch(
            """
            SELECT id, wallet_id, symbol, side, entry_price, qty, reserved_margin
            FROM positions
            WHERE closed_at IS NULL
            ORDER BY opened_at
            """
        )
    )


async def flatten_position(
    conn: asyncpg.Connection,
    position_id: UUID | str,
    exit_price: float,
    *,
    actor: str,
    reason: str = "emergency_flatten",
    close_reason: str = "KILL_SWITCH",
    correlation_id: UUID | None = None,
) -> dict[str, Any]:
    """Close one open position via the transactional close path."""
    if exit_price <= 0:
        raise ValueError("exit_price must be positive")
    if not actor or not str(actor).strip():
        raise ValueError("actor is required")

    try:
        pnl = await close_position(
            position_id,
            exit_price,
            conn,
            close_reason=close_reason,
        )
        return {
            "position_id": str(position_id),
            "status": "closed",
            "pnl": float(pnl),
            "actor": str(actor).strip(),
            "reason": reason,
            "close_reason": close_reason,
        }
    except PositionAlreadyClosed:
        return {
            "position_id": str(position_id),
            "status": "already_closed",
            "actor": str(actor).strip(),
            "reason": reason,
            "close_reason": close_reason,
        }
    except Exception as exc:  # noqa: BLE001 — classify per-position failure
        return {
            "position_id": str(position_id),
            "status": "failed",
            "error": str(exc),
            "actor": str(actor).strip(),
            "reason": reason,
            "close_reason": close_reason,
        }


async def emergency_halt_and_flatten(
    conn: asyncpg.Connection,
    *,
    actor: str,
    reason: str,
    exit_prices: dict[str, float],
    activate_kill: bool = True,
) -> dict[str, Any]:
    """Activate kill switch (optional) then flatten all open positions.

    ``exit_prices`` maps symbol → mark/exit price. Positions whose symbol is
    missing from the map are reported as ``skipped_no_price`` (no partial
    capital mutation for that row).

    Aggregate ``status`` is ``SAFE`` only when ``residual_count == 0``.
    """
    correlation_id = new_correlation_id()
    await emit_event_async(
        conn,
        "flatten_start",
        severity="warning",
        correlation_id=correlation_id,
        actor=actor,
        source="emergency_control",
        reason=reason,
    )

    if activate_kill:
        await activate_kill_switch(conn, reason=reason, actor=actor)

    results: list[dict[str, Any]] = []
    opens = await list_open_positions(conn)
    set_open_exposure(float(len(opens)))
    for pos in opens:
        symbol = pos["symbol"]
        price = exit_prices.get(symbol)
        if price is None or float(price) <= 0:
            results.append(
                {
                    "position_id": str(pos["id"]),
                    "status": "skipped_no_price",
                    "symbol": symbol,
                }
            )
            continue
        results.append(
            await flatten_position(
                conn,
                pos["id"],
                float(price),
                actor=actor,
                reason=reason,
                close_reason="KILL_SWITCH",
                correlation_id=correlation_id,
            )
        )

    closed_count = sum(1 for r in results if r["status"] == "closed")
    already_closed_count = sum(
        1 for r in results if r["status"] == "already_closed"
    )
    skipped_no_price_count = sum(
        1 for r in results if r["status"] == "skipped_no_price"
    )
    failed_count = sum(1 for r in results if r["status"] == "failed")
    residual_ids = [
        r["position_id"]
        for r in results
        if r["status"] in ("skipped_no_price", "failed")
    ]
    # Also count any positions still open after attempts.
    still_open = await list_open_positions(conn)
    for row in still_open:
        pid = str(row["id"])
        if pid not in residual_ids:
            residual_ids.append(pid)
    residual_count = len(residual_ids)
    status = "SAFE" if residual_count == 0 else "NOT_SAFE"
    set_flatten_residual(float(residual_count))

    state = await get_kill_switch(conn)
    aggregate = {
        "kill_switch_active": bool(state["active"]),
        "kill_reason": state["reason"],
        "actor": actor,
        "correlation_id": str(correlation_id),
        "results": results,
        "closed_count": closed_count,
        "already_closed_count": already_closed_count,
        "skipped_no_price_count": skipped_no_price_count,
        "failed_count": failed_count,
        "residual_count": residual_count,
        "residual_position_ids": residual_ids,
        "status": status,
    }
    await emit_event_async(
        conn,
        "flatten_complete",
        severity="info" if status == "SAFE" else "critical",
        correlation_id=correlation_id,
        actor=actor,
        source="emergency_control",
        reason=reason,
        detail={
            "status": status,
            "residual_count": residual_count,
            "closed_count": closed_count,
        },
    )
    if residual_count > 0:
        await emit_event_async(
            conn,
            "flatten_residual",
            severity="critical",
            correlation_id=correlation_id,
            actor=actor,
            source="emergency_control",
            reason="residual_exposure",
            detail={
                "residual_count": residual_count,
                "residual_position_ids": residual_ids,
            },
        )
    return aggregate
