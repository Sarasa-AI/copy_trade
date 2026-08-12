"""Phase 6 — Equity Risk Engine (authoritative open-path control).

Authority: realized + unrealized equity vs UTC Start-of-Day (SoD) snapshot.
``check_daily_loss`` in mock_exchange is compatibility-only and MUST NOT be
treated as the final authority for opens.

PnL label: GROSS / PRE-COST PAPER PnL (fees/slippage/funding not modeled).
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import asyncpg

from wallet_repository import parse_wallet_id

DAILY_EQUITY_LOSS_LIMIT_PCT = float(
    os.getenv("DAILY_EQUITY_LOSS_LIMIT_PCT", "0.03")
)


class RiskDenied(PermissionError):
    """Raised when the Risk Engine denies a new open."""

    def __init__(
        self,
        reason_code: str,
        *,
        detail: str | None = None,
        wallet_id: UUID | None = None,
        sod_equity: float | None = None,
        current_equity: float | None = None,
        loss_pct: float | None = None,
        limit_pct: float | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.detail = detail
        self.wallet_id = wallet_id
        self.sod_equity = sod_equity
        self.current_equity = current_equity
        self.loss_pct = loss_pct
        self.limit_pct = limit_pct
        super().__init__(detail or reason_code)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "risk_denied",
            "reason_code": self.reason_code,
            "detail": self.detail,
            "wallet_id": str(self.wallet_id) if self.wallet_id else None,
            "sod_equity": self.sod_equity,
            "current_equity": self.current_equity,
            "loss_pct": self.loss_pct,
            "limit_pct": self.limit_pct,
        }


def utc_today(now: datetime | None = None) -> date:
    """UTC calendar date used for SoD / day-boundary risk."""
    ts = now if now is not None else datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).date()


async def lock_risk_controls(conn: asyncpg.Connection) -> None:
    """Serialize authoritative risk / kill decisions (singleton row)."""
    row = await conn.fetchrow(
        "SELECT id FROM risk_control_lock WHERE id = 1 FOR UPDATE"
    )
    if row is None:
        raise RuntimeError("risk_control_lock missing; run migrations")


async def current_global_equity(conn: asyncpg.Connection) -> float:
    """Authoritative paper equity: sum of wallet current_equity (incl. unrealized)."""
    total = await conn.fetchval(
        """
        SELECT COALESCE(SUM(current_equity), 0)
        FROM wallet_balances
        """
    )
    return float(total or 0)


async def current_wallet_equity(
    conn: asyncpg.Connection, wallet_id: UUID | str
) -> float:
    wid = parse_wallet_id(wallet_id)
    val = await conn.fetchval(
        """
        SELECT current_equity
        FROM wallet_balances
        WHERE wallet_id = $1
        """,
        wid,
    )
    if val is None:
        return 0.0
    return float(val)


async def ensure_global_sod(
    conn: asyncpg.Connection,
    *,
    as_of: date | None = None,
    now: datetime | None = None,
) -> float:
    """Return SoD equity for the UTC date, creating a snapshot if missing.

    Must be called while ``risk_control_lock`` is held so concurrent first
    opens of the day cannot create divergent SoD baselines.
    """
    day = as_of if as_of is not None else utc_today(now)
    existing = await conn.fetchval(
        """
        SELECT equity
        FROM equity_sod_snapshots
        WHERE as_of_date = $1 AND scope = 'GLOBAL'
        """,
        day,
    )
    if existing is not None:
        return float(existing)

    equity = await current_global_equity(conn)
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'GLOBAL', NULL, $2)
        ON CONFLICT DO NOTHING
        """,
        day,
        equity,
    )
    # Re-read in case of race under weaker isolation (lock should prevent).
    stored = await conn.fetchval(
        """
        SELECT equity
        FROM equity_sod_snapshots
        WHERE as_of_date = $1 AND scope = 'GLOBAL'
        """,
        day,
    )
    return float(stored)


async def ensure_wallet_sod(
    conn: asyncpg.Connection,
    wallet_id: UUID | str,
    *,
    as_of: date | None = None,
    now: datetime | None = None,
) -> float:
    wid = parse_wallet_id(wallet_id)
    day = as_of if as_of is not None else utc_today(now)
    existing = await conn.fetchval(
        """
        SELECT equity
        FROM equity_sod_snapshots
        WHERE as_of_date = $1 AND scope = 'WALLET' AND wallet_id = $2
        """,
        day,
        wid,
    )
    if existing is not None:
        return float(existing)

    equity = await current_wallet_equity(conn, wid)
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'WALLET', $2, $3)
        ON CONFLICT DO NOTHING
        """,
        day,
        wid,
        equity,
    )
    stored = await conn.fetchval(
        """
        SELECT equity
        FROM equity_sod_snapshots
        WHERE as_of_date = $1 AND scope = 'WALLET' AND wallet_id = $2
        """,
        day,
        wid,
    )
    return float(stored)


async def adjust_sod_for_equity_funding(
    conn: asyncpg.Connection,
    *,
    delta: float,
    wallet_id: UUID | str | None = None,
    now: datetime | None = None,
) -> None:
    """Policy B — bump existing same-day SoD when capital enters risk equity.

    Classification (paper capital model):
    - MASTER_INIT: funds master_pool only — does **not** change
      ``SUM(wallet_balances.current_equity)`` → no SoD adjust.
    - allocate_to_wallet: capital enters the risk equity perimeter → adjust
      GLOBAL SoD (and WALLET SoD when present) by ``delta``.
    - No separate internal-transfer API exists.

    Must run under ``risk_control_lock``. No-ops when no snapshot exists yet
    for today (first open will capture post-funding equity naturally).
    """
    if delta <= 0:
        return
    day = utc_today(now)
    await conn.execute(
        """
        UPDATE equity_sod_snapshots
        SET equity = equity + $2
        WHERE as_of_date = $1 AND scope = 'GLOBAL'
        """,
        day,
        float(delta),
    )
    if wallet_id is not None:
        wid = parse_wallet_id(wallet_id)
        await conn.execute(
            """
            UPDATE equity_sod_snapshots
            SET equity = equity + $3
            WHERE as_of_date = $1 AND scope = 'WALLET' AND wallet_id = $2
            """,
            day,
            wid,
            float(delta),
        )


async def record_denial(
    conn: asyncpg.Connection,
    *,
    reason_code: str,
    detail: str | None = None,
    wallet_id: UUID | None = None,
    sod_equity: float | None = None,
    current_equity: float | None = None,
    loss_pct: float | None = None,
    limit_pct: float | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO risk_denials (
            reason_code, detail, wallet_id,
            sod_equity, current_equity, loss_pct, limit_pct
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        reason_code,
        detail,
        wallet_id,
        sod_equity,
        current_equity,
        loss_pct,
        limit_pct,
    )


def _loss_pct(sod: float, current: float) -> float:
    if sod <= 0:
        # No positive baseline: any negative equity is a full loss signal.
        return 1.0 if current < sod else 0.0
    loss = sod - current
    if loss <= 0:
        return 0.0
    return loss / sod


async def assert_open_allowed(
    conn: asyncpg.Connection,
    *,
    wallet_id: UUID | str | None = None,
    symbol: str | None = None,
    limit_pct: float | None = None,
    now: datetime | None = None,
) -> None:
    """Authoritative risk gate. Caller must hold an open transaction.

    Order under ``risk_control_lock``:
    1. Kill switch
    2. Fresh required mark for ``symbol`` (fail-closed)
    3. SoD / equity drawdown
    """
    await lock_risk_controls(conn)
    # Local import avoids circular import at module load.
    from kill_switch import assert_kill_switch_inactive
    import price_feed

    await assert_kill_switch_inactive(conn)

    if not symbol or not str(symbol).strip():
        raise ValueError("symbol is required for assert_open_allowed mark gate")
    sym = str(symbol).strip()
    wid = parse_wallet_id(wallet_id) if wallet_id is not None else None
    try:
        price_feed.require_fresh_mark(sym)
    except price_feed.MarkMissing:
        raise RiskDenied(
            "MARK_MISSING",
            detail=f"no mark available for {sym}; denying new exposure",
            wallet_id=wid,
        )
    except price_feed.MarkStale as exc:
        raise RiskDenied(
            "MARK_STALE",
            detail=(
                f"stale mark for {sym}: age_sec={exc.age_sec:.3f} "
                f"max_age_sec={exc.max_age_sec:.3f}; denying new exposure"
            ),
            wallet_id=wid,
        )

    pct = float(limit_pct if limit_pct is not None else DAILY_EQUITY_LOSS_LIMIT_PCT)
    if pct < 0:
        raise ValueError("limit_pct must be non-negative")

    day = utc_today(now)
    sod = await ensure_global_sod(conn, as_of=day, now=now)
    current = await current_global_equity(conn)
    loss = _loss_pct(sod, current)
    if loss > pct + 1e-12:
        detail = (
            f"global equity drawdown {loss:.6f} exceeds limit {pct:.6f} "
            f"(sod={sod} current={current} date={day.isoformat()} UTC)"
        )
        raise RiskDenied(
            "GLOBAL_EQUITY_DRAWDOWN",
            detail=detail,
            wallet_id=wid,
            sod_equity=sod,
            current_equity=current,
            loss_pct=loss,
            limit_pct=pct,
        )

    if wallet_id is not None:
        assert wid is not None
        w_sod = await ensure_wallet_sod(conn, wid, as_of=day, now=now)
        w_cur = await current_wallet_equity(conn, wid)
        w_loss = _loss_pct(w_sod, w_cur)
        if w_loss > pct + 1e-12:
            detail = (
                f"wallet equity drawdown {w_loss:.6f} exceeds limit {pct:.6f} "
                f"(sod={w_sod} current={w_cur} wallet={wid} "
                f"date={day.isoformat()} UTC)"
            )
            raise RiskDenied(
                "WALLET_EQUITY_DRAWDOWN",
                detail=detail,
                wallet_id=wid,
                sod_equity=w_sod,
                current_equity=w_cur,
                loss_pct=w_loss,
                limit_pct=pct,
            )
