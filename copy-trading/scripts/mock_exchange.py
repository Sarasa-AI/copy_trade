"""Paper trading mock exchange backed by PostgreSQL via asyncpg."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from uuid import UUID

import asyncpg

from capital import (
    CapitalAccountMissing,
    InsufficientAvailableBalance,
    get_master_total_capital,
    get_wallet_balance,
    margin_required,
    realize_pnl,
    release_margin,
    reserve_margin,
)
from kill_switch import KillSwitchActive
from observability import (
    Timer,
    emit_event_async,
    inc_order_failure,
    inc_risk_denial,
    new_correlation_id,
    record_order_latency_ms,
)
from risk_engine import (
    DAILY_EQUITY_LOSS_LIMIT_PCT,
    RiskDenied,
    assert_open_allowed,
    record_denial,
)
from wallet_repository import (
    InvalidWalletId,
    WalletNotFound,
    get_wallet_by_id,
    parse_wallet_id,
)

# Compatibility alias — authoritative limit lives in risk_engine.
DAILY_LOSS_LIMIT_PCT = DAILY_EQUITY_LOSS_LIMIT_PCT
MAX_LEVERAGE = 3
STOP_LOSS_MIN = 0.03
STOP_LOSS_MAX = 0.05
TAKE_PROFIT_MIN = 0.03
TAKE_PROFIT_MAX = 0.20
EXECUTION_DELAY_SEC = int(os.getenv("EXECUTION_DELAY_SEC", "30"))

ORDER_STATUSES = frozenset({"PENDING", "FILLED", "REJECTED", "FAILED"})
TERMINAL_ORDER_STATUSES = frozenset({"FILLED", "REJECTED", "FAILED"})

CLOSE_REASONS = frozenset(
    {
        "STOP_LOSS",
        "TAKE_PROFIT",
        "ADMIN",
        "RISK",
        "KILL_SWITCH",
        "LEAD_CLOSE",
    }
)

_pool: asyncpg.Pool | None = None

# Re-export domain wallet / capital errors for callers
__all__ = (
    "CLOSE_REASONS",
    "PositionAlreadyClosed",
    "InvalidCloseReason",
    "InvalidWalletId",
    "WalletNotFound",
    "CapitalAccountMissing",
    "InsufficientAvailableBalance",
    "place_order",
    "close_position",
    "check_daily_loss",
    "get_position",
    "get_pool",
    "close_pool",
)


class PositionAlreadyClosed(Exception):
    """Raised when a close is attempted on an already-closed position."""

    def __init__(self, position_id: UUID | str) -> None:
        self.position_id = position_id
        super().__init__(f"position_already_closed: {position_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "position_already_closed",
            "position_id": str(self.position_id),
        }


class InvalidCloseReason(ValueError):
    """Raised when close_reason is not in the approved vocabulary."""

    def __init__(self, close_reason: str) -> None:
        self.close_reason = close_reason
        super().__init__(f"invalid_close_reason: {close_reason}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "invalid_close_reason",
            "close_reason": self.close_reason,
        }


def _is_wallet_fk_violation(exc: asyncpg.ForeignKeyViolationError) -> bool:
    """True only when the FK failure clearly references wallets / wallet_id."""
    detail = " ".join(
        str(part)
        for part in (getattr(exc, "constraint_name", None), exc.args, getattr(exc, "detail", None))
        if part
    ).lower()
    return (
        "wallet" in detail
        or "wallets" in detail
        or "paper_orders_wallet_id_fkey" in detail
        or "positions_wallet_id_fkey" in detail
    )


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "copytrading")
    if not user or not password:
        raise RuntimeError(
            "DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD environment "
            "variables are required"
        )
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


async def get_pool() -> asyncpg.Pool:
    """Return a process-wide asyncpg pool (created on first use)."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(_database_url())
    return _pool


async def close_pool() -> None:
    """Close the shared pool if it was created."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def _acquire(
    conn: asyncpg.Connection | None = None,
) -> AsyncIterator[asyncpg.Connection]:
    """Yield a connection: reuse caller conn, or acquire one from the pool."""
    if conn is not None:
        yield conn
        return
    pool = await get_pool()
    async with pool.acquire() as acquired:
        yield acquired


def _validate_close_reason(close_reason: str | None) -> str:
    if close_reason is None or str(close_reason).strip() == "":
        raise ValueError("close_reason is required")
    reason_u = str(close_reason).upper()
    if reason_u not in CLOSE_REASONS:
        raise InvalidCloseReason(close_reason)
    return reason_u


def _realized_pnl(
    side: str, entry_price: float, exit_price: float, qty: float
) -> float:
    if side == "BUY":
        return (exit_price - entry_price) * qty
    return (entry_price - exit_price) * qty


def _sl_tp_prices(
    side: str, price: float, stop_loss_pct: float, take_profit_pct: float
) -> tuple[float, float]:
    """Side-aware stop-loss and take-profit absolute prices."""
    if side == "BUY":
        return price * (1 - stop_loss_pct), price * (1 + take_profit_pct)
    return price * (1 + stop_loss_pct), price * (1 - take_profit_pct)


async def _record_terminal_order(
    db: asyncpg.Connection,
    *,
    symbol: str,
    side: str,
    qty: float,
    price: float,
    wallet_id: UUID,
    status: str,
) -> Any:
    if status not in ("REJECTED", "FAILED"):
        raise ValueError(f"terminal failure status required, got {status}")
    return await db.fetchval(
        """
        INSERT INTO paper_orders
            (symbol, side, qty, price, status, wallet_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
        """,
        symbol,
        side,
        qty,
        price,
        status,
        wallet_id,
    )


async def place_order(
    symbol: str,
    side: str,
    qty: float,
    price: float,
    conn: asyncpg.Connection | None = None,
    *,
    wallet_id: UUID | str,
    leverage: float = 1.0,
    stop_loss_pct: float = 0.03,
    take_profit_pct: float = 0.06,
) -> Any:
    """Paper-trading open: delay, then atomic FILLED order + position + reserve.

    Execution delay runs **before** any DB connection is acquired by this
    function. Callers must not hold a pool connection across ``place_order``
    when ``EXECUTION_DELAY_SEC > 0`` (see ``agent_trader.tick``).

    Flow: Intent → sleep → acquire → TX(PENDING→FILLED + position.order_id +
    reserve). On insufficient capital a separate TX records ``REJECTED`` and
    re-raises. Paper fill price may be stale relative to the pre-delay signal;
    that is accepted for the current paper model.

    ``wallet_id`` must be a valid UUID of an existing ``wallets.id`` row.
    """
    if leverage > MAX_LEVERAGE:
        raise ValueError(f"leverage exceeds max {MAX_LEVERAGE}x")
    if not (STOP_LOSS_MIN <= stop_loss_pct <= STOP_LOSS_MAX):
        raise ValueError(
            f"stop_loss_pct must be between {STOP_LOSS_MIN} and {STOP_LOSS_MAX}"
        )
    if not (TAKE_PROFIT_MIN <= take_profit_pct <= TAKE_PROFIT_MAX):
        raise ValueError(
            f"take_profit_pct must be between {TAKE_PROFIT_MIN} and "
            f"{TAKE_PROFIT_MAX}"
        )

    side_u = side.upper()
    if side_u not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")
    if qty <= 0 or price <= 0:
        raise ValueError("qty and price must be positive")

    wid = parse_wallet_id(wallet_id)
    margin = margin_required(qty, price, leverage)
    correlation_id = new_correlation_id()
    timer = Timer()

    # Delay outside connection acquisition by this function.
    await asyncio.sleep(EXECUTION_DELAY_SEC)

    async with _acquire(conn) as db:
        wallet = await get_wallet_by_id(db, wid)
        if wallet is None:
            raise WalletNotFound(wid)

        stop_loss_price, take_profit_price = _sl_tp_prices(
            side_u, price, stop_loss_pct, take_profit_pct
        )

        await emit_event_async(
            db,
            "order_attempt",
            severity="info",
            correlation_id=correlation_id,
            actor="system",
            source="mock_exchange.place_order",
            wallet_id=wid,
            symbol=symbol,
            detail={"side": side_u, "qty": qty, "price": price},
        )

        try:
            async with db.transaction():
                # Authoritative Phase 6/7 gate: kill + fresh mark + equity risk
                # under risk_control_lock before any order/position/reserve write.
                await assert_open_allowed(db, wallet_id=wid, symbol=symbol)

                order_id = await db.fetchval(
                    """
                    INSERT INTO paper_orders
                        (symbol, side, qty, price, status, wallet_id)
                    VALUES ($1, $2, $3, $4, 'PENDING', $5)
                    RETURNING id
                    """,
                    symbol,
                    side_u,
                    qty,
                    price,
                    wid,
                )
                await db.execute(
                    """
                    UPDATE paper_orders
                    SET status = 'FILLED'
                    WHERE id = $1 AND status = 'PENDING'
                    """,
                    order_id,
                )
                position_id = await db.fetchval(
                    """
                    INSERT INTO positions (
                        symbol, entry_price, qty, wallet_id, side,
                        stop_loss_price, take_profit_price,
                        reserved_margin, order_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id
                    """,
                    symbol,
                    price,
                    qty,
                    wid,
                    side_u,
                    stop_loss_price,
                    take_profit_price,
                    margin,
                    order_id,
                )
                await reserve_margin(
                    db,
                    wid,
                    margin,
                    position_id=position_id,
                    note="place_order",
                    actor="system",
                    source="mock_exchange.place_order",
                    reason="open_reserve",
                    correlation_id=correlation_id,
                    order_id=order_id,
                )
                await emit_event_async(
                    db,
                    "risk_allow",
                    severity="info",
                    correlation_id=correlation_id,
                    actor="system",
                    source="mock_exchange.place_order",
                    wallet_id=wid,
                    order_id=order_id,
                    position_id=position_id,
                    symbol=symbol,
                )
        except RiskDenied as exc:
            inc_risk_denial()
            if exc.reason_code in ("MARK_MISSING", "MARK_STALE"):
                from observability import inc_stale_mark

                if exc.reason_code == "MARK_STALE":
                    inc_stale_mark()
            # Persist denial outside the rolled-back open TX.
            async with db.transaction():
                await record_denial(
                    db,
                    reason_code=exc.reason_code,
                    detail=exc.detail,
                    wallet_id=exc.wallet_id,
                    sod_equity=exc.sod_equity,
                    current_equity=exc.current_equity,
                    loss_pct=exc.loss_pct,
                    limit_pct=exc.limit_pct,
                )
                await emit_event_async(
                    db,
                    "risk_deny",
                    severity="critical"
                    if exc.reason_code in ("MARK_MISSING", "MARK_STALE")
                    else "warning",
                    correlation_id=correlation_id,
                    actor="system",
                    source="mock_exchange.place_order",
                    reason=exc.reason_code,
                    wallet_id=wid,
                    symbol=symbol,
                    detail=exc.to_dict(),
                )
            raise
        except KillSwitchActive as exc:
            inc_risk_denial()
            async with db.transaction():
                await record_denial(
                    db,
                    reason_code="KILL_SWITCH_ACTIVE",
                    detail=str(exc.reason or "kill_switch_active"),
                    wallet_id=wid,
                )
                await emit_event_async(
                    db,
                    "risk_deny",
                    severity="warning",
                    correlation_id=correlation_id,
                    actor="system",
                    source="mock_exchange.place_order",
                    reason="KILL_SWITCH_ACTIVE",
                    wallet_id=wid,
                    symbol=symbol,
                )
            raise
        except InsufficientAvailableBalance:
            inc_order_failure()
            async with db.transaction():
                await _record_terminal_order(
                    db,
                    symbol=symbol,
                    side=side_u,
                    qty=qty,
                    price=price,
                    wallet_id=wid,
                    status="REJECTED",
                )
            raise
        except asyncpg.ForeignKeyViolationError as exc:
            if _is_wallet_fk_violation(exc):
                raise WalletNotFound(wid) from exc
            inc_order_failure()
            try:
                async with db.transaction():
                    await _record_terminal_order(
                        db,
                        symbol=symbol,
                        side=side_u,
                        qty=qty,
                        price=price,
                        wallet_id=wid,
                        status="FAILED",
                    )
            except Exception:
                pass
            raise

        record_order_latency_ms(timer.ms())
        print(f"[mock_exchange] placed order wallet_id={wid}", flush=True)
        return order_id


async def close_position(
    position_id: UUID | str,
    exit_price: float,
    conn: asyncpg.Connection | None = None,
    *,
    close_reason: str,
    wallet_id: UUID | str | None = None,
) -> float:
    """Close a position, release margin, realize PnL, and upsert daily_stats.

    ``wallet_id`` is accepted for backward API compatibility only and is
    intentionally unused: close is keyed solely by ``position_id``.
    """
    _ = wallet_id  # unused; retained for call-site compatibility
    if exit_price <= 0:
        raise ValueError("exit_price must be positive")

    reason_u = _validate_close_reason(close_reason)

    async with _acquire(conn) as db:
        async with db.transaction():
            row = await db.fetchrow(
                """
                SELECT *
                FROM positions
                WHERE id = $1 AND closed_at IS NULL
                FOR UPDATE
                """,
                position_id,
            )
            if row is None:
                raise PositionAlreadyClosed(position_id)

            pnl = _realized_pnl(
                row["side"],
                float(row["entry_price"]),
                float(exit_price),
                float(row["qty"]),
            )
            reserved = float(row["reserved_margin"] or 0)
            pos_wallet = row["wallet_id"]

            await db.execute(
                """
                UPDATE positions
                SET pnl = $2,
                    closed_at = NOW(),
                    exit_price = $3,
                    close_reason = $4
                WHERE id = $1
                """,
                position_id,
                pnl,
                exit_price,
                reason_u,
            )

            # Capital settle: release reserved margin then realize PnL.
            # Legacy opens with no wallet_balances and no reserved margin close
            # without capital updates (migration did not invent accounts).
            bal = await get_wallet_balance(db, pos_wallet)
            if bal is None:
                if reserved > 0:
                    raise CapitalAccountMissing(pos_wallet)
            else:
                await release_margin(
                    db,
                    pos_wallet,
                    reserved,
                    position_id=row["id"],
                    note="close_position",
                )
                await realize_pnl(
                    db,
                    pos_wallet,
                    pnl,
                    position_id=row["id"],
                    note="close_position",
                )

            equity_base = await get_master_total_capital(db)
            await db.execute(
                """
                INSERT INTO daily_stats (date, total_pnl, trade_count, max_drawdown)
                VALUES (
                    CURRENT_DATE,
                    $1::numeric,
                    1,
                    GREATEST(
                        0::numeric,
                        ($2::numeric - ($2::numeric + $1::numeric)) / $2::numeric
                    )
                )
                ON CONFLICT (date) DO UPDATE SET
                    total_pnl = daily_stats.total_pnl + EXCLUDED.total_pnl,
                    trade_count = daily_stats.trade_count + 1,
                    max_drawdown = GREATEST(
                        daily_stats.max_drawdown,
                        GREATEST(
                            0::numeric,
                            (
                                $2::numeric
                                - ($2::numeric + daily_stats.total_pnl + EXCLUDED.total_pnl)
                            ) / $2::numeric
                        )
                    )
                """,
                pnl,
                equity_base,
            )
            return pnl


async def check_daily_loss(conn: asyncpg.Connection | None = None) -> bool:
    """COMPATIBILITY ONLY — realized daily_stats check.

    **Not authoritative for opens.** Phase 6 Risk Engine
    (``risk_engine.assert_open_allowed``) enforces equity drawdown
    (realized + unrealized vs UTC SoD) inside ``place_order``.

    Returns True when today's ``daily_stats.total_pnl`` loss exceeds
    ``DAILY_LOSS_LIMIT_PCT`` of master pool total — soft pre-check only.
    """
    async with _acquire(conn) as db:
        total_pnl = await db.fetchval(
            """
            SELECT total_pnl
            FROM daily_stats
            WHERE date = CURRENT_DATE
            """
        )
        if total_pnl is None:
            return False
        equity_base = await get_master_total_capital(db)
        return float(total_pnl) < -(equity_base * DAILY_LOSS_LIMIT_PCT)


async def get_position(
    symbol: str,
    conn: asyncpg.Connection | None = None,
    *,
    wallet_id: UUID | str,
) -> asyncpg.Record | None:
    """Return the most recent open position for ``wallet_id`` + ``symbol``.

    Lifecycle ops should prefer ``position_id``. Symbol-only lookup is
    intentionally unsupported (multi-wallet ambiguity).
    """
    wid = parse_wallet_id(wallet_id)
    async with _acquire(conn) as db:
        return await db.fetchrow(
            """
            SELECT id, symbol, entry_price, qty, pnl, side, opened_at, wallet_id
            FROM positions
            WHERE symbol = $1 AND wallet_id = $2 AND closed_at IS NULL
            ORDER BY opened_at DESC
            LIMIT 1
            """,
            symbol,
            wid,
        )
