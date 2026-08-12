"""Master pool + isolated wallet capital accounting.

Schema (Open Decision #4): ``master_pool``, ``wallet_balances``, ``capital_ledger``.
APIs: allocate, reserve_margin, release_margin, realize_pnl, mark_unrealized.
"""

from __future__ import annotations

import os
from typing import Any
from uuid import UUID

import asyncpg

from wallet_repository import parse_wallet_id

MASTER_POOL_USDT = float(os.getenv("MASTER_POOL_USDT", "100000"))
AGENT_ALLOCATION_USDT = float(os.getenv("AGENT_ALLOCATION_USDT", "10000"))

LEDGER_TYPES = frozenset(
    {
        "MASTER_INIT",
        "ALLOCATE",
        "RESERVE",
        "RELEASE",
        "REALIZE_PNL",
        "MARK_UNREALIZED",
    }
)


class CapitalAccountMissing(LookupError):
    """Raised when a wallet has no ``wallet_balances`` row."""

    def __init__(self, wallet_id: UUID) -> None:
        self.wallet_id = wallet_id
        super().__init__(f"capital_account_missing: {wallet_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "capital_account_missing",
            "wallet_id": str(self.wallet_id),
        }


class InsufficientAvailableBalance(ValueError):
    """Raised when available balance cannot cover a reserve request."""

    def __init__(
        self,
        wallet_id: UUID,
        required: float,
        available: float,
    ) -> None:
        self.wallet_id = wallet_id
        self.required = required
        self.available = available
        super().__init__(
            f"insufficient_available_balance: wallet={wallet_id} "
            f"required={required} available={available}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "insufficient_available_balance",
            "wallet_id": str(self.wallet_id),
            "required": self.required,
            "available": self.available,
        }


class InsufficientMasterCapital(ValueError):
    """Raised when master pool available capital cannot fund an allocation."""

    def __init__(self, required: float, available: float) -> None:
        self.required = required
        self.available = available
        super().__init__(
            f"insufficient_master_capital: required={required} "
            f"available={available}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "insufficient_master_capital",
            "required": self.required,
            "available": self.available,
        }


class InsufficientReservedMargin(ValueError):
    """Raised when release amount exceeds currently reserved margin."""

    def __init__(
        self,
        wallet_id: UUID,
        required: float,
        reserved: float,
    ) -> None:
        self.wallet_id = wallet_id
        self.required = required
        self.reserved = reserved
        super().__init__(
            f"insufficient_reserved_margin: wallet={wallet_id} "
            f"required={required} reserved={reserved}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "insufficient_reserved_margin",
            "wallet_id": str(self.wallet_id),
            "required": self.required,
            "reserved": self.reserved,
        }


class DuplicateCapitalSettlement(ValueError):
    """Raised when a position already has a settlement ledger entry."""

    def __init__(self, entry_type: str, position_id: UUID) -> None:
        self.entry_type = entry_type
        self.position_id = position_id
        super().__init__(
            f"duplicate_capital_settlement: entry_type={entry_type} "
            f"position_id={position_id}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "duplicate_capital_settlement",
            "entry_type": self.entry_type,
            "position_id": str(self.position_id),
        }


def margin_required(qty: float, price: float, leverage: float) -> float:
    """Notional / leverage — amount to reserve from available balance."""
    if leverage <= 0:
        raise ValueError("leverage must be positive")
    if qty <= 0 or price <= 0:
        raise ValueError("qty and price must be positive")
    return (qty * price) / leverage


def _equity(
    available: float, reserved: float, unrealized: float
) -> float:
    return available + reserved + unrealized


async def get_master_pool(conn: asyncpg.Connection) -> asyncpg.Record:
    """Return the singleton master pool row (unlocked read).

    Callers that need exclusive access must use ``_lock_master_pool``.
    """
    row = await conn.fetchrow("SELECT * FROM master_pool WHERE id = 1")
    if row is None:
        raise RuntimeError("master_pool singleton missing; run migrations")
    return row


async def get_master_total_capital(conn: asyncpg.Connection) -> float:
    """Authority equity base for daily-loss / drawdown (DB, not hardcoded)."""
    total = await conn.fetchval(
        "SELECT total_capital FROM master_pool WHERE id = 1"
    )
    if total is None:
        raise RuntimeError("master_pool singleton missing; run migrations")
    return float(total)


async def get_wallet_balance(
    conn: asyncpg.Connection, wallet_id: UUID | str
) -> asyncpg.Record | None:
    wid = parse_wallet_id(wallet_id)
    return await conn.fetchrow(
        "SELECT * FROM wallet_balances WHERE wallet_id = $1", wid
    )


async def _lock_wallet_balance(
    conn: asyncpg.Connection, wallet_id: UUID
) -> asyncpg.Record:
    row = await conn.fetchrow(
        """
        SELECT *
        FROM wallet_balances
        WHERE wallet_id = $1
        FOR UPDATE
        """,
        wallet_id,
    )
    if row is None:
        raise CapitalAccountMissing(wallet_id)
    return row


async def _lock_master_pool(conn: asyncpg.Connection) -> asyncpg.Record:
    row = await conn.fetchrow(
        "SELECT * FROM master_pool WHERE id = 1 FOR UPDATE"
    )
    if row is None:
        raise RuntimeError("master_pool singleton missing; run migrations")
    return row


async def _ledger(
    conn: asyncpg.Connection,
    *,
    entry_type: str,
    amount: float,
    wallet_id: UUID | None = None,
    available_after: float | None = None,
    reserved_after: float | None = None,
    position_id: UUID | None = None,
    note: str | None = None,
    actor: str | None = None,
    source: str | None = None,
    reason: str | None = None,
    correlation_id: UUID | None = None,
    order_id: UUID | None = None,
) -> None:
    if entry_type not in LEDGER_TYPES:
        raise ValueError(f"invalid ledger entry_type: {entry_type}")
    await conn.execute(
        """
        INSERT INTO capital_ledger (
            wallet_id, entry_type, amount,
            balance_after_available, balance_after_reserved,
            position_id, note,
            actor, source, reason, correlation_id, order_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        wallet_id,
        entry_type,
        amount,
        available_after,
        reserved_after,
        position_id,
        note,
        actor,
        source,
        reason,
        correlation_id,
        order_id,
    )


async def _assert_no_settlement_ledger(
    conn: asyncpg.Connection,
    *,
    entry_type: str,
    position_id: UUID | None,
) -> None:
    """Reject duplicate per-position RESERVE / RELEASE / REALIZE_PNL."""
    if position_id is None:
        return
    if entry_type not in ("RESERVE", "RELEASE", "REALIZE_PNL"):
        return
    exists = await conn.fetchval(
        """
        SELECT 1
        FROM capital_ledger
        WHERE position_id = $1 AND entry_type = $2
        LIMIT 1
        """,
        position_id,
        entry_type,
    )
    if exists is not None:
        raise DuplicateCapitalSettlement(entry_type, position_id)


async def ensure_master_pool(
    conn: asyncpg.Connection,
    *,
    total_capital: float | None = None,
) -> asyncpg.Record:
    """Create singleton master pool if missing (idempotent for tests/ops)."""
    total = float(total_capital if total_capital is not None else MASTER_POOL_USDT)
    if total < 0:
        raise ValueError("total_capital must be non-negative")
    existing = await conn.fetchrow("SELECT * FROM master_pool WHERE id = 1")
    if existing is not None:
        return existing
    await conn.execute(
        """
        INSERT INTO master_pool (
            id, total_capital, allocated_capital, available_capital
        )
        VALUES (1, $1, 0, $1)
        """,
        total,
    )
    await _ledger(
        conn,
        entry_type="MASTER_INIT",
        amount=total,
        note="ensure_master_pool",
    )
    return await get_master_pool(conn)


async def allocate_to_wallet(
    conn: asyncpg.Connection,
    wallet_id: UUID | str,
    amount: float,
    *,
    note: str | None = None,
) -> asyncpg.Record:
    """Transfer ``amount`` from master available into a new wallet balance row.

    New wallets receive a ``wallet_balances`` row. Existing funded wallets are
    rejected (isolation: one initial allocation per agent in paper).
    """
    wid = parse_wallet_id(wallet_id)
    if amount <= 0:
        raise ValueError("allocation amount must be positive")

    async with conn.transaction():
        existing = await conn.fetchrow(
            """
            SELECT wallet_id FROM wallet_balances
            WHERE wallet_id = $1 FOR UPDATE
            """,
            wid,
        )
        if existing is not None:
            raise ValueError(f"wallet already has capital allocation: {wid}")

        await _lock_master_pool(conn)
        master = await get_master_pool(conn)
        available_master = float(master["available_capital"])
        if amount > available_master:
            raise InsufficientMasterCapital(amount, available_master)

        await conn.execute(
            """
            UPDATE master_pool
            SET allocated_capital = allocated_capital + $1,
                available_capital = available_capital - $1,
                updated_at = NOW()
            WHERE id = 1
            """,
            amount,
        )
        row = await conn.fetchrow(
            """
            INSERT INTO wallet_balances (
                wallet_id, initial_capital, current_equity,
                available_balance, reserved_margin, unrealized_pnl, realized_pnl
            )
            VALUES ($1, $2, $2, $2, 0, 0, 0)
            RETURNING *
            """,
            wid,
            amount,
        )
        await _ledger(
            conn,
            entry_type="ALLOCATE",
            amount=amount,
            wallet_id=wid,
            available_after=amount,
            reserved_after=0.0,
            note=note or "allocate_to_wallet",
        )
        # Policy B: capital entering wallet equity adjusts same-day SoD baseline.
        from risk_engine import adjust_sod_for_equity_funding, lock_risk_controls

        await lock_risk_controls(conn)
        await adjust_sod_for_equity_funding(
            conn, delta=amount, wallet_id=wid
        )
        assert row is not None
        return row


async def ensure_funded_wallet(
    conn: asyncpg.Connection,
    wallet_id: UUID | str,
    amount: float | None = None,
) -> asyncpg.Record:
    """Allocate ``amount`` to wallet if it has no balance row yet.

    If the master pool lacks available capital, expands ``total_capital`` so
    paper/tests can keep funding new agents without manual ops.
    """
    wid = parse_wallet_id(wallet_id)
    existing = await get_wallet_balance(conn, wid)
    if existing is not None:
        return existing

    alloc = float(amount if amount is not None else AGENT_ALLOCATION_USDT)
    if alloc <= 0:
        raise ValueError("funding amount must be positive")

    async with conn.transaction():
        await ensure_master_pool(conn)
        master = await _lock_master_pool(conn)
        available = float(master["available_capital"])
        if available < alloc:
            delta = alloc - available
            await conn.execute(
                """
                UPDATE master_pool
                SET total_capital = total_capital + $1,
                    available_capital = available_capital + $1,
                    updated_at = NOW()
                WHERE id = 1
                """,
                delta,
            )
            await _ledger(
                conn,
                entry_type="MASTER_INIT",
                amount=delta,
                note="ensure_funded_wallet master expansion",
            )

        # allocate_to_wallet opens a nested savepoint transaction
        return await allocate_to_wallet(
            conn, wid, alloc, note="ensure_funded_wallet"
        )


async def initialize_paper_allocations(
    conn: asyncpg.Connection,
    *,
    per_agent: float | None = None,
    master_total: float | None = None,
) -> int:
    """Ensure master pool exists and fund wallets that lack balances.

    Returns the number of newly funded wallets.
    """
    await ensure_master_pool(conn, total_capital=master_total)
    amount = float(
        per_agent if per_agent is not None else AGENT_ALLOCATION_USDT
    )
    if amount <= 0:
        raise ValueError("per_agent allocation must be positive")

    wallets = await conn.fetch(
        """
        SELECT w.id
        FROM wallets w
        LEFT JOIN wallet_balances b ON b.wallet_id = w.id
        WHERE b.wallet_id IS NULL
        ORDER BY w.last_updated, w.id
        """
    )
    funded = 0
    for wallet in wallets:
        master = await get_master_pool(conn)
        avail = float(master["available_capital"])
        if avail <= 0:
            break
        alloc = min(amount, avail)
        try:
            await allocate_to_wallet(
                conn,
                wallet["id"],
                alloc,
                note="initialize_paper_allocations",
            )
        except InsufficientMasterCapital:
            break
        funded += 1
    return funded


async def reserve_margin(
    conn: asyncpg.Connection,
    wallet_id: UUID | str,
    amount: float,
    *,
    position_id: UUID | None = None,
    note: str | None = None,
    actor: str | None = None,
    source: str | None = None,
    reason: str | None = None,
    correlation_id: UUID | None = None,
    order_id: UUID | None = None,
) -> asyncpg.Record:
    """Move ``amount`` from available → reserved (row-locked).

    Opens its own transaction (SAVEPOINT when nested under an outer TX) so
    balance update + ledger commit or roll back together.
    """
    wid = parse_wallet_id(wallet_id)
    if amount <= 0:
        raise ValueError("reserve amount must be positive")

    async with conn.transaction():
        await _assert_no_settlement_ledger(
            conn, entry_type="RESERVE", position_id=position_id
        )
        bal = await _lock_wallet_balance(conn, wid)
        available = float(bal["available_balance"])
        if amount > available:
            raise InsufficientAvailableBalance(wid, amount, available)

        reserved = float(bal["reserved_margin"]) + amount
        available_after = available - amount
        unrealized = float(bal["unrealized_pnl"])
        equity = _equity(available_after, reserved, unrealized)

        row = await conn.fetchrow(
            """
            UPDATE wallet_balances
            SET available_balance = $2,
                reserved_margin = $3,
                current_equity = $4,
                updated_at = NOW()
            WHERE wallet_id = $1
            RETURNING *
            """,
            wid,
            available_after,
            reserved,
            equity,
        )
        await _ledger(
            conn,
            entry_type="RESERVE",
            amount=amount,
            wallet_id=wid,
            available_after=available_after,
            reserved_after=reserved,
            position_id=position_id,
            note=note or "reserve_margin",
            actor=actor or "system",
            source=source or "capital.reserve_margin",
            reason=reason or note or "reserve_margin",
            correlation_id=correlation_id,
            order_id=order_id,
        )
        assert row is not None
        return row


async def release_margin(
    conn: asyncpg.Connection,
    wallet_id: UUID | str,
    amount: float,
    *,
    position_id: UUID | None = None,
    note: str | None = None,
    actor: str | None = None,
    source: str | None = None,
    reason: str | None = None,
    correlation_id: UUID | None = None,
    order_id: UUID | None = None,
) -> asyncpg.Record:
    """Move ``amount`` from reserved → available (row-locked).

    Over-release is rejected (no silent clamp). Opens its own transaction
    (SAVEPOINT when nested) so balance + ledger stay atomic.
    """
    wid = parse_wallet_id(wallet_id)
    if amount < 0:
        raise ValueError("release amount must be non-negative")

    async with conn.transaction():
        if amount == 0:
            return await _lock_wallet_balance(conn, wid)

        await _assert_no_settlement_ledger(
            conn, entry_type="RELEASE", position_id=position_id
        )
        bal = await _lock_wallet_balance(conn, wid)
        reserved = float(bal["reserved_margin"])
        if amount > reserved:
            raise InsufficientReservedMargin(wid, amount, reserved)

        available_after = float(bal["available_balance"]) + amount
        reserved_after = reserved - amount
        unrealized = float(bal["unrealized_pnl"])
        equity = _equity(available_after, reserved_after, unrealized)

        row = await conn.fetchrow(
            """
            UPDATE wallet_balances
            SET available_balance = $2,
                reserved_margin = $3,
                current_equity = $4,
                updated_at = NOW()
            WHERE wallet_id = $1
            RETURNING *
            """,
            wid,
            available_after,
            reserved_after,
            equity,
        )
        await _ledger(
            conn,
            entry_type="RELEASE",
            amount=amount,
            wallet_id=wid,
            available_after=available_after,
            reserved_after=reserved_after,
            position_id=position_id,
            note=note or "release_margin",
            actor=actor or "system",
            source=source or "capital.release_margin",
            reason=reason or note or "release_margin",
            correlation_id=correlation_id,
            order_id=order_id,
        )
        assert row is not None
        return row


async def realize_pnl(
    conn: asyncpg.Connection,
    wallet_id: UUID | str,
    pnl: float,
    *,
    position_id: UUID | None = None,
    note: str | None = None,
    actor: str | None = None,
    source: str | None = None,
    reason: str | None = None,
    correlation_id: UUID | None = None,
    order_id: UUID | None = None,
) -> asyncpg.Record:
    """Apply realized PnL to available balance and realized_pnl (row-locked).

    Caller must ``release_margin`` for the position first. Negative PnL that
    would drive ``available_balance`` below zero raises
    ``InsufficientAvailableBalance`` (paper insolvency guard).

    Opens its own transaction (SAVEPOINT when nested) so balance + ledger
    stay atomic.
    """
    wid = parse_wallet_id(wallet_id)

    async with conn.transaction():
        await _assert_no_settlement_ledger(
            conn, entry_type="REALIZE_PNL", position_id=position_id
        )
        bal = await _lock_wallet_balance(conn, wid)
        available = float(bal["available_balance"]) + pnl
        if available < 0:
            raise InsufficientAvailableBalance(
                wid, abs(pnl), float(bal["available_balance"])
            )

        reserved = float(bal["reserved_margin"])
        realized = float(bal["realized_pnl"]) + pnl
        # Unrealized for this closed position is cleared by mark_unrealized
        unrealized = float(bal["unrealized_pnl"])
        equity = _equity(available, reserved, unrealized)

        row = await conn.fetchrow(
            """
            UPDATE wallet_balances
            SET available_balance = $2,
                realized_pnl = $3,
                current_equity = $4,
                updated_at = NOW()
            WHERE wallet_id = $1
            RETURNING *
            """,
            wid,
            available,
            realized,
            equity,
        )
        await _ledger(
            conn,
            entry_type="REALIZE_PNL",
            amount=pnl,
            wallet_id=wid,
            available_after=available,
            reserved_after=reserved,
            position_id=position_id,
            note=note or "realize_pnl",
            actor=actor or "system",
            source=source or "capital.realize_pnl",
            reason=reason or note or "realize_pnl",
            correlation_id=correlation_id,
            order_id=order_id,
        )
        assert row is not None
        return row


def _position_unrealized(
    side: str, entry_price: float, qty: float, mark_price: float
) -> float:
    if side == "BUY":
        return (mark_price - entry_price) * qty
    return (entry_price - mark_price) * qty


async def mark_unrealized(
    conn: asyncpg.Connection,
    *,
    mark_prices: dict[str, float],
) -> int:
    """Recompute per-wallet unrealized PnL from open positions and mark prices.

    Wallets with no open positions get unrealized_pnl = 0.
    Returns number of wallet_balances rows updated.
    """
    opens = await conn.fetch(
        """
        SELECT wallet_id, symbol, side, entry_price, qty
        FROM positions
        WHERE closed_at IS NULL
        """
    )
    by_wallet: dict[UUID, float] = {}
    for row in opens:
        mark = mark_prices.get(row["symbol"])
        if mark is None or mark <= 0:
            continue
        wid = row["wallet_id"]
        by_wallet[wid] = by_wallet.get(wid, 0.0) + _position_unrealized(
            row["side"],
            float(row["entry_price"]),
            float(row["qty"]),
            float(mark),
        )

    wallets = await conn.fetch("SELECT wallet_id FROM wallet_balances")
    updated = 0
    for w in wallets:
        wid = w["wallet_id"]
        unrealized = by_wallet.get(wid, 0.0)
        bal = await _lock_wallet_balance(conn, wid)
        available = float(bal["available_balance"])
        reserved = float(bal["reserved_margin"])
        equity = _equity(available, reserved, unrealized)
        if float(bal["unrealized_pnl"]) == unrealized and float(
            bal["current_equity"]
        ) == equity:
            continue
        await conn.execute(
            """
            UPDATE wallet_balances
            SET unrealized_pnl = $2,
                current_equity = $3,
                updated_at = NOW()
            WHERE wallet_id = $1
            """,
            wid,
            unrealized,
            equity,
        )
        await _ledger(
            conn,
            entry_type="MARK_UNREALIZED",
            amount=unrealized,
            wallet_id=wid,
            available_after=available,
            reserved_after=reserved,
            note="mark_unrealized",
        )
        updated += 1
    return updated
