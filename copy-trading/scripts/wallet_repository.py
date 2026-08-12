"""Minimal wallet identity helpers: UUID canonical id, address as external id."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg


class InvalidWalletId(ValueError):
    """Raised when a value cannot be coerced to a valid UUID wallet id."""

    def __init__(self, value: Any) -> None:
        self.value = value
        super().__init__(f"invalid_wallet_id: {value!r}")


class WalletNotFound(LookupError):
    """Raised when a wallet UUID does not exist in wallets."""

    def __init__(self, wallet_id: UUID) -> None:
        self.wallet_id = wallet_id
        super().__init__(f"wallet_not_found: {wallet_id}")


def parse_wallet_id(value: UUID | str) -> UUID:
    """Coerce UUID | str to UUID; raise InvalidWalletId if malformed."""
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise InvalidWalletId(value) from exc


async def create_wallet(
    conn: asyncpg.Connection,
    address: str,
    *,
    win_rate: float = 0.0,
    total_trades: int = 0,
) -> asyncpg.Record:
    """Insert a new wallet row and return it (id, address, ...)."""
    if not address or not str(address).strip():
        raise ValueError("address must be a non-empty string")
    return await conn.fetchrow(
        """
        INSERT INTO wallets (address, win_rate, total_trades)
        VALUES ($1, $2, $3)
        RETURNING id, address, win_rate, total_trades, last_updated
        """,
        address.strip(),
        win_rate,
        total_trades,
    )


async def get_wallet_by_id(
    conn: asyncpg.Connection, wallet_id: UUID | str
) -> asyncpg.Record | None:
    """Return wallet by canonical UUID id, or None."""
    wid = parse_wallet_id(wallet_id)
    return await conn.fetchrow(
        """
        SELECT id, address, win_rate, total_trades, last_updated
        FROM wallets
        WHERE id = $1
        """,
        wid,
    )


async def get_wallet_by_address(
    conn: asyncpg.Connection, address: str
) -> asyncpg.Record | None:
    """Return wallet by external address, or None."""
    return await conn.fetchrow(
        """
        SELECT id, address, win_rate, total_trades, last_updated
        FROM wallets
        WHERE address = $1
        """,
        address,
    )


async def get_or_create_wallet(
    conn: asyncpg.Connection,
    address: str,
    *,
    win_rate: float = 0.0,
    total_trades: int = 0,
) -> asyncpg.Record:
    """Resolve external address → wallet row (upsert on address)."""
    if not address or not str(address).strip():
        raise ValueError("address must be a non-empty string")
    return await conn.fetchrow(
        """
        INSERT INTO wallets (address, win_rate, total_trades)
        VALUES ($1, $2, $3)
        ON CONFLICT (address) DO UPDATE
        SET last_updated = NOW()
        RETURNING id, address, win_rate, total_trades, last_updated
        """,
        address.strip(),
        win_rate,
        total_trades,
    )
