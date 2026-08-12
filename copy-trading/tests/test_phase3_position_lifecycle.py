"""Phase 3 — position lifecycle foundation (side, close metadata)."""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from mock_exchange import (  # noqa: E402
    InvalidCloseReason,
    PositionAlreadyClosed,
    close_position,
    place_order,
)
from capital import ensure_funded_wallet  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "copytrading")
    if host == "postgres":
        host = "localhost"
    if not user or not password:
        pytest.skip("DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD required")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@pytest.fixture
async def conn():
    connection = await asyncpg.connect(_database_url())
    try:
        await reset_critical_db_state(connection)
        yield connection
    finally:
        await connection.close()


async def _open_position(
    conn: asyncpg.Connection,
    *,
    side: str = "BUY",
    entry: float = 65000.0,
    qty: float = 0.01,
) -> asyncpg.Record:
    address = f"p3_{uuid.uuid4().hex[:12]}"
    wallet = await create_wallet(conn, address)
    await ensure_funded_wallet(conn, wallet["id"], amount=1000.0)
    await place_order(
        "BTCUSDT", side, qty, entry, conn, wallet_id=wallet["id"]
    )
    row = await conn.fetchrow(
        """
        SELECT *
        FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC
        LIMIT 1
        """,
        wallet["id"],
    )
    assert row is not None
    return row


@pytest.mark.asyncio
async def test_open_position_lifecycle_invariants(conn: asyncpg.Connection):
    row = await _open_position(conn, side="SELL")
    assert row["side"] == "SELL"
    assert row["closed_at"] is None
    assert row["exit_price"] is None
    assert row["close_reason"] is None


@pytest.mark.asyncio
async def test_place_order_persists_side(conn: asyncpg.Connection):
    row = await _open_position(conn, side="BUY")
    assert row["side"] == "BUY"


@pytest.mark.asyncio
async def test_valid_close_sets_metadata(conn: asyncpg.Connection):
    row = await _open_position(conn)
    exit_price = 66000.0
    pnl = await close_position(
        row["id"],
        exit_price,
        conn,
        close_reason="TAKE_PROFIT",
    )
    assert pnl == pytest.approx((exit_price - float(row["entry_price"])) * float(row["qty"]))

    closed = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", row["id"]
    )
    assert closed is not None
    assert closed["closed_at"] is not None
    assert float(closed["exit_price"]) == pytest.approx(exit_price)
    assert closed["close_reason"] == "TAKE_PROFIT"


@pytest.mark.asyncio
async def test_invalid_close_reason_rejected(conn: asyncpg.Connection):
    row = await _open_position(conn)
    with pytest.raises(InvalidCloseReason):
        await close_position(
            row["id"], 66000.0, conn, close_reason="MANUAL_OVERRIDE"
        )


@pytest.mark.asyncio
async def test_null_close_reason_rejected(conn: asyncpg.Connection):
    row = await _open_position(conn)
    with pytest.raises(ValueError, match="close_reason"):
        await close_position(row["id"], 66000.0, conn, close_reason="")


@pytest.mark.asyncio
async def test_invalid_exit_price_rejected(conn: asyncpg.Connection):
    row = await _open_position(conn)
    with pytest.raises(ValueError, match="exit_price"):
        await close_position(row["id"], 0, conn, close_reason="ADMIN")


@pytest.mark.asyncio
async def test_second_close_rejected(conn: asyncpg.Connection):
    row = await _open_position(conn)
    await close_position(row["id"], 66000.0, conn, close_reason="ADMIN")
    with pytest.raises(PositionAlreadyClosed):
        await close_position(row["id"], 66100.0, conn, close_reason="ADMIN")


@pytest.mark.parametrize(
    "close_reason",
    ["STOP_LOSS", "TAKE_PROFIT", "ADMIN"],
)
@pytest.mark.asyncio
async def test_active_close_reasons_persisted(
    conn: asyncpg.Connection, close_reason: str
):
    row = await _open_position(conn)
    await close_position(row["id"], 65500.0, conn, close_reason=close_reason)
    stored = await conn.fetchval(
        "SELECT close_reason FROM positions WHERE id = $1", row["id"]
    )
    assert stored == close_reason


@pytest.mark.parametrize(
    "close_reason",
    ["RISK", "KILL_SWITCH", "LEAD_CLOSE"],
)
@pytest.mark.asyncio
async def test_reserved_close_reasons_accepted_not_auto_triggered(
    conn: asyncpg.Connection, close_reason: str
):
    row = await _open_position(conn)
    await close_position(row["id"], 65500.0, conn, close_reason=close_reason)
    stored = await conn.fetchval(
        "SELECT close_reason FROM positions WHERE id = $1", row["id"]
    )
    assert stored == close_reason


@pytest.mark.parametrize(
    "side, entry, exit_price, expected_pnl",
    [
        ("BUY", 65000.0, 66000.0, 10.0),
        ("SELL", 65000.0, 64000.0, 10.0),
    ],
)
@pytest.mark.asyncio
async def test_side_aware_pnl(
    conn: asyncpg.Connection,
    side: str,
    entry: float,
    exit_price: float,
    expected_pnl: float,
):
    row = await _open_position(conn, side=side, entry=entry)
    pnl = await close_position(
        row["id"], exit_price, conn, close_reason="ADMIN"
    )
    assert pnl == pytest.approx(expected_pnl)
