"""Phase 4 — capital & balance model (master pool + isolated allocations)."""

from __future__ import annotations

import asyncio
import os
import uuid

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import (  # noqa: E402
    InsufficientAvailableBalance,
    allocate_to_wallet,
    ensure_funded_wallet,
    ensure_master_pool,
    get_master_pool,
    get_wallet_balance,
    margin_required,
    mark_unrealized,
)
from mock_exchange import close_position, place_order  # noqa: E402
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


async def _funded_wallet(
    conn: asyncpg.Connection, *, amount: float = 1000.0
) -> asyncpg.Record:
    wallet = await create_wallet(conn, f"p4_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=amount)
    return wallet


@pytest.mark.asyncio
async def test_cannot_open_beyond_available(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=100.0)
    # margin = qty * price / leverage = 0.01 * 65000 / 1 = 650 > 100
    with pytest.raises(InsufficientAvailableBalance):
        await place_order(
            "BTCUSDT",
            "BUY",
            0.01,
            65000.0,
            conn,
            wallet_id=wallet["id"],
            leverage=1.0,
        )
    bal = await get_wallet_balance(conn, wallet["id"])
    assert bal is not None
    assert float(bal["available_balance"]) == pytest.approx(100.0)
    assert float(bal["reserved_margin"]) == pytest.approx(0.0)
    opens = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE wallet_id = $1 AND closed_at IS NULL",
        wallet["id"],
    )
    assert opens == 0


@pytest.mark.asyncio
async def test_reserve_on_open_and_settle_on_close(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=1000.0)
    qty, price, leverage = 0.01, 65000.0, 1.0
    margin = margin_required(qty, price, leverage)
    assert margin == pytest.approx(650.0)

    await place_order(
        "BTCUSDT",
        "BUY",
        qty,
        price,
        conn,
        wallet_id=wallet["id"],
        leverage=leverage,
    )
    bal = await get_wallet_balance(conn, wallet["id"])
    assert float(bal["available_balance"]) == pytest.approx(1000.0 - margin)
    assert float(bal["reserved_margin"]) == pytest.approx(margin)

    pos = await conn.fetchrow(
        """
        SELECT id, reserved_margin FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
        """,
        wallet["id"],
    )
    assert float(pos["reserved_margin"]) == pytest.approx(margin)

    exit_price = 66000.0
    pnl = await close_position(
        pos["id"], exit_price, conn, close_reason="TAKE_PROFIT"
    )
    expected_pnl = (exit_price - price) * qty
    assert pnl == pytest.approx(expected_pnl)

    bal2 = await get_wallet_balance(conn, wallet["id"])
    assert float(bal2["reserved_margin"]) == pytest.approx(0.0)
    assert float(bal2["available_balance"]) == pytest.approx(
        1000.0 + expected_pnl
    )
    assert float(bal2["realized_pnl"]) == pytest.approx(expected_pnl)
    assert float(bal2["current_equity"]) == pytest.approx(1000.0 + expected_pnl)


@pytest.mark.asyncio
async def test_per_agent_isolation(conn: asyncpg.Connection):
    a = await _funded_wallet(conn, amount=500.0)
    b = await _funded_wallet(conn, amount=500.0)

    await place_order(
        "BTCUSDT",
        "BUY",
        0.005,
        65000.0,
        conn,
        wallet_id=a["id"],
        leverage=1.0,
    )
    bal_a = await get_wallet_balance(conn, a["id"])
    bal_b = await get_wallet_balance(conn, b["id"])
    assert float(bal_a["reserved_margin"]) == pytest.approx(325.0)
    assert float(bal_b["available_balance"]) == pytest.approx(500.0)
    assert float(bal_b["reserved_margin"]) == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_concurrent_opens_cannot_double_spend(conn: asyncpg.Connection):
    """Two concurrent opens racing for scarce margin — only one may succeed."""
    wallet = await _funded_wallet(conn, amount=650.0)
    url = _database_url()

    async def _attempt() -> str:
        c = await asyncpg.connect(url)
        try:
            await place_order(
                "BTCUSDT",
                "BUY",
                0.01,
                65000.0,
                c,
                wallet_id=wallet["id"],
                leverage=1.0,
            )
            return "ok"
        except InsufficientAvailableBalance:
            return "insufficient"
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(), _attempt())
    assert sorted(results) == ["insufficient", "ok"]

    bal = await get_wallet_balance(conn, wallet["id"])
    assert float(bal["reserved_margin"]) == pytest.approx(650.0)
    assert float(bal["available_balance"]) == pytest.approx(0.0)
    opens = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE wallet_id = $1 AND closed_at IS NULL",
        wallet["id"],
    )
    assert opens == 1


@pytest.mark.asyncio
async def test_mark_unrealized_updates_equity(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=1000.0)
    await place_order(
        "BTCUSDT",
        "BUY",
        0.01,
        65000.0,
        conn,
        wallet_id=wallet["id"],
        leverage=1.0,
    )
    await mark_unrealized(conn, mark_prices={"BTCUSDT": 66000.0})
    bal = await get_wallet_balance(conn, wallet["id"])
    # unrealized = (66000-65000)*0.01 = 10
    assert float(bal["unrealized_pnl"]) == pytest.approx(10.0)
    assert float(bal["current_equity"]) == pytest.approx(1010.0)


@pytest.mark.asyncio
async def test_equity_authority_uses_master_pool_not_hardcoded(
    conn: asyncpg.Connection,
):
    import mock_exchange

    await ensure_master_pool(conn)
    pool = await get_master_pool(conn)
    assert await mock_exchange.check_daily_loss(conn) is False
    assert not hasattr(mock_exchange, "EQUITY")
    assert float(pool["total_capital"]) > 0


@pytest.mark.asyncio
async def test_allocate_rejects_double_allocation(conn: asyncpg.Connection):
    wallet = await create_wallet(conn, f"p4_dbl_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=100.0)
    with pytest.raises(ValueError, match="already has capital"):
        await allocate_to_wallet(conn, wallet["id"], 50.0)
