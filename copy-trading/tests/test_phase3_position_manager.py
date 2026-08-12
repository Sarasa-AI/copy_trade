"""Phase 3 — Position Manager: open-position monitoring and SL/TP triggers."""

from __future__ import annotations

import os
import uuid
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

import price_feed  # noqa: E402
from capital import ensure_funded_wallet  # noqa: E402
from mock_exchange import close_position  # noqa: E402
from position_manager import (  # noqa: E402
    evaluate_close_trigger,
    tick,
)
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


def _set_mark_price(symbol: str, price: float | None) -> None:
    with price_feed.lock:
        if price is None:
            price_feed.prices.pop(symbol, None)
        else:
            price_feed.prices[symbol] = price


async def _insert_open_position(
    conn: asyncpg.Connection,
    *,
    side: str = "BUY",
    entry: float = 65000.0,
    qty: float = 0.01,
    symbol: str = "BTCUSDT",
    stop_loss_price: float | None = None,
    take_profit_price: float | None = None,
) -> asyncpg.Record:
    address = f"pm_{uuid.uuid4().hex[:12]}"
    wallet = await create_wallet(conn, address)
    await ensure_funded_wallet(conn, wallet["id"], amount=1000.0)
    row = await conn.fetchrow(
        """
        INSERT INTO positions (
            symbol, entry_price, qty, wallet_id, side,
            stop_loss_price, take_profit_price
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
        """,
        symbol,
        entry,
        qty,
        wallet["id"],
        side,
        stop_loss_price,
        take_profit_price,
    )
    assert row is not None
    return row


async def _insert_closed_position(
    conn: asyncpg.Connection,
    *,
    side: str = "BUY",
    entry: float = 65000.0,
    qty: float = 0.01,
    exit_price: float = 66000.0,
    stop_loss_price: float | None = 64000.0,
) -> asyncpg.Record:
    address = f"pm_closed_{uuid.uuid4().hex[:12]}"
    wallet = await create_wallet(conn, address)
    row = await conn.fetchrow(
        """
        INSERT INTO positions (
            symbol, entry_price, qty, wallet_id, side,
            stop_loss_price, exit_price, close_reason, closed_at, pnl
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'ADMIN', NOW(), $8)
        RETURNING *
        """,
        "BTCUSDT",
        entry,
        qty,
        wallet["id"],
        side,
        stop_loss_price,
        exit_price,
        (exit_price - entry) * qty,
    )
    assert row is not None
    return row


# --- A: only open positions evaluated ---


@pytest.mark.asyncio
async def test_only_open_positions_evaluated(conn: asyncpg.Connection):
    closed = await _insert_closed_position(conn, stop_loss_price=70000.0)
    open_row = await _insert_open_position(
        conn, stop_loss_price=64000.0, take_profit_price=70000.0
    )
    _set_mark_price("BTCUSDT", 63000.0)

    await tick(conn)

    still_open = await conn.fetchrow(
        "SELECT closed_at, close_reason FROM positions WHERE id = $1",
        open_row["id"],
    )
    assert still_open is not None
    assert still_open["closed_at"] is not None
    assert still_open["close_reason"] == "STOP_LOSS"

    closed_after = await conn.fetchrow(
        "SELECT exit_price, close_reason FROM positions WHERE id = $1",
        closed["id"],
    )
    assert closed_after is not None
    assert float(closed_after["exit_price"]) == pytest.approx(66000.0)
    assert closed_after["close_reason"] == "ADMIN"


# --- B: BUY STOP_LOSS ---


@pytest.mark.asyncio
async def test_buy_stop_loss_triggers(conn: asyncpg.Connection):
    row = await _insert_open_position(conn, stop_loss_price=64000.0)
    _set_mark_price("BTCUSDT", 63000.0)

    await tick(conn)

    closed = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", row["id"]
    )
    assert closed is not None
    assert closed["closed_at"] is not None
    assert closed["close_reason"] == "STOP_LOSS"
    assert float(closed["exit_price"]) == pytest.approx(63000.0)


# --- C: BUY TAKE_PROFIT ---


@pytest.mark.asyncio
async def test_buy_take_profit_triggers(conn: asyncpg.Connection):
    row = await _insert_open_position(conn, take_profit_price=66000.0)
    _set_mark_price("BTCUSDT", 67000.0)

    await tick(conn)

    closed = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", row["id"]
    )
    assert closed is not None
    assert closed["close_reason"] == "TAKE_PROFIT"
    assert float(closed["exit_price"]) == pytest.approx(67000.0)


# --- D: simultaneous SL + TP, STOP_LOSS wins ---


@pytest.mark.asyncio
async def test_simultaneous_sl_tp_stop_loss_wins(conn: asyncpg.Connection):
    row = await _insert_open_position(
        conn, stop_loss_price=70000.0, take_profit_price=60000.0
    )
    _set_mark_price("BTCUSDT", 65000.0)

    await tick(conn)

    closed = await conn.fetchrow(
        "SELECT close_reason FROM positions WHERE id = $1", row["id"]
    )
    assert closed is not None
    assert closed["close_reason"] == "STOP_LOSS"


def test_evaluate_close_trigger_sl_wins_over_tp():
    row = {
        "side": "BUY",
        "stop_loss_price": 70000.0,
        "take_profit_price": 60000.0,
    }
    assert evaluate_close_trigger(row, 65000.0) == "STOP_LOSS"


# --- E: no trigger ---


@pytest.mark.asyncio
async def test_no_trigger_position_remains_open(conn: asyncpg.Connection):
    row = await _insert_open_position(
        conn, stop_loss_price=60000.0, take_profit_price=70000.0
    )
    _set_mark_price("BTCUSDT", 65000.0)

    await tick(conn)

    still = await conn.fetchrow(
        "SELECT closed_at FROM positions WHERE id = $1", row["id"]
    )
    assert still is not None
    assert still["closed_at"] is None


# --- F: no SL/TP levels ---


@pytest.mark.asyncio
async def test_position_without_sltp_remains_open(conn: asyncpg.Connection):
    row = await _insert_open_position(
        conn, stop_loss_price=None, take_profit_price=None
    )
    _set_mark_price("BTCUSDT", 50000.0)

    await tick(conn)

    still = await conn.fetchrow(
        "SELECT closed_at, stop_loss_price, take_profit_price FROM positions WHERE id = $1",
        row["id"],
    )
    assert still is not None
    assert still["closed_at"] is None
    assert still["stop_loss_price"] is None
    assert still["take_profit_price"] is None


# --- G: close_position integration ---


@pytest.mark.asyncio
async def test_close_position_integration_lifecycle_fields(conn: asyncpg.Connection):
    row = await _insert_open_position(conn, stop_loss_price=64000.0)
    mark = 63500.0
    _set_mark_price("BTCUSDT", mark)

    await tick(conn)

    closed = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", row["id"]
    )
    assert closed is not None
    assert closed["closed_at"] is not None
    assert float(closed["exit_price"]) == pytest.approx(mark)
    assert closed["close_reason"] == "STOP_LOSS"
    expected_pnl = (mark - float(row["entry_price"])) * float(row["qty"])
    assert float(closed["pnl"]) == pytest.approx(expected_pnl)


# --- H: already-closed race ---


@pytest.mark.asyncio
async def test_already_closed_race_does_not_crash(conn: asyncpg.Connection):
    row = await _insert_open_position(conn, stop_loss_price=64000.0)
    _set_mark_price("BTCUSDT", 63000.0)
    await close_position(row["id"], 63000.0, conn, close_reason="ADMIN")

    await tick(conn)


# --- I: missing mark price ---


@pytest.mark.asyncio
async def test_missing_mark_price_does_not_close(conn: asyncpg.Connection):
    row = await _insert_open_position(conn, stop_loss_price=64000.0)
    _set_mark_price("BTCUSDT", None)

    await tick(conn)

    still = await conn.fetchrow(
        "SELECT closed_at FROM positions WHERE id = $1", row["id"]
    )
    assert still is not None
    assert still["closed_at"] is None


# --- J: agent ordering ---


@pytest.mark.asyncio
async def test_agent_tick_calls_position_manager_before_place_order():
    import agent_trader

    call_order: list[str] = []

    async def mock_pm_tick(conn):
        call_order.append("position_manager.tick")

    async def mock_place_order(*args, **kwargs):
        call_order.append("place_order")
        return uuid.uuid4()

    conn = AsyncMock()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "wallet_id": uuid.uuid4(),
                "wallet_address": "test_addr",
                "win_rate": 0.75,
            }
        ]
    )

    class _FakeAcquire:
        def __init__(self, connection):
            self._connection = connection

        async def __aenter__(self):
            return self._connection

        async def __aexit__(self, exc_type, exc, tb):
            return False

    pool = AsyncMock()
    pool.acquire = lambda: _FakeAcquire(conn)

    with price_feed.lock:
        price_feed.prices["BTCUSDT"] = 65000.0

    with (
        patch.object(agent_trader.position_manager, "tick", mock_pm_tick),
        patch.object(agent_trader, "place_order", mock_place_order),
        patch.object(agent_trader, "check_daily_loss", AsyncMock(return_value=False)),
        patch.object(
            agent_trader,
            "initialize_paper_allocations",
            AsyncMock(return_value=0),
        ),
    ):
        await agent_trader.tick(pool)

    assert call_order.index("position_manager.tick") < call_order.index("place_order")


# --- K: SELL SL/TP (side-aware) ---


@pytest.mark.asyncio
async def test_sell_stop_loss_triggers(conn: asyncpg.Connection):
    row = await _insert_open_position(
        conn,
        side="SELL",
        entry=65000.0,
        stop_loss_price=70000.0,
        take_profit_price=60000.0,
    )
    _set_mark_price("BTCUSDT", 71000.0)

    await tick(conn)

    closed = await conn.fetchrow(
        "SELECT closed_at, close_reason FROM positions WHERE id = $1", row["id"]
    )
    assert closed is not None
    assert closed["closed_at"] is not None
    assert closed["close_reason"] == "STOP_LOSS"


@pytest.mark.asyncio
async def test_sell_take_profit_triggers(conn: asyncpg.Connection):
    row = await _insert_open_position(
        conn,
        side="SELL",
        entry=65000.0,
        stop_loss_price=70000.0,
        take_profit_price=60000.0,
    )
    _set_mark_price("BTCUSDT", 59000.0)

    await tick(conn)

    closed = await conn.fetchrow(
        "SELECT closed_at, close_reason FROM positions WHERE id = $1", row["id"]
    )
    assert closed is not None
    assert closed["closed_at"] is not None
    assert closed["close_reason"] == "TAKE_PROFIT"


def test_evaluate_close_trigger_sell_formulas():
    row = {
        "side": "SELL",
        "stop_loss_price": 70000.0,
        "take_profit_price": 60000.0,
    }
    assert evaluate_close_trigger(row, 65000.0) is None
    assert evaluate_close_trigger(row, 71000.0) == "STOP_LOSS"
    assert evaluate_close_trigger(row, 59000.0) == "TAKE_PROFIT"
