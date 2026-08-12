"""P7-004 — fail-closed mark gate on authoritative open path."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"
os.environ["MAX_MARK_AGE_SEC"] = "30"

from capital import ensure_funded_wallet, get_wallet_balance  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402
from mock_exchange import place_order  # noqa: E402
import price_feed  # noqa: E402
from risk_engine import RiskDenied  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        pytest.skip("DATABASE_URL required")
    return url


@pytest.fixture
async def conn():
    connection = await asyncpg.connect(_database_url())
    try:
        await reset_critical_db_state(connection)
        yield connection
    finally:
        await connection.close()


async def _funded(conn: asyncpg.Connection):
    w = await create_wallet(conn, f"p7fc_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, w["id"], amount=1000.0)
    return w


async def _counts(conn: asyncpg.Connection, wallet_id):
    return {
        "orders": int(
            await conn.fetchval(
                "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1",
                wallet_id,
            )
        ),
        "positions": int(
            await conn.fetchval(
                "SELECT COUNT(*) FROM positions WHERE wallet_id = $1",
                wallet_id,
            )
        ),
        "reserves": int(
            await conn.fetchval(
                """
                SELECT COUNT(*) FROM capital_ledger
                WHERE wallet_id = $1 AND entry_type = 'RESERVE'
                """,
                wallet_id,
            )
        ),
    }


@pytest.mark.asyncio
async def test_missing_mark_denies_place_order(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    price_feed.clear_mark("BTCUSDT")
    before = await get_wallet_balance(conn, wallet["id"])
    with pytest.raises(RiskDenied) as ei:
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
        )
    assert ei.value.reason_code == "MARK_MISSING"
    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == float(before["available_balance"])
    assert float(after["reserved_margin"]) == float(before["reserved_margin"])
    counts = await _counts(conn, wallet["id"])
    assert counts == {"orders": 0, "positions": 0, "reserves": 0}
    denials = await conn.fetchval(
        """
        SELECT COUNT(*) FROM risk_denials
        WHERE reason_code = 'MARK_MISSING' AND wallet_id = $1
        """,
        wallet["id"],
    )
    assert int(denials) >= 1


@pytest.mark.asyncio
async def test_stale_mark_denies_place_order(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    now = datetime.now(timezone.utc)
    price_feed.update_mark(
        "BTCUSDT",
        65000.0,
        source="stale_test",
        ts_utc=now - timedelta(seconds=120),
    )
    before = await get_wallet_balance(conn, wallet["id"])
    with pytest.raises(RiskDenied) as ei:
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
        )
    assert ei.value.reason_code == "MARK_STALE"
    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == float(before["available_balance"])
    counts = await _counts(conn, wallet["id"])
    assert counts == {"orders": 0, "positions": 0, "reserves": 0}


@pytest.mark.asyncio
async def test_fresh_mark_allows_place_order(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    price_feed.update_mark("BTCUSDT", 65000.0, source="fresh_test")
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
    )
    assert order_id is not None
    counts = await _counts(conn, wallet["id"])
    assert counts["orders"] == 1
    assert counts["positions"] == 1
    assert counts["reserves"] == 1
