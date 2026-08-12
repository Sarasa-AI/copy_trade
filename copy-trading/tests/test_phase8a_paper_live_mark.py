"""Phase 8A — opt-in live market-mark E2E for PaperExchangeAdapter.

Skipped unless ``PAPER_LIVE_MARK=1``. Requires internet for the public
``price_feed`` stream. Never sends real orders or reads exchange credentials.

If live data is unavailable the test fails clearly (no silent fake fallback).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from decimal import Decimal

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"
os.environ["MAX_MARK_AGE_SEC"] = "60"
os.environ["EXECUTION_RESOLVE_BACKOFF_SEC"] = "0"

from capital import ensure_funded_wallet  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402
from exchange_adapter.paper_adapter import PaperExchangeAdapter  # noqa: E402
from execution_engine import ExecutionEngine  # noqa: E402
import price_feed  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402

SYMBOL = "BTCUSDT"
QTY = Decimal("0.001")

pytestmark = pytest.mark.skipif(
    os.getenv("PAPER_LIVE_MARK", "").strip() not in {"1", "true", "yes"},
    reason="set PAPER_LIVE_MARK=1 to enable live market-mark E2E",
)


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


async def _wait_for_live_mark(*, timeout_sec: float = 20.0) -> float:
    """Start the public feed thread and wait for a fresh BTCUSDT mark."""
    price_feed.clear_mark(SYMBOL)
    price_feed.start()
    deadline = asyncio.get_event_loop().time() + timeout_sec
    while asyncio.get_event_loop().time() < deadline:
        quote = price_feed.get_mark(SYMBOL)
        if quote is not None and quote.is_fresh():
            return float(quote.price)
        await asyncio.sleep(0.25)
    pytest.fail(
        "live market mark unavailable for BTCUSDT within timeout; "
        "refusing to fall back to a fake price"
    )


@pytest.mark.asyncio
async def test_live_mark_paper_open(conn: asyncpg.Connection):
    mark = await _wait_for_live_mark()
    assert mark > 0

    wallet = await create_wallet(conn, f"p8a_live_{uuid.uuid4().hex[:8]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=50_000.0)

    adapter = PaperExchangeAdapter(
        fees_enabled=False, slippage_bps=Decimal("0")
    )
    engine = ExecutionEngine(adapter)
    result = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=Decimal(str(mark)),
    )
    assert result.status == "FILLED"
    assert result.adapter_name == "paper"
    assert result.fee_paid == Decimal("0")
    assert result.avg_fill_price is not None
    # Fill must track the live mark at execution, not a hardcoded fixture.
    assert float(result.avg_fill_price) > 0
    assert abs(float(result.avg_fill_price) - mark) / mark < 0.05
