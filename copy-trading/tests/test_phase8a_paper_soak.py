"""Phase 8A — opt-in Paper Adapter soak test.

Skipped unless ``PAPER_SOAK=1``. Hermetic by default (deterministic marks).
Set ``PAPER_LIVE_MARK=1`` as well to consume live marks during the soak.

Not part of the normal unit-test suite.
"""

from __future__ import annotations

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
from exchange_adapter.paper_adapter import (  # noqa: E402
    PaperExchangeAdapter,
    PaperFailureMode,
)
from execution_engine import (  # noqa: E402
    ExecutionEngine,
    ExecutionUncertain,
    OrderNotFilled,
)
import price_feed  # noqa: E402
from reconcile_capital import reconcile  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402

SYMBOL = "BTCUSDT"
MARK = Decimal("65000")
QTY = Decimal("0.001")
ITERATIONS = int(os.getenv("PAPER_SOAK_ITERATIONS", "20"))

pytestmark = pytest.mark.skipif(
    os.getenv("PAPER_SOAK", "").strip() not in {"1", "true", "yes"},
    reason="set PAPER_SOAK=1 to enable paper soak test",
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
        price_feed.update_mark(SYMBOL, float(MARK), source="p8a_soak")
        yield connection
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_paper_soak_open_close_reconcile(conn: asyncpg.Connection):
    wallet = await create_wallet(conn, f"p8a_soak_{uuid.uuid4().hex[:8]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=100_000.0)

    modes = [
        PaperFailureMode.SUCCESS,
        PaperFailureMode.RESPONSE_LOST_AFTER_ACCEPT,
        PaperFailureMode.PARTIAL_FILL,
        PaperFailureMode.CREATE_REJECT,
        PaperFailureMode.CREATE_TIMEOUT,
        PaperFailureMode.SUCCESS,
    ]

    for i in range(ITERATIONS):
        price_feed.update_mark(
            SYMBOL, float(MARK) + (i % 7), source="p8a_soak"
        )
        mode = modes[i % len(modes)]
        adapter = PaperExchangeAdapter(
            failure_mode=mode,
            fees_enabled=False,
            slippage_bps=Decimal("0"),
            partial_fill_ratio=Decimal("0.5"),
        )
        engine = ExecutionEngine(adapter)
        try:
            opened = await engine.open_position(
                conn,
                wallet_id=wallet["id"],
                symbol=SYMBOL,
                side="BUY" if i % 2 == 0 else "SELL",
                qty=QTY,
                signal_price=MARK,
            )
        except (OrderNotFilled, ExecutionUncertain):
            # Expected under injected failures; kill may be armed.
            from kill_switch import deactivate_kill_switch, get_kill_switch

            ks = await get_kill_switch(conn)
            if ks["active"]:
                await deactivate_kill_switch(
                    conn, reason="soak reset", actor="pytest"
                )
            continue

        if opened.status in ("FILLED", "CANCELLED") and opened.position_id:
            # Reset failure mode for clean close.
            adapter.set_failure_mode(PaperFailureMode.SUCCESS)
            await engine.close_position(
                opened.position_id, conn, close_reason="ADMIN"
            )

        if i % 5 == 4:
            result = await reconcile(conn)
            assert result.ok, result.to_dict()

    # Final invariants.
    result = await reconcile(conn)
    assert result.ok, result.to_dict()

    pending = int(
        await conn.fetchval(
            """
            SELECT COUNT(*) FROM exchange_orders
            WHERE adapter_name = 'paper'
              AND status IN ('PENDING', 'PARTIALLY_FILLED')
            """
        )
    )
    assert pending == 0

    open_positions = int(
        await conn.fetchval(
            "SELECT COUNT(*) FROM positions WHERE closed_at IS NULL"
        )
    )
    assert open_positions == 0

    # No duplicate fill rows for the same (order, venue_fill_id) when set.
    dupes = int(
        await conn.fetchval(
            """
            SELECT COUNT(*) FROM (
                SELECT exchange_order_row_id, venue_fill_id, COUNT(*) AS c
                FROM exchange_fills
                WHERE venue_fill_id IS NOT NULL
                GROUP BY 1, 2
                HAVING COUNT(*) > 1
            ) d
            """
        )
    )
    assert dupes == 0
