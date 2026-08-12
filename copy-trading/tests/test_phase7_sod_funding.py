"""P7-005 — SoD / Funding Policy B."""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import (  # noqa: E402
    allocate_to_wallet,
    ensure_funded_wallet,
    ensure_master_pool,
)
from db_isolation import reset_critical_db_state  # noqa: E402
from mock_exchange import place_order  # noqa: E402
from risk_engine import (  # noqa: E402
    DAILY_EQUITY_LOSS_LIMIT_PCT,
    RiskDenied,
    _loss_pct,
    current_global_equity,
    ensure_global_sod,
    lock_risk_controls,
    utc_today,
)
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


async def _global_sod(conn: asyncpg.Connection) -> float | None:
    val = await conn.fetchval(
        """
        SELECT equity FROM equity_sod_snapshots
        WHERE as_of_date = $1 AND scope = 'GLOBAL'
        """,
        utc_today(),
    )
    return None if val is None else float(val)


@pytest.mark.asyncio
async def test_initial_sod_then_allocate_adjusts_baseline(conn: asyncpg.Connection):
    w1 = await create_wallet(conn, f"p7sod_a_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w1["id"], amount=1000.0)
    async with conn.transaction():
        await lock_risk_controls(conn)
        sod0 = await ensure_global_sod(conn)
    assert sod0 == pytest.approx(1000.0)

    w2 = await create_wallet(conn, f"p7sod_b_{uuid.uuid4().hex[:10]}")
    await allocate_to_wallet(conn, w2["id"], 500.0, note="policy_b_alloc")
    sod1 = await _global_sod(conn)
    assert sod1 == pytest.approx(1500.0)
    current = await current_global_equity(conn)
    assert current == pytest.approx(1500.0)
    # Drawdown unchanged by funding (Policy B).
    assert _loss_pct(sod1, current) == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_master_init_alone_does_not_adjust_sod(conn: asyncpg.Connection):
    w1 = await create_wallet(conn, f"p7sod_m_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w1["id"], amount=1000.0)
    async with conn.transaction():
        await lock_risk_controls(conn)
        await ensure_global_sod(conn)
    sod_before = await _global_sod(conn)
    await ensure_master_pool(conn)  # already exists — no-op
    # Force MASTER_INIT expansion without allocate via ensure_funded path prep:
    # expand master by direct ensure_funded on new wallet after draining...
    # Instead: call expansion through ensure_funded_wallet which does MASTER_INIT
    # then allocate — we isolate MASTER_INIT by checking mid-state is hard;
    # verify classification: pure MASTER_INIT ledger expansion via ensure_funded
    # when available is low — Sod adjusts only on allocate portion.
    master = await conn.fetchrow("SELECT * FROM master_pool WHERE id = 1")
    # Drain available without touching equity: allocate remaining to a sink.
    avail = float(master["available_capital"])
    if avail > 0:
        sink = await create_wallet(conn, f"p7sod_sink_{uuid.uuid4().hex[:8]}")
        await allocate_to_wallet(conn, sink["id"], avail, note="drain")
    sod_after_drain = await _global_sod(conn)
    assert sod_after_drain == pytest.approx(sod_before + avail)

    # New funding: MASTER_INIT + allocate; net SoD bump equals allocate amount.
    sod_pre = await _global_sod(conn)
    w_new = await create_wallet(conn, f"p7sod_n_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w_new["id"], amount=2000.0)
    sod_post = await _global_sod(conn)
    assert sod_post == pytest.approx(sod_pre + 2000.0)


@pytest.mark.asyncio
async def test_drawdown_after_funding_still_enforced(conn: asyncpg.Connection):
    w = await create_wallet(conn, f"p7sod_dd_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w["id"], amount=1000.0)
    current = await current_global_equity(conn)
    day = utc_today()
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = $1 AND scope = 'GLOBAL'",
        day,
    )
    # 10% drawdown: after +1000 allocate with Policy B additive SoD bump,
    # absolute loss is preserved → loss_pct stays above 3% limit.
    sod = current / (1.0 - 0.10)
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'GLOBAL', NULL, $2)
        """,
        day,
        sod,
    )
    loss_before = _loss_pct(sod, current)
    assert loss_before > DAILY_EQUITY_LOSS_LIMIT_PCT

    w2 = await create_wallet(conn, f"p7sod_dd2_{uuid.uuid4().hex[:10]}")
    await allocate_to_wallet(conn, w2["id"], 1000.0)
    sod2 = await _global_sod(conn)
    cur2 = await current_global_equity(conn)
    # Absolute loss dollars preserved; % declines but remains > limit.
    assert (sod2 - cur2) == pytest.approx(sod - current, rel=1e-6)
    assert _loss_pct(sod2, cur2) > DAILY_EQUITY_LOSS_LIMIT_PCT
    # Without Policy B, current would exceed old SoD and wipe drawdown.
    assert cur2 > sod

    with pytest.raises(RiskDenied) as ei:
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=w["id"]
        )
    assert ei.value.reason_code == "GLOBAL_EQUITY_DRAWDOWN"


@pytest.mark.asyncio
async def test_allocate_before_sod_does_not_create_sod(conn: asyncpg.Connection):
    w = await create_wallet(conn, f"p7sod_pre_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w["id"], amount=500.0)
    assert await _global_sod(conn) is None
