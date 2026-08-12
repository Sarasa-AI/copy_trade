"""P7-006/007 ledger provenance + append-only; P7-012 reconcile history; P7-013 flatten."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import ensure_funded_wallet, reserve_margin  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402
from emergency_control import emergency_halt_and_flatten  # noqa: E402
from mock_exchange import place_order  # noqa: E402
from observability import (  # noqa: E402
    clear_events_for_tests,
    metrics_snapshot,
    recent_events,
    reset_metrics_for_tests,
)
from reconcile_capital import (  # noqa: E402
    ReconciliationResult,
    persist_reconciliation_run,
    reconcile,
)
from wallet_repository import create_wallet  # noqa: E402
from uuid import uuid4


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
        reset_metrics_for_tests()
        clear_events_for_tests()
        yield connection
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_ledger_provenance_on_reserve(conn: asyncpg.Connection):
    w = await create_wallet(conn, f"p7led_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w["id"], amount=1000.0)
    corr = uuid4()
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=w["id"]
    )
    row = await conn.fetchrow(
        """
        SELECT actor, source, reason, correlation_id, order_id, entry_type
        FROM capital_ledger
        WHERE entry_type = 'RESERVE'
        ORDER BY created_at DESC LIMIT 1
        """
    )
    assert row["actor"] == "system"
    assert row["source"] == "mock_exchange.place_order"
    assert row["reason"] == "open_reserve"
    assert row["correlation_id"] is not None
    assert row["order_id"] == order_id


@pytest.mark.asyncio
async def test_ledger_append_only_blocks_update_delete(conn: asyncpg.Connection):
    w = await create_wallet(conn, f"p7ao_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w["id"], amount=500.0)
    lid = await conn.fetchval(
        """
        SELECT id FROM capital_ledger
        WHERE entry_type = 'ALLOCATE' ORDER BY created_at DESC LIMIT 1
        """
    )
    assert lid is not None
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await conn.execute(
            "UPDATE capital_ledger SET note = 'x' WHERE id = $1", lid
        )
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await conn.execute("DELETE FROM capital_ledger WHERE id = $1", lid)


@pytest.mark.asyncio
async def test_reconciliation_history_persisted(conn: asyncpg.Connection):
    result = await reconcile(conn)
    run_id = await persist_reconciliation_run(
        conn,
        result,
        correlation_id=uuid4(),
        started_at=datetime.now(timezone.utc),
        strict=True,
        notes="unit",
    )
    row = await conn.fetchrow(
        "SELECT ok, strict, mismatch_count FROM reconciliation_runs WHERE id = $1",
        run_id,
    )
    assert row["ok"] is True
    assert row["strict"] is True


@pytest.mark.asyncio
async def test_emergency_flatten_residual_not_safe(conn: asyncpg.Connection):
    w = await create_wallet(conn, f"p7fl_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w["id"], amount=2000.0)
    await place_order(
        "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=w["id"]
    )
    eth_wallet = await create_wallet(conn, f"p7fle_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, eth_wallet["id"], amount=2000.0)
    await place_order(
        "ETHUSDT", "BUY", 0.01, 3500.0, conn, wallet_id=eth_wallet["id"]
    )
    result = await emergency_halt_and_flatten(
        conn,
        actor="pytest",
        reason="p7_residual",
        exit_prices={"BTCUSDT": 65000.0},  # ETH missing → residual
    )
    assert result["skipped_no_price_count"] >= 1
    assert result["residual_count"] >= 1
    assert result["status"] == "NOT_SAFE"
    assert any(e["event_type"] == "flatten_residual" for e in recent_events())
    snap = metrics_snapshot()
    assert snap.get("gauge.flatten_residual_count", 0) >= 1


@pytest.mark.asyncio
async def test_kill_emits_critical_event(conn: asyncpg.Connection):
    from kill_switch import activate_kill_switch, deactivate_kill_switch

    await activate_kill_switch(conn, reason="p7_alert", actor="pytest")
    events = recent_events()
    assert any(
        e["event_type"] == "kill_activate" and e["severity"] == "critical"
        for e in events
    )
    await deactivate_kill_switch(conn, reason="done", actor="pytest")
