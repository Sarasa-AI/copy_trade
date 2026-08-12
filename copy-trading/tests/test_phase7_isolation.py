"""P7-001 — hermetic critical test isolation proofs."""

from __future__ import annotations

import os
import uuid
from unittest import mock

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import ensure_funded_wallet, get_master_pool  # noqa: E402
from db_isolation import (  # noqa: E402
    assert_reset_allowed,
    assert_using_dedicated_test_db,
    current_database_name,
    get_stored_live_fingerprint,
    live_db_escape_hatch_allowed,
    live_database_name,
    read_db_fingerprint,
    reset_critical_db_state,
)
from kill_switch import activate_kill_switch, get_kill_switch  # noqa: E402
from mock_exchange import place_order  # noqa: E402
from risk_engine import current_global_equity, utc_today  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url
    pytest.skip("DATABASE_URL required")


@pytest.fixture
async def conn():
    connection = await asyncpg.connect(_database_url())
    try:
        await reset_critical_db_state(connection)
        yield connection
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_reset_clears_capital_and_positions(conn: asyncpg.Connection):
    wallet = await create_wallet(conn, f"p7iso_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=500.0)
    await place_order(
        "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
    )
    await activate_kill_switch(conn, reason="iso_pollute", actor="pytest")
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = $1 AND scope = 'GLOBAL'",
        utc_today(),
    )
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'GLOBAL', NULL, 99999)
        """,
        utc_today(),
    )

    assert await conn.fetchval("SELECT COUNT(*) FROM wallets") >= 1
    assert await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE closed_at IS NULL"
    ) >= 1
    assert (await get_kill_switch(conn))["active"] is True

    await reset_critical_db_state(conn)

    assert await conn.fetchval("SELECT COUNT(*) FROM wallets") == 0
    assert await conn.fetchval("SELECT COUNT(*) FROM positions") == 0
    assert await conn.fetchval("SELECT COUNT(*) FROM paper_orders") == 0
    assert await conn.fetchval("SELECT COUNT(*) FROM capital_ledger") == 1  # MASTER_INIT
    assert await conn.fetchval("SELECT COUNT(*) FROM equity_sod_snapshots") == 0
    assert await conn.fetchval("SELECT COUNT(*) FROM risk_denials") == 0
    assert await conn.fetchval("SELECT COUNT(*) FROM kill_switch_events") == 0
    state = await get_kill_switch(conn)
    assert state["active"] is False
    master = await get_master_pool(conn)
    assert float(master["allocated_capital"]) == 0.0
    assert float(master["available_capital"]) == float(master["total_capital"])
    assert await current_global_equity(conn) == 0.0


@pytest.mark.asyncio
async def test_critical_tests_independent_order(conn: asyncpg.Connection):
    """Second scenario must not inherit first scenario's equity/SoD pollution."""
    # Scenario A: fund and force a high SoD relative to empty later state.
    w1 = await create_wallet(conn, f"p7iso_a_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w1["id"], amount=1000.0)
    equity_a = await current_global_equity(conn)
    assert equity_a == 1000.0
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'GLOBAL', NULL, $2)
        """,
        utc_today(),
        equity_a * 2,
    )

    await reset_critical_db_state(conn)

    # Scenario B: clean slate — no wallets, no SoD, master full.
    assert await current_global_equity(conn) == 0.0
    sod_n = await conn.fetchval(
        "SELECT COUNT(*) FROM equity_sod_snapshots WHERE as_of_date = $1",
        utc_today(),
    )
    assert sod_n == 0
    w2 = await create_wallet(conn, f"p7iso_b_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, w2["id"], amount=250.0)
    assert await current_global_equity(conn) == 250.0
    # Only the new wallet exists.
    assert await conn.fetchval("SELECT COUNT(*) FROM wallets") == 1


@pytest.mark.asyncio
async def test_live_paper_db_untouched(conn: asyncpg.Connection):
    stored = get_stored_live_fingerprint()
    if stored is None:
        err = os.environ.get("P7_LIVE_PAPER_FINGERPRINT_ERROR", "")
        pytest.skip(f"live fingerprint unavailable: {err or 'not recorded'}")

    live_name = live_database_name()
    assert live_name
    assert current_database_name() != live_name
    assert_using_dedicated_test_db()

    # Mutate test DB heavily.
    for _ in range(3):
        w = await create_wallet(conn, f"p7iso_live_{uuid.uuid4().hex[:10]}")
        await ensure_funded_wallet(conn, w["id"], amount=100.0)

    # Re-read live paper fingerprint; must match session-start snapshot.
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    if host == "postgres":
        host = "localhost"
    port = os.environ.get("POSTGRES_PORT", "5432")
    live_url = f"postgresql://{user}:{password}@{host}:{port}/{live_name}"
    live_conn = await asyncpg.connect(live_url)
    try:
        after = await read_db_fingerprint(live_conn, live_name)
    finally:
        await live_conn.close()

    assert after["db_name"] == stored["db_name"]
    assert after["wallet_count"] == stored["wallet_count"]
    assert after["open_position_count"] == stored["open_position_count"]


def test_pytest_use_live_db_blocked_without_allow():
    with mock.patch.dict(
        os.environ,
        {
            "PYTEST_USE_LIVE_DB": "1",
            "PYTEST_ALLOW_LIVE_DB": "0",
            "CI": "",
            "PHASE7_VERIFY": "",
        },
        clear=False,
    ):
        assert live_db_escape_hatch_allowed() is False


def test_pytest_use_live_db_blocked_under_ci_even_with_allow():
    with mock.patch.dict(
        os.environ,
        {
            "PYTEST_USE_LIVE_DB": "1",
            "PYTEST_ALLOW_LIVE_DB": "1",
            "CI": "1",
            "PHASE7_VERIFY": "",
        },
        clear=False,
    ):
        assert live_db_escape_hatch_allowed() is False


def test_active_db_is_dedicated_test():
    assert_using_dedicated_test_db()
    assert current_database_name().endswith("_test") or os.environ.get(
        "TEST_DATABASE_URL"
    )


@pytest.mark.asyncio
async def test_single_test_runs_on_clean_state(conn: asyncpg.Connection):
    """Fixture reset leaves a usable clean capital baseline for a normal open."""
    assert_reset_allowed()
    master = await get_master_pool(conn)
    assert float(master["allocated_capital"]) == 0.0
    wallet = await create_wallet(conn, f"p7iso_clean_{uuid.uuid4().hex[:10]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=1000.0)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
    )
    assert order_id is not None
    opens = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE wallet_id = $1 AND closed_at IS NULL",
        wallet["id"],
    )
    assert opens == 1
