"""Phase 6 — Equity Risk Engine, Kill Switch, Emergency Flatten."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import (  # noqa: E402
    ensure_funded_wallet,
    get_wallet_balance,
    mark_unrealized,
)
from emergency_control import (  # noqa: E402
    emergency_halt_and_flatten,
    flatten_position,
)
from kill_switch import (  # noqa: E402
    KillSwitchActive,
    activate_kill_switch,
    deactivate_kill_switch,
    get_kill_switch,
)
from mock_exchange import close_position, place_order  # noqa: E402
from reconcile_capital import reconcile  # noqa: E402
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
    wallet = await create_wallet(conn, f"p6_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=amount)
    return wallet


async def _force_global_drawdown(
    conn: asyncpg.Connection, *, drawdown_pct: float = 0.05
) -> tuple[float, float]:
    """Set today's GLOBAL SoD so current equity implies ``drawdown_pct`` loss."""
    current = await current_global_equity(conn)
    if current <= 0:
        # Seed minimal equity so SoD math is defined.
        w = await _funded_wallet(conn, amount=1000.0)
        current = await current_global_equity(conn)
        assert current > 0, w
    sod = current / (1.0 - drawdown_pct)
    day = utc_today()
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = $1 AND scope = 'GLOBAL'",
        day,
    )
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'GLOBAL', NULL, $2)
        """,
        day,
        sod,
    )
    return sod, current


async def _clear_today_sod(conn: asyncpg.Connection) -> None:
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = $1 AND scope = 'GLOBAL'",
        utc_today(),
    )


# --- Risk ---


@pytest.mark.asyncio
async def test_equity_loss_triggers_deny(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=500.0)
    await _force_global_drawdown(conn, drawdown_pct=0.05)
    with pytest.raises(RiskDenied) as ei:
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
        )
    assert ei.value.reason_code == "GLOBAL_EQUITY_DRAWDOWN"
    denial_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM risk_denials
        WHERE reason_code = 'GLOBAL_EQUITY_DRAWDOWN'
          AND wallet_id = $1
        """,
        wallet["id"],
    )
    assert denial_n >= 1
    assert (
        await conn.fetchval(
            "SELECT COUNT(*) FROM positions WHERE wallet_id = $1", wallet["id"]
        )
        == 0
    )
    assert (
        await conn.fetchval(
            "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1",
            wallet["id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_realized_and_unrealized_included(conn: asyncpg.Connection):
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=1000.0)
    await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    await mark_unrealized(conn, mark_prices={"BTCUSDT": 60000.0})
    bal = await get_wallet_balance(conn, wallet["id"])
    assert float(bal["unrealized_pnl"]) < 0
    # SoD = pre-mark equity approximation: force drawdown from marked equity.
    await _force_global_drawdown(conn, drawdown_pct=0.05)
    w2 = await _funded_wallet(conn, amount=50.0)
    # Funding reduces drawdown — force again after funding.
    await _force_global_drawdown(conn, drawdown_pct=0.05)
    with pytest.raises(RiskDenied):
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=w2["id"]
        )


@pytest.mark.asyncio
async def test_sod_snapshot_utc_date(conn: asyncpg.Connection):
    day = date(2026, 8, 2)
    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    assert utc_today(now) == day
    async with conn.transaction():
        await lock_risk_controls(conn)
        first = await ensure_global_sod(conn, as_of=day, now=now)
    again = await ensure_global_sod(conn, as_of=day, now=now)
    assert float(again) == pytest.approx(float(first))


@pytest.mark.asyncio
async def test_midnight_rollover_new_sod(conn: asyncpg.Connection):
    d0 = date(2026, 8, 1)
    d1 = date(2026, 8, 2)
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = ANY($1::date[])",
        [d0, d1],
    )
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'GLOBAL', NULL, 12345.0)
        """,
        d0,
    )
    now = datetime(2026, 8, 2, 0, 0, 1, tzinfo=timezone.utc)
    assert utc_today(now) == d1
    async with conn.transaction():
        await lock_risk_controls(conn)
        sod = await ensure_global_sod(conn, as_of=d1, now=now)
    prev = await conn.fetchval(
        "SELECT equity FROM equity_sod_snapshots WHERE as_of_date = $1", d0
    )
    assert float(prev) == 12345.0
    n = await conn.fetchval(
        "SELECT COUNT(*) FROM equity_sod_snapshots WHERE as_of_date = $1", d1
    )
    assert n == 1
    assert float(sod) >= 0


@pytest.mark.asyncio
async def test_risk_deny_no_order_position_reserve(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=200.0)
    await _force_global_drawdown(conn, drawdown_pct=0.05)
    before_res = float(
        await conn.fetchval(
            "SELECT COALESCE(SUM(reserved_margin),0) FROM wallet_balances"
        )
    )
    with pytest.raises(RiskDenied):
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
        )
    after_res = float(
        await conn.fetchval(
            "SELECT COALESCE(SUM(reserved_margin),0) FROM wallet_balances"
        )
    )
    assert after_res == pytest.approx(before_res)
    assert (
        await conn.fetchval(
            "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1",
            wallet["id"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_concurrent_opens_cannot_bypass_risk(conn: asyncpg.Connection):
    w1 = await _funded_wallet(conn, amount=100.0)
    w2 = await _funded_wallet(conn, amount=100.0)
    await _force_global_drawdown(conn, drawdown_pct=0.05)
    url = _database_url()

    async def _attempt(wid) -> str:
        c = await asyncpg.connect(url)
        try:
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, c, wallet_id=wid
            )
            return "ok"
        except RiskDenied:
            return "denied"
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(w1["id"]), _attempt(w2["id"]))
    assert results == ["denied", "denied"]


@pytest.mark.asyncio
async def test_place_order_allowed_when_healthy(conn: asyncpg.Connection):
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=1000.0)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
    )
    assert order_id is not None
    pos_id = await conn.fetchval(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    await close_position(pos_id, 65100.0, conn, close_reason="ADMIN")


# --- Kill switch ---


@pytest.mark.asyncio
async def test_kill_switch_denies_place_order(conn: asyncpg.Connection):
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=500.0)
    await activate_kill_switch(conn, reason="test_halt", actor="pytest")
    try:
        with pytest.raises(KillSwitchActive):
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
            )
        assert (
            await conn.fetchval(
                "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1",
                wallet["id"],
            )
            == 0
        )
        denial = await conn.fetchval(
            """
            SELECT COUNT(*) FROM risk_denials
            WHERE reason_code = 'KILL_SWITCH_ACTIVE'
              AND wallet_id = $1
            """,
            wallet["id"],
        )
        assert denial >= 1
    finally:
        await deactivate_kill_switch(
            conn, reason="test_clear", actor="pytest"
        )


@pytest.mark.asyncio
async def test_kill_switch_persistence_and_audit(conn: asyncpg.Connection):
    await activate_kill_switch(conn, reason="persist", actor="ops")
    state = await get_kill_switch(conn)
    assert state["active"] is True
    assert state["reason"] == "persist"
    assert state["actor"] == "ops"
    assert state["activated_at"] is not None
    n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM kill_switch_events
        WHERE event_type = 'ACTIVATE' AND reason = 'persist'
        """
    )
    assert n >= 1
    await deactivate_kill_switch(conn, reason="resume", actor="ops")
    state = await get_kill_switch(conn)
    assert state["active"] is False
    assert state["deactivated_at"] is not None


@pytest.mark.asyncio
async def test_inactive_kill_allows_entry(conn: asyncpg.Connection):
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=500.0)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
    )
    assert order_id is not None


# --- Emergency flatten ---


@pytest.mark.asyncio
async def test_emergency_flatten_closes_and_releases(conn: asyncpg.Connection):
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=1000.0)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    pos_id = await conn.fetchval(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    before = await get_wallet_balance(conn, wallet["id"])
    result = await emergency_halt_and_flatten(
        conn,
        actor="pytest",
        reason="drill",
        exit_prices={"BTCUSDT": 66000.0},
    )
    assert result["kill_switch_active"] is True
    closed = await conn.fetchrow("SELECT * FROM positions WHERE id = $1", pos_id)
    assert closed["closed_at"] is not None
    assert closed["close_reason"] == "KILL_SWITCH"
    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["reserved_margin"]) == pytest.approx(0.0)
    assert float(after["available_balance"]) >= float(before["available_balance"])
    release_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RELEASE'
        """,
        pos_id,
    )
    assert release_n == 1
    await deactivate_kill_switch(conn, reason="drill_done", actor="pytest")


@pytest.mark.asyncio
async def test_flatten_double_close_safe(conn: asyncpg.Connection):
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=1000.0)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    pos_id = await conn.fetchval(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    r1 = await flatten_position(
        conn, pos_id, 65500.0, actor="pytest", reason="once"
    )
    r2 = await flatten_position(
        conn, pos_id, 65500.0, actor="pytest", reason="twice"
    )
    assert r1["status"] == "closed"
    assert r2["status"] == "already_closed"


# --- Phase 6 Exit Gate proof tests (A–G, persistence, flatten) ---


async def _snapshot_wallet_capital(
    conn: asyncpg.Connection, wallet_id
) -> dict[str, float | int]:
    bal = await get_wallet_balance(conn, wallet_id)
    orders = await conn.fetchval(
        "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1", wallet_id
    )
    positions = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE wallet_id = $1", wallet_id
    )
    ledger = await conn.fetchval(
        "SELECT COUNT(*) FROM capital_ledger WHERE wallet_id = $1", wallet_id
    )
    return {
        "available": float(bal["available_balance"]),
        "reserved": float(bal["reserved_margin"]),
        "equity": float(bal["current_equity"]),
        "orders": int(orders),
        "positions": int(positions),
        "ledger": int(ledger),
    }


@pytest.mark.asyncio
async def test_concurrent_sod_first_write_single_baseline(conn: asyncpg.Connection):
    """TEST A — Concurrent first SoD of the day yields one GLOBAL snapshot."""
    await _clear_today_sod(conn)
    day = utc_today()
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = $1 AND scope = 'GLOBAL'",
        day,
    )
    w1 = await _funded_wallet(conn, amount=800.0)
    w2 = await _funded_wallet(conn, amount=800.0)
    url = _database_url()

    async def _attempt(wid) -> str:
        c = await asyncpg.connect(url)
        try:
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, c, wallet_id=wid
            )
            return "ok"
        except RiskDenied:
            return "denied"
        except KillSwitchActive:
            return "kill"
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(w1["id"]), _attempt(w2["id"]))
    assert set(results) <= {"ok", "denied"}
    assert results.count("ok") >= 1
    n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM equity_sod_snapshots
        WHERE as_of_date = $1 AND scope = 'GLOBAL'
        """,
        day,
    )
    assert n == 1
    current = await current_global_equity(conn)
    sod = float(
        await conn.fetchval(
            """
            SELECT equity FROM equity_sod_snapshots
            WHERE as_of_date = $1 AND scope = 'GLOBAL'
            """,
            day,
        )
    )
    assert _loss_pct(sod, current) <= DAILY_EQUITY_LOSS_LIMIT_PCT + 1e-12
    result = await reconcile(conn)
    assert result.ok


@pytest.mark.asyncio
async def test_concurrent_sod_limit_cannot_be_exceeded(conn: asyncpg.Connection):
    """TEST A — Concurrent opens while over SoD limit: all denied, no mutation."""
    w1 = await _funded_wallet(conn, amount=100.0)
    w2 = await _funded_wallet(conn, amount=100.0)
    await _force_global_drawdown(conn, drawdown_pct=0.05)
    before1 = await _snapshot_wallet_capital(conn, w1["id"])
    before2 = await _snapshot_wallet_capital(conn, w2["id"])
    url = _database_url()

    async def _attempt(wid) -> str:
        c = await asyncpg.connect(url)
        try:
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, c, wallet_id=wid
            )
            return "ok"
        except RiskDenied:
            return "denied"
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(w1["id"]), _attempt(w2["id"]))
    assert results == ["denied", "denied"]
    assert await _snapshot_wallet_capital(conn, w1["id"]) == before1
    assert await _snapshot_wallet_capital(conn, w2["id"]) == before2
    result = await reconcile(conn)
    assert result.ok


@pytest.mark.asyncio
async def test_concurrent_healthy_opens_no_overspend(conn: asyncpg.Connection):
    """TEST B — Multiple simultaneous place_order calls: no capital corruption."""
    await _clear_today_sod(conn)
    wallets = [await _funded_wallet(conn, amount=500.0) for _ in range(4)]
    url = _database_url()

    async def _attempt(wid) -> str:
        c = await asyncpg.connect(url)
        try:
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, c, wallet_id=wid
            )
            return "ok"
        except (RiskDenied, KillSwitchActive):
            return "denied"
        finally:
            await c.close()

    results = await asyncio.gather(*[_attempt(w["id"]) for w in wallets])
    assert results.count("ok") == 4
    for w in wallets:
        bal = await get_wallet_balance(conn, w["id"])
        assert float(bal["reserved_margin"]) == pytest.approx(65.0)
        assert float(bal["available_balance"]) == pytest.approx(435.0)
        pos_n = await conn.fetchval(
            "SELECT COUNT(*) FROM positions WHERE wallet_id = $1 AND closed_at IS NULL",
            w["id"],
        )
        assert pos_n == 1
    result = await reconcile(conn)
    assert result.ok


@pytest.mark.asyncio
async def test_kill_switch_race_with_open(conn: asyncpg.Connection):
    """TEST C — Concurrent open + kill activation: deterministic, no bypass."""
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=800.0)
    url = _database_url()
    before = await _snapshot_wallet_capital(conn, wallet["id"])

    async def _open() -> str:
        c = await asyncpg.connect(url)
        try:
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, c, wallet_id=wallet["id"]
            )
            return "ok"
        except KillSwitchActive:
            return "kill"
        except RiskDenied:
            return "denied"
        finally:
            await c.close()

    async def _kill() -> str:
        c = await asyncpg.connect(url)
        try:
            await activate_kill_switch(c, reason="race_halt", actor="pytest")
            return "activated"
        finally:
            await c.close()

    try:
        open_result, kill_result = await asyncio.gather(_open(), _kill())
        assert kill_result == "activated"
        assert open_result in ("ok", "kill")
        state = await get_kill_switch(conn)
        assert state["active"] is True
        # No unauthorized open after kill is active.
        with pytest.raises(KillSwitchActive):
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
            )
        after = await _snapshot_wallet_capital(conn, wallet["id"])
        if open_result == "kill":
            assert after == before
        else:
            assert after["orders"] == before["orders"] + 1
            assert after["positions"] == before["positions"] + 1
            # Exactly one RESERVE for this wallet's new position.
            open_n = await conn.fetchval(
                """
                SELECT COUNT(*) FROM positions
                WHERE wallet_id = $1 AND closed_at IS NULL
                """,
                wallet["id"],
            )
            assert open_n == 1
        result = await reconcile(conn)
        assert result.ok
    finally:
        await deactivate_kill_switch(conn, reason="race_clear", actor="pytest")


@pytest.mark.asyncio
async def test_concurrent_opens_plus_kill_stress(conn: asyncpg.Connection):
    """TEST D — N concurrent place_order + kill: no orphans / no bypass."""
    await _clear_today_sod(conn)
    wallets = [await _funded_wallet(conn, amount=600.0) for _ in range(5)]
    url = _database_url()

    async def _open(wid) -> str:
        c = await asyncpg.connect(url)
        try:
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, c, wallet_id=wid
            )
            return "ok"
        except KillSwitchActive:
            return "kill"
        except RiskDenied:
            return "denied"
        finally:
            await c.close()

    async def _kill() -> str:
        c = await asyncpg.connect(url)
        try:
            await activate_kill_switch(c, reason="stress_halt", actor="pytest")
            return "activated"
        finally:
            await c.close()

    try:
        outcomes = await asyncio.gather(
            *[_open(w["id"]) for w in wallets], _kill()
        )
        assert outcomes[-1] == "activated"
        open_outcomes = outcomes[:-1]
        assert set(open_outcomes) <= {"ok", "kill"}
        assert await get_kill_switch(conn)
        state = await get_kill_switch(conn)
        assert state["active"] is True

        for w in wallets:
            filled = await conn.fetchval(
                """
                SELECT COUNT(*) FROM paper_orders
                WHERE wallet_id = $1 AND status = 'FILLED'
                """,
                w["id"],
            )
            open_pos = await conn.fetchval(
                """
                SELECT COUNT(*) FROM positions
                WHERE wallet_id = $1 AND closed_at IS NULL
                """,
                w["id"],
            )
            assert filled == open_pos
            bal = await get_wallet_balance(conn, w["id"])
            if open_pos == 0:
                assert float(bal["reserved_margin"]) == pytest.approx(0.0)
            else:
                assert float(bal["reserved_margin"]) == pytest.approx(
                    65.0 * open_pos
                )
            # No FILLED order without position.
            orphans = await conn.fetchval(
                """
                SELECT COUNT(*) FROM paper_orders o
                WHERE o.wallet_id = $1 AND o.status = 'FILLED'
                  AND NOT EXISTS (
                    SELECT 1 FROM positions p WHERE p.order_id = o.id
                  )
                """,
                w["id"],
            )
            assert orphans == 0

        # Kill remains authoritative for new opens.
        probe = await _funded_wallet(conn, amount=400.0)
        with pytest.raises(KillSwitchActive):
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=probe["id"]
            )
        result = await reconcile(conn)
        assert result.ok
    finally:
        await deactivate_kill_switch(conn, reason="stress_clear", actor="pytest")


@pytest.mark.asyncio
async def test_risk_deny_atomicity_full(conn: asyncpg.Connection):
    """TEST E — Risk denial: no order, position, reserve, or ledger mutation."""
    wallet = await _funded_wallet(conn, amount=250.0)
    await _force_global_drawdown(conn, drawdown_pct=0.05)
    before = await _snapshot_wallet_capital(conn, wallet["id"])
    with pytest.raises(RiskDenied):
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
        )
    after = await _snapshot_wallet_capital(conn, wallet["id"])
    assert after == before


@pytest.mark.asyncio
async def test_kill_deny_atomicity_full(conn: asyncpg.Connection):
    """TEST F — Kill denial via direct place_order: capital frozen."""
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=250.0)
    await activate_kill_switch(conn, reason="deny_atomicity", actor="pytest")
    try:
        before = await _snapshot_wallet_capital(conn, wallet["id"])
        with pytest.raises(KillSwitchActive):
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
            )
        after = await _snapshot_wallet_capital(conn, wallet["id"])
        assert after == before
    finally:
        await deactivate_kill_switch(conn, reason="deny_clear", actor="pytest")


@pytest.mark.asyncio
async def test_rollback_after_risk_check_before_commit(conn: asyncpg.Connection):
    """TEST G — Exception after risk gate, before commit: no partial mutation."""
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=1000.0)
    before = await _snapshot_wallet_capital(conn, wallet["id"])

    with patch(
        "mock_exchange.reserve_margin",
        new_callable=AsyncMock,
        side_effect=RuntimeError("injected_post_risk_failure"),
    ):
        with pytest.raises(RuntimeError, match="injected_post_risk_failure"):
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
            )

    after = await _snapshot_wallet_capital(conn, wallet["id"])
    assert after == before
    result = await reconcile(conn)
    assert result.ok


@pytest.mark.asyncio
async def test_kill_switch_persists_across_new_connection(conn: asyncpg.Connection):
    """Kill switch survives process/connection restart (new asyncpg connect)."""
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=400.0)
    await activate_kill_switch(conn, reason="restart_persist", actor="pytest")

    url = _database_url()
    # New process-equivalent connection (do not close the fixture conn).
    new_conn = await asyncpg.connect(url)
    try:
        state = await get_kill_switch(new_conn)
        assert state["active"] is True
        with pytest.raises(KillSwitchActive):
            await place_order(
                "BTCUSDT",
                "BUY",
                0.001,
                65000.0,
                new_conn,
                wallet_id=wallet["id"],
            )
        await deactivate_kill_switch(
            new_conn, reason="restart_clear", actor="pytest"
        )
    finally:
        await new_conn.close()


@pytest.mark.asyncio
async def test_emergency_flatten_multi_wallet_and_reconcile(
    conn: asyncpg.Connection,
):
    """Multi-position / multi-wallet flatten + missing price skip + reconcile."""
    await _clear_today_sod(conn)
    w1 = await _funded_wallet(conn, amount=2000.0)
    w2 = await _funded_wallet(conn, amount=2000.0)
    o1 = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=w1["id"]
    )
    o2 = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=w2["id"]
    )
    # Second symbol open on w1 — will be skipped (no exit price).
    o3 = await place_order(
        "ETHUSDT", "BUY", 0.1, 3000.0, conn, wallet_id=w1["id"]
    )
    p1 = await conn.fetchval(
        "SELECT id FROM positions WHERE order_id = $1", o1
    )
    p2 = await conn.fetchval(
        "SELECT id FROM positions WHERE order_id = $1", o2
    )
    p3 = await conn.fetchval(
        "SELECT id FROM positions WHERE order_id = $1", o3
    )

    result = await emergency_halt_and_flatten(
        conn,
        actor="pytest",
        reason="multi_flatten",
        exit_prices={"BTCUSDT": 66000.0},
    )
    assert result["kill_switch_active"] is True
    statuses = {r["position_id"]: r["status"] for r in result["results"]}
    assert statuses[str(p1)] == "closed"
    assert statuses[str(p2)] == "closed"
    assert statuses[str(p3)] == "skipped_no_price"

    for pid, expected_closed in ((p1, True), (p2, True), (p3, False)):
        row = await conn.fetchrow("SELECT * FROM positions WHERE id = $1", pid)
        if expected_closed:
            assert row["closed_at"] is not None
            assert row["close_reason"] == "KILL_SWITCH"
        else:
            assert row["closed_at"] is None

    bal1 = await get_wallet_balance(conn, w1["id"])
    bal2 = await get_wallet_balance(conn, w2["id"])
    assert float(bal2["reserved_margin"]) == pytest.approx(0.0)
    # ETH position still reserved on w1.
    assert float(bal1["reserved_margin"]) == pytest.approx(300.0)

    # Duplicate flatten is safe.
    again = await flatten_position(
        conn, p1, 66000.0, actor="pytest", reason="dup"
    )
    assert again["status"] == "already_closed"

    recon = await reconcile(conn)
    assert recon.ok

    # Close remaining ETH so we do not leave session orphans tied to kill.
    await close_position(p3, 3010.0, conn, close_reason="ADMIN")
    await deactivate_kill_switch(conn, reason="multi_done", actor="pytest")


@pytest.mark.asyncio
async def test_wallet_equity_drawdown_denies(conn: asyncpg.Connection):
    """Per-wallet SoD drawdown gate (WALLET_EQUITY_DRAWDOWN)."""
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=1000.0)
    day = utc_today()
    # Healthy global SoD at current global equity.
    async with conn.transaction():
        await lock_risk_controls(conn)
        await ensure_global_sod(conn, as_of=day)

    bal = await get_wallet_balance(conn, wallet["id"])
    w_eq = float(bal["current_equity"])
    w_sod = w_eq / (1.0 - 0.05)
    await conn.execute(
        """
        DELETE FROM equity_sod_snapshots
        WHERE as_of_date = $1 AND scope = 'WALLET' AND wallet_id = $2
        """,
        day,
        wallet["id"],
    )
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'WALLET', $2, $3)
        """,
        day,
        wallet["id"],
        w_sod,
    )
    before = await _snapshot_wallet_capital(conn, wallet["id"])
    with pytest.raises(RiskDenied) as ei:
        await place_order(
            "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
        )
    assert ei.value.reason_code == "WALLET_EQUITY_DRAWDOWN"
    assert await _snapshot_wallet_capital(conn, wallet["id"]) == before
