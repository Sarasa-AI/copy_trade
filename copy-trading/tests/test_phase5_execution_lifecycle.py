"""Phase 5 — execution & position lifecycle integrity."""

from __future__ import annotations

import asyncio
import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import (  # noqa: E402
    InsufficientAvailableBalance,
    ensure_funded_wallet,
    get_wallet_balance,
)
from mock_exchange import (  # noqa: E402
    close_position,
    get_position,
    place_order,
)
from position_manager import evaluate_close_trigger, tick as pm_tick
from reconcile_capital import reconcile  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402
import price_feed  # noqa: E402
import agent_trader  # noqa: E402
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
    wallet = await create_wallet(conn, f"p5_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=amount)
    return wallet


def _set_mark(symbol: str, price: float | None) -> None:
    with price_feed.lock:
        if price is None:
            price_feed.prices.pop(symbol, None)
        else:
            price_feed.prices[symbol] = price


# --- 5.1 Order ↔ position linkage ---


@pytest.mark.asyncio
async def test_place_order_sets_order_id_link(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    pos = await conn.fetchrow(
        "SELECT * FROM positions WHERE order_id = $1", order_id
    )
    assert pos is not None
    assert pos["order_id"] == order_id
    order = await conn.fetchrow(
        "SELECT status FROM paper_orders WHERE id = $1", order_id
    )
    assert order["status"] == "FILLED"


@pytest.mark.asyncio
async def test_duplicate_order_id_rejected(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    try:
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                """
                INSERT INTO positions (
                    symbol, entry_price, qty, wallet_id, side, order_id
                )
                VALUES ('BTCUSDT', 65000, 0.01, $1, 'BUY', $2)
                """,
                wallet["id"],
                order_id,
            )
    finally:
        await conn.execute("ROLLBACK")


@pytest.mark.asyncio
async def test_invalid_order_id_fk_rejected(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    try:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                """
                INSERT INTO positions (
                    symbol, entry_price, qty, wallet_id, side, order_id
                )
                VALUES ('BTCUSDT', 65000, 0.01, $1, 'BUY', $2)
                """,
                wallet["id"],
                uuid.uuid4(),
            )
    finally:
        await conn.execute("ROLLBACK")


@pytest.mark.asyncio
async def test_open_rollback_removes_order_position_reserve(
    conn: asyncpg.Connection,
):
    wallet = await _funded_wallet(conn, amount=100.0)
    before_orders = await conn.fetchval(
        "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1", wallet["id"]
    )
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
    # Failed open TX rolled back; REJECTED recorded in separate TX
    filled = await conn.fetchval(
        """
        SELECT COUNT(*) FROM paper_orders
        WHERE wallet_id = $1 AND status = 'FILLED'
        """,
        wallet["id"],
    )
    assert filled == 0
    opens = await conn.fetchval(
        """
        SELECT COUNT(*) FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        """,
        wallet["id"],
    )
    assert opens == 0
    rejected = await conn.fetchval(
        """
        SELECT COUNT(*) FROM paper_orders
        WHERE wallet_id = $1 AND status = 'REJECTED'
        """,
        wallet["id"],
    )
    assert rejected >= 1
    assert before_orders + rejected == await conn.fetchval(
        "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1", wallet["id"]
    )
    bal = await get_wallet_balance(conn, wallet["id"])
    assert float(bal["reserved_margin"]) == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_legacy_null_order_id_allowed(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    row = await conn.fetchrow(
        """
        INSERT INTO positions (
            symbol, entry_price, qty, wallet_id, side
        )
        VALUES ('BTCUSDT', 65000, 0.01, $1, 'BUY')
        RETURNING id, order_id
        """,
        wallet["id"],
    )
    assert row["order_id"] is None


# --- 5.2 Order state machine ---


@pytest.mark.asyncio
async def test_order_status_check_rejects_invalid(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                """
                INSERT INTO paper_orders
                    (symbol, side, qty, price, status, wallet_id)
                VALUES ('BTCUSDT', 'BUY', 0.01, 65000, 'OPEN', $1)
                """,
                wallet["id"],
            )
    finally:
        await conn.execute("ROLLBACK")


@pytest.mark.asyncio
async def test_rejected_order_has_no_position(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=50.0)
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
    rej = await conn.fetchrow(
        """
        SELECT id FROM paper_orders
        WHERE wallet_id = $1 AND status = 'REJECTED'
        ORDER BY created_at DESC LIMIT 1
        """,
        wallet["id"],
    )
    assert rej is not None
    n = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE order_id = $1", rej["id"]
    )
    assert n == 0


@pytest.mark.asyncio
async def test_filled_has_exactly_one_position_and_one_reserve(
    conn: asyncpg.Connection,
):
    wallet = await _funded_wallet(conn)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    n_pos = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE order_id = $1", order_id
    )
    assert n_pos == 1
    pos = await conn.fetchrow(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    n_res = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RESERVE'
        """,
        pos["id"],
    )
    assert n_res == 1


# --- 5.4 Delay / connection ---


@pytest.mark.asyncio
async def test_execution_delay_does_not_hold_caller_connection(
    conn: asyncpg.Connection,
):
    """With delay > 0, place_order must sleep before using caller conn."""
    import mock_exchange as me

    wallet = await _funded_wallet(conn)
    held = {"during_sleep": False}
    real_sleep = asyncio.sleep

    async def tracked_sleep(delay, *args, **kwargs):
        await conn.fetchval("SELECT 1")
        held["during_sleep"] = True
        # Do not actually wait full delay in unit test.
        await real_sleep(0)

    with (
        patch.object(me, "EXECUTION_DELAY_SEC", 1),
        patch.object(me.asyncio, "sleep", tracked_sleep),
    ):
        order_id = await me.place_order(
            "BTCUSDT",
            "BUY",
            0.01,
            65000.0,
            conn,
            wallet_id=wallet["id"],
        )
    assert held["during_sleep"] is True
    assert order_id is not None


@pytest.mark.asyncio
async def test_agent_tick_releases_conn_before_place_order():
    call_order: list[str] = []

    async def mock_init(conn):
        call_order.append("init")

    async def mock_pm(conn=None):
        call_order.append("pm")

    async def mock_place(*args, **kwargs):
        call_order.append("place_order")
        assert kwargs.get("conn") is None
        assert len(args) < 5 or args[4] is None
        return uuid.uuid4()

    pool = MagicMock()
    conn = AsyncMock()
    conn.fetch = AsyncMock(
        return_value=[
            {
                "wallet_id": uuid.uuid4(),
                "wallet_address": "addr",
                "win_rate": 0.7,
            }
        ]
    )

    class CM:
        async def __aenter__(self):
            call_order.append("acquire")
            return conn

        async def __aexit__(self, *args):
            call_order.append("release")

    pool.acquire = MagicMock(return_value=CM())

    with (
        patch.object(agent_trader, "initialize_paper_allocations", mock_init),
        patch.object(agent_trader.position_manager, "tick", mock_pm),
        patch.object(agent_trader, "place_order", mock_place),
        patch.object(
            agent_trader, "check_daily_loss", AsyncMock(return_value=False)
        ),
        patch.object(agent_trader, "_current_btc_price", return_value=65000.0),
    ):
        await agent_trader.tick(pool)

    assert call_order.index("release") < call_order.index("place_order")


# --- 5.6 SL/TP on paper open path ---


@pytest.mark.asyncio
async def test_buy_open_persists_sl_and_tp(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    await place_order(
        "BTCUSDT",
        "BUY",
        0.01,
        65000.0,
        conn,
        wallet_id=wallet["id"],
        stop_loss_pct=0.03,
        take_profit_pct=0.06,
    )
    pos = await conn.fetchrow(
        """
        SELECT stop_loss_price, take_profit_price, side
        FROM positions WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
        """,
        wallet["id"],
    )
    assert float(pos["stop_loss_price"]) == pytest.approx(65000 * 0.97)
    assert float(pos["take_profit_price"]) == pytest.approx(65000 * 1.06)


@pytest.mark.asyncio
async def test_sell_open_persists_side_aware_sl_tp(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    await place_order(
        "BTCUSDT",
        "SELL",
        0.01,
        65000.0,
        conn,
        wallet_id=wallet["id"],
        stop_loss_pct=0.03,
        take_profit_pct=0.06,
    )
    pos = await conn.fetchrow(
        """
        SELECT stop_loss_price, take_profit_price
        FROM positions WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
        """,
        wallet["id"],
    )
    assert float(pos["stop_loss_price"]) == pytest.approx(65000 * 1.03)
    assert float(pos["take_profit_price"]) == pytest.approx(65000 * 0.94)


@pytest.mark.asyncio
async def test_pm_tp_on_real_open_path(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    await place_order(
        "BTCUSDT",
        "BUY",
        0.01,
        65000.0,
        conn,
        wallet_id=wallet["id"],
        take_profit_pct=0.06,
    )
    pos = await conn.fetchrow(
        "SELECT id, take_profit_price FROM positions WHERE wallet_id = $1 "
        "AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1",
        wallet["id"],
    )
    _set_mark("BTCUSDT", float(pos["take_profit_price"]) + 1)
    await pm_tick(conn)
    closed = await conn.fetchrow(
        "SELECT close_reason, closed_at FROM positions WHERE id = $1", pos["id"]
    )
    assert closed["closed_at"] is not None
    assert closed["close_reason"] == "TAKE_PROFIT"


def test_evaluate_dual_trigger_sl_wins():
    row = {
        "side": "BUY",
        "stop_loss_price": 70000.0,
        "take_profit_price": 60000.0,
    }
    assert evaluate_close_trigger(row, 65000.0) == "STOP_LOSS"


# --- 5.7 Wallet-scoped get_position ---


@pytest.mark.asyncio
async def test_get_position_is_wallet_scoped(conn: asyncpg.Connection):
    a = await _funded_wallet(conn)
    b = await _funded_wallet(conn)
    await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=a["id"]
    )
    await place_order(
        "BTCUSDT", "SELL", 0.01, 64000.0, conn, wallet_id=b["id"]
    )
    pa = await get_position("BTCUSDT", conn, wallet_id=a["id"])
    pb = await get_position("BTCUSDT", conn, wallet_id=b["id"])
    assert pa is not None and pb is not None
    assert pa["wallet_id"] == a["id"]
    assert pb["wallet_id"] == b["id"]
    assert pa["id"] != pb["id"]
    assert pa["side"] == "BUY"
    assert pb["side"] == "SELL"


@pytest.mark.asyncio
async def test_get_position_requires_wallet_id(conn: asyncpg.Connection):
    with pytest.raises(TypeError):
        await get_position("BTCUSDT", conn)  # type: ignore[call-arg]


# --- 5.8 Reconciliation ---


@pytest.mark.asyncio
async def test_reconcile_pass_after_healthy_open_close(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    pos = await conn.fetchrow(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    await close_position(pos["id"], 66000.0, conn, close_reason="ADMIN")
    result = await reconcile(conn)
    critical = [m for m in result.mismatches if not m.reason.startswith("legacy_")]
    assert critical == []
    assert result.ok


@pytest.mark.asyncio
async def test_reconcile_fail_on_new_filled_orphan(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    order_id = await conn.fetchval(
        """
        INSERT INTO paper_orders
            (symbol, side, qty, price, status, wallet_id)
        VALUES ('BTCUSDT', 'BUY', 0.01, 65000, 'FILLED', $1)
        RETURNING id
        """,
        wallet["id"],
    )
    try:
        result = await reconcile(conn)
        reasons = {m.reason for m in result.mismatches}
        assert "filled_order_without_position" in reasons
        assert result.ok is False
    finally:
        await conn.execute(
            "DELETE FROM paper_orders WHERE id = $1", order_id
        )


@pytest.mark.asyncio
async def test_reconcile_fail_on_rejected_with_position(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn)
    order_id = await conn.fetchval(
        """
        INSERT INTO paper_orders
            (symbol, side, qty, price, status, wallet_id)
        VALUES ('BTCUSDT', 'BUY', 0.01, 65000, 'REJECTED', $1)
        RETURNING id
        """,
        wallet["id"],
    )
    pos_id = await conn.fetchval(
        """
        INSERT INTO positions (
            symbol, entry_price, qty, wallet_id, side, order_id
        )
        VALUES ('BTCUSDT', 65000, 0.01, $1, 'BUY', $2)
        RETURNING id
        """,
        wallet["id"],
        order_id,
    )
    try:
        result = await reconcile(conn)
        assert any(
            m.reason == "rejected_or_failed_order_has_position"
            for m in result.mismatches
        )
        assert result.ok is False
    finally:
        await conn.execute("DELETE FROM positions WHERE id = $1", pos_id)
        await conn.execute("DELETE FROM paper_orders WHERE id = $1", order_id)


@pytest.mark.asyncio
async def test_reconcile_fail_on_new_null_order_id_funded_position(
    conn: asyncpg.Connection,
):
    """NEW funded NULL order_id is critical unless in the legacy registry."""
    wallet = await _funded_wallet(conn)
    pos_id = await conn.fetchval(
        """
        INSERT INTO positions (
            symbol, entry_price, qty, wallet_id, side, reserved_margin
        )
        VALUES ('BTCUSDT', 65000, 0.001, $1, 'BUY', 65.0)
        RETURNING id
        """,
        wallet["id"],
    )
    try:
        # Direct ledger insert (test fixture) — proves reconcile classification
        # without mutating wallet_balances through capital.py.
        await conn.execute(
            """
            INSERT INTO capital_ledger (
                wallet_id, entry_type, amount, position_id, note
            )
            VALUES ($1, 'RESERVE', 65.0, $2, 'audit_new_null_link')
            """,
            wallet["id"],
            pos_id,
        )
        result = await reconcile(conn)
        assert any(
            m.reason == "position_missing_order_link"
            and m.position_id == str(pos_id)
            for m in result.mismatches
        )
        assert result.ok is False
    finally:
        await conn.execute(
            """
            INSERT INTO phase5_legacy_unlinked_positions (position_id)
            VALUES ($1) ON CONFLICT DO NOTHING
            """,
            pos_id,
        )
        await conn.execute(
            """
            UPDATE positions
            SET closed_at = COALESCE(closed_at, NOW()),
                close_reason = COALESCE(close_reason, 'ADMIN'),
                exit_price = COALESCE(exit_price, entry_price)
            WHERE id = $1
            """,
            pos_id,
        )


@pytest.mark.asyncio
async def test_reconcile_legacy_registry_position_nonfatal(
    conn: asyncpg.Connection,
):
    """Registry-listed NULL order_id + RESERVE remains a non-fatal legacy note."""
    wallet = await _funded_wallet(conn)
    pos_id = await conn.fetchval(
        """
        INSERT INTO positions (
            symbol, entry_price, qty, wallet_id, side, reserved_margin
        )
        VALUES ('BTCUSDT', 65000, 0.001, $1, 'BUY', 65.0)
        RETURNING id
        """,
        wallet["id"],
    )
    try:
        await conn.execute(
            """
            INSERT INTO capital_ledger (
                wallet_id, entry_type, amount, position_id, note
            )
            VALUES ($1, 'RESERVE', 65.0, $2, 'audit_legacy_reg')
            """,
            wallet["id"],
            pos_id,
        )
        await conn.execute(
            """
            INSERT INTO phase5_legacy_unlinked_positions (position_id)
            VALUES ($1)
            """,
            pos_id,
        )
        result = await reconcile(conn)
        assert any(
            m.reason == "legacy_position_missing_order_link"
            and m.position_id == str(pos_id)
            for m in result.mismatches
        )
        assert not any(
            m.reason == "position_missing_order_link"
            and m.position_id == str(pos_id)
            for m in result.mismatches
        )
    finally:
        # Append-only ledger: neutralize via legacy registry; do not DELETE.
        await conn.execute(
            """
            INSERT INTO phase5_legacy_unlinked_positions (position_id)
            VALUES ($1) ON CONFLICT DO NOTHING
            """,
            pos_id,
        )
        await conn.execute(
            """
            UPDATE positions
            SET closed_at = COALESCE(closed_at, NOW()),
                close_reason = COALESCE(close_reason, 'ADMIN'),
                exit_price = COALESCE(exit_price, entry_price)
            WHERE id = $1
            """,
            pos_id,
        )


# --- 5.5 / 5.3 close regression ---


@pytest.mark.asyncio
async def test_double_close_still_rejected(conn: asyncpg.Connection):
    from mock_exchange import PositionAlreadyClosed

    wallet = await _funded_wallet(conn)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    pos = await conn.fetchrow(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    await close_position(pos["id"], 66000.0, conn, close_reason="ADMIN")
    with pytest.raises(PositionAlreadyClosed):
        await close_position(pos["id"], 66100.0, conn, close_reason="ADMIN")


@pytest.mark.asyncio
async def test_concurrent_close_one_settlement(conn: asyncpg.Connection):
    from mock_exchange import PositionAlreadyClosed

    wallet = await _funded_wallet(conn)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    pos = await conn.fetchrow(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    url = _database_url()

    async def _attempt():
        c = await asyncpg.connect(url)
        try:
            return await close_position(
                pos["id"], 65500.0, c, close_reason="ADMIN"
            )
        except PositionAlreadyClosed as exc:
            return exc
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(), _attempt())
    successes = [r for r in results if isinstance(r, float)]
    closed = [r for r in results if isinstance(r, PositionAlreadyClosed)]
    assert len(successes) == 1
    assert len(closed) == 1


# --- 5.9 concurrency with delay > 0 ---


@pytest.mark.asyncio
async def test_concurrent_opens_with_delay_no_overspend(conn: asyncpg.Connection):
    import mock_exchange as me

    wallet = await _funded_wallet(conn, amount=650.0)
    url = _database_url()
    real_sleep = asyncio.sleep

    async def fast_sleep(delay, *args, **kwargs):
        await real_sleep(min(float(delay), 0.05))

    async def _attempt():
        c = await asyncpg.connect(url)
        try:
            await me.place_order(
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

    with (
        patch.object(me, "EXECUTION_DELAY_SEC", 1),
        patch.object(me.asyncio, "sleep", fast_sleep),
    ):
        results = await asyncio.gather(_attempt(), _attempt())
    assert sorted(results) == ["insufficient", "ok"]
    bal = await get_wallet_balance(conn, wallet["id"])
    assert float(bal["reserved_margin"]) == pytest.approx(650.0)
    opens = await conn.fetchval(
        """
        SELECT COUNT(*) FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        """,
        wallet["id"],
    )
    assert opens == 1
