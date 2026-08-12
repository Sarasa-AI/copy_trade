"""Phase 4.5 — capital integrity, reconciliation, rollback, concurrency."""

from __future__ import annotations

import asyncio
import os
import uuid
from decimal import Decimal

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import (  # noqa: E402
    DuplicateCapitalSettlement,
    InsufficientAvailableBalance,
    InsufficientMasterCapital,
    InsufficientReservedMargin,
    allocate_to_wallet,
    ensure_funded_wallet,
    ensure_master_pool,
    get_master_pool,
    get_wallet_balance,
    realize_pnl,
    release_margin,
    reserve_margin,
)
from mock_exchange import (  # noqa: E402
    PositionAlreadyClosed,
    close_position,
    place_order,
)
from reconcile_capital import format_human, reconcile  # noqa: E402
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
    wallet = await create_wallet(conn, f"p45_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=amount)
    return wallet


async def _stub_open_position(
    conn: asyncpg.Connection, wallet_id
) -> uuid.UUID:
    """Insert a minimal open position row for ledger FK / uniqueness tests."""
    pos_id = await conn.fetchval(
        """
        INSERT INTO positions (symbol, entry_price, qty, wallet_id, side)
        VALUES ('BTCUSDT', 65000, 0.001, $1, 'BUY')
        RETURNING id
        """,
        wallet_id,
    )
    assert pos_id is not None
    return pos_id


async def _purge_stub_positions(
    conn: asyncpg.Connection, *position_ids: uuid.UUID
) -> None:
    """Neutralize NULL-order_id stubs for reconcile under append-only ledger.

    Production ``capital_ledger`` forbids DELETE. Register stubs in the Phase 5
    legacy registry so leftover RESERVE rows are non-fatal notes, then mark
    positions closed when still open (no capital mutation).
    """
    ids = [pid for pid in position_ids if pid is not None]
    if not ids:
        return
    for pid in ids:
        await conn.execute(
            """
            INSERT INTO phase5_legacy_unlinked_positions (position_id)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            """,
            pid,
        )
        await conn.execute(
            """
            UPDATE positions
            SET closed_at = COALESCE(closed_at, NOW()),
                close_reason = COALESCE(close_reason, 'ADMIN'),
                exit_price = COALESCE(exit_price, entry_price)
            WHERE id = $1
            """,
            pid,
        )


async def _open_small_position(
    conn: asyncpg.Connection, wallet_id
) -> asyncpg.Record:
    await place_order(
        "BTCUSDT",
        "BUY",
        0.001,
        65000.0,
        conn,
        wallet_id=wallet_id,
        leverage=1.0,
    )
    pos = await conn.fetchrow(
        """
        SELECT * FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
        """,
        wallet_id,
    )
    assert pos is not None
    return pos


# --- Scenario A: reserve then failure rolls back ---


@pytest.mark.asyncio
async def test_reserve_then_failure_rolls_back(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=500.0)
    before = await get_wallet_balance(conn, wallet["id"])
    fake_pos = await _stub_open_position(conn, wallet["id"])

    with pytest.raises(RuntimeError, match="forced_failure"):
        async with conn.transaction():
            await reserve_margin(
                conn, wallet["id"], 100.0, position_id=fake_pos, note="A"
            )
            raise RuntimeError("forced_failure")

    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == pytest.approx(
        float(before["available_balance"])
    )
    assert float(after["reserved_margin"]) == pytest.approx(
        float(before["reserved_margin"])
    )
    ledger_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RESERVE'
        """,
        fake_pos,
    )
    assert ledger_n == 0
    # Remove stub so reserved-vs-open reconcile stays clean (no capital reserved).
    await conn.execute("DELETE FROM positions WHERE id = $1", fake_pos)


# --- Scenario B: release then realize failure — no partial settlement ---


@pytest.mark.asyncio
async def test_release_then_realize_failure_rolls_back(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=1000.0)
    pos = await _open_small_position(conn, wallet["id"])
    reserved = float(pos["reserved_margin"])
    before = await get_wallet_balance(conn, wallet["id"])

    # Pre-seed REALIZE_PNL so realize_pnl rejects as duplicate inside close TX.
    await conn.execute(
        """
        INSERT INTO capital_ledger (
            wallet_id, entry_type, amount, position_id, note
        )
        VALUES ($1, 'REALIZE_PNL', 0, $2, 'poison')
        """,
        wallet["id"],
        pos["id"],
    )

    with pytest.raises(DuplicateCapitalSettlement):
        await close_position(pos["id"], 66000.0, conn, close_reason="ADMIN")

    still_open = await conn.fetchrow(
        "SELECT closed_at FROM positions WHERE id = $1", pos["id"]
    )
    assert still_open["closed_at"] is None
    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["reserved_margin"]) == pytest.approx(reserved)
    assert float(after["available_balance"]) == pytest.approx(
        float(before["available_balance"])
    )
    release_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RELEASE'
        """,
        pos["id"],
    )
    assert release_n == 0

    # Cleanup poison: append-only ledger forbids DELETE — reset dedicated test DB.
    from db_isolation import reset_critical_db_state

    await reset_critical_db_state(conn)


# --- Scenario C: ledger insert failure leaves balances unchanged ---


@pytest.mark.asyncio
async def test_ledger_failure_rolls_back_balance(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=500.0)
    before = await get_wallet_balance(conn, wallet["id"])
    pos_id = await _stub_open_position(conn, wallet["id"])
    await reserve_margin(conn, wallet["id"], 50.0, position_id=pos_id)
    # Keep positions.reserved_margin aligned for reconcile during the test window.
    await conn.execute(
        "UPDATE positions SET reserved_margin = 50 WHERE id = $1", pos_id
    )

    mid = await get_wallet_balance(conn, wallet["id"])
    assert float(mid["reserved_margin"]) == pytest.approx(50.0)

    with pytest.raises(DuplicateCapitalSettlement):
        await reserve_margin(conn, wallet["id"], 10.0, position_id=pos_id)

    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == pytest.approx(
        float(mid["available_balance"])
    )
    assert float(after["reserved_margin"]) == pytest.approx(50.0)
    reserve_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RESERVE'
        """,
        pos_id,
    )
    assert reserve_n == 1

    # Release + realize so capital-managed position settles cleanly, then purge.
    await release_margin(conn, wallet["id"], 50.0, position_id=pos_id)
    await realize_pnl(conn, wallet["id"], 0.0, position_id=pos_id)
    await _purge_stub_positions(conn, pos_id)
    final = await get_wallet_balance(conn, wallet["id"])
    assert float(final["available_balance"]) == pytest.approx(
        float(before["available_balance"])
    )


# --- Scenario D: concurrent close ---


@pytest.mark.asyncio
async def test_concurrent_close_exactly_one_settlement(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=1000.0)
    pos = await _open_small_position(conn, wallet["id"])
    url = _database_url()

    async def _attempt() -> str:
        c = await asyncpg.connect(url)
        try:
            await close_position(pos["id"], 66000.0, c, close_reason="ADMIN")
            return "ok"
        except PositionAlreadyClosed:
            return "already_closed"
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(), _attempt())
    assert sorted(results) == ["already_closed", "ok"]

    release_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RELEASE'
        """,
        pos["id"],
    )
    realize_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'REALIZE_PNL'
        """,
        pos["id"],
    )
    assert release_n == 1
    assert realize_n == 1
    bal = await get_wallet_balance(conn, wallet["id"])
    assert float(bal["reserved_margin"]) == pytest.approx(0.0)


# --- Scenario E: concurrent reserves cannot overspend ---


@pytest.mark.asyncio
async def test_concurrent_reserves_cannot_overspend(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=100.0)
    url = _database_url()
    p1 = await _stub_open_position(conn, wallet["id"])
    p2 = await _stub_open_position(conn, wallet["id"])

    async def _attempt(pos_id) -> str:
        c = await asyncpg.connect(url)
        try:
            await reserve_margin(c, wallet["id"], 80.0, position_id=pos_id)
            return "ok"
        except InsufficientAvailableBalance:
            return "insufficient"
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(p1), _attempt(p2))
    assert sorted(results) == ["insufficient", "ok"]
    bal = await get_wallet_balance(conn, wallet["id"])
    assert float(bal["reserved_margin"]) == pytest.approx(80.0)
    assert float(bal["available_balance"]) == pytest.approx(20.0)

    # Cleanup — settle winner capital-managed stub; purge both stubs.
    winner = p1 if results[0] == "ok" else p2
    loser = p2 if winner == p1 else p1
    await release_margin(conn, wallet["id"], 80.0, position_id=winner)
    await realize_pnl(conn, wallet["id"], 0.0, position_id=winner)
    await _purge_stub_positions(conn, winner, loser)


@pytest.mark.asyncio
async def test_concurrent_allocate_respects_master_pool(conn: asyncpg.Connection):
    await ensure_master_pool(conn)
    master = await get_master_pool(conn)
    available = float(master["available_capital"])
    # Shrink available by allocating a large chunk to a sink wallet if needed,
    # then race two allocations for more than half of remaining.
    # Use two new wallets allocating the same amount where only one fits.
    amount = max(available / 2 + 1.0, 1.0)
    if available < amount:
        # Expand then allocate sink so only `amount` remains... simpler:
        # lock by setting up two wallets racing for all remaining capital.
        amount = available
    if amount <= 0:
        # Expand pool so we have a known scarce amount.
        await conn.execute(
            """
            UPDATE master_pool
            SET total_capital = total_capital + 50,
                available_capital = available_capital + 50
            WHERE id = 1
            """
        )
        amount = 50.0

    w1 = await create_wallet(conn, f"p45_alloc_a_{uuid.uuid4().hex[:8]}")
    w2 = await create_wallet(conn, f"p45_alloc_b_{uuid.uuid4().hex[:8]}")
    url = _database_url()

    # Leave exactly `amount` available for the race (one succeeds).
    master = await get_master_pool(conn)
    surplus = float(master["available_capital"]) - amount
    if surplus > 0:
        sink = await create_wallet(conn, f"p45_sink_{uuid.uuid4().hex[:8]}")
        await allocate_to_wallet(conn, sink["id"], surplus, note="sink")

    async def _attempt(wid) -> str:
        c = await asyncpg.connect(url)
        try:
            await allocate_to_wallet(c, wid, amount, note="race")
            return "ok"
        except InsufficientMasterCapital:
            return "insufficient"
        finally:
            await c.close()

    results = await asyncio.gather(_attempt(w1["id"]), _attempt(w2["id"]))
    assert sorted(results) == ["insufficient", "ok"]
    master_after = await get_master_pool(conn)
    assert float(master_after["available_capital"]) == pytest.approx(0.0)
    assert float(master_after["allocated_capital"]) + float(
        master_after["available_capital"]
    ) == pytest.approx(float(master_after["total_capital"]))


@pytest.mark.asyncio
async def test_cross_wallet_concurrency_isolation(conn: asyncpg.Connection):
    a = await _funded_wallet(conn, amount=500.0)
    b = await _funded_wallet(conn, amount=500.0)
    url = _database_url()
    pa = await _stub_open_position(conn, a["id"])
    pb = await _stub_open_position(conn, b["id"])

    async def _reserve(wid, pos_id) -> None:
        c = await asyncpg.connect(url)
        try:
            await reserve_margin(c, wid, 100.0, position_id=pos_id)
        finally:
            await c.close()

    await asyncio.gather(_reserve(a["id"], pa), _reserve(b["id"], pb))
    bal_a = await get_wallet_balance(conn, a["id"])
    bal_b = await get_wallet_balance(conn, b["id"])
    assert float(bal_a["reserved_margin"]) == pytest.approx(100.0)
    assert float(bal_b["reserved_margin"]) == pytest.approx(100.0)
    assert float(bal_a["available_balance"]) == pytest.approx(400.0)
    assert float(bal_b["available_balance"]) == pytest.approx(400.0)
    await release_margin(conn, a["id"], 100.0, position_id=pa)
    await release_margin(conn, b["id"], 100.0, position_id=pb)
    await realize_pnl(conn, a["id"], 0.0, position_id=pa)
    await realize_pnl(conn, b["id"], 0.0, position_id=pb)
    await _purge_stub_positions(conn, pa, pb)


@pytest.mark.asyncio
async def test_double_release_and_realize_rejected(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=1000.0)
    pos = await _open_small_position(conn, wallet["id"])
    reserved = float(pos["reserved_margin"])
    await close_position(pos["id"], 65500.0, conn, close_reason="ADMIN")

    with pytest.raises(DuplicateCapitalSettlement):
        await release_margin(
            conn, wallet["id"], reserved, position_id=pos["id"]
        )
    with pytest.raises(DuplicateCapitalSettlement):
        await realize_pnl(conn, wallet["id"], 1.0, position_id=pos["id"])

    other = await _stub_open_position(conn, wallet["id"])
    with pytest.raises(InsufficientReservedMargin):
        await release_margin(conn, wallet["id"], 10.0, position_id=other)
    await _purge_stub_positions(conn, other)


@pytest.mark.asyncio
async def test_reconcile_pass_on_healthy_cycle(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=1000.0)
    pos = await _open_small_position(conn, wallet["id"])
    await close_position(pos["id"], 66000.0, conn, close_reason="TAKE_PROFIT")
    result = await reconcile(conn)
    assert result.ok, format_human(result)


@pytest.mark.asyncio
async def test_reconcile_fail_on_corrupted_reserved(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=1000.0)
    # Keep conservation + equity valid while drifting reserved vs open positions.
    await conn.execute(
        """
        UPDATE wallet_balances
        SET available_balance = $2::numeric,
            reserved_margin = $3::numeric,
            unrealized_pnl = 0,
            realized_pnl = 0,
            initial_capital = $4::numeric,
            current_equity = ($2::numeric + $3::numeric)
        WHERE wallet_id = $1
        """,
        wallet["id"],
        Decimal("975"),
        Decimal("25"),
        Decimal("1000"),
    )

    result = await reconcile(conn)
    assert not result.ok
    reasons = {m.reason for m in result.mismatches}
    assert "wallet_reserved_vs_open_positions" in reasons
    text = format_human(result)
    assert "CAPITAL RECONCILIATION: FAILED" in text

    # Restore so shared DB stays healthy for other tests / global reconcile.
    await conn.execute(
        """
        UPDATE wallet_balances
        SET available_balance = $2,
            reserved_margin = 0,
            unrealized_pnl = 0,
            realized_pnl = 0,
            initial_capital = $2,
            current_equity = $2
        WHERE wallet_id = $1
        """,
        wallet["id"],
        Decimal("1000"),
    )


@pytest.mark.asyncio
async def test_over_release_rejected(conn: asyncpg.Connection):
    wallet = await _funded_wallet(conn, amount=200.0)
    pos_id = await _stub_open_position(conn, wallet["id"])
    other_id = await _stub_open_position(conn, wallet["id"])
    await reserve_margin(conn, wallet["id"], 50.0, position_id=pos_id)
    await conn.execute(
        "UPDATE positions SET reserved_margin = 50 WHERE id = $1", pos_id
    )
    with pytest.raises(InsufficientReservedMargin):
        await release_margin(conn, wallet["id"], 60.0, position_id=other_id)
    await release_margin(conn, wallet["id"], 50.0, position_id=pos_id)
    await realize_pnl(conn, wallet["id"], 0.0, position_id=pos_id)
    await _purge_stub_positions(conn, pos_id, other_id)


@pytest.mark.asyncio
async def test_stub_reserve_purge_leaves_no_critical_orphan(
    conn: asyncpg.Connection,
):
    """Regression: Phase 4.5 stubs must not become unregistered funded orphans.

    Historical defect: ``_stub_open_position`` + ``reserve_margin`` left
    closed/open NULL-``order_id`` rows with RESERVE ledger and no legacy
    registry membership. That is a critical reconcile failure under Phase 5
    registry rules — not a production ``place_order`` path.
    """
    wallet = await _funded_wallet(conn, amount=300.0)
    pos_id = await _stub_open_position(conn, wallet["id"])
    await reserve_margin(conn, wallet["id"], 40.0, position_id=pos_id)
    await release_margin(conn, wallet["id"], 40.0, position_id=pos_id)
    await realize_pnl(conn, wallet["id"], 0.0, position_id=pos_id)
    await _purge_stub_positions(conn, pos_id)

    # Append-only ledger: stub row may remain, but must not be an unregistered
    # critical orphan (legacy registry membership is required).
    remaining_critical = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM positions p
        WHERE p.order_id IS NULL
          AND EXISTS (
              SELECT 1 FROM capital_ledger cl
              WHERE cl.position_id = p.id AND cl.entry_type = 'RESERVE'
          )
          AND NOT EXISTS (
              SELECT 1 FROM phase5_legacy_unlinked_positions l
              WHERE l.position_id = p.id
          )
          AND p.wallet_id = $1
        """,
        wallet["id"],
    )
    assert remaining_critical == 0
    result = await reconcile(conn)
    critical = [
        m for m in result.mismatches if not str(m.reason).startswith("legacy_")
    ]
    assert not any(
        m.reason == "position_missing_order_link" for m in critical
    ), critical
