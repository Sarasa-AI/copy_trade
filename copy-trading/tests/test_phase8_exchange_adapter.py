"""Phase 8 — exchange adapter boundary and venue execution path (hermetic).

Every test runs against ``NullAdapter``: no sockets, no venue credentials, no
network. Venue failure modes are scripted, so CI asserts on behaviour instead of
waiting for a real exchange to misbehave.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"
os.environ["MAX_MARK_AGE_SEC"] = "30"
os.environ["EXECUTION_RESOLVE_BACKOFF_SEC"] = "0"

from capital import ensure_funded_wallet, get_wallet_balance  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402
from exchange_adapter.interface import (  # noqa: E402
    NetworkTimeout,
    OrderFill,
    PlaceOrderRequest,
)
from exchange_adapter.null_adapter import FillPlan, NullAdapter  # noqa: E402
from execution_engine import (  # noqa: E402
    CloseAlreadyInFlight,
    ExecutionEngine,
    ExecutionError,
    ExecutionUncertain,
    OrderNotFilled,
    PartialCloseUnsupported,
    SettlementCapitalShortfall,
)
from kill_switch import KillSwitchActive, activate_kill_switch, get_kill_switch  # noqa: E402
import price_feed  # noqa: E402
from risk_engine import RiskDenied  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402

SYMBOL = "BTCUSDT"
MARK = Decimal("65000")


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


def _adapter(**kwargs) -> NullAdapter:
    kwargs.setdefault("mark_prices", {SYMBOL: MARK})
    return NullAdapter(**kwargs)


def _engine(adapter: NullAdapter | None = None) -> ExecutionEngine:
    return ExecutionEngine(adapter or _adapter())


async def _funded(conn: asyncpg.Connection, amount: float = 5000.0):
    wallet = await create_wallet(conn, f"p8_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=amount)
    return wallet


async def _order_row(conn: asyncpg.Connection, order_row_id) -> asyncpg.Record:
    row = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1", order_row_id
    )
    assert row is not None
    return row


async def _counts(conn: asyncpg.Connection, wallet_id) -> dict[str, int]:
    return {
        "positions": int(
            await conn.fetchval(
                "SELECT COUNT(*) FROM positions WHERE wallet_id = $1", wallet_id
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
        "exchange_orders": int(
            await conn.fetchval(
                "SELECT COUNT(*) FROM exchange_orders WHERE wallet_id = $1",
                wallet_id,
            )
        ),
    }


# --- interface invariants (no DB, no adapter) ---------------------------------


def test_place_order_request_validates_inputs():
    with pytest.raises(ValueError):
        PlaceOrderRequest(
            client_order_id=uuid.uuid4(),
            symbol=SYMBOL,
            side="BUY",
            order_type="MARKET",
            qty=Decimal("0"),
        )
    with pytest.raises(ValueError):
        PlaceOrderRequest(
            client_order_id=uuid.uuid4(),
            symbol=SYMBOL,
            side="LONG",  # type: ignore[arg-type]
            order_type="MARKET",
            qty=Decimal("1"),
        )
    with pytest.raises(ValueError):
        PlaceOrderRequest(
            client_order_id=uuid.uuid4(),
            symbol=SYMBOL,
            side="BUY",
            order_type="LIMIT",
            qty=Decimal("1"),
            price=None,
        )


def test_order_fill_rejects_fill_without_price():
    with pytest.raises(ValueError):
        OrderFill(
            client_order_id=uuid.uuid4(),
            exchange_order_id="x",
            symbol=SYMBOL,
            side="BUY",
            status="FILLED",
            qty_requested=Decimal("1"),
            qty_filled=Decimal("1"),
            avg_fill_price=None,
        )


# --- adapter behaviour (no DB) ------------------------------------------------


@pytest.mark.asyncio
async def test_null_adapter_is_idempotent_on_client_order_id():
    adapter = _adapter()
    req = PlaceOrderRequest(
        client_order_id=uuid.uuid4(),
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=Decimal("0.01"),
    )
    first = await adapter.place_order(req)
    second = await adapter.place_order(req)
    assert adapter.sent_count == 1
    assert adapter.duplicate_send_count == 1
    assert first.exchange_order_id == second.exchange_order_id


@pytest.mark.asyncio
async def test_null_adapter_slippage_is_adverse_for_both_sides():
    adapter = _adapter(slippage_bps=Decimal("20"))
    buy = await adapter.place_order(
        PlaceOrderRequest(
            client_order_id=uuid.uuid4(),
            symbol=SYMBOL,
            side="BUY",
            order_type="MARKET",
            qty=Decimal("0.01"),
        )
    )
    sell = await adapter.place_order(
        PlaceOrderRequest(
            client_order_id=uuid.uuid4(),
            symbol=SYMBOL,
            side="SELL",
            order_type="MARKET",
            qty=Decimal("0.01"),
        )
    )
    assert buy.avg_fill_price == Decimal("65130.00000000")
    assert sell.avg_fill_price == Decimal("64870.00000000")


# --- open path ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_open_reserves_margin_at_fill_price_not_signal_price(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    engine = _engine(_adapter(slippage_bps=Decimal("20")))

    result = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    assert result.status == "FILLED"
    assert result.avg_fill_price == Decimal("65130.00000000")
    assert result.signal_price == MARK
    assert result.slippage_bps == pytest.approx(20.0, rel=1e-9)

    expected_margin = 0.01 * 65130.0
    assert result.reserved_margin == pytest.approx(expected_margin, rel=1e-9)

    balance = await get_wallet_balance(conn, wallet["id"])
    assert float(balance["reserved_margin"]) == pytest.approx(
        expected_margin, rel=1e-9
    )

    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", result.position_id
    )
    # Entry price is venue truth, not the pre-trade estimate.
    assert float(position["entry_price"]) == pytest.approx(65130.0, rel=1e-9)
    assert position["exchange_order_id"] == result.order_row_id
    assert position["order_id"] is None

    order = await _order_row(conn, result.order_row_id)
    assert order["status"] == "FILLED"
    assert float(order["signal_price"]) == pytest.approx(65000.0, rel=1e-9)
    assert float(order["avg_fill_price"]) == pytest.approx(65130.0, rel=1e-9)
    assert order["position_id"] == result.position_id
    assert order["adapter_name"] == "null"

    fills = await conn.fetch(
        "SELECT * FROM exchange_fills WHERE exchange_order_row_id = $1",
        result.order_row_id,
    )
    assert len(fills) == 1
    assert float(fills[0]["qty_filled"]) == pytest.approx(0.01, rel=1e-9)

    ledger = await conn.fetchrow(
        """
        SELECT * FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RESERVE'
        """,
        result.position_id,
    )
    assert float(ledger["amount"]) == pytest.approx(expected_margin, rel=1e-9)
    assert ledger["order_id"] == result.order_row_id
    assert ledger["correlation_id"] == result.correlation_id


@pytest.mark.asyncio
async def test_rejected_order_moves_no_capital(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter()
    adapter.queue_plan(FillPlan(kind="REJECT", reject_code="INSUFFICIENT_MARGIN"))
    engine = _engine(adapter)
    before = await get_wallet_balance(conn, wallet["id"])

    with pytest.raises(OrderNotFilled) as ei:
        await engine.open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.01"),
            signal_price=MARK,
        )

    assert ei.value.result.status == "REJECTED"
    order = await _order_row(conn, ei.value.result.order_row_id)
    assert order["status"] == "REJECTED"
    assert order["reject_code"] == "INSUFFICIENT_MARGIN"

    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == float(before["available_balance"])
    assert float(after["reserved_margin"]) == float(before["reserved_margin"])
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 0
    assert counts["reserves"] == 0


@pytest.mark.asyncio
async def test_partial_fill_cancels_remainder_and_reserves_filled_qty_only(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter()
    adapter.queue_plan(FillPlan(kind="PARTIAL", fill_ratio=Decimal("0.4")))
    engine = _engine(adapter)

    result = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    # Remainder is cancelled so the position carries exactly one RESERVE.
    assert result.status == "CANCELLED"
    assert result.qty_filled == Decimal("0.00400000")
    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", result.position_id
    )
    assert float(position["qty"]) == pytest.approx(0.004, rel=1e-9)

    reserves = int(
        await conn.fetchval(
            """
            SELECT COUNT(*) FROM capital_ledger
            WHERE position_id = $1 AND entry_type = 'RESERVE'
            """,
            result.position_id,
        )
    )
    assert reserves == 1
    balance = await get_wallet_balance(conn, wallet["id"])
    assert float(balance["reserved_margin"]) == pytest.approx(
        0.004 * 65000.0, rel=1e-9
    )


@pytest.mark.asyncio
async def test_lost_response_after_fill_recovers_without_second_order(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter()
    adapter.queue_plan(FillPlan(kind="TIMEOUT_AFTER_FILL"))
    engine = _engine(adapter)

    result = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    assert result.status == "FILLED"
    assert adapter.sent_count == 1
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 1
    assert counts["reserves"] == 1
    assert counts["exchange_orders"] == 1


@pytest.mark.asyncio
async def test_never_sent_order_does_not_arm_kill_switch(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter()
    adapter.queue_plan(FillPlan(kind="TIMEOUT_BEFORE_SEND"))
    engine = _engine(adapter)

    with pytest.raises(OrderNotFilled) as ei:
        await engine.open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.01"),
            signal_price=MARK,
        )

    order = await _order_row(conn, ei.value.result.order_row_id)
    assert order["status"] == "CANCELLED"
    assert order["reject_code"] == "NEVER_SENT"
    assert (await get_kill_switch(conn))["active"] is False
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 0
    assert counts["reserves"] == 0


class _BlackHoleAdapter(NullAdapter):
    """Fills, then refuses to ever disclose order state."""

    async def get_order_status(self, client_order_id, symbol):
        raise NetworkTimeout(
            "status endpoint unavailable", client_order_id=client_order_id
        )


@pytest.mark.asyncio
async def test_unresolvable_state_arms_kill_switch_and_leaves_capital_untouched(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _BlackHoleAdapter(mark_prices={SYMBOL: MARK})
    adapter.queue_plan(FillPlan(kind="TIMEOUT_AFTER_FILL"))
    engine = _engine(adapter)
    before = await get_wallet_balance(conn, wallet["id"])

    with pytest.raises(ExecutionUncertain) as ei:
        await engine.open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.01"),
            signal_price=MARK,
        )

    order = await _order_row(conn, ei.value.order_row_id)
    assert order["status"] == "UNKNOWN"
    assert (await get_kill_switch(conn))["active"] is True

    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == float(before["available_balance"])
    assert float(after["reserved_margin"]) == 0.0
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 0
    assert counts["reserves"] == 0


@pytest.mark.asyncio
async def test_kill_switch_blocks_open_before_any_venue_call(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    await activate_kill_switch(conn, reason="phase8 test", actor="pytest")

    with pytest.raises(KillSwitchActive):
        await engine.open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.01"),
            signal_price=MARK,
        )

    assert adapter.sent_count == 0
    counts = await _counts(conn, wallet["id"])
    assert counts["exchange_orders"] == 0
    assert counts["positions"] == 0


@pytest.mark.asyncio
async def test_missing_mark_denies_open_before_any_venue_call(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    price_feed.clear_mark(SYMBOL)

    with pytest.raises(RiskDenied) as ei:
        await engine.open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.01"),
            signal_price=MARK,
        )

    assert ei.value.reason_code == "MARK_MISSING"
    assert adapter.sent_count == 0
    counts = await _counts(conn, wallet["id"])
    assert counts["exchange_orders"] == 0


@pytest.mark.asyncio
async def test_open_requires_connection_without_open_transaction(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    engine = _engine()
    async with conn.transaction():
        with pytest.raises(ExecutionError):
            await engine.open_position(
                conn,
                wallet_id=wallet["id"],
                symbol=SYMBOL,
                side="BUY",
                qty=Decimal("0.01"),
                signal_price=MARK,
            )


# --- close path ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_settles_capital_at_fill_price(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    adapter.set_mark(SYMBOL, Decimal("66000"))
    closed = await engine.close_position(
        opened.position_id, conn, close_reason="ADMIN"
    )

    assert closed.status == "FILLED"
    assert closed.avg_fill_price == Decimal("66000.00000000")
    expected_pnl = (66000.0 - 65000.0) * 0.01
    assert closed.realized_pnl == pytest.approx(expected_pnl, rel=1e-9)

    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", opened.position_id
    )
    assert position["closed_at"] is not None
    assert float(position["exit_price"]) == pytest.approx(66000.0, rel=1e-9)
    assert position["close_reason"] == "ADMIN"

    balance = await get_wallet_balance(conn, wallet["id"])
    assert float(balance["reserved_margin"]) == 0.0
    assert float(balance["realized_pnl"]) == pytest.approx(
        expected_pnl, rel=1e-9
    )
    assert float(balance["available_balance"]) == pytest.approx(
        5000.0 + expected_pnl, rel=1e-9
    )

    close_order = await _order_row(conn, closed.order_row_id)
    assert close_order["intent"] == "CLOSE"
    assert close_order["reduce_only"] is True
    assert close_order["position_id"] == opened.position_id
    assert close_order["side"] == "SELL"

    trades = int(
        await conn.fetchval(
            "SELECT trade_count FROM daily_stats WHERE date = CURRENT_DATE"
        )
    )
    assert trades == 1


@pytest.mark.asyncio
async def test_partial_close_is_refused_and_arms_kill_switch(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    adapter.queue_plan(FillPlan(kind="PARTIAL", fill_ratio=Decimal("0.5")))
    with pytest.raises(PartialCloseUnsupported):
        await engine.close_position(
            opened.position_id, conn, close_reason="ADMIN"
        )

    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", opened.position_id
    )
    assert position["closed_at"] is None
    assert (await get_kill_switch(conn))["active"] is True
    releases = int(
        await conn.fetchval(
            """
            SELECT COUNT(*) FROM capital_ledger
            WHERE position_id = $1 AND entry_type = 'RELEASE'
            """,
            opened.position_id,
        )
    )
    assert releases == 0


@pytest.mark.asyncio
async def test_close_refuses_paper_opened_position(conn: asyncpg.Connection):
    from execution_engine import PositionNotExchangeOpened
    from mock_exchange import place_order

    wallet = await _funded(conn)
    price_feed.update_mark(SYMBOL, 65000.0, source="p8_paper")
    order_id = await place_order(
        SYMBOL, "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    position_id = await conn.fetchval(
        "SELECT id FROM positions WHERE order_id = $1", order_id
    )
    with pytest.raises(PositionNotExchangeOpened):
        await _engine().close_position(
            position_id, conn, close_reason="ADMIN"
        )


# --- audit / reconciliation ---------------------------------------------------


@pytest.mark.asyncio
async def test_exchange_fills_is_append_only(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    result = await _engine().open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )
    fill_id = await conn.fetchval(
        "SELECT id FROM exchange_fills WHERE exchange_order_row_id = $1",
        result.order_row_id,
    )
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await conn.execute(
            "UPDATE exchange_fills SET fee_paid = 1 WHERE id = $1", fill_id
        )
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await conn.execute("DELETE FROM exchange_fills WHERE id = $1", fill_id)


@pytest.mark.asyncio
async def test_capital_reconciliation_accepts_venue_opened_position(
    conn: asyncpg.Connection,
):
    from reconcile_capital import reconcile

    wallet = await _funded(conn)
    await _engine().open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )
    result = await reconcile(conn)
    assert result.ok, result.to_dict()


@pytest.mark.asyncio
async def test_reconcile_probes_schema_before_using_phase8_columns(
    conn: asyncpg.Connection,
):
    # Guards the pre-migration path: reconcile must not assume Phase 8 objects.
    from reconcile_capital import _column_exists, _table_exists

    assert await _table_exists(conn, "exchange_orders")
    assert not await _table_exists(conn, "exchange_orders_nope")
    assert await _column_exists(conn, "positions", "exchange_order_id")
    assert not await _column_exists(conn, "positions", "nope")


@pytest.mark.asyncio
async def test_position_reconciliation_detects_venue_divergence(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = ExecutionEngine(adapter)
    await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    matched = await engine.reconcile_positions(conn)
    assert matched["ok"], matched

    # A venue that has forgotten our position must fail reconciliation.
    blind = await ExecutionEngine(_adapter()).reconcile_positions(conn)
    assert not blind["ok"]
    reasons = {m["reason"] for m in blind["mismatches"]}
    assert "exchange_position_qty_mismatch" in reasons

    runs = int(
        await conn.fetchval("SELECT COUNT(*) FROM reconciliation_runs")
    )
    assert runs == 2


@pytest.mark.asyncio
async def test_mark_bridge_feeds_price_feed_from_adapter_stream():
    from execution_engine import bridge_marks

    adapter = _adapter()
    price_feed.clear_mark(SYMBOL)
    await bridge_marks(adapter, [SYMBOL])
    await adapter.push_mark(SYMBOL, Decimal("64321.5"))

    quote = price_feed.get_mark(SYMBOL)
    assert quote is not None
    assert quote.price == pytest.approx(64321.5, rel=1e-9)
    assert quote.source == "null_ws"
    assert quote.is_fresh()


# --- paper ↔ venue parity -----------------------------------------------------


@pytest.mark.asyncio
async def test_zero_slippage_venue_open_matches_paper_open(
    conn: asyncpg.Connection,
):
    from mock_exchange import place_order

    paper_wallet = await _funded(conn)
    venue_wallet = await _funded(conn)
    price_feed.update_mark(SYMBOL, 65000.0, source="p8_parity")

    paper_order = await place_order(
        SYMBOL, "BUY", 0.01, 65000.0, conn, wallet_id=paper_wallet["id"]
    )
    paper_position = await conn.fetchrow(
        "SELECT * FROM positions WHERE order_id = $1", paper_order
    )

    venue = await _engine().open_position(
        conn,
        wallet_id=venue_wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )
    venue_position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", venue.position_id
    )

    assert float(venue_position["entry_price"]) == pytest.approx(
        float(paper_position["entry_price"]), rel=1e-9
    )
    assert float(venue_position["reserved_margin"]) == pytest.approx(
        float(paper_position["reserved_margin"]), rel=1e-9
    )
    assert float(venue_position["stop_loss_price"]) == pytest.approx(
        float(paper_position["stop_loss_price"]), rel=1e-9
    )
    assert float(venue_position["take_profit_price"]) == pytest.approx(
        float(paper_position["take_profit_price"]), rel=1e-9
    )


# --- audit remediations (C1–C4 / H1 / H3) -------------------------------------


@pytest.mark.asyncio
async def test_settlement_shortfall_persists_fill_and_arms_kill_switch(
    conn: asyncpg.Connection,
):
    """C4: fill audit survives capital shortfall; reconcile can see the orphan."""
    from reconcile_capital import _reconcile_exchange_orders

    # Enough for signal+buffer (~656.5) but not for a 100k fill (margin 1000).
    wallet = await _funded(conn, amount=700.0)
    adapter = _adapter()
    adapter.queue_plan(FillPlan(kind="FILL", price=Decimal("100000")))
    engine = _engine(adapter)

    with pytest.raises(SettlementCapitalShortfall):
        await engine.open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.01"),
            signal_price=MARK,
        )

    order = await conn.fetchrow(
        """
        SELECT status, qty_filled, position_id FROM exchange_orders
        WHERE wallet_id = $1 ORDER BY created_at DESC LIMIT 1
        """,
        wallet["id"],
    )
    assert order["status"] == "UNKNOWN"
    assert float(order["qty_filled"]) == pytest.approx(0.01, rel=1e-9)
    assert order["position_id"] is None
    assert (await get_kill_switch(conn))["active"] is True

    mismatches = await _reconcile_exchange_orders(conn)
    reasons = {m.reason for m in mismatches}
    assert "exchange_order_filled_without_position" in reasons
    assert "exchange_order_state_unresolved" in reasons


@pytest.mark.asyncio
async def test_unknown_orders_block_subsequent_opens(conn: asyncpg.Connection):
    """C3 belt: UNKNOWN count gates new opens even before kill is checked."""
    wallet = await _funded(conn)
    await conn.execute(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested
        )
        VALUES (
            $1, 'null', $2, $3, 'BUY', 'MARKET', 'OPEN', 'UNKNOWN', 0.01
        )
        """,
        uuid.uuid4(),
        wallet["id"],
        SYMBOL,
    )

    with pytest.raises(ExecutionUncertain):
        await _engine().open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.01"),
            signal_price=MARK,
        )
    assert (await _counts(conn, wallet["id"]))["positions"] == 0


@pytest.mark.asyncio
async def test_concurrent_close_second_caller_sees_in_flight(
    conn: asyncpg.Connection,
):
    """H1: FOR UPDATE + inflight CLOSE claim blocks a second sender."""
    wallet = await _funded(conn)
    engine = _engine()
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    # Inject a PENDING CLOSE as if another worker already claimed.
    await conn.execute(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, position_id, reduce_only
        )
        VALUES (
            $1, 'null', $2, $3, 'SELL', 'MARKET', 'CLOSE', 'PENDING', 0.01, $4,
            true
        )
        """,
        uuid.uuid4(),
        wallet["id"],
        SYMBOL,
        opened.position_id,
    )

    with pytest.raises(CloseAlreadyInFlight):
        await engine.close_position(
            opened.position_id, conn, close_reason="ADMIN"
        )


@pytest.mark.asyncio
async def test_recover_pending_never_sent_marks_cancelled(
    conn: asyncpg.Connection,
):
    """H3: startup sweeper resolves orphan PENDING intents."""
    wallet = await _funded(conn)
    cid = uuid.uuid4()
    await conn.execute(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, created_at
        )
        VALUES (
            $1, 'null', $2, $3, 'BUY', 'MARKET', 'OPEN', 'PENDING', 0.01,
            NOW() - INTERVAL '2 minutes'
        )
        """,
        cid,
        wallet["id"],
        SYMBOL,
    )

    outcomes = await _engine().recover_pending_orders(conn, min_age_sec=60.0)
    assert any(o["outcome"] == "never_sent" for o in outcomes)
    row = await conn.fetchrow(
        "SELECT status, reject_code FROM exchange_orders WHERE client_order_id = $1",
        cid,
    )
    assert row["status"] == "CANCELLED"
    assert row["reject_code"] == "NEVER_SENT"


@pytest.mark.asyncio
async def test_reconcile_flags_stale_pending(conn: asyncpg.Connection):
    from reconcile_capital import _reconcile_exchange_orders

    wallet = await _funded(conn)
    await conn.execute(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, created_at
        )
        VALUES (
            $1, 'null', $2, $3, 'BUY', 'MARKET', 'OPEN', 'PENDING', 0.01,
            NOW() - INTERVAL '2 minutes'
        )
        """,
        uuid.uuid4(),
        wallet["id"],
        SYMBOL,
    )
    mismatches = await _reconcile_exchange_orders(conn)
    assert any(
        m.reason == "exchange_order_nonterminal_stale" for m in mismatches
    )


@pytest.mark.asyncio
async def test_close_settlement_failure_arms_kill_switch(
    conn: asyncpg.Connection,
):
    """C2: venue close fill + capital settle failure → UNKNOWN + kill."""
    from unittest.mock import AsyncMock, patch

    wallet = await _funded(conn)
    engine = _engine()
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
    )

    with patch(
        "execution_engine.settle_close_in_capital",
        new=AsyncMock(side_effect=RuntimeError("simulated settle boom")),
    ):
        with pytest.raises(ExecutionUncertain):
            await engine.close_position(
                opened.position_id, conn, close_reason="ADMIN"
            )

    assert (await get_kill_switch(conn))["active"] is True
    order = await conn.fetchrow(
        """
        SELECT status, qty_filled FROM exchange_orders
        WHERE intent = 'CLOSE' AND position_id = $1
        ORDER BY created_at DESC LIMIT 1
        """,
        opened.position_id,
    )
    assert order["status"] == "UNKNOWN"
    assert float(order["qty_filled"]) == pytest.approx(0.01, rel=1e-9)
    # DB position still open — venue was flattened in NullAdapter.
    still = await conn.fetchval(
        "SELECT closed_at FROM positions WHERE id = $1", opened.position_id
    )
    assert still is None


# ---------------------------------------------------------------------------
# Phase 8A-1 — SL/TP persistence regression tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_preserves_custom_sltp(conn: asyncpg.Connection):
    """H-SL1: recover_pending_orders uses persisted stop_loss_pct/take_profit_pct.

    Simulates: process crashes after exchange fill but before capital settlement.
    The orphaned PENDING row carries non-default SL/TP percentages.
    Recovery must reconstruct the position with the *original* levels, not the
    hardcoded 0.03/0.06 fallback.
    """
    from mock_exchange import _sl_tp_prices  # noqa: PLC0415

    wallet = await _funded(conn)
    sl_pct, tp_pct = 0.05, 0.10
    fill_price = Decimal("65000")

    adapter = _adapter(mark_prices={SYMBOL: fill_price})
    engine = ExecutionEngine(adapter)

    # Pre-populate the NullAdapter with a FILLED state so get_order_status
    # returns a fill on recovery (mirrors the exchange having accepted the order
    # before the crash).
    coid = uuid.uuid4()
    req = PlaceOrderRequest(
        client_order_id=coid,
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=Decimal("0.001"),
    )
    adapter.queue_plan(FillPlan(kind="FILL", price=fill_price))
    await adapter.place_order(req)

    # Insert the orphaned PENDING intent row as the engine would have committed
    # it before the crash — with the new SL/TP columns populated.
    order_row_id = await conn.fetchval(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, signal_price,
            leverage, stop_loss_pct, take_profit_pct, reduce_only,
            correlation_id, created_at
        )
        VALUES (
            $1, $2, $3, $4, 'BUY', 'MARKET', 'OPEN', 'PENDING',
            0.001, 65000, 1.0, $5, $6, false, $7,
            NOW() - INTERVAL '2 minutes'
        )
        RETURNING id
        """,
        coid,
        adapter.name,
        wallet["id"],
        SYMBOL,
        sl_pct,
        tp_pct,
        uuid.uuid4(),
    )

    outcomes = await engine.recover_pending_orders(conn, min_age_sec=60.0)
    assert any(o["outcome"] == "settled_open" for o in outcomes), outcomes

    pos = await conn.fetchrow(
        "SELECT stop_loss_price, take_profit_price, entry_price"
        " FROM positions WHERE exchange_order_id = $1",
        order_row_id,
    )
    assert pos is not None, "recovered position not found"

    entry = float(pos["entry_price"])
    expected_sl, expected_tp = _sl_tp_prices("BUY", entry, sl_pct, tp_pct)
    default_sl, default_tp = _sl_tp_prices("BUY", entry, 0.03, 0.06)

    assert float(pos["stop_loss_price"]) == pytest.approx(expected_sl, rel=1e-9)
    assert float(pos["take_profit_price"]) == pytest.approx(expected_tp, rel=1e-9)
    # Guard: the recovered levels must differ from the old hardcoded defaults.
    assert expected_sl != pytest.approx(default_sl, rel=1e-9)
    assert expected_tp != pytest.approx(default_tp, rel=1e-9)


@pytest.mark.asyncio
async def test_recovery_null_sltp_falls_back_to_defaults(conn: asyncpg.Connection):
    """H-SL2: legacy rows with NULL SL/TP columns recover with 0.03 / 0.06.

    Ensures backward compatibility: any PENDING row that predates this migration
    (stop_loss_pct IS NULL) still recovers cleanly using the historic defaults.
    """
    from mock_exchange import _sl_tp_prices  # noqa: PLC0415

    wallet = await _funded(conn)
    fill_price = Decimal("65000")

    adapter = _adapter(mark_prices={SYMBOL: fill_price})
    engine = ExecutionEngine(adapter)

    coid = uuid.uuid4()
    req = PlaceOrderRequest(
        client_order_id=coid,
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=Decimal("0.001"),
    )
    adapter.queue_plan(FillPlan(kind="FILL", price=fill_price))
    await adapter.place_order(req)

    # Legacy row: stop_loss_pct and take_profit_pct deliberately omitted
    # (they will be NULL in the DB, simulating a pre-migration row).
    order_row_id = await conn.fetchval(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, signal_price,
            leverage, reduce_only, correlation_id, created_at
        )
        VALUES (
            $1, $2, $3, $4, 'BUY', 'MARKET', 'OPEN', 'PENDING',
            0.001, 65000, 1.0, false, $5,
            NOW() - INTERVAL '2 minutes'
        )
        RETURNING id
        """,
        coid,
        adapter.name,
        wallet["id"],
        SYMBOL,
        uuid.uuid4(),
    )

    outcomes = await engine.recover_pending_orders(conn, min_age_sec=60.0)
    assert any(o["outcome"] == "settled_open" for o in outcomes), outcomes

    pos = await conn.fetchrow(
        "SELECT stop_loss_price, take_profit_price, entry_price"
        " FROM positions WHERE exchange_order_id = $1",
        order_row_id,
    )
    assert pos is not None, "recovered position not found"

    entry = float(pos["entry_price"])
    default_sl, default_tp = _sl_tp_prices("BUY", entry, 0.03, 0.06)

    assert float(pos["stop_loss_price"]) == pytest.approx(default_sl, rel=1e-9)
    assert float(pos["take_profit_price"]) == pytest.approx(default_tp, rel=1e-9)


@pytest.mark.asyncio
async def test_open_position_persists_sltp_on_intent_row(conn: asyncpg.Connection):
    """H-SL3: open_position writes stop_loss_pct/take_profit_pct to exchange_orders.

    Verifies the normal (non-recovery) path: the intent row committed before
    the venue call now carries the original percentages so a subsequent
    recovery has the data it needs.
    """
    wallet = await _funded(conn)
    result = await _engine().open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("0.01"),
        signal_price=MARK,
        stop_loss_pct=0.04,
        take_profit_pct=0.08,
    )
    row = await conn.fetchrow(
        "SELECT stop_loss_pct, take_profit_pct"
        " FROM exchange_orders WHERE id = $1",
        result.order_row_id,
    )
    assert row is not None
    assert float(row["stop_loss_pct"]) == pytest.approx(0.04, rel=1e-9)
    assert float(row["take_profit_pct"]) == pytest.approx(0.08, rel=1e-9)
