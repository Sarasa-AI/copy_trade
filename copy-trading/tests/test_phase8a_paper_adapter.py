"""Phase 8A — Paper Exchange Adapter E2E (hermetic).

Exercises ExecutionEngine + Risk + Capital + PaperExchangeAdapter + Database.
No internet, no credentials, no real exchange. Marks come from price_feed
fixtures only.
"""

from __future__ import annotations

import ast
import os
import uuid
from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"
os.environ["MAX_MARK_AGE_SEC"] = "30"
os.environ["EXECUTION_RESOLVE_BACKOFF_SEC"] = "0"
os.environ["EXECUTION_NOT_FOUND_CONFIRMATIONS"] = "2"

from capital import ensure_funded_wallet, get_wallet_balance  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402
from exchange_adapter.factory import (  # noqa: E402
    create_exchange_adapter,
    resolve_adapter_name,
)
from exchange_adapter.interface import (  # noqa: E402
    NetworkTimeout,
    OrderNotFound,
    PlaceOrderRequest,
)
from exchange_adapter.paper_adapter import (  # noqa: E402
    PaperConfigError,
    PaperExchangeAdapter,
    PaperFailureMode,
)
from execution_engine import (  # noqa: E402
    CloseAlreadyInFlight,
    ExecutionEngine,
    ExecutionUncertain,
    OrderNotFilled,
    PartialCloseUnsupported,
)
from kill_switch import activate_kill_switch, get_kill_switch  # noqa: E402
import price_feed  # noqa: E402
from risk_engine import RiskDenied  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402

SYMBOL = "BTCUSDT"
MARK = Decimal("65000")
QTY = Decimal("0.01")

_ROOT = Path(__file__).resolve().parents[1]
_PAPER_SRC = (
    _ROOT / "scripts" / "exchange_adapter" / "paper_adapter.py"
).read_text(encoding="utf-8")
_FACTORY_SRC = (
    _ROOT / "scripts" / "exchange_adapter" / "factory.py"
).read_text(encoding="utf-8")


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
        price_feed.update_mark(SYMBOL, float(MARK), source="p8a_fixture")
        yield connection
    finally:
        await connection.close()


def _adapter(**kwargs) -> PaperExchangeAdapter:
    kwargs.setdefault("failure_mode", PaperFailureMode.SUCCESS)
    kwargs.setdefault("fees_enabled", False)
    kwargs.setdefault("slippage_bps", Decimal("0"))
    return PaperExchangeAdapter(**kwargs)


def _engine(adapter: PaperExchangeAdapter | None = None) -> ExecutionEngine:
    return ExecutionEngine(adapter or _adapter())


async def _funded(conn: asyncpg.Connection, amount: float = 5000.0):
    wallet = await create_wallet(conn, f"p8a_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=amount)
    return wallet


async def _counts(conn: asyncpg.Connection, wallet_id) -> dict[str, int]:
    return {
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
        "releases": int(
            await conn.fetchval(
                """
                SELECT COUNT(*) FROM capital_ledger
                WHERE wallet_id = $1 AND entry_type = 'RELEASE'
                """,
                wallet_id,
            )
        ),
        "realizes": int(
            await conn.fetchval(
                """
                SELECT COUNT(*) FROM capital_ledger
                WHERE wallet_id = $1 AND entry_type = 'REALIZE_PNL'
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
        "exchange_fills": int(
            await conn.fetchval(
                """
                SELECT COUNT(*) FROM exchange_fills ef
                JOIN exchange_orders eo ON eo.id = ef.exchange_order_row_id
                WHERE eo.wallet_id = $1
                """,
                wallet_id,
            )
        ),
    }


# --- safety -------------------------------------------------------------------


def test_paper_source_never_imports_bybit():
    tree = ast.parse(_PAPER_SRC)
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    assert not any("bybit" in name.lower() for name in imports)
    assert "httpx" not in imports
    # Must never *read* Bybit credentials (docstring may mention them).
    assert "os.getenv(\"BYBIT_API_KEY\"" not in _PAPER_SRC
    assert "os.getenv(\"BYBIT_API_SECRET\"" not in _PAPER_SRC
    assert "os.environ.get(\"BYBIT_API_KEY\"" not in _PAPER_SRC
    assert "os.environ[\"BYBIT_API_KEY\"]" not in _PAPER_SRC
    assert "os.environ[\"BYBIT_API_SECRET\"]" not in _PAPER_SRC


def test_factory_paper_path_does_not_import_bybit():
    """Paper branch must not reference BybitAdapter at module load time."""
    # factory.py may mention bybit only inside the bybit branch (lazy import).
    tree = ast.parse(_FACTORY_SRC)
    top_imports: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            top_imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top_imports.append(node.module)
    assert not any("bybit" in n.lower() for n in top_imports)


def test_factory_paper_ignores_bybit_credentials(monkeypatch):
    monkeypatch.setenv("EXCHANGE_ADAPTER", "paper")
    monkeypatch.setenv("BYBIT_API_KEY", "should-never-be-read")
    monkeypatch.setenv("BYBIT_API_SECRET", "should-never-be-read")
    adapter = create_exchange_adapter()
    assert isinstance(adapter, PaperExchangeAdapter)
    assert adapter.name == "paper"
    assert adapter.is_live is False
    assert not hasattr(adapter, "api_key")


def test_factory_unknown_adapter_fails_closed(monkeypatch):
    monkeypatch.setenv("EXCHANGE_ADAPTER", "mainnet_oops")
    with pytest.raises(PaperConfigError):
        create_exchange_adapter()


def test_paper_mode_conflicts_with_bybit_selection(monkeypatch):
    monkeypatch.setenv("PAPER_MODE", "true")
    monkeypatch.setenv("EXCHANGE_ADAPTER", "bybit")
    with pytest.raises(PaperConfigError):
        resolve_adapter_name()


def test_paper_adapter_identity_and_ids():
    adapter = _adapter()
    assert adapter.name == "paper"
    assert adapter.is_live is False


@pytest.mark.asyncio
async def test_paper_order_ids_are_not_bybit_shaped():
    adapter = _adapter()
    price_feed.update_mark(SYMBOL, float(MARK), source="p8a")
    req = PlaceOrderRequest(
        client_order_id=uuid.uuid4(),
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=QTY,
    )
    fill = await adapter.place_order(req)
    assert fill.exchange_order_id is not None
    assert fill.exchange_order_id.startswith("paper-order-")
    assert fill.fee_paid == Decimal("0")
    assert fill.avg_fill_price == MARK
    assert fill.raw["fills"][0]["venue_fill_id"].startswith("paper-fill-")


# --- adapter contract ---------------------------------------------------------


@pytest.mark.asyncio
async def test_successful_market_open(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    result = await _engine().open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    assert result.status == "FILLED"
    assert result.adapter_name == "paper"
    assert result.qty_filled == QTY
    assert result.avg_fill_price == MARK
    assert result.fee_paid == Decimal("0")
    assert result.exchange_order_id.startswith("paper-order-")
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 1
    assert counts["reserves"] == 1
    assert counts["exchange_fills"] == 1
    order = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1", result.order_row_id
    )
    assert float(order["fee_paid"]) == 0.0
    assert order["adapter_name"] == "paper"


@pytest.mark.asyncio
async def test_successful_market_close(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    engine = _engine()
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    closed = await engine.close_position(
        opened.position_id, conn, close_reason="ADMIN"
    )
    assert closed.status == "FILLED"
    assert closed.fee_paid == Decimal("0")
    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", opened.position_id
    )
    assert position["closed_at"] is not None
    counts = await _counts(conn, wallet["id"])
    assert counts["releases"] == 1
    assert counts["realizes"] == 1


@pytest.mark.asyncio
async def test_ensure_one_way_mode():
    adapter = _adapter()
    await adapter.ensure_one_way_mode()
    assert adapter._one_way_verified is True


@pytest.mark.asyncio
async def test_order_status_and_cancel():
    adapter = _adapter(failure_mode=PaperFailureMode.PARTIAL_FILL)
    price_feed.update_mark(SYMBOL, float(MARK), source="p8a")
    req = PlaceOrderRequest(
        client_order_id=uuid.uuid4(),
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=QTY,
    )
    fill = await adapter.place_order(req)
    assert fill.status == "PARTIALLY_FILLED"
    status = await adapter.get_order_status(req.client_order_id, SYMBOL)
    assert status.qty_filled == fill.qty_filled
    cancelled = await adapter.cancel_order(req.client_order_id, SYMBOL)
    assert cancelled.status == "CANCELLED"
    assert cancelled.qty_filled == fill.qty_filled


# --- price --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deterministic_zero_slippage_fill_price():
    adapter = _adapter()
    price_feed.update_mark(SYMBOL, 100000.0, source="p8a_det")
    req = PlaceOrderRequest(
        client_order_id=uuid.uuid4(),
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=QTY,
        price=Decimal("99999"),  # signal-like; must NOT be fill price
    )
    fill = await adapter.place_order(req)
    assert fill.avg_fill_price == Decimal("100000.0")
    assert fill.fee_paid == Decimal("0")


@pytest.mark.asyncio
async def test_injected_mark_resolver():
    adapter = _adapter(mark_resolver=lambda _s: Decimal("42424"))
    fill = await adapter.place_order(
        PlaceOrderRequest(
            client_order_id=uuid.uuid4(),
            symbol=SYMBOL,
            side="SELL",
            order_type="MARKET",
            qty=QTY,
        )
    )
    assert fill.avg_fill_price == Decimal("42424")


@pytest.mark.asyncio
async def test_stale_mark_rejects_open(conn: asyncpg.Connection):
    from datetime import datetime, timedelta, timezone

    wallet = await _funded(conn)
    old = datetime.now(timezone.utc) - timedelta(seconds=120)
    price_feed.update_mark(SYMBOL, float(MARK), source="stale", ts_utc=old)
    with pytest.raises(RiskDenied) as ei:
        await _engine().open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=QTY,
            signal_price=MARK,
        )
    assert ei.value.reason_code == "MARK_STALE"


@pytest.mark.asyncio
async def test_missing_mark_rejects_open(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    price_feed.clear_mark(SYMBOL)
    with pytest.raises(RiskDenied) as ei:
        await _engine().open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=QTY,
            signal_price=MARK,
        )
    assert ei.value.reason_code == "MARK_MISSING"


# --- failure injection --------------------------------------------------------


@pytest.mark.asyncio
async def test_create_timeout_never_sent(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter(failure_mode=PaperFailureMode.CREATE_TIMEOUT)
    with pytest.raises(OrderNotFilled) as ei:
        await _engine(adapter).open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=QTY,
            signal_price=MARK,
        )
    order = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1",
        ei.value.result.order_row_id,
    )
    assert order["status"] == "CANCELLED"
    assert order["reject_code"] == "NEVER_SENT"
    assert (await get_kill_switch(conn))["active"] is False
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 0
    assert counts["reserves"] == 0


@pytest.mark.asyncio
async def test_create_reject(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter(failure_mode=PaperFailureMode.CREATE_REJECT)
    before = await get_wallet_balance(conn, wallet["id"])
    with pytest.raises(OrderNotFilled) as ei:
        await _engine(adapter).open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=QTY,
            signal_price=MARK,
        )
    assert ei.value.result.status == "REJECTED"
    order = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1",
        ei.value.result.order_row_id,
    )
    assert order["status"] == "REJECTED"
    assert order["reject_code"] == "PAPER_REJECT"
    assert float(order["fee_paid"]) == 0.0
    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == float(before["available_balance"])
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 0
    assert counts["reserves"] == 0


@pytest.mark.asyncio
async def test_response_lost_after_accept_recovers(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter(
        failure_mode=PaperFailureMode.RESPONSE_LOST_AFTER_ACCEPT
    )
    result = await _engine(adapter).open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    assert result.status == "FILLED"
    assert adapter.sent_count == 1
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 1
    assert counts["reserves"] == 1


@pytest.mark.asyncio
async def test_status_timeout_then_recover(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter(
        failure_mode=PaperFailureMode.STATUS_TIMEOUT,
        status_timeout_count=1,
    )
    result = await _engine(adapter).open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    assert result.status == "FILLED"
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 1


@pytest.mark.asyncio
async def test_not_found_once_recovers(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter(failure_mode=PaperFailureMode.NOT_FOUND_ONCE)
    result = await _engine(adapter).open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    assert result.status == "FILLED"
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 1
    assert counts["reserves"] == 1


@pytest.mark.asyncio
async def test_not_found_twice_classified_never_sent(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter(failure_mode=PaperFailureMode.NOT_FOUND_TWICE)
    with pytest.raises(OrderNotFilled) as ei:
        await _engine(adapter).open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=QTY,
            signal_price=MARK,
        )
    order = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1",
        ei.value.result.order_row_id,
    )
    assert order["status"] == "CANCELLED"
    assert order["reject_code"] == "NEVER_SENT"
    assert (await get_kill_switch(conn))["active"] is False


@pytest.mark.asyncio
async def test_partial_fill_cancels_remainder_and_settles_filled(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter(
        failure_mode=PaperFailureMode.PARTIAL_FILL,
        partial_fill_ratio=Decimal("0.5"),
    )
    result = await _engine(adapter).open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    assert result.status == "CANCELLED"
    assert result.qty_filled == Decimal("0.005")
    counts = await _counts(conn, wallet["id"])
    assert counts["positions"] == 1
    assert counts["reserves"] == 1
    balance = await get_wallet_balance(conn, wallet["id"])
    assert float(balance["reserved_margin"]) == pytest.approx(
        0.005 * 65000.0, rel=1e-9
    )


@pytest.mark.asyncio
async def test_unknown_arms_kill_switch(conn: asyncpg.Connection):
    """CLOSE_UNKNOWN-style: fill accepted, status never discloses."""

    class _HiddenAfterFill(PaperExchangeAdapter):
        async def place_order(self, req):
            fill = await super().place_order(req)
            # Force response-lost + permanent status hide.
            state = self._orders[req.client_order_id]
            state.status_hidden = True
            raise NetworkTimeout(
                "forced unknown", client_order_id=req.client_order_id
            )

        async def get_order_status(self, client_order_id, symbol):
            raise NetworkTimeout(
                "status unavailable", client_order_id=client_order_id
            )

    wallet = await _funded(conn)
    adapter = _HiddenAfterFill()
    before = await get_wallet_balance(conn, wallet["id"])
    with pytest.raises(ExecutionUncertain) as ei:
        await _engine(adapter).open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=QTY,
            signal_price=MARK,
        )
    order = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1", ei.value.order_row_id
    )
    assert order["status"] == "UNKNOWN"
    assert (await get_kill_switch(conn))["active"] is True
    after = await get_wallet_balance(conn, wallet["id"])
    assert float(after["available_balance"]) == float(before["available_balance"])
    assert float(after["reserved_margin"]) == 0.0


@pytest.mark.asyncio
async def test_duplicate_fill_observation_does_not_double_count(
    conn: asyncpg.Connection,
):
    wallet = await _funded(conn)
    adapter = _adapter(failure_mode=PaperFailureMode.DUPLICATE_EVENT)
    engine = _engine(adapter)
    result = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    # Re-observe the same cumulative fill via _record_state.
    fill = await adapter.get_order_status(
        result.client_order_id, SYMBOL
    )
    async with conn.transaction():
        await engine._record_state(conn, result.order_row_id, fill)
    counts = await _counts(conn, wallet["id"])
    assert counts["exchange_fills"] == 1
    assert counts["reserves"] == 1


@pytest.mark.asyncio
async def test_close_timeout_never_sent_on_close(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    adapter.set_failure_mode(PaperFailureMode.CLOSE_TIMEOUT)
    with pytest.raises(OrderNotFilled):
        await engine.close_position(
            opened.position_id, conn, close_reason="ADMIN"
        )
    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", opened.position_id
    )
    assert position["closed_at"] is None


@pytest.mark.asyncio
async def test_close_unknown_arms_kill(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    adapter.set_failure_mode(PaperFailureMode.CLOSE_UNKNOWN)
    with pytest.raises(ExecutionUncertain):
        await engine.close_position(
            opened.position_id, conn, close_reason="ADMIN"
        )
    assert (await get_kill_switch(conn))["active"] is True
    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", opened.position_id
    )
    assert position["closed_at"] is None


@pytest.mark.asyncio
async def test_ws_disconnect_status_still_works():
    adapter = _adapter(failure_mode=PaperFailureMode.WS_DISCONNECT)
    price_feed.update_mark(SYMBOL, float(MARK), source="p8a")
    seen: list = []

    async def on_fill(fill):
        seen.append(fill)

    await adapter.subscribe_fills(on_fill)
    req = PlaceOrderRequest(
        client_order_id=uuid.uuid4(),
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=QTY,
    )
    fill = await adapter.place_order(req)
    assert fill.status == "FILLED"
    assert seen == []  # push suppressed while disconnected
    status = await adapter.get_order_status(req.client_order_id, SYMBOL)
    assert status.status == "FILLED"
    adapter.reconnect_ws()
    await adapter.push_fill(status)
    assert len(seen) == 1


# --- capital ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_release_realize_exactly_once(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    engine = _engine()
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    await engine.close_position(
        opened.position_id, conn, close_reason="ADMIN"
    )
    counts = await _counts(conn, wallet["id"])
    assert counts["reserves"] == 1
    assert counts["releases"] == 1
    assert counts["realizes"] == 1


@pytest.mark.asyncio
async def test_partial_close_refused(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    adapter.set_failure_mode(PaperFailureMode.PARTIAL_FILL)
    with pytest.raises(PartialCloseUnsupported):
        await engine.close_position(
            opened.position_id, conn, close_reason="ADMIN"
        )
    assert (await get_kill_switch(conn))["active"] is True


# --- recovery -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_recover_pending_intent_never_sent(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    client_order_id = uuid.uuid4()
    order_row_id = await conn.fetchval(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, signal_price,
            leverage, stop_loss_pct, take_profit_pct, reduce_only,
            correlation_id, created_at
        )
        VALUES (
            $1, 'paper', $2, $3, 'BUY', 'MARKET', 'OPEN', 'PENDING',
            $4, $5, 1.0, 0.03, 0.06, false, $6,
            NOW() - INTERVAL '2 minutes'
        )
        RETURNING id
        """,
        client_order_id,
        wallet["id"],
        SYMBOL,
        QTY,
        MARK,
        uuid.uuid4(),
    )
    outcomes = await engine.recover_pending_orders(conn, min_age_sec=60.0)
    assert outcomes
    row = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1", order_row_id
    )
    assert row["status"] == "CANCELLED"
    assert row["reject_code"] == "NEVER_SENT"


@pytest.mark.asyncio
async def test_recover_accepted_response_lost(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    adapter = _adapter()
    engine = _engine(adapter)
    client_order_id = uuid.uuid4()
    sl_pct, tp_pct = 0.04, 0.08
    order_row_id = await conn.fetchval(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, signal_price,
            leverage, stop_loss_pct, take_profit_pct, reduce_only,
            correlation_id, created_at
        )
        VALUES (
            $1, 'paper', $2, $3, 'BUY', 'MARKET', 'OPEN', 'PENDING',
            $4, $5, 1.0, $6, $7, false, $8,
            NOW() - INTERVAL '2 minutes'
        )
        RETURNING id
        """,
        client_order_id,
        wallet["id"],
        SYMBOL,
        QTY,
        MARK,
        sl_pct,
        tp_pct,
        uuid.uuid4(),
    )
    # Venue accepted + filled, but process crashed before settlement.
    req = PlaceOrderRequest(
        client_order_id=client_order_id,
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=QTY,
    )
    fill = await adapter.place_order(req)
    assert fill.status == "FILLED"
    await conn.execute(
        """
        UPDATE exchange_orders
        SET exchange_order_id = $2
        WHERE id = $1
        """,
        order_row_id,
        fill.exchange_order_id,
    )
    outcomes = await engine.recover_pending_orders(conn, min_age_sec=60.0)
    assert any(o.get("outcome") == "settled_open" for o in outcomes), outcomes
    row = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1", order_row_id
    )
    assert row["status"] == "FILLED"
    assert row["position_id"] is not None
    position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", row["position_id"]
    )
    entry = float(position["entry_price"])
    assert float(position["stop_loss_price"]) == pytest.approx(
        entry * (1 - sl_pct), rel=1e-9
    )
    assert float(position["take_profit_price"]) == pytest.approx(
        entry * (1 + tp_pct), rel=1e-9
    )


@pytest.mark.asyncio
async def test_kill_blocks_new_open_after_unknown(conn: asyncpg.Connection):
    from kill_switch import KillSwitchActive

    wallet = await _funded(conn)
    await activate_kill_switch(conn, reason="p8a", actor="pytest")
    with pytest.raises(KillSwitchActive):
        await _engine().open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=QTY,
            signal_price=MARK,
        )


# --- concurrency --------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_close_refused(conn: asyncpg.Connection):
    wallet = await _funded(conn)
    engine = _engine()
    opened = await engine.open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    await conn.execute(
        """
        INSERT INTO exchange_orders (
            client_order_id, adapter_name, wallet_id, symbol, side,
            order_type, intent, status, qty_requested, position_id, reduce_only
        )
        VALUES (
            $1, 'paper', $2, $3, 'SELL', 'MARKET', 'CLOSE', 'PENDING', $4, $5,
            true
        )
        """,
        uuid.uuid4(),
        wallet["id"],
        SYMBOL,
        QTY,
        opened.position_id,
    )
    with pytest.raises(CloseAlreadyInFlight):
        await engine.close_position(
            opened.position_id, conn, close_reason="ADMIN"
        )


# --- equivalence --------------------------------------------------------------


@pytest.mark.asyncio
async def test_zero_slippage_paper_adapter_open_matches_mock_exchange(
    conn: asyncpg.Connection,
):
    from mock_exchange import place_order

    paper_wallet = await _funded(conn)
    venue_wallet = await _funded(conn)
    price_feed.update_mark(SYMBOL, 65000.0, source="p8a_parity")

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
        qty=QTY,
        signal_price=MARK,
    )
    venue_position = await conn.fetchrow(
        "SELECT * FROM positions WHERE id = $1", venue.position_id
    )

    assert float(venue_position["entry_price"]) == pytest.approx(
        float(paper_position["entry_price"]), rel=1e-9
    )
    assert float(venue_position["qty"]) == pytest.approx(
        float(paper_position["qty"]), rel=1e-9
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
    assert venue.fee_paid == Decimal("0")


@pytest.mark.asyncio
async def test_reconcile_accepts_paper_adapter_position(
    conn: asyncpg.Connection,
):
    from reconcile_capital import reconcile

    wallet = await _funded(conn)
    await _engine().open_position(
        conn,
        wallet_id=wallet["id"],
        symbol=SYMBOL,
        side="BUY",
        qty=QTY,
        signal_price=MARK,
    )
    result = await reconcile(conn)
    assert result.ok, result.to_dict()


@pytest.mark.asyncio
async def test_idempotent_place_order_replay():
    adapter = _adapter()
    price_feed.update_mark(SYMBOL, float(MARK), source="p8a")
    cid = uuid.uuid4()
    req = PlaceOrderRequest(
        client_order_id=cid,
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        qty=QTY,
    )
    first = await adapter.place_order(req)
    second = await adapter.place_order(req)
    assert first.exchange_order_id == second.exchange_order_id
    assert adapter.sent_count == 1
    assert adapter.duplicate_send_count == 1


@pytest.mark.asyncio
async def test_get_order_status_missing_raises():
    adapter = _adapter()
    with pytest.raises(OrderNotFound):
        await adapter.get_order_status(uuid.uuid4(), SYMBOL)
