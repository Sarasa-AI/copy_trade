"""Phase 8 — websocket supervision and Bybit wire translation (hermetic).

The Bybit REST path is exercised through ``httpx.MockTransport``, so the real
request building, signing, status mapping and settlement run offline with no
credentials and no venue.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal

import asyncpg
import httpx
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"
os.environ["MAX_MARK_AGE_SEC"] = "30"
os.environ["EXECUTION_RESOLVE_BACKOFF_SEC"] = "0"
os.environ.setdefault("BYBIT_API_KEY", "testkey")
os.environ.setdefault("BYBIT_API_SECRET", "testsecret")

from capital import ensure_funded_wallet, get_wallet_balance  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402
from exchange_adapter.bybit_adapter import (  # noqa: E402
    BybitAdapter,
    BybitConfigError,
    ExchangeRejected,
    floor_to_step,
    map_order_status,
    parse_instrument,
    parse_order,
    parse_position,
    parse_ticker_marks,
    sign_request,
    sign_ws_auth,
)
from exchange_adapter.reconnect import (  # noqa: E402
    BackoffPolicy,
    ReconnectingStream,
    StreamStalled,
)
from execution_engine import ExecutionEngine  # noqa: E402
import price_feed  # noqa: E402
from wallet_repository import create_wallet  # noqa: E402

SYMBOL = "BTCUSDT"


# --- backoff policy -----------------------------------------------------------


def test_backoff_grows_then_caps():
    policy = BackoffPolicy(initial_sec=1.0, max_sec=8.0, multiplier=2.0, jitter=0)
    assert [policy.delay_for(n) for n in range(1, 6)] == [
        1.0,
        2.0,
        4.0,
        8.0,
        8.0,
    ]


def test_backoff_jitter_stays_within_bounds():
    policy = BackoffPolicy(initial_sec=2.0, max_sec=10.0, multiplier=2.0, jitter=0.5)
    assert policy.delay_for(1, rand=0.0) == pytest.approx(1.0)
    assert policy.delay_for(1, rand=1.0) == pytest.approx(3.0)
    assert policy.delay_for(9, rand=1.0) <= 10.0


def test_backoff_rejects_invalid_configuration():
    with pytest.raises(ValueError):
        BackoffPolicy(initial_sec=0)
    with pytest.raises(ValueError):
        BackoffPolicy(initial_sec=5, max_sec=1)
    with pytest.raises(ValueError):
        BackoffPolicy(jitter=1.0)


# --- reconnecting stream ------------------------------------------------------


class _FakeSocket:
    def __init__(self) -> None:
        self.closed = False


def _connector(socket: _FakeSocket):
    @asynccontextmanager
    async def connect():
        try:
            yield socket
        finally:
            socket.closed = True

    return connect


@pytest.mark.asyncio
async def test_stream_reconnects_after_handler_failure():
    states: list[str] = []
    calls = {"n": 0}

    async def handler(ws, stream):
        calls["n"] += 1
        raise ConnectionResetError("socket died")

    async def on_state(state, detail):
        states.append(state)

    stream = ReconnectingStream(
        name="test",
        connect=_connector(_FakeSocket()),
        handler=handler,
        policy=BackoffPolicy(initial_sec=0.01, max_sec=0.01, jitter=0),
        on_state=on_state,
        max_attempts=2,
    )
    await asyncio.wait_for(stream.run(), timeout=5)

    assert calls["n"] == 3
    assert states.count("connected") == 3
    assert "error" in states
    assert states[-1] == "give_up"


@pytest.mark.asyncio
async def test_stream_detects_silent_socket_and_recycles_it():
    errors: list[str] = []

    async def handler(ws, stream):
        # Connected but never reports a frame: the dangerous failure mode.
        await asyncio.sleep(30)

    async def on_state(state, detail):
        if state == "error":
            errors.append(detail.get("error_type", ""))

    stream = ReconnectingStream(
        name="silent",
        connect=_connector(_FakeSocket()),
        handler=handler,
        policy=BackoffPolicy(initial_sec=0.01, max_sec=0.01, jitter=0),
        stall_timeout_sec=0.1,
        on_state=on_state,
        max_attempts=1,
    )
    await asyncio.wait_for(stream.run(), timeout=5)

    assert errors and set(errors) == {StreamStalled.__name__}
    assert stream.reconnect_count >= 1


@pytest.mark.asyncio
async def test_stream_stops_on_request():
    async def handler(ws, stream):
        while True:
            stream.note_message()
            await asyncio.sleep(0.01)

    stream = ReconnectingStream(
        name="stoppable",
        connect=_connector(_FakeSocket()),
        handler=handler,
        stall_timeout_sec=1.0,
    )
    task = asyncio.create_task(stream.run())
    await asyncio.sleep(0.05)
    stream.stop()
    await asyncio.wait_for(task, timeout=5)
    assert stream.stopped
    assert stream.connected is False


# --- Bybit wire translation ---------------------------------------------------


def test_signature_matches_known_vector():
    assert sign_request(
        "testsecret",
        timestamp_ms="1700000000000",
        api_key="testkey",
        recv_window="5000",
        payload='{"category":"linear"}',
    ) == "5a914142307b92ae470a40400513711ce5f4a182b4d5063418f4af7f1413d09d"
    assert (
        sign_ws_auth("testsecret", 1700000000000)
        == "64ea204643e548a1723959592ed1afb01f1043a3b0d36cc6e7bdd8053971fbc3"
    )


def test_status_mapping_covers_venue_vocabulary():
    assert map_order_status("New") == "PENDING"
    assert map_order_status("PartiallyFilled") == "PARTIALLY_FILLED"
    assert map_order_status("Filled") == "FILLED"
    assert map_order_status("Cancelled") == "CANCELLED"
    # A cancelled remainder still counts as a terminal cancel, fills intact.
    assert map_order_status("PartiallyFilledCanceled") == "CANCELLED"
    assert map_order_status("Rejected") == "REJECTED"
    with pytest.raises(ExchangeRejected):
        map_order_status("SomethingNew")


def test_parse_order_reads_cumulative_fill_state():
    cid = uuid.uuid4()
    fill = parse_order(
        {
            "orderId": "1234",
            "orderLinkId": str(cid),
            "symbol": SYMBOL,
            "side": "Buy",
            "orderStatus": "Filled",
            "qty": "0.010",
            "cumExecQty": "0.010",
            "avgPrice": "65130.5",
            "cumExecFee": "0.39",
            "feeCurrency": "USDT",
            "rejectReason": "EC_NoError",
            "updatedTime": "1700000000000",
        },
        client_order_id=cid,
    )
    assert fill.status == "FILLED"
    assert fill.qty_filled == Decimal("0.010")
    assert fill.avg_fill_price == Decimal("65130.5")
    assert fill.fee_paid == Decimal("0.39")
    assert fill.reject_reason is None
    assert fill.is_terminal


def test_parse_order_derives_avg_price_from_exec_value():
    cid = uuid.uuid4()
    fill = parse_order(
        {
            "orderId": "1",
            "symbol": SYMBOL,
            "side": "Sell",
            "orderStatus": "PartiallyFilled",
            "qty": "0.010",
            "cumExecQty": "0.004",
            "avgPrice": "",
            "cumExecValue": "260.0",
        },
        client_order_id=cid,
    )
    assert fill.side == "SELL"
    assert fill.status == "PARTIALLY_FILLED"
    assert fill.avg_fill_price == Decimal("65000")
    assert not fill.is_terminal


def test_parse_position_ignores_flat_rows():
    assert parse_position({"symbol": SYMBOL, "size": "0"}) is None
    position = parse_position(
        {
            "symbol": SYMBOL,
            "side": "Buy",
            "size": "0.01",
            "avgPrice": "65000",
            "unrealisedPnl": "-1.5",
        }
    )
    assert position is not None
    assert position.qty == Decimal("0.01")
    assert position.unrealized_pnl == Decimal("-1.5")


def test_parse_ticker_marks_prefers_mark_price():
    marks = parse_ticker_marks(
        {
            "topic": f"tickers.{SYMBOL}",
            "ts": 1700000000000,
            "data": {
                "symbol": SYMBOL,
                "lastPrice": "65010",
                "markPrice": "65000",
            },
        }
    )
    assert [m.price for m in marks] == [Decimal("65000")]
    assert marks[0].source == "bybit_ws_tickers"

    assert parse_ticker_marks({"topic": "order", "data": []}) == []
    assert (
        parse_ticker_marks(
            {"topic": f"tickers.{SYMBOL}", "data": [{"symbol": "", "markPrice": "1"}]}
        )
        == []
    )


def test_instrument_filters_floor_quantities_downward():
    info = parse_instrument(
        {
            "symbol": SYMBOL,
            "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
            "priceFilter": {"tickSize": "0.1"},
        }
    )
    assert info.qty_step == Decimal("0.001")
    assert floor_to_step(Decimal("0.0129"), info.qty_step) == Decimal("0.012")
    assert floor_to_step(Decimal("65000.17"), info.tick_size) == Decimal("65000.1")
    assert floor_to_step(Decimal("5"), Decimal("0")) == Decimal("5")


def test_adapter_refuses_mainnet_without_explicit_approval(monkeypatch):
    monkeypatch.delenv("BYBIT_ALLOW_MAINNET", raising=False)
    with pytest.raises(BybitConfigError):
        BybitAdapter(testnet=False)


def test_adapter_requires_credentials(monkeypatch):
    monkeypatch.delenv("BYBIT_API_KEY", raising=False)
    monkeypatch.delenv("BYBIT_API_SECRET", raising=False)
    with pytest.raises(BybitConfigError):
        BybitAdapter(testnet=True)


# --- Bybit REST path against a mocked transport -------------------------------

_INSTRUMENT = {
    "retCode": 0,
    "result": {
        "list": [
            {
                "symbol": SYMBOL,
                "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                "priceFilter": {"tickSize": "0.1"},
            }
        ]
    },
}


def _order_payload(link_id: str, **overrides) -> dict:
    payload = {
        "orderId": "OID-1",
        "orderLinkId": link_id,
        "symbol": SYMBOL,
        "side": "Buy",
        "orderStatus": "Filled",
        "qty": "0.012",
        "cumExecQty": "0.012",
        "avgPrice": "65100",
        "cumExecFee": "0.47",
        "feeCurrency": "USDT",
        "updatedTime": "1700000000000",
    }
    payload.update(overrides)
    return payload


def _bybit_adapter(*, order_overrides: dict | None = None) -> BybitAdapter:
    """Adapter wired to an in-process fake venue."""
    sent: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v5/market/instrumentsInfo":
            return httpx.Response(200, json=_INSTRUMENT)
        if path == "/v5/order/create":
            body = json.loads(request.content)
            sent.append(body)
            assert request.headers["X-BAPI-SIGN"]
            return httpx.Response(
                200,
                json={
                    "retCode": 0,
                    "result": {
                        "orderId": "OID-1",
                        "orderLinkId": body["orderLinkId"],
                    },
                },
            )
        if path in ("/v5/order/realtime", "/v5/order/history"):
            link = request.url.params.get("orderLinkId")
            return httpx.Response(
                200,
                json={
                    "retCode": 0,
                    "result": {
                        "list": [
                            _order_payload(link, **(order_overrides or {}))
                        ]
                    },
                },
            )
        if path == "/v5/position/list":
            return httpx.Response(200, json={"retCode": 0, "result": {"list": []}})
        return httpx.Response(404, json={"retCode": 404, "retMsg": path})

    client = httpx.AsyncClient(
        base_url="https://api-testnet.bybit.com",
        transport=httpx.MockTransport(handler),
    )
    adapter = BybitAdapter(testnet=True, client=client)
    adapter.sent_bodies = sent  # type: ignore[attr-defined]
    return adapter


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


@pytest.mark.asyncio
async def test_bybit_place_order_sends_client_order_id_as_order_link_id():
    adapter = _bybit_adapter()
    try:
        qty, price = await adapter.quantize_order(
            SYMBOL, Decimal("0.0129"), Decimal("65000.17")
        )
        assert qty == Decimal("0.012")
        assert price == Decimal("65000.1")

        from exchange_adapter.interface import PlaceOrderRequest

        cid = uuid.uuid4()
        fill = await adapter.place_order(
            PlaceOrderRequest(
                client_order_id=cid,
                symbol=SYMBOL,
                side="BUY",
                order_type="MARKET",
                qty=qty,
            )
        )
        body = adapter.sent_bodies[0]  # type: ignore[attr-defined]
        assert body["orderLinkId"] == str(cid)
        assert body["qty"] == "0.012"
        assert body["timeInForce"] == "IOC"
        assert body["positionIdx"] == 0
        assert "price" not in body
        assert fill.status == "FILLED"
        assert fill.avg_fill_price == Decimal("65100")
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_bybit_create_error_raises_network_timeout_not_local_reject():
    """C1: create retCode!=0 + single not-found must not become REJECTED."""
    from exchange_adapter.interface import NetworkTimeout, PlaceOrderRequest

    sent: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v5/market/instrumentsInfo":
            return httpx.Response(200, json=_INSTRUMENT)
        if path == "/v5/position/list":
            return httpx.Response(200, json={"retCode": 0, "result": {"list": []}})
        if path == "/v5/order/create":
            body = json.loads(request.content)
            sent.append(body)
            return httpx.Response(
                200,
                json={"retCode": 10001, "retMsg": "Request parameter error"},
            )
        if path in ("/v5/order/realtime", "/v5/order/history"):
            return httpx.Response(
                200, json={"retCode": 0, "result": {"list": []}}
            )
        return httpx.Response(404, json={"retCode": 404, "retMsg": path})

    client = httpx.AsyncClient(
        base_url="https://api-testnet.bybit.com",
        transport=httpx.MockTransport(handler),
    )
    adapter = BybitAdapter(testnet=True, client=client)
    try:
        with pytest.raises(NetworkTimeout):
            await adapter.place_order(
                PlaceOrderRequest(
                    client_order_id=uuid.uuid4(),
                    symbol=SYMBOL,
                    side="BUY",
                    order_type="MARKET",
                    qty=Decimal("0.01"),
                )
            )
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_bybit_get_order_status_retcode_error_is_timeout_not_not_found():
    """C1: API retCode errors must not masquerade as OrderNotFound."""
    from exchange_adapter.interface import NetworkTimeout

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v5/order/realtime":
            return httpx.Response(
                200,
                json={"retCode": 10003, "retMsg": "Invalid API key", "result": {}},
            )
        return httpx.Response(404, json={"retCode": 404, "retMsg": path})

    client = httpx.AsyncClient(
        base_url="https://api-testnet.bybit.com",
        transport=httpx.MockTransport(handler),
    )
    adapter = BybitAdapter(testnet=True, client=client)
    try:
        with pytest.raises(NetworkTimeout):
            await adapter.get_order_status(uuid.uuid4(), SYMBOL)
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_bybit_refuses_hedge_mode_account():
    """H4: hedge-mode positionIdx 1/2 is refused before orders are sent."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v5/position/list":
            return httpx.Response(
                200,
                json={
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "symbol": SYMBOL,
                                "side": "Buy",
                                "size": "0.01",
                                "positionIdx": 1,
                                "avgPrice": "65000",
                            }
                        ]
                    },
                },
            )
        if request.url.path == "/v5/market/instrumentsInfo":
            return httpx.Response(200, json=_INSTRUMENT)
        return httpx.Response(404, json={"retCode": 404, "retMsg": "x"})

    client = httpx.AsyncClient(
        base_url="https://api-testnet.bybit.com",
        transport=httpx.MockTransport(handler),
    )
    adapter = BybitAdapter(testnet=True, client=client)
    try:
        from exchange_adapter.interface import PlaceOrderRequest

        with pytest.raises(BybitConfigError, match="hedge mode"):
            await adapter.place_order(
                PlaceOrderRequest(
                    client_order_id=uuid.uuid4(),
                    symbol=SYMBOL,
                    side="BUY",
                    order_type="MARKET",
                    qty=Decimal("0.01"),
                )
            )
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_bybit_rejects_below_minimum_quantity_without_sending():
    adapter = _bybit_adapter()
    try:
        from exchange_adapter.interface import PlaceOrderRequest

        fill = await adapter.place_order(
            PlaceOrderRequest(
                client_order_id=uuid.uuid4(),
                symbol=SYMBOL,
                side="BUY",
                order_type="MARKET",
                qty=Decimal("0.0001"),
            )
        )
        assert fill.status == "REJECTED"
        assert fill.reject_code == "BELOW_MIN_QTY"
        assert adapter.sent_bodies == []  # type: ignore[attr-defined]
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_execution_engine_settles_bybit_fill_at_venue_price(
    conn: asyncpg.Connection,
):
    wallet = await create_wallet(conn, f"p8by_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=5000.0)
    price_feed.update_mark(SYMBOL, 65000.0, source="p8_bybit_test")

    adapter = _bybit_adapter()
    try:
        engine = ExecutionEngine(adapter)
        result = await engine.open_position(
            conn,
            wallet_id=wallet["id"],
            symbol=SYMBOL,
            side="BUY",
            qty=Decimal("0.0129"),
            signal_price=Decimal("65000"),
        )
    finally:
        await adapter.close()

    assert result.status == "FILLED"
    assert result.avg_fill_price == Decimal("65100")
    # Quantized down to the venue step before the intent row was written.
    assert result.qty_requested == Decimal("0.012")
    assert result.qty_filled == Decimal("0.012")

    order = await conn.fetchrow(
        "SELECT * FROM exchange_orders WHERE id = $1", result.order_row_id
    )
    assert order["adapter_name"] == "bybit_testnet"
    assert float(order["qty_requested"]) == pytest.approx(0.012, rel=1e-9)
    assert float(order["fee_paid"]) == pytest.approx(0.47, rel=1e-9)

    expected_margin = 0.012 * 65100.0
    balance = await get_wallet_balance(conn, wallet["id"])
    assert float(balance["reserved_margin"]) == pytest.approx(
        expected_margin, rel=1e-9
    )
