"""Bybit V5 adapter (testnet by default).

Chosen over Binance for Phase 8 because ``orderLinkId`` is a first-class
idempotency key on every order endpoint, and the private websocket needs no
listen-key renewal.

Safety
------
- Mainnet is refused unless ``BYBIT_ALLOW_MAINNET=1`` is set explicitly. Real
  capital is not an authorized phase.
- Credentials come from the environment only; nothing is logged. ``raw``
  payloads stored for audit are venue responses, which contain no secrets.
- Every REST failure that leaves order state ambiguous is raised as
  ``NetworkTimeout`` so the caller recovers through ``orderLinkId`` instead of
  assuming the order never landed.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any, Iterable
from uuid import UUID

import httpx
import websockets

from exchange_adapter.interface import (
    ExchangeAdapter,
    ExchangePosition,
    ExchangeRejected,
    FillCallback,
    MarkCallback,
    MarkUpdate,
    NetworkTimeout,
    OrderFill,
    OrderNotFound,
    OrderStatus,
    PlaceOrderRequest,
)
from exchange_adapter.reconnect import BackoffPolicy, ReconnectingStream

_STALL_DEFAULT = object()

TESTNET_REST = "https://api-testnet.bybit.com"
MAINNET_REST = "https://api.bybit.com"
TESTNET_WS_PUBLIC = "wss://stream-testnet.bybit.com/v5/public/linear"
MAINNET_WS_PUBLIC = "wss://stream.bybit.com/v5/public/linear"
TESTNET_WS_PRIVATE = "wss://stream-testnet.bybit.com/v5/private"
MAINNET_WS_PRIVATE = "wss://stream.bybit.com/v5/private"

DEFAULT_CATEGORY = "linear"
RECV_WINDOW_MS = "5000"

# Venue order state → boundary status.
_STATUS_MAP: dict[str, OrderStatus] = {
    "Created": "PENDING",
    "New": "PENDING",
    "Untriggered": "PENDING",
    "Triggered": "PENDING",
    "PartiallyFilled": "PARTIALLY_FILLED",
    "Filled": "FILLED",
    "Cancelled": "CANCELLED",
    "Deactivated": "CANCELLED",
    "PartiallyFilledCanceled": "CANCELLED",
    "Rejected": "REJECTED",
}

# retCode values that mean "this order does not exist".
_NOT_FOUND_CODES = frozenset({"110001", "170213", "20001"})
# retCode values that mean "the venue is unwell"; state is ambiguous.
_TRANSIENT_CODES = frozenset({"10002", "10016", "10429", "130150"})


class BybitConfigError(RuntimeError):
    """Missing or unsafe configuration."""


def map_order_status(venue_status: str) -> OrderStatus:
    """Translate a Bybit ``orderStatus`` into the boundary vocabulary."""
    mapped = _STATUS_MAP.get(str(venue_status).strip())
    if mapped is None:
        raise ExchangeRejected(
            "UNKNOWN_ORDER_STATUS", f"unmapped orderStatus: {venue_status!r}"
        )
    return mapped


def sign_request(
    secret: str,
    *,
    timestamp_ms: str,
    api_key: str,
    recv_window: str,
    payload: str,
) -> str:
    """Bybit V5 signature over ``timestamp + api_key + recv_window + payload``."""
    message = f"{timestamp_ms}{api_key}{recv_window}{payload}"
    return hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def sign_ws_auth(secret: str, expires_ms: int) -> str:
    """Signature for the private websocket ``auth`` op."""
    return hmac.new(
        secret.encode("utf-8"),
        f"GET/realtime{expires_ms}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _dec_or_none(value: Any) -> Decimal | None:
    """Parse a venue decimal string; ``None`` for empty or unparseable input."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:  # noqa: BLE001 — venue may send unexpected junk
        return None


def _dec(value: Any, default: str = "0") -> Decimal:
    parsed = _dec_or_none(value)
    return Decimal(default) if parsed is None else parsed


def _side_to_venue(side: str) -> str:
    return "Buy" if side == "BUY" else "Sell"


def _side_from_venue(side: str) -> str:
    return "BUY" if str(side).lower().startswith("b") else "SELL"


def _order_type_to_venue(order_type: str) -> str:
    return "Market" if order_type == "MARKET" else "Limit"


def _ts_from_ms(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return datetime.now(timezone.utc)


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Round ``value`` down to a multiple of ``step`` (``step <= 0`` is a no-op).

    Always downward: rounding a quantity up could exceed the margin the risk
    gate approved.
    """
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


@dataclass(frozen=True)
class InstrumentFilter:
    """Venue trading rules for one symbol."""

    symbol: str
    qty_step: Decimal
    min_qty: Decimal
    tick_size: Decimal


def parse_instrument(payload: dict[str, Any]) -> InstrumentFilter:
    lot = payload.get("lotSizeFilter") or {}
    price = payload.get("priceFilter") or {}
    return InstrumentFilter(
        symbol=str(payload.get("symbol", "")),
        qty_step=_dec(lot.get("qtyStep"), "0"),
        min_qty=_dec(lot.get("minOrderQty"), "0"),
        tick_size=_dec(price.get("tickSize"), "0"),
    )


def parse_order(
    payload: dict[str, Any], *, client_order_id: UUID
) -> OrderFill:
    """Build an :class:`OrderFill` from a Bybit order object.

    Works for both REST ``/v5/order/*`` results and the private ``order`` topic,
    which share field names.
    """
    qty_filled = _dec(payload.get("cumExecQty"))
    avg_price = _dec_or_none(payload.get("avgPrice"))
    if qty_filled > 0 and (avg_price is None or avg_price <= 0):
        # Bybit may omit avgPrice on the first partial event; derive it.
        exec_value = _dec_or_none(payload.get("cumExecValue"))
        avg_price = (
            (exec_value / qty_filled)
            if exec_value and exec_value > 0
            else None
        )
    status = map_order_status(payload.get("orderStatus", ""))
    reject_reason = str(payload.get("rejectReason") or "").strip()
    if reject_reason == "EC_NoError":
        reject_reason = ""
    return OrderFill(
        client_order_id=client_order_id,
        exchange_order_id=str(payload.get("orderId") or "") or None,
        symbol=str(payload.get("symbol", "")),
        side=_side_from_venue(payload.get("side", "Buy")),  # type: ignore[arg-type]
        status=status,
        qty_requested=_dec(payload.get("qty")),
        qty_filled=qty_filled,
        avg_fill_price=avg_price,
        fee_paid=_dec(payload.get("cumExecFee")),
        fee_asset=str(payload.get("feeCurrency") or "USDT"),
        reject_code=reject_reason if status == "REJECTED" else None,
        reject_reason=reject_reason or None,
        timestamp_utc=_ts_from_ms(
            payload.get("updatedTime") or payload.get("createdTime")
        ),
        raw=dict(payload),
    )


def parse_position(payload: dict[str, Any]) -> ExchangePosition | None:
    qty = _dec(payload.get("size"))
    if qty <= 0:
        return None
    return ExchangePosition(
        symbol=str(payload.get("symbol", "")),
        side=_side_from_venue(payload.get("side", "Buy")),  # type: ignore[arg-type]
        qty=qty,
        entry_price=_dec(payload.get("avgPrice") or payload.get("entryPrice")),
        unrealized_pnl=_dec(payload.get("unrealisedPnl")),
        raw=dict(payload),
    )


def parse_ticker_marks(message: dict[str, Any]) -> list[MarkUpdate]:
    """Extract marks from a ``tickers.*`` websocket frame.

    Prefers ``markPrice`` (the venue's liquidation reference) over ``lastPrice``.
    """
    topic = str(message.get("topic", ""))
    if not topic.startswith("tickers"):
        return []
    data = message.get("data")
    rows: Iterable[dict[str, Any]]
    if isinstance(data, dict):
        rows = [data]
    elif isinstance(data, list):
        rows = data
    else:
        return []
    ts = _ts_from_ms(message.get("ts"))
    out: list[MarkUpdate] = []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        price = _dec_or_none(row.get("markPrice")) or _dec_or_none(
            row.get("lastPrice")
        )
        if not symbol or price is None or price <= 0:
            continue
        out.append(
            MarkUpdate(
                symbol=symbol,
                price=price,
                timestamp_utc=ts,
                source="bybit_ws_tickers",
            )
        )
    return out


class BybitAdapter(ExchangeAdapter):
    """Bybit V5 linear-perpetual adapter."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        testnet: bool | None = None,
        category: str = DEFAULT_CATEGORY,
        timeout_sec: float = 10.0,
        client: httpx.AsyncClient | None = None,
        stall_timeout_sec: float = 30.0,
    ) -> None:
        self.testnet = (
            _env_flag("BYBIT_TESTNET", default=True)
            if testnet is None
            else bool(testnet)
        )
        if not self.testnet and not _env_flag("BYBIT_ALLOW_MAINNET"):
            raise BybitConfigError(
                "mainnet is not an authorized phase; set BYBIT_ALLOW_MAINNET=1 "
                "only with explicit owner approval"
            )
        self.api_key = api_key or os.getenv("BYBIT_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BYBIT_API_SECRET", "")
        if not self.api_key or not self.api_secret:
            raise BybitConfigError(
                "BYBIT_API_KEY and BYBIT_API_SECRET are required"
            )
        self.category = category
        self.timeout_sec = timeout_sec
        self.stall_timeout_sec = stall_timeout_sec
        self.name = "bybit_testnet" if self.testnet else "bybit_mainnet"
        self.is_live = True
        self._base = TESTNET_REST if self.testnet else MAINNET_REST
        self._ws_public = (
            TESTNET_WS_PUBLIC if self.testnet else MAINNET_WS_PUBLIC
        )
        self._ws_private = (
            TESTNET_WS_PRIVATE if self.testnet else MAINNET_WS_PRIVATE
        )
        self._client = client or httpx.AsyncClient(
            base_url=self._base, timeout=timeout_sec
        )
        self._owns_client = client is None
        self._instruments: dict[str, InstrumentFilter] = {}
        self._streams: list[ReconnectingStream] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._closed = False
        # Phase 8 supports one-way mode only (positionIdx=0). Verified lazily.
        self._position_mode_verified = False

    # --- REST plumbing ----------------------------------------------------

    def _headers(self, payload: str) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
            "X-BAPI-SIGN": sign_request(
                self.api_secret,
                timestamp_ms=timestamp,
                api_key=self.api_key,
                recv_window=RECV_WINDOW_MS,
                payload=payload,
            ),
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        signed: bool = True,
    ) -> dict[str, Any]:
        """Call the venue and normalise failures.

        ``retCode != 0`` is returned to the caller (order-scoped decisions are
        made upstream); transport and server-side faults raise
        ``NetworkTimeout`` because they leave order state ambiguous.
        """
        if method == "GET":
            query = httpx.QueryParams(params or {})
            payload = str(query)
            headers = self._headers(payload) if signed else {}
            try:
                response = await self._client.request(
                    method, path, params=query, headers=headers
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                raise NetworkTimeout(f"{method} {path}: {exc}") from exc
        else:
            payload = json.dumps(body or {}, separators=(",", ":"))
            headers = self._headers(payload) if signed else {}
            try:
                response = await self._client.request(
                    method, path, content=payload, headers=headers
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                raise NetworkTimeout(f"{method} {path}: {exc}") from exc

        if response.status_code >= 500:
            raise NetworkTimeout(
                f"{method} {path}: HTTP {response.status_code}"
            )
        try:
            data = response.json()
        except ValueError as exc:
            raise NetworkTimeout(
                f"{method} {path}: non-JSON response ({response.status_code})"
            ) from exc
        if response.status_code >= 400 and not isinstance(data, dict):
            raise ExchangeRejected(
                str(response.status_code), response.text[:500]
            )
        ret_code = str(data.get("retCode", ""))
        if ret_code in _TRANSIENT_CODES:
            raise NetworkTimeout(
                f"{method} {path}: transient retCode={ret_code} "
                f"{data.get('retMsg')}"
            )
        return data

    @staticmethod
    def _ret_ok(data: dict[str, Any]) -> bool:
        return str(data.get("retCode", "")) == "0"

    # --- instrument filters ------------------------------------------------

    async def load_instrument(self, symbol: str) -> InstrumentFilter:
        cached = self._instruments.get(symbol)
        if cached is not None:
            return cached
        data = await self._request(
            "GET",
            "/v5/market/instrumentsInfo",
            params={"category": self.category, "symbol": symbol},
            signed=False,
        )
        if not self._ret_ok(data):
            raise ExchangeRejected(
                str(data.get("retCode")), str(data.get("retMsg"))
            )
        rows = (data.get("result") or {}).get("list") or []
        if not rows:
            raise ExchangeRejected("UNKNOWN_SYMBOL", f"no instrument {symbol}")
        info = parse_instrument(rows[0])
        self._instruments[symbol] = info
        return info

    async def quantize_order(
        self, symbol: str, qty: Decimal, price: Decimal | None = None
    ) -> tuple[Decimal, Decimal | None]:
        info = await self.load_instrument(symbol)
        snapped_qty = floor_to_step(qty, info.qty_step)
        snapped_price = (
            floor_to_step(price, info.tick_size) if price is not None else None
        )
        return snapped_qty, snapped_price

    # --- ExchangeAdapter --------------------------------------------------

    async def ensure_one_way_mode(self) -> None:
        """Refuse hedge-mode accounts (Phase 8 only supports positionIdx=0).

        Hedge mode is detected from open-position ``positionIdx`` values 1/2.
        An empty account is accepted; the first order still sends
        ``positionIdx=0``, which Bybit rejects clearly if the account is hedged.
        """
        if self._position_mode_verified:
            return
        data = await self._request(
            "GET",
            "/v5/position/list",
            params={"category": self.category, "settleCoin": "USDT"},
        )
        if not self._ret_ok(data):
            raise ExchangeRejected(
                str(data.get("retCode")), str(data.get("retMsg"))
            )
        rows = (data.get("result") or {}).get("list") or []
        for row in rows:
            try:
                idx = int(row.get("positionIdx") or 0)
            except (TypeError, ValueError):
                idx = 0
            if idx in (1, 2):
                raise BybitConfigError(
                    "Bybit hedge mode is not supported in Phase 8; "
                    "switch the account to one-way (merged) mode before trading"
                )
        self._position_mode_verified = True

    async def place_order(self, req: PlaceOrderRequest) -> OrderFill:
        await self.ensure_one_way_mode()
        info = await self.load_instrument(req.symbol)
        if info.min_qty > 0 and req.qty < info.min_qty:
            return _local_reject(
                req,
                code="BELOW_MIN_QTY",
                reason=f"qty {req.qty} below venue minimum {info.min_qty}",
            )

        body: dict[str, Any] = {
            "category": self.category,
            "symbol": req.symbol,
            "side": _side_to_venue(req.side),
            "orderType": _order_type_to_venue(req.order_type),
            "qty": _plain(req.qty),
            "orderLinkId": str(req.client_order_id),
            "timeInForce": "IOC" if req.order_type == "MARKET" else "GTC",
            # One-way mode. Hedge accounts are refused by ensure_one_way_mode.
            "positionIdx": 0,
        }
        if req.price is not None and req.order_type == "LIMIT":
            body["price"] = _plain(req.price)
        if req.reduce_only:
            body["reduceOnly"] = True

        data = await self._request("POST", "/v5/order/create", body=body)
        if not self._ret_ok(data):
            # Create failed, but the venue may still have accepted the order
            # (duplicate orderLinkId, index lag). Never declare REJECTED from a
            # single not-found — raise NetworkTimeout so the engine recovers
            # with EXECUTION_NOT_FOUND_CONFIRMATIONS.
            try:
                return await self.get_order_status(
                    req.client_order_id, req.symbol
                )
            except OrderNotFound as exc:
                raise NetworkTimeout(
                    f"create retCode={data.get('retCode')} "
                    f"{data.get('retMsg')}; order not yet visible",
                    client_order_id=req.client_order_id,
                ) from exc
        # /v5/order/create returns only ids; fetch the authoritative state.
        return await self.get_order_status(req.client_order_id, req.symbol)

    async def cancel_order(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        data = await self._request(
            "POST",
            "/v5/order/cancel",
            body={
                "category": self.category,
                "symbol": symbol,
                "orderLinkId": str(client_order_id),
                "positionIdx": 0,
            },
        )
        if not self._ret_ok(data):
            code = str(data.get("retCode"))
            if code in _NOT_FOUND_CODES:
                raise OrderNotFound(client_order_id)
            # Losing a cancel race is normal; the readback tells us what happened.
        return await self.get_order_status(client_order_id, symbol)

    def _raise_unless_order_row(
        self,
        data: dict[str, Any],
        *,
        client_order_id: UUID,
        endpoint: str,
    ) -> dict[str, Any] | None:
        """Return the first order row, or raise — never treat API errors as not-found."""
        if not self._ret_ok(data):
            code = str(data.get("retCode"))
            if code in _NOT_FOUND_CODES:
                raise OrderNotFound(client_order_id)
            # Auth / param / rate-limit errors must not look like "never sent".
            raise NetworkTimeout(
                f"{endpoint} retCode={code} {data.get('retMsg')}",
                client_order_id=client_order_id,
            )
        return _first_order(data)

    async def get_order_status(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        params = {
            "category": self.category,
            "symbol": symbol,
            "orderLinkId": str(client_order_id),
        }
        data = await self._request("GET", "/v5/order/realtime", params=params)
        row = self._raise_unless_order_row(
            data, client_order_id=client_order_id, endpoint="realtime"
        )
        if row is None:
            # Open-order view is empty once terminal; history holds the rest.
            data = await self._request(
                "GET", "/v5/order/history", params=params
            )
            row = self._raise_unless_order_row(
                data, client_order_id=client_order_id, endpoint="history"
            )
        if row is None:
            raise OrderNotFound(client_order_id)
        return parse_order(row, client_order_id=client_order_id)

    async def get_open_positions(self) -> list[ExchangePosition]:
        data = await self._request(
            "GET",
            "/v5/position/list",
            params={"category": self.category, "settleCoin": "USDT"},
        )
        if not self._ret_ok(data):
            raise ExchangeRejected(
                str(data.get("retCode")), str(data.get("retMsg"))
            )
        rows = (data.get("result") or {}).get("list") or []
        out: list[ExchangePosition] = []
        for row in rows:
            parsed = parse_position(row)
            if parsed is not None:
                out.append(parsed)
        return out

    async def subscribe_fills(self, callback: FillCallback) -> None:
        """Consume the private ``order`` topic, which carries cumulative state."""

        async def handler(ws: Any, stream: ReconnectingStream) -> None:
            expires = int(time.time() * 1000) + 5000
            await ws.send(
                json.dumps(
                    {
                        "op": "auth",
                        "args": [
                            self.api_key,
                            expires,
                            sign_ws_auth(self.api_secret, expires),
                        ],
                    }
                )
            )
            # Wait for auth ack before subscribe — silent auth failure must
            # not look like a healthy fill stream.
            auth_deadline = time.monotonic() + 5.0
            authenticated = False
            while time.monotonic() < auth_deadline:
                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                stream.note_message()
                message = json.loads(raw)
                if message.get("op") == "auth":
                    if message.get("success") is True:
                        authenticated = True
                        break
                    raise ExchangeRejected(
                        str(message.get("retCode") or "AUTH"),
                        str(
                            message.get("retMsg")
                            or message.get("conn_id")
                            or "private websocket auth failed"
                        ),
                    )
            if not authenticated:
                raise ExchangeRejected(
                    "AUTH_TIMEOUT", "private websocket auth ack not received"
                )
            await ws.send(json.dumps({"op": "subscribe", "args": ["order"]}))
            async for raw in ws:
                stream.note_message()
                message = json.loads(raw)
                if str(message.get("topic", "")) != "order":
                    continue
                for row in message.get("data") or []:
                    link = row.get("orderLinkId")
                    if not link:
                        continue
                    try:
                        client_order_id = UUID(str(link))
                    except ValueError:
                        # Not one of ours (manual venue order).
                        continue
                    await callback(
                        parse_order(row, client_order_id=client_order_id)
                    )

        # Idle private order topics must not use ticker stall detection —
        # no fills for 30s is normal, not a dead socket.
        self._spawn_stream(
            "bybit_private_order",
            self._ws_private,
            handler,
            stall_timeout_sec=None,
        )

    async def subscribe_marks(
        self, symbols: list[str], callback: MarkCallback
    ) -> None:
        topics = [f"tickers.{s}" for s in symbols]

        async def handler(ws: Any, stream: ReconnectingStream) -> None:
            await ws.send(json.dumps({"op": "subscribe", "args": topics}))
            async for raw in ws:
                stream.note_message()
                for update in parse_ticker_marks(json.loads(raw)):
                    await callback(update)

        self._spawn_stream("bybit_public_tickers", self._ws_public, handler)

    def _spawn_stream(
        self,
        name: str,
        url: str,
        handler: Any,
        *,
        stall_timeout_sec: float | None | object = _STALL_DEFAULT,
    ) -> ReconnectingStream:
        stall: float | None
        if stall_timeout_sec is _STALL_DEFAULT:
            stall = self.stall_timeout_sec
        else:
            stall = stall_timeout_sec  # type: ignore[assignment]
        stream = ReconnectingStream(
            name=name,
            connect=lambda: websockets.connect(url, ping_interval=20),
            handler=handler,
            policy=BackoffPolicy(),
            stall_timeout_sec=stall,
        )
        self._streams.append(stream)
        self._tasks.append(asyncio.create_task(stream.run()))
        return stream

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for stream in self._streams:
            stream.stop()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._streams.clear()
        self._tasks.clear()
        if self._owns_client:
            await self._client.aclose()


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes")


def _plain(value: Decimal) -> str:
    """Decimal without exponent notation — Bybit rejects ``1E-3``."""
    return format(value.normalize(), "f")


def _first_order(data: dict[str, Any]) -> dict[str, Any] | None:
    rows = ((data.get("result") or {}).get("list")) or []
    return rows[0] if rows else None


def _local_reject(
    req: PlaceOrderRequest, *, code: str, reason: str
) -> OrderFill:
    """Order-scoped refusal expressed in the boundary vocabulary."""
    return OrderFill(
        client_order_id=req.client_order_id,
        exchange_order_id=None,
        symbol=req.symbol,
        side=req.side,
        status="REJECTED",
        qty_requested=req.qty,
        qty_filled=Decimal("0"),
        avg_fill_price=None,
        reject_code=code,
        reject_reason=reason,
    )


__all__ = (
    "BybitAdapter",
    "BybitConfigError",
    "InstrumentFilter",
    "floor_to_step",
    "map_order_status",
    "parse_instrument",
    "parse_order",
    "parse_position",
    "parse_ticker_marks",
    "sign_request",
    "sign_ws_auth",
)
