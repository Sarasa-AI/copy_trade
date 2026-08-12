"""Paper Exchange Adapter — simulated venue backed by live (or injected) marks.

Safety
------
This adapter is physically incapable of sending a real exchange order:

* it never imports ``bybit_adapter``;
* it never reads ``BYBIT_API_KEY`` / ``BYBIT_API_SECRET``;
* it never opens REST/private WebSocket sessions to any venue;
* ``is_live`` is always ``False``.

Live **market data** is consumed only through the existing ``price_feed``
abstraction (or an injected mark callable). Live **order execution** is
forbidden.

Failure injection is centralized in :class:`PaperFailureMode` so tests exercise
the real ``ExecutionEngine`` recovery paths without inventing a second engine.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

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
    PlaceOrderRequest,
)

QUANT = Decimal("0.00000001")

MarkResolver = Callable[[str], Decimal | None]


class PaperFailureMode(str, Enum):
    """Deterministic failure-injection modes (disabled by default = SUCCESS)."""

    SUCCESS = "success"
    CREATE_TIMEOUT = "create_timeout"
    CREATE_REJECT = "create_reject"
    RESPONSE_LOST_AFTER_ACCEPT = "response_lost_after_accept"
    STATUS_TIMEOUT = "status_timeout"
    NOT_FOUND_ONCE = "not_found_once"
    NOT_FOUND_TWICE = "not_found_twice"
    PARTIAL_FILL = "partial_fill"
    CLOSE_SUCCESS = "close_success"
    CLOSE_TIMEOUT = "close_timeout"
    CLOSE_UNKNOWN = "close_unknown"
    WS_DISCONNECT = "ws_disconnect"
    DUPLICATE_EVENT = "duplicate_event"


class PaperConfigError(RuntimeError):
    """Invalid Paper Adapter configuration."""


def _q(value: Decimal) -> Decimal:
    return value.quantize(QUANT, rounding=ROUND_DOWN)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_failure_mode(raw: str | PaperFailureMode | None) -> PaperFailureMode:
    if raw is None or raw == "":
        return PaperFailureMode.SUCCESS
    if isinstance(raw, PaperFailureMode):
        return raw
    try:
        return PaperFailureMode(str(raw).strip().lower())
    except ValueError as exc:
        raise PaperConfigError(
            f"unknown PAPER_FAILURE_MODE={raw!r}; "
            f"expected one of {[m.value for m in PaperFailureMode]}"
        ) from exc


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name, default)
    try:
        return Decimal(str(raw))
    except Exception as exc:
        raise PaperConfigError(f"{name} must be a decimal, got {raw!r}") from exc


def _default_mark_resolver(symbol: str) -> Decimal | None:
    """Read the project's authoritative mark from ``price_feed``."""
    import price_feed

    quote = price_feed.get_mark(symbol)
    if quote is None:
        return None
    return Decimal(str(quote.price))


@dataclass
class _PaperFillEvent:
    venue_fill_id: str
    qty: Decimal
    price: Decimal
    fee: Decimal
    ts: datetime = field(default_factory=_utcnow)


@dataclass
class _OrderState:
    req: PlaceOrderRequest
    exchange_order_id: str
    status: str
    qty_filled: Decimal
    avg_fill_price: Decimal | None
    fee_paid: Decimal
    reject_code: str | None = None
    reject_reason: str | None = None
    fills: list[_PaperFillEvent] = field(default_factory=list)
    created_at: datetime = field(default_factory=_utcnow)
    # Close-unknown: order exists but status queries refuse to disclose it.
    status_hidden: bool = False

    @property
    def is_buy(self) -> bool:
        return self.req.side == "BUY"

    def to_fill(self) -> OrderFill:
        return OrderFill(
            client_order_id=self.req.client_order_id,
            exchange_order_id=self.exchange_order_id,
            symbol=self.req.symbol,
            side=self.req.side,
            status=self.status,  # type: ignore[arg-type]
            qty_requested=self.req.qty,
            qty_filled=self.qty_filled,
            avg_fill_price=self.avg_fill_price,
            fee_paid=self.fee_paid,
            fee_asset="USDT",
            reject_code=self.reject_code,
            reject_reason=self.reject_reason,
            raw={
                "adapter": "paper",
                "venue": "paper",
                "fills": [
                    {
                        "venue_fill_id": f.venue_fill_id,
                        "qty": str(f.qty),
                        "price": str(f.price),
                        "fee": str(f.fee),
                        "ts": f.ts.isoformat(),
                    }
                    for f in self.fills
                ],
            },
        )


class PaperExchangeAdapter(ExchangeAdapter):
    """Simulated venue that fills at the current ``price_feed`` mark.

    Capital / risk / kill-switch authority stay outside this class. The
    execution engine owns intent persistence, settlement and recovery.
    """

    name = "paper"
    is_live = False

    def __init__(
        self,
        *,
        failure_mode: PaperFailureMode | str = PaperFailureMode.SUCCESS,
        fees_enabled: bool = False,
        fee_rate: Decimal = Decimal("0"),
        slippage_bps: Decimal = Decimal("0"),
        partial_fill_ratio: Decimal = Decimal("0.5"),
        mark_resolver: MarkResolver | None = None,
        status_timeout_count: int = 1,
    ) -> None:
        self.failure_mode = parse_failure_mode(failure_mode)
        self.fees_enabled = bool(fees_enabled)
        self.fee_rate = (
            Decimal(fee_rate) if self.fees_enabled else Decimal("0")
        )
        self.slippage_bps = Decimal(slippage_bps)
        self.partial_fill_ratio = Decimal(partial_fill_ratio)
        if self.partial_fill_ratio <= 0 or self.partial_fill_ratio > 1:
            raise PaperConfigError(
                "partial_fill_ratio must be in (0, 1]"
            )
        self._mark_resolver: MarkResolver = (
            mark_resolver or _default_mark_resolver
        )
        self._status_timeout_count = max(0, int(status_timeout_count))

        self._orders: dict[UUID, _OrderState] = {}
        self._fill_callbacks: list[FillCallback] = []
        self._mark_callbacks: list[MarkCallback] = []
        self._closed = False
        self._ws_disconnected = (
            self.failure_mode == PaperFailureMode.WS_DISCONNECT
        )

        # Per-client_order_id counters for status-query failure injection.
        self._status_not_found_counts: dict[UUID, int] = {}
        self._status_timeout_counts: dict[UUID, int] = {}

        self.sent_count = 0
        self.duplicate_send_count = 0
        self.failure_injection_count = 0
        self._one_way_verified = False

        # Metrics (local counters; also mirrored into observability when used).
        self.orders_total = 0
        self.fills_total = 0
        self.partial_fills_total = 0
        self.failures_total = 0
        self.recoveries_total = 0

    # --- construction helpers -----------------------------------------------

    @classmethod
    def from_env(
        cls, *, mark_resolver: MarkResolver | None = None
    ) -> "PaperExchangeAdapter":
        """Build from ``PAPER_*`` env vars. Never reads Bybit credentials."""
        fees_enabled = _env_bool("PAPER_FEES_ENABLED", default=False)
        return cls(
            failure_mode=parse_failure_mode(
                os.getenv("PAPER_FAILURE_MODE", "success")
            ),
            fees_enabled=fees_enabled,
            fee_rate=(
                _env_decimal("PAPER_FEE_RATE", "0")
                if fees_enabled
                else Decimal("0")
            ),
            slippage_bps=_env_decimal("PAPER_SLIPPAGE_BPS", "0"),
            partial_fill_ratio=_env_decimal(
                "PAPER_PARTIAL_FILL_RATIO", "0.5"
            ),
            mark_resolver=mark_resolver,
        )

    def set_failure_mode(self, mode: PaperFailureMode | str) -> None:
        self.failure_mode = parse_failure_mode(mode)
        if self.failure_mode == PaperFailureMode.WS_DISCONNECT:
            self._ws_disconnected = True

    def reconnect_ws(self) -> None:
        """Clear simulated stream interruption (status queries always work)."""
        self._ws_disconnected = False

    async def ensure_one_way_mode(self) -> None:
        """Paper venue supports ONE_WAY only — always succeeds."""
        self._one_way_verified = True

    # --- mark / fill mechanics ----------------------------------------------

    def _assert_open(self) -> None:
        if self._closed:
            raise ExchangeRejected("ADAPTER_CLOSED", "adapter already closed")

    def _next_exchange_id(self) -> str:
        return f"paper-order-{uuid4()}"

    def _next_fill_id(self) -> str:
        return f"paper-fill-{uuid4()}"

    def resolve_mark(self, symbol: str) -> Decimal:
        """Return the authoritative current market mark for ``symbol``."""
        price = self._mark_resolver(symbol)
        if price is None:
            raise ExchangeRejected(
                "NO_MARK", f"no market mark available for {symbol}"
            )
        price_d = Decimal(str(price))
        if price_d <= 0:
            raise ExchangeRejected("BAD_PRICE", "market mark non-positive")
        return price_d

    def _fill_price(self, req: PlaceOrderRequest) -> Decimal:
        base = self.resolve_mark(req.symbol)
        if self.slippage_bps == 0:
            # Zero-slippage contract: do not round prematurely before return.
            return base
        drift = base * self.slippage_bps / Decimal("10000")
        signed = drift if req.side == "BUY" else -drift
        price = base + signed
        if price <= 0:
            raise ExchangeRejected("BAD_PRICE", "simulated price non-positive")
        return price

    def _apply_fill(
        self,
        state: _OrderState,
        *,
        qty: Decimal,
        price: Decimal,
    ) -> _PaperFillEvent:
        qty = _q(qty)
        if qty <= 0:
            raise ExchangeRejected("BAD_QTY", "fill quantity must be positive")
        fee_rate = self.fee_rate if self.fees_enabled else Decimal("0")
        fee = _q(qty * price * fee_rate) if fee_rate else Decimal("0")
        # Explicit zero-fee contract when fees disabled.
        if not self.fees_enabled:
            fee = Decimal("0")

        prior_notional = (
            (state.avg_fill_price or Decimal("0")) * state.qty_filled
        )
        total_qty = state.qty_filled + qty
        # Preserve mark precision when zero-slippage single fill.
        if state.qty_filled == 0 and self.slippage_bps == 0:
            state.avg_fill_price = price
        else:
            state.avg_fill_price = _q(
                (prior_notional + qty * price) / total_qty
            )
        state.qty_filled = total_qty
        state.fee_paid = _q(state.fee_paid + fee)

        event = _PaperFillEvent(
            venue_fill_id=self._next_fill_id(),
            qty=qty,
            price=price,
            fee=fee,
        )
        state.fills.append(event)
        state.status = (
            "FILLED"
            if state.qty_filled >= state.req.qty
            else "PARTIALLY_FILLED"
        )
        self.fills_total += 1
        if state.status == "PARTIALLY_FILLED":
            self.partial_fills_total += 1
        return event

    def _mode_for(self, req: PlaceOrderRequest) -> PaperFailureMode:
        mode = self.failure_mode
        if req.reduce_only:
            if mode == PaperFailureMode.CLOSE_TIMEOUT:
                return PaperFailureMode.CREATE_TIMEOUT
            if mode == PaperFailureMode.CLOSE_UNKNOWN:
                return PaperFailureMode.CLOSE_UNKNOWN
            if mode == PaperFailureMode.CLOSE_SUCCESS:
                return PaperFailureMode.SUCCESS
        elif mode in (
            PaperFailureMode.CLOSE_TIMEOUT,
            PaperFailureMode.CLOSE_UNKNOWN,
            PaperFailureMode.CLOSE_SUCCESS,
        ):
            # Close-only modes do not affect opens.
            return PaperFailureMode.SUCCESS
        return mode

    def _bump_failure(self, mode: PaperFailureMode) -> None:
        self.failure_injection_count += 1
        self.failures_total += 1
        try:
            from observability import inc_paper_failure

            inc_paper_failure(mode.value)
        except Exception:
            pass

    async def _emit_fill_callbacks(self, fill: OrderFill) -> None:
        if self._ws_disconnected:
            return
        for cb in list(self._fill_callbacks):
            await cb(fill)

    # --- ExchangeAdapter ----------------------------------------------------

    async def place_order(self, req: PlaceOrderRequest) -> OrderFill:
        self._assert_open()
        await self.ensure_one_way_mode()

        existing = self._orders.get(req.client_order_id)
        if existing is not None:
            self.duplicate_send_count += 1
            return existing.to_fill()

        mode = self._mode_for(req)
        t0 = time.perf_counter()

        if mode == PaperFailureMode.CREATE_TIMEOUT:
            self._bump_failure(mode)
            raise NetworkTimeout(
                "paper create timeout (order never accepted)",
                client_order_id=req.client_order_id,
            )

        # Capture authoritative mark before accepting (except pure rejects).
        price: Decimal | None = None
        if mode != PaperFailureMode.CREATE_REJECT:
            price = self._fill_price(req)

        state = _OrderState(
            req=req,
            exchange_order_id=self._next_exchange_id(),
            status="PENDING",
            qty_filled=Decimal("0"),
            avg_fill_price=None,
            fee_paid=Decimal("0"),
        )
        self._orders[req.client_order_id] = state
        self.sent_count += 1
        self.orders_total += 1

        if mode == PaperFailureMode.CREATE_REJECT:
            self._bump_failure(mode)
            state.status = "REJECTED"
            state.reject_code = "PAPER_REJECT"
            state.reject_reason = "simulated paper venue rejection"
            state.fee_paid = Decimal("0")
            return state.to_fill()

        assert price is not None

        if mode == PaperFailureMode.CLOSE_UNKNOWN and req.reduce_only:
            self._bump_failure(mode)
            self._apply_fill(state, qty=req.qty, price=price)
            state.status_hidden = True
            raise NetworkTimeout(
                "paper close accepted but response lost; status hidden",
                client_order_id=req.client_order_id,
            )

        ratio = (
            self.partial_fill_ratio
            if mode == PaperFailureMode.PARTIAL_FILL
            else Decimal("1")
        )
        if mode == PaperFailureMode.PARTIAL_FILL:
            self._bump_failure(mode)

        fill_qty = _q(req.qty * ratio)
        event = self._apply_fill(state, qty=fill_qty, price=price)

        # Explicit fee representation on the order.
        if not self.fees_enabled:
            state.fee_paid = Decimal("0")
            event.fee = Decimal("0")

        fill = state.to_fill()
        await self._emit_fill_callbacks(fill)

        if mode == PaperFailureMode.DUPLICATE_EVENT:
            self._bump_failure(mode)
            # Re-emit the same cumulative state / same fill IDs (stable).
            await self._emit_fill_callbacks(fill)

        try:
            from observability import (
                inc_paper_fill,
                inc_paper_order,
                inc_paper_partial_fill,
                record_paper_fill_latency_seconds,
            )

            record_paper_fill_latency_seconds(time.perf_counter() - t0)
            inc_paper_order()
            inc_paper_fill()
            if state.status == "PARTIALLY_FILLED":
                inc_paper_partial_fill()
        except Exception:
            pass

        if mode == PaperFailureMode.RESPONSE_LOST_AFTER_ACCEPT:
            self._bump_failure(mode)
            raise NetworkTimeout(
                "paper response lost after accept/fill",
                client_order_id=req.client_order_id,
            )

        if mode == PaperFailureMode.STATUS_TIMEOUT:
            # Create succeeded and filled; client must recover via status,
            # which will time out ``status_timeout_count`` times first.
            self._bump_failure(mode)
            raise NetworkTimeout(
                "paper create response lost; status will initially timeout",
                client_order_id=req.client_order_id,
            )

        if mode in (
            PaperFailureMode.NOT_FOUND_ONCE,
            PaperFailureMode.NOT_FOUND_TWICE,
        ):
            # Order exists on the venue; status will deny knowledge N times.
            self._bump_failure(mode)
            raise NetworkTimeout(
                "paper create response lost; status may return not-found",
                client_order_id=req.client_order_id,
            )

        return fill

    async def cancel_order(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        self._assert_open()
        state = self._orders.get(client_order_id)
        if state is None:
            raise OrderNotFound(client_order_id)
        if state.status in ("FILLED", "REJECTED", "CANCELLED"):
            return state.to_fill()
        # Partial remainder dropped; already-filled quantity survives.
        state.status = "CANCELLED"
        return state.to_fill()

    async def get_order_status(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        self._assert_open()
        state = self._orders.get(client_order_id)

        # Status timeout injection (create succeeded; status flaky).
        if self.failure_mode == PaperFailureMode.STATUS_TIMEOUT:
            seen = self._status_timeout_counts.get(client_order_id, 0)
            if seen < self._status_timeout_count:
                self._status_timeout_counts[client_order_id] = seen + 1
                self._bump_failure(PaperFailureMode.STATUS_TIMEOUT)
                raise NetworkTimeout(
                    "paper status timeout",
                    client_order_id=client_order_id,
                )

        # not_found_once / not_found_twice — even when the order exists.
        if self.failure_mode in (
            PaperFailureMode.NOT_FOUND_ONCE,
            PaperFailureMode.NOT_FOUND_TWICE,
        ):
            needed = (
                1
                if self.failure_mode == PaperFailureMode.NOT_FOUND_ONCE
                else 2
            )
            seen = self._status_not_found_counts.get(client_order_id, 0)
            if seen < needed:
                self._status_not_found_counts[client_order_id] = seen + 1
                self._bump_failure(self.failure_mode)
                raise OrderNotFound(client_order_id)

        if state is None:
            raise OrderNotFound(client_order_id)

        if state.status_hidden:
            self._bump_failure(PaperFailureMode.CLOSE_UNKNOWN)
            raise NetworkTimeout(
                "paper order status hidden (close_unknown)",
                client_order_id=client_order_id,
            )

        self.recoveries_total += 1
        try:
            from observability import inc_paper_recovery

            inc_paper_recovery()
        except Exception:
            pass
        return state.to_fill()

    async def get_open_positions(self) -> list[ExchangePosition]:
        self._assert_open()
        net: dict[str, tuple[Decimal, Decimal]] = {}
        for state in self._orders.values():
            if state.qty_filled <= 0 or state.avg_fill_price is None:
                continue
            if state.req.reduce_only:
                # Reduce-only fills close exposure; approximate by netting.
                signed = (
                    -state.qty_filled
                    if state.is_buy
                    else state.qty_filled
                )
            else:
                signed = (
                    state.qty_filled if state.is_buy else -state.qty_filled
                )
            qty, notional = net.get(
                state.req.symbol, (Decimal("0"), Decimal("0"))
            )
            net[state.req.symbol] = (
                qty + signed,
                notional + signed * state.avg_fill_price,
            )
        out: list[ExchangePosition] = []
        for symbol, (qty, notional) in net.items():
            if qty == 0:
                continue
            out.append(
                ExchangePosition(
                    symbol=symbol,
                    side="BUY" if qty > 0 else "SELL",
                    qty=abs(qty),
                    entry_price=_q(abs(notional / qty)),
                )
            )
        return out

    async def subscribe_fills(self, callback: FillCallback) -> None:
        self._assert_open()
        self._fill_callbacks.append(callback)

    async def subscribe_marks(
        self, symbols: list[str], callback: MarkCallback
    ) -> None:
        self._assert_open()
        self._mark_callbacks.append(callback)

    async def close(self) -> None:
        self._closed = True
        self._fill_callbacks.clear()
        self._mark_callbacks.clear()

    # --- test-driven event pushes -------------------------------------------

    async def push_fill(self, fill: OrderFill) -> None:
        await self._emit_fill_callbacks(fill)

    async def push_mark(
        self,
        symbol: str,
        price: Decimal | float,
        *,
        source: str = "paper_inject",
    ) -> None:
        """Push a mark to subscribers and optionally update price_feed."""
        import price_feed

        price_feed.update_mark(symbol, float(price), source=source)
        update = MarkUpdate(
            symbol=symbol, price=Decimal(str(price)), source=source
        )
        if self._ws_disconnected:
            return
        for cb in list(self._mark_callbacks):
            await cb(update)


__all__ = (
    "PaperConfigError",
    "PaperExchangeAdapter",
    "PaperFailureMode",
    "parse_failure_mode",
)
