"""Deterministic in-memory adapter — the CI/hermetic implementation.

``NullAdapter`` never opens a socket, so the whole execution path (risk gate →
adapter → capital) can be exercised offline. Failure modes that only appear on
a real venue (reject, partial fill, lost response, duplicate send) are scripted
through :class:`FillPlan` so tests assert on behaviour, not on luck.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any, Literal
from uuid import UUID

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

PlanKind = Literal[
    "FILL",
    "PARTIAL",
    "PENDING",
    "REJECT",
    "TIMEOUT_AFTER_FILL",
    "TIMEOUT_BEFORE_SEND",
]


def _q(value: Decimal) -> Decimal:
    return value.quantize(QUANT, rounding=ROUND_DOWN)


@dataclass(frozen=True)
class FillPlan:
    """What the simulated venue should do with the next order.

    ``TIMEOUT_AFTER_FILL`` is the dangerous one: the venue fills the order but
    the response never arrives, so the caller must recover through
    ``get_order_status`` instead of assuming nothing happened.
    """

    kind: PlanKind = "FILL"
    fill_ratio: Decimal = Decimal("1")
    price: Decimal | None = None
    fee_rate: Decimal = Decimal("0")
    reject_code: str = "SIMULATED_REJECT"
    reject_reason: str = "simulated exchange rejection"


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
    fills: list[dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

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
            raw={"adapter": "null", "fills": list(self.fills)},
        )


class NullAdapter(ExchangeAdapter):
    """Scriptable simulated venue.

    ``mark_prices`` drives fills; ``slippage_bps`` moves the fill away from the
    mark against the taker so paper-vs-venue deviation is measurable offline.
    """

    name = "null"
    is_live = False

    def __init__(
        self,
        *,
        mark_prices: dict[str, Decimal] | None = None,
        slippage_bps: Decimal = Decimal("0"),
        fee_rate: Decimal = Decimal("0"),
    ) -> None:
        self.mark_prices: dict[str, Decimal] = dict(mark_prices or {})
        self.slippage_bps = Decimal(slippage_bps)
        self.fee_rate = Decimal(fee_rate)
        self._orders: dict[UUID, _OrderState] = {}
        self._plans: deque[FillPlan] = deque()
        self._fill_callbacks: list[FillCallback] = []
        self._mark_callbacks: list[MarkCallback] = []
        self._seq = 0
        self.closed = False
        self.duplicate_send_count = 0
        self.sent_count = 0

    # --- test scripting -------------------------------------------------

    def set_mark(self, symbol: str, price: Decimal | float) -> None:
        self.mark_prices[symbol] = Decimal(str(price))

    def queue_plan(self, plan: FillPlan) -> None:
        """Schedule the behaviour of the next ``place_order`` call (FIFO)."""
        self._plans.append(plan)

    def _next_plan(self) -> FillPlan:
        if self._plans:
            return self._plans.popleft()
        return FillPlan(fee_rate=self.fee_rate)

    # --- simulated venue mechanics --------------------------------------

    def _assert_open(self) -> None:
        if self.closed:
            raise ExchangeRejected("ADAPTER_CLOSED", "adapter already closed")

    def _next_exchange_id(self) -> str:
        self._seq += 1
        return f"NULL-{self._seq:08d}"

    def _fill_price(self, req: PlaceOrderRequest, plan: FillPlan) -> Decimal:
        if plan.price is not None:
            return _q(plan.price)
        base = self.mark_prices.get(req.symbol)
        if base is None:
            base = req.price
        if base is None:
            raise ExchangeRejected(
                "NO_MARK", f"no simulated mark for {req.symbol}"
            )
        drift = Decimal(base) * self.slippage_bps / Decimal("10000")
        signed = drift if req.side == "BUY" else -drift
        price = Decimal(base) + signed
        if price <= 0:
            raise ExchangeRejected("BAD_PRICE", "simulated price non-positive")
        return _q(price)

    def _apply_fill(
        self,
        state: _OrderState,
        *,
        qty: Decimal,
        price: Decimal,
        fee_rate: Decimal,
    ) -> None:
        qty = _q(qty)
        if qty <= 0:
            return
        prior_notional = (state.avg_fill_price or Decimal("0")) * state.qty_filled
        total_qty = state.qty_filled + qty
        state.avg_fill_price = _q((prior_notional + qty * price) / total_qty)
        state.qty_filled = total_qty
        fee = _q(qty * price * fee_rate)
        state.fee_paid = _q(state.fee_paid + fee)
        state.fills.append(
            {
                "qty": str(qty),
                "price": str(price),
                "fee": str(fee),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        state.status = (
            "FILLED" if state.qty_filled >= state.req.qty else "PARTIALLY_FILLED"
        )

    # --- ExchangeAdapter -------------------------------------------------

    async def place_order(self, req: PlaceOrderRequest) -> OrderFill:
        self._assert_open()
        existing = self._orders.get(req.client_order_id)
        if existing is not None:
            # Idempotent replay: never create a second venue order.
            self.duplicate_send_count += 1
            return existing.to_fill()

        plan = self._next_plan()
        if plan.kind == "TIMEOUT_BEFORE_SEND":
            raise NetworkTimeout(
                "simulated timeout before send",
                client_order_id=req.client_order_id,
            )

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

        if plan.kind == "REJECT":
            state.status = "REJECTED"
            state.reject_code = plan.reject_code
            state.reject_reason = plan.reject_reason
            return state.to_fill()

        if plan.kind == "PENDING":
            return state.to_fill()

        ratio = Decimal("1") if plan.kind != "PARTIAL" else plan.fill_ratio
        price = self._fill_price(req, plan)
        fee_rate = plan.fee_rate if plan.fee_rate else self.fee_rate
        self._apply_fill(
            state, qty=req.qty * ratio, price=price, fee_rate=fee_rate
        )

        if plan.kind == "TIMEOUT_AFTER_FILL":
            raise NetworkTimeout(
                "simulated timeout after venue fill",
                client_order_id=req.client_order_id,
            )
        return state.to_fill()

    async def cancel_order(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        self._assert_open()
        state = self._orders.get(client_order_id)
        if state is None:
            raise OrderNotFound(client_order_id)
        if state.status in ("FILLED", "REJECTED", "CANCELLED"):
            return state.to_fill()
        # Partial remainder is dropped; already-filled quantity survives.
        state.status = "CANCELLED"
        return state.to_fill()

    async def get_order_status(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        self._assert_open()
        state = self._orders.get(client_order_id)
        if state is None:
            raise OrderNotFound(client_order_id)
        return state.to_fill()

    async def get_open_positions(self) -> list[ExchangePosition]:
        self._assert_open()
        net: dict[str, tuple[Decimal, Decimal]] = {}
        for state in self._orders.values():
            if state.qty_filled <= 0 or state.avg_fill_price is None:
                continue
            signed = state.qty_filled if state.is_buy else -state.qty_filled
            qty, notional = net.get(state.req.symbol, (Decimal("0"), Decimal("0")))
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
        self.closed = True
        self._fill_callbacks.clear()
        self._mark_callbacks.clear()

    # --- test-driven event pushes ---------------------------------------

    async def push_fill(self, fill: OrderFill) -> None:
        for cb in list(self._fill_callbacks):
            await cb(fill)

    async def push_mark(
        self, symbol: str, price: Decimal | float, *, source: str = "null_ws"
    ) -> None:
        update = MarkUpdate(
            symbol=symbol, price=Decimal(str(price)), source=source
        )
        self.set_mark(symbol, update.price)
        for cb in list(self._mark_callbacks):
            await cb(update)


__all__ = ("FillPlan", "NullAdapter")
