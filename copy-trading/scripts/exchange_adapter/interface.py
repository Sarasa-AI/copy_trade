"""Phase 8 — abstract Exchange Adapter interface and transport data models.

Boundary contract
-----------------
An adapter translates between exchange wire formats and the models below.
It performs **no** capital accounting, **no** risk decisions and **no** DB
writes. ``execution_engine`` is the only caller; ``capital.py`` remains the
single writer of balances.

Money is carried as ``Decimal`` across this boundary because exchange payloads
are decimal strings. Conversion to the ``float`` capital model happens once, in
``execution_engine``, so rounding is visible in one place.

Rejection policy
----------------
- Order-scoped rejections (margin, tick size, reduce-only violation) are
  returned as ``OrderFill(status="REJECTED")`` so the caller can persist the
  exchange reason.
- Non-order-scoped failures (auth, malformed request, unknown symbol) raise
  ``ExchangeRejected``.
- Transport failures raise ``NetworkTimeout`` and the caller MUST resolve the
  order state with ``get_order_status`` before touching capital.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

OrderStatus = Literal[
    "PENDING",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELLED",
    "REJECTED",
]

ORDER_STATUSES: frozenset[str] = frozenset(
    {"PENDING", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"}
)

# Statuses the exchange will not move away from on its own.
TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"FILLED", "CANCELLED", "REJECTED"}
)

Side = Literal["BUY", "SELL"]
OrderType = Literal["MARKET", "LIMIT"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class PlaceOrderRequest:
    """A single order intent. ``client_order_id`` is the idempotency key.

    The same ``client_order_id`` must be reused for every retry of the same
    intent so a duplicate send can never create a second exchange order.
    """

    client_order_id: UUID
    symbol: str
    side: Side
    order_type: OrderType
    qty: Decimal
    price: Decimal | None = None
    reduce_only: bool = False

    def __post_init__(self) -> None:
        if not self.symbol or not str(self.symbol).strip():
            raise ValueError("symbol is required")
        if self.side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        if self.order_type not in ("MARKET", "LIMIT"):
            raise ValueError("order_type must be MARKET or LIMIT")
        if self.qty <= 0:
            raise ValueError("qty must be positive")
        if self.order_type == "LIMIT":
            if self.price is None or self.price <= 0:
                raise ValueError("LIMIT order requires a positive price")
        elif self.price is not None and self.price <= 0:
            raise ValueError("price must be positive when provided")


@dataclass(frozen=True)
class OrderFill:
    """Authoritative exchange-side state of one order at a point in time.

    ``qty_filled`` / ``avg_fill_price`` are cumulative for the order, not for
    the latest partial fill.
    """

    client_order_id: UUID
    exchange_order_id: str | None
    symbol: str
    side: Side
    status: OrderStatus
    qty_requested: Decimal
    qty_filled: Decimal
    avg_fill_price: Decimal | None
    fee_paid: Decimal = Decimal("0")
    fee_asset: str = "USDT"
    reject_code: str | None = None
    reject_reason: str | None = None
    timestamp_utc: datetime = field(default_factory=_utcnow)
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in ORDER_STATUSES:
            raise ValueError(f"unknown order status: {self.status}")
        if self.qty_filled < 0:
            raise ValueError("qty_filled must be non-negative")
        if self.qty_filled > 0 and (
            self.avg_fill_price is None or self.avg_fill_price <= 0
        ):
            raise ValueError("filled order requires a positive avg_fill_price")
        if self.status == "FILLED" and self.qty_filled <= 0:
            raise ValueError("FILLED order must report qty_filled > 0")

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def qty_remaining(self) -> Decimal:
        return max(Decimal("0"), self.qty_requested - self.qty_filled)

    @property
    def filled_notional(self) -> Decimal:
        if self.qty_filled <= 0 or self.avg_fill_price is None:
            return Decimal("0")
        return self.qty_filled * self.avg_fill_price

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_order_id": str(self.client_order_id),
            "exchange_order_id": self.exchange_order_id,
            "symbol": self.symbol,
            "side": self.side,
            "status": self.status,
            "qty_requested": str(self.qty_requested),
            "qty_filled": str(self.qty_filled),
            "avg_fill_price": (
                str(self.avg_fill_price)
                if self.avg_fill_price is not None
                else None
            ),
            "fee_paid": str(self.fee_paid),
            "fee_asset": self.fee_asset,
            "reject_code": self.reject_code,
            "reject_reason": self.reject_reason,
            "timestamp_utc": self.timestamp_utc.isoformat(),
        }


@dataclass(frozen=True)
class MarkUpdate:
    """A timestamped mark, shaped for ``price_feed.update_mark``."""

    symbol: str
    price: Decimal
    timestamp_utc: datetime = field(default_factory=_utcnow)
    source: str = "exchange_ws"


@dataclass(frozen=True)
class ExchangePosition:
    """Exchange-side open position, used only for reconciliation."""

    symbol: str
    side: Side
    qty: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal = Decimal("0")
    raw: dict[str, Any] = field(default_factory=dict)


class AdapterError(RuntimeError):
    """Base class for exchange boundary failures."""

    def to_dict(self) -> dict[str, Any]:
        return {"error": "adapter_error", "detail": str(self)}


class OrderAlreadyExists(AdapterError):
    """The exchange already knows this ``client_order_id``.

    Adapters should prefer returning the existing ``OrderFill`` (idempotent
    replay). This is raised only when the exchange refuses the duplicate and
    its current state cannot be read back.
    """

    def __init__(self, client_order_id: UUID) -> None:
        self.client_order_id = client_order_id
        super().__init__(f"order_already_exists: {client_order_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "order_already_exists",
            "client_order_id": str(self.client_order_id),
        }


class ExchangeRejected(AdapterError):
    """Non-order-scoped API rejection (auth, malformed request, bad symbol)."""

    def __init__(self, code: str, message: str) -> None:
        self.code = str(code)
        self.message = str(message)
        super().__init__(f"exchange_rejected [{self.code}]: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "exchange_rejected",
            "code": self.code,
            "message": self.message,
        }


class NetworkTimeout(AdapterError):
    """Transport failed; order state at the exchange is unknown.

    The caller must retry with the same ``client_order_id`` or resolve state
    via ``get_order_status`` before any capital movement.
    """

    def __init__(
        self, detail: str, *, client_order_id: UUID | None = None
    ) -> None:
        self.client_order_id = client_order_id
        super().__init__(f"network_timeout: {detail}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "network_timeout",
            "detail": str(self),
            "client_order_id": (
                str(self.client_order_id) if self.client_order_id else None
            ),
        }


class OrderNotFound(AdapterError):
    """The exchange has no record of this ``client_order_id``."""

    def __init__(self, client_order_id: UUID) -> None:
        self.client_order_id = client_order_id
        super().__init__(f"order_not_found: {client_order_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "order_not_found",
            "client_order_id": str(self.client_order_id),
        }


class PositionNotFound(AdapterError):
    """Reconciliation expected an exchange position that does not exist."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        super().__init__(f"position_not_found: {symbol}")

    def to_dict(self) -> dict[str, Any]:
        return {"error": "position_not_found", "symbol": self.symbol}


FillCallback = Callable[[OrderFill], Awaitable[None]]
MarkCallback = Callable[[MarkUpdate], Awaitable[None]]


class ExchangeAdapter(ABC):
    """The system's only outbound door to an exchange.

    Implementations must be idempotent on ``client_order_id`` and must never
    raise for an order-scoped rejection — those come back as ``OrderFill``
    with ``status="REJECTED"``.
    """

    #: Stable identifier persisted on every order row (``bybit_testnet``, ``null``).
    name: str = "abstract"

    #: True only for adapters that can move real or testnet funds.
    is_live: bool = False

    async def quantize_order(
        self, symbol: str, qty: Decimal, price: Decimal | None = None
    ) -> tuple[Decimal, Decimal | None]:
        """Snap qty/price to the venue's instrument filters.

        Called before the intent row is persisted so the recorded request equals
        what the venue is asked to do. The default is identity; venues with step
        sizes must override.
        """
        return qty, price

    @abstractmethod
    async def place_order(self, req: PlaceOrderRequest) -> OrderFill:
        """Submit an order and return its current exchange state.

        Replaying the same ``client_order_id`` must return the existing order's
        state instead of creating a second order.
        """

    @abstractmethod
    async def cancel_order(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        """Cancel an order; a race-lost cancel returns the resulting fill."""

    @abstractmethod
    async def get_order_status(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill:
        """Read authoritative order state — the recovery path after a timeout."""

    @abstractmethod
    async def get_open_positions(self) -> list[ExchangePosition]:
        """Exchange-side open positions for reconciliation."""

    @abstractmethod
    async def subscribe_fills(self, callback: FillCallback) -> None:
        """Stream private fill events until ``close`` is called."""

    @abstractmethod
    async def subscribe_marks(
        self, symbols: list[str], callback: MarkCallback
    ) -> None:
        """Stream public marks until ``close`` is called."""

    @abstractmethod
    async def close(self) -> None:
        """Release sockets and sessions. Must be safe to call twice."""

    async def __aenter__(self) -> "ExchangeAdapter":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
