"""Phase 8 — exchange boundary package.

The only place in the system that talks to a real exchange. Capital, risk and
kill-switch authority stay outside this package; adapters never import them.

Phase 8A adds :class:`PaperExchangeAdapter`: a simulated venue backed by
``price_feed`` marks. It implements the same :class:`ExchangeAdapter` contract
used by ``ExecutionEngine`` and is physically incapable of sending real orders.
"""

from __future__ import annotations

from exchange_adapter.factory import create_exchange_adapter, resolve_adapter_name
from exchange_adapter.interface import (
    AdapterError,
    ExchangeAdapter,
    ExchangePosition,
    ExchangeRejected,
    MarkUpdate,
    NetworkTimeout,
    OrderAlreadyExists,
    OrderFill,
    OrderNotFound,
    OrderStatus,
    PlaceOrderRequest,
    PositionNotFound,
    TERMINAL_STATUSES,
)
from exchange_adapter.null_adapter import NullAdapter
from exchange_adapter.paper_adapter import (
    PaperConfigError,
    PaperExchangeAdapter,
    PaperFailureMode,
    parse_failure_mode,
)

__all__ = (
    "AdapterError",
    "ExchangeAdapter",
    "ExchangePosition",
    "ExchangeRejected",
    "MarkUpdate",
    "NetworkTimeout",
    "NullAdapter",
    "OrderAlreadyExists",
    "OrderFill",
    "OrderNotFound",
    "OrderStatus",
    "PaperConfigError",
    "PaperExchangeAdapter",
    "PaperFailureMode",
    "PlaceOrderRequest",
    "PositionNotFound",
    "TERMINAL_STATUSES",
    "create_exchange_adapter",
    "parse_failure_mode",
    "resolve_adapter_name",
)
