"""Exchange adapter factory — explicit venue selection with fail-closed safety.

``EXCHANGE_ADAPTER`` selects the implementation:

* ``paper`` — :class:`PaperExchangeAdapter` (simulated venue; never live)
* ``null``  — :class:`NullAdapter` (hermetic CI scripting)
* ``bybit`` — :class:`BybitAdapter` (testnet/mainnet; requires credentials)

Hard safety for paper mode
--------------------------
When ``EXCHANGE_ADAPTER=paper`` (or ``PAPER_MODE=true``):

* Bybit credentials are **never** read or consumed;
* ``BybitAdapter`` is **never** imported or instantiated;
* there is **no** silent fallback to Bybit or Null.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from exchange_adapter.paper_adapter import PaperConfigError, PaperExchangeAdapter

if TYPE_CHECKING:
    from exchange_adapter.interface import ExchangeAdapter


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def resolve_adapter_name() -> str:
    """Return the configured adapter name (normalized lowercase)."""
    if _env_flag("PAPER_MODE", default=False):
        explicit = os.getenv("EXCHANGE_ADAPTER", "").strip().lower()
        if explicit and explicit not in {"paper", ""}:
            raise PaperConfigError(
                f"PAPER_MODE=true conflicts with EXCHANGE_ADAPTER={explicit!r}; "
                "paper mode cannot instantiate a live or alternate venue"
            )
        return "paper"
    raw = os.getenv("EXCHANGE_ADAPTER", "null").strip().lower()
    return raw or "null"


def create_exchange_adapter(**kwargs: object) -> "ExchangeAdapter":
    """Construct the configured :class:`ExchangeAdapter`.

    Keyword args are forwarded to the selected adapter constructor where
    supported. Unknown ``EXCHANGE_ADAPTER`` values fail closed.
    """
    name = resolve_adapter_name()

    if name == "paper":
        # Never touch Bybit credentials or clients in paper mode.
        # Presence of BYBIT_* in the environment is ignored on purpose.
        paper_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k
            in {
                "failure_mode",
                "fees_enabled",
                "fee_rate",
                "slippage_bps",
                "partial_fill_ratio",
                "mark_resolver",
                "status_timeout_count",
            }
        }
        if paper_kwargs:
            return PaperExchangeAdapter(**paper_kwargs)  # type: ignore[arg-type]
        return PaperExchangeAdapter.from_env(
            mark_resolver=kwargs.get("mark_resolver")  # type: ignore[arg-type]
            if "mark_resolver" in kwargs
            else None
        )

    if name == "null":
        from exchange_adapter.null_adapter import NullAdapter

        null_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k in {"mark_prices", "slippage_bps", "fee_rate"}
        }
        return NullAdapter(**null_kwargs)  # type: ignore[arg-type]

    if name == "bybit":
        # Import lazily so paper/null paths never load httpx/bybit modules.
        from exchange_adapter.bybit_adapter import BybitAdapter

        bybit_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k
            in {
                "api_key",
                "api_secret",
                "testnet",
                "category",
                "timeout_sec",
                "client",
                "stall_timeout_sec",
            }
        }
        return BybitAdapter(**bybit_kwargs)  # type: ignore[arg-type]

    raise PaperConfigError(
        f"unknown EXCHANGE_ADAPTER={name!r}; expected paper|null|bybit"
    )


__all__ = (
    "create_exchange_adapter",
    "resolve_adapter_name",
)
