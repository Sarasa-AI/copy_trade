"""Monitor open positions and close via mock_exchange when SL/TP triggers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import asyncpg

import price_feed
from capital import mark_unrealized
from mock_exchange import PositionAlreadyClosed, close_position

__all__ = (
    "PositionManager",
    "evaluate_close_trigger",
    "get_fresh_mark_price",
    "get_mark_price",
    "tick",
)

_OPEN_POSITIONS_QUERY = """
SELECT id, wallet_id, symbol, side, entry_price, qty,
       stop_loss_price, take_profit_price, opened_at
FROM positions
WHERE closed_at IS NULL
"""


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(msg: str) -> None:
    print(f"[{_ts()}] [position_manager] {msg}", flush=True)


def get_mark_price(symbol: str) -> float | None:
    """Return latest mark price if present (may be stale — check freshness)."""
    quote = price_feed.get_mark(symbol)
    return None if quote is None else quote.price


def get_fresh_mark_price(symbol: str) -> float | None:
    """Return mark price only when fresh; None if missing or stale."""
    try:
        return price_feed.require_fresh_mark(symbol).price
    except (price_feed.MarkMissing, price_feed.MarkStale):
        return None


def _all_mark_prices() -> dict[str, float]:
    """Fresh marks only — stale/missing symbols are omitted (not treated as fresh)."""
    return price_feed.fresh_mark_prices()


def evaluate_close_trigger(row: Any, mark_price: float | None) -> str | None:
    """Return close reason if SL/TP should fire for BUY or SELL, else None.

    STOP_LOSS wins when both SL and TP would trigger simultaneously.
    """
    side = row["side"]
    if side not in ("BUY", "SELL"):
        return None

    sl = row["stop_loss_price"]
    tp = row["take_profit_price"]
    if sl is None and tp is None:
        return None

    if mark_price is None or mark_price <= 0:
        return None

    mark = float(mark_price)
    if side == "BUY":
        sl_triggered = sl is not None and mark <= float(sl)
        tp_triggered = tp is not None and mark >= float(tp)
    else:
        # SELL: stop above entry, take-profit below entry.
        sl_triggered = sl is not None and mark >= float(sl)
        tp_triggered = tp is not None and mark <= float(tp)

    if sl_triggered:
        return "STOP_LOSS"
    if tp_triggered:
        return "TAKE_PROFIT"
    return None


class PositionManager:
    """Evaluate open positions against mark prices and close when triggered."""

    async def tick(self, conn: asyncpg.Connection | None = None) -> None:
        await tick(conn)


async def tick(conn: asyncpg.Connection | None = None) -> None:
    """Mark unrealized equity, then evaluate SL/TP and close via close_position()."""
    from mock_exchange import _acquire

    async with _acquire(conn) as db:
        await mark_unrealized(db, mark_prices=_all_mark_prices())

        rows = await db.fetch(_OPEN_POSITIONS_QUERY)
        if not rows:
            return

        _log(f"evaluating {len(rows)} open position(s)")

        for row in rows:
            symbol = row["symbol"]
            try:
                quote = price_feed.require_fresh_mark(symbol)
            except price_feed.MarkMissing:
                _log(
                    f"no mark price for {symbol}; skipping position_id={row['id']}"
                )
                continue
            except price_feed.MarkStale as exc:
                _log(
                    f"stale mark for {symbol} age_sec={exc.age_sec:.3f} "
                    f"max_age_sec={exc.max_age_sec:.3f}; "
                    f"skipping SL/TP position_id={row['id']} "
                    "(not treating stale price as fresh)"
                )
                continue

            mark = quote.price
            reason = evaluate_close_trigger(row, mark)
            if reason is None:
                continue

            _log(
                f"trigger {reason} position_id={row['id']} symbol={symbol} "
                f"mark={mark} side={row['side']}"
            )
            try:
                pnl = await close_position(
                    row["id"],
                    mark,
                    db,
                    close_reason=reason,
                )
                _log(
                    f"closed position_id={row['id']} reason={reason} "
                    f"exit_price={mark} pnl={pnl}"
                )
            except PositionAlreadyClosed:
                _log(
                    f"position already closed position_id={row['id']}; continuing"
                )
            except Exception as exc:
                _log(f"close failed position_id={row['id']}: {exc}")
                raise
