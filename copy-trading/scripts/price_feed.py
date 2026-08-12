"""Timestamped market marks (paper feed) with freshness policy.

P7-002 / P7-003: marks carry symbol, price, UTC timestamp, and source.
``MAX_MARK_AGE_SEC`` (default 30) is read from the environment — not hardcoded
into business comparisons.

Authoritative fail-closed deny on open is P7-004 (risk / place_order).
This module provides the mark model and freshness API only.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import websockets

URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
DEFAULT_SOURCE = "binance_ws_trade"


def max_mark_age_sec() -> float:
    """Configurable maximum mark age in seconds (env ``MAX_MARK_AGE_SEC``)."""
    raw = os.getenv("MAX_MARK_AGE_SEC", "30")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"MAX_MARK_AGE_SEC must be a number, got {raw!r}"
        ) from exc
    if value < 0:
        raise ValueError("MAX_MARK_AGE_SEC must be non-negative")
    return value


@dataclass(frozen=True)
class MarkQuote:
    symbol: str
    price: float
    ts_utc: datetime
    source: str

    def age_sec(self, *, now: datetime | None = None) -> float:
        ts = self.ts_utc
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        wall = now if now is not None else datetime.now(timezone.utc)
        if wall.tzinfo is None:
            wall = wall.replace(tzinfo=timezone.utc)
        return max(0.0, (wall.astimezone(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds())

    def is_fresh(
        self,
        *,
        now: datetime | None = None,
        max_age_sec: float | None = None,
    ) -> bool:
        limit = max_mark_age_sec() if max_age_sec is None else float(max_age_sec)
        return self.age_sec(now=now) <= limit + 1e-12


class MarkMissing(LookupError):
    """No mark is available for the requested symbol."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        super().__init__(f"mark missing for {symbol}")

    def to_dict(self) -> dict[str, Any]:
        return {"error": "mark_missing", "symbol": self.symbol}


class MarkStale(LookupError):
    """Mark exists but exceeds the configured maximum age."""

    def __init__(
        self,
        symbol: str,
        *,
        age_sec: float,
        max_age_sec: float,
        quote: MarkQuote | None = None,
    ) -> None:
        self.symbol = symbol
        self.age_sec = age_sec
        self.max_age_sec = max_age_sec
        self.quote = quote
        super().__init__(
            f"mark stale for {symbol}: age_sec={age_sec:.3f} "
            f"max_age_sec={max_age_sec:.3f}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "mark_stale",
            "symbol": self.symbol,
            "age_sec": self.age_sec,
            "max_age_sec": self.max_age_sec,
            "price": self.quote.price if self.quote else None,
            "ts_utc": self.quote.ts_utc.isoformat() if self.quote else None,
            "source": self.quote.source if self.quote else None,
        }


lock = threading.RLock()
_quotes: dict[str, MarkQuote] = {}
_last_heartbeat_utc: datetime | None = None


class _PricesView(dict):
    """Backward-compatible float map synced from quotes.

    Writes go through ``update_mark`` so tests that assign
    ``price_feed.prices[symbol] = x`` still produce timestamped marks.
    """

    def __setitem__(self, key: str, value: float) -> None:  # type: ignore[override]
        update_mark(str(key), float(value), source="legacy_prices_write")

    def __delitem__(self, key: str) -> None:  # type: ignore[override]
        clear_mark(str(key))

    def pop(self, key: str, *args):  # type: ignore[override]
        with lock:
            quote = _quotes.pop(key, None)
            if quote is None:
                if args:
                    return args[0]
                raise KeyError(key)
            if key in self:
                dict.__delitem__(self, key)
            return quote.price

    def clear(self) -> None:  # type: ignore[override]
        clear_all_marks()


prices: _PricesView = _PricesView()


def _normalize_ts(ts_utc: datetime | None) -> datetime:
    if ts_utc is None:
        return datetime.now(timezone.utc)
    if ts_utc.tzinfo is None:
        return ts_utc.replace(tzinfo=timezone.utc)
    return ts_utc.astimezone(timezone.utc)


def _update_mark_unlocked(
    symbol: str,
    price: float,
    *,
    source: str,
    ts_utc: datetime | None,
) -> MarkQuote:
    global _last_heartbeat_utc
    if not symbol or not str(symbol).strip():
        raise ValueError("symbol is required")
    px = float(price)
    if px <= 0:
        raise ValueError("mark price must be positive")
    quote = MarkQuote(
        symbol=str(symbol).strip(),
        price=px,
        ts_utc=_normalize_ts(ts_utc),
        source=str(source).strip() or DEFAULT_SOURCE,
    )
    _quotes[quote.symbol] = quote
    _last_heartbeat_utc = quote.ts_utc
    dict.__setitem__(prices, quote.symbol, quote.price)
    return quote


def update_mark(
    symbol: str,
    price: float,
    *,
    source: str = DEFAULT_SOURCE,
    ts_utc: datetime | None = None,
) -> MarkQuote:
    """Store a timestamped mark. Positive price required."""
    with lock:
        return _update_mark_unlocked(
            symbol, price, source=source, ts_utc=ts_utc
        )


def clear_mark(symbol: str) -> None:
    with lock:
        _quotes.pop(symbol, None)
        if symbol in prices:
            dict.__delitem__(prices, symbol)


def clear_all_marks() -> None:
    global _last_heartbeat_utc
    with lock:
        _quotes.clear()
        dict.clear(prices)
        _last_heartbeat_utc = None


def get_mark(symbol: str) -> MarkQuote | None:
    with lock:
        return _quotes.get(symbol)


def last_update(symbol: str | None = None) -> datetime | None:
    """Per-symbol last mark time, or global heartbeat when symbol is None."""
    with lock:
        if symbol is None:
            return _last_heartbeat_utc
        quote = _quotes.get(symbol)
        return quote.ts_utc if quote else None


def heartbeat_age_sec(*, now: datetime | None = None) -> float | None:
    """Seconds since any mark update (feed heartbeat). None if never updated."""
    with lock:
        hb = _last_heartbeat_utc
    if hb is None:
        return None
    wall = now if now is not None else datetime.now(timezone.utc)
    if wall.tzinfo is None:
        wall = wall.replace(tzinfo=timezone.utc)
    if hb.tzinfo is None:
        hb = hb.replace(tzinfo=timezone.utc)
    return max(0.0, (wall.astimezone(timezone.utc) - hb.astimezone(timezone.utc)).total_seconds())


def mark_age_sec(symbol: str, *, now: datetime | None = None) -> float | None:
    quote = get_mark(symbol)
    if quote is None:
        return None
    return quote.age_sec(now=now)


def is_mark_fresh(
    symbol: str,
    *,
    now: datetime | None = None,
    max_age_sec: float | None = None,
) -> bool:
    quote = get_mark(symbol)
    if quote is None:
        return False
    return quote.is_fresh(now=now, max_age_sec=max_age_sec)


def require_fresh_mark(
    symbol: str,
    *,
    now: datetime | None = None,
    max_age_sec: float | None = None,
) -> MarkQuote:
    """Return a fresh mark or raise ``MarkMissing`` / ``MarkStale``."""
    quote = get_mark(symbol)
    if quote is None:
        raise MarkMissing(symbol)
    limit = max_mark_age_sec() if max_age_sec is None else float(max_age_sec)
    age = quote.age_sec(now=now)
    if age > limit + 1e-12:
        raise MarkStale(
            symbol, age_sec=age, max_age_sec=limit, quote=quote
        )
    return quote


def fresh_mark_prices(
    *,
    now: datetime | None = None,
    max_age_sec: float | None = None,
) -> dict[str, float]:
    """Symbol → price for marks that are currently fresh only."""
    with lock:
        items = list(_quotes.items())
    out: dict[str, float] = {}
    for symbol, quote in items:
        if quote.is_fresh(now=now, max_age_sec=max_age_sec):
            out[symbol] = quote.price
    return out


def all_mark_quotes() -> dict[str, MarkQuote]:
    with lock:
        return dict(_quotes)


async def _listen() -> None:
    async with websockets.connect(URL) as ws:
        async for raw in ws:
            data = json.loads(raw)
            # Binance trade payload: "p" price; "T" trade time ms when present.
            ts: datetime | None = None
            trade_ms = data.get("T") or data.get("E")
            if trade_ms is not None:
                try:
                    ts = datetime.fromtimestamp(
                        float(trade_ms) / 1000.0, tz=timezone.utc
                    )
                except (TypeError, ValueError, OSError):
                    ts = None
            update_mark(
                "BTCUSDT",
                float(data["p"]),
                source=DEFAULT_SOURCE,
                ts_utc=ts,
            )


def start() -> None:
    threading.Thread(target=lambda: asyncio.run(_listen()), daemon=True).start()


if __name__ == "__main__":
    start()
    threading.Event().wait()
