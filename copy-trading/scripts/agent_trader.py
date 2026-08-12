"""Main trading loop: copy top wallets with paper orders via mock exchange."""

from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timezone

import asyncpg

import price_feed
from capital import (
    InsufficientAvailableBalance,
    initialize_paper_allocations,
)
from kill_switch import KillSwitchActive
from mock_exchange import check_daily_loss, place_order
from risk_engine import RiskDenied
import position_manager

SYMBOL = "BTCUSDT"
POSITION_SIZE_USDT = 100.0
MAX_LEVERAGE = 3
LOOP_INTERVAL_SEC = 60
WALLETS_QUERY = """
    SELECT p.wallet_id,
           w.address AS wallet_address,
           COUNT(*) FILTER (WHERE p.pnl > 0)::float
               / NULLIF(COUNT(*), 0) AS win_rate
    FROM positions p
    LEFT JOIN wallets w ON w.id = p.wallet_id
    WHERE p.closed_at IS NOT NULL
    GROUP BY p.wallet_id, w.address
    HAVING COUNT(*) >= 10
       AND COUNT(*) FILTER (WHERE p.pnl > 0)::float / NULLIF(COUNT(*), 0) > 0.6
    ORDER BY win_rate DESC
    LIMIT 3
"""


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _current_btc_price() -> float | None:
    """Fresh BTCUSDT mark only; missing/stale → None (agent skip, not risk authority)."""
    try:
        return price_feed.require_fresh_mark(SYMBOL).price
    except price_feed.MarkMissing:
        return None
    except price_feed.MarkStale:
        return None


async def tick(pool: asyncpg.Pool) -> None:
    """One agent cycle: fund, monitor, then open without holding conn across delay.

    Connection is released before ``place_order`` so ``EXECUTION_DELAY_SEC``
    cannot starve the pool.
    """
    async with pool.acquire() as conn:
        await initialize_paper_allocations(conn)
        await position_manager.tick(conn)

        try:
            quote = price_feed.require_fresh_mark(SYMBOL)
        except price_feed.MarkMissing:
            _log(f"no {SYMBOL} mark yet; skipping open path")
            return
        except price_feed.MarkStale as exc:
            _log(
                f"stale {SYMBOL} mark age_sec={exc.age_sec:.3f} "
                f"max_age_sec={exc.max_age_sec:.3f}; skipping open path "
                "(agent-level; authoritative deny is risk/place_order)"
            )
            return

        price = quote.price
        _log(
            f"{SYMBOL} price={price} ts={quote.ts_utc.isoformat()} "
            f"source={quote.source}"
        )

        wallets = await conn.fetch(WALLETS_QUERY)
        _log(f"fetched {len(wallets)} wallet(s) with win_rate > 0.6")
        wallet_rows = [dict(w) for w in wallets]

    # Pool connection released before delayed opens.
    for wallet in wallet_rows:
        side = random.choice(["BUY", "SELL"])
        wallet_uuid = wallet["wallet_id"]
        wallet_addr = wallet["wallet_address"] or "?"
        _log(
            f"signal wallet_id={wallet_uuid} address={wallet_addr} "
            f"win_rate={wallet['win_rate']} side={side}"
        )

        # Soft compatibility pre-check only. Authoritative kill/risk runs
        # inside place_order (cannot be bypassed by calling place_order directly).
        if await check_daily_loss():
            _log(
                "compatibility daily_stats loss pre-check hit; "
                "place_order still applies equity Risk Engine"
            )

        notional = POSITION_SIZE_USDT * MAX_LEVERAGE
        qty = notional / price
        try:
            # conn=None: place_order sleeps then acquires its own connection.
            order_id = await place_order(
                SYMBOL,
                side,
                qty,
                price,
                wallet_id=wallet_uuid,
                leverage=MAX_LEVERAGE,
            )
        except KillSwitchActive as exc:
            _log(f"kill switch active; halting further opens: {exc}")
            break
        except RiskDenied as exc:
            _log(
                f"risk engine denied open wallet_id={wallet_uuid}: {exc}; "
                f"skipping order"
            )
            continue
        except InsufficientAvailableBalance as exc:
            _log(
                f"insufficient available balance wallet_id={wallet_uuid}: "
                f"{exc}; skipping order"
            )
            continue
        _log(
            f"placed paper order id={order_id} {side} qty={qty:.8f} "
            f"{SYMBOL} @ {price} (margin={POSITION_SIZE_USDT} "
            f"leverage={MAX_LEVERAGE}x) wallet_id={wallet_uuid}"
        )


async def run() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required")

    price_feed.start()
    _log("price_feed started")

    pool = await asyncpg.create_pool(database_url)
    _log("connected to PostgreSQL")
    try:
        while True:
            try:
                await tick(pool)
            except Exception as exc:
                _log(f"tick error: {exc}")
            await asyncio.sleep(LOOP_INTERVAL_SEC)
    finally:
        await pool.close()
        _log("pool closed")


if __name__ == "__main__":
    asyncio.run(run())
