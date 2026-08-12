"""Smoke test: seed wallets, place paper orders, print positions and daily PnL."""

from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import asyncpg

from mock_exchange import check_daily_loss, close_position, get_position, place_order
from wallet_repository import get_or_create_wallet

ORDERS: list[tuple[str, str, float, float]] = [
    ("BTCUSDT", "BUY", 0.01, 65000.0),
    ("BTCUSDT", "SELL", 0.005, 65200.0),
    ("BTCUSDT", "BUY", 0.02, 64800.0),
    ("BTCUSDT", "BUY", 0.01, 65100.0),
    ("BTCUSDT", "SELL", 0.01, 65500.0),
]

WALLET_ADDRESSES = ("mock_wallet_050", "mock_wallet_065", "mock_wallet_075")
WALLET_WIN_RATES = (0.50, 0.65, 0.75)


async def insert_mock_wallets(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    rows: list[asyncpg.Record] = []
    for address, win_rate in zip(WALLET_ADDRESSES, WALLET_WIN_RATES):
        row = await get_or_create_wallet(conn, address, win_rate=win_rate)
        # Refresh win_rate if wallet already existed
        await conn.execute(
            """
            UPDATE wallets
            SET win_rate = $2, last_updated = NOW()
            WHERE id = $1
            """,
            row["id"],
            win_rate,
        )
        refreshed = await conn.fetchrow(
            """
            SELECT id, address, win_rate, total_trades, last_updated
            FROM wallets WHERE id = $1
            """,
            row["id"],
        )
        assert refreshed is not None
        rows.append(refreshed)
    return rows


async def fetch_open_positions(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT id, symbol, entry_price, qty, pnl, wallet_id, side, opened_at
        FROM positions
        WHERE closed_at IS NULL
        ORDER BY opened_at
        """
    )


async def fetch_position_by_id(
    conn: asyncpg.Connection, position_id: Any
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT id, symbol, entry_price, qty, pnl, wallet_id, side,
               exit_price, close_reason, opened_at, closed_at
        FROM positions
        WHERE id = $1
        """,
        position_id,
    )


async def fetch_daily_pnl(conn: asyncpg.Connection) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT date, total_pnl, trade_count, max_drawdown
        FROM daily_stats
        WHERE date = CURRENT_DATE
        """
    )


def _print_record(label: str, row: Any) -> None:
    if row is None:
        print(f"  {label}: None")
        return
    if hasattr(row, "items"):
        print(f"  {label}: {dict(row)}")
    else:
        print(f"  {label}: {row}")


def _print_daily(daily: asyncpg.Record | None) -> None:
    if daily is None:
        print("  no daily_stats row yet (total_pnl=0)")
    else:
        print(
            f"  date={daily['date']} total_pnl={daily['total_pnl']} "
            f"trade_count={daily['trade_count']} "
            f"max_drawdown={daily['max_drawdown']}"
        )


async def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL environment variable is required")

    conn = await asyncpg.connect(database_url)
    try:
        wallets = await insert_mock_wallets(conn)
        print("Wallets:")
        wallet_ids: list[UUID] = []
        for row in wallets:
            wallet_ids.append(row["id"])
            print(
                f"  id={row['id']} address={row['address']} "
                f"win_rate={row['win_rate']}"
            )

        print("\nOrders:")
        for i, (symbol, side, qty, price) in enumerate(ORDERS):
            wallet_uuid = wallet_ids[i % len(wallet_ids)]
            address = wallets[i % len(wallets)]["address"]
            order_id = await place_order(
                symbol, side, qty, price, conn, wallet_id=wallet_uuid
            )
            print(
                f"  {side} {qty} {symbol} @ {price} "
                f"wallet_id={wallet_uuid} address={address} -> order_id={order_id}"
            )

        print("\nOpen positions (query):")
        positions = await fetch_open_positions(conn)
        if not positions:
            print("  (none)")
        else:
            for row in positions:
                print(
                    f"  id={row['id']} {row['symbol']} entry={row['entry_price']} "
                    f"qty={row['qty']} pnl={row['pnl']} wallet_id={row['wallet_id']}"
                )

        print("\nPosition via get_position(BTCUSDT, wallet_id=...):")
        sample_wallet = positions[0]["wallet_id"] if positions else None
        if sample_wallet is not None:
            _print_record(
                "BTCUSDT",
                await get_position("BTCUSDT", conn, wallet_id=sample_wallet),
            )
        else:
            print("  (no open positions)")

        print("\nDaily PnL (before close):")
        _print_daily(await fetch_daily_pnl(conn))

        print("\n--- Phase 2: close one open position ---")
        if not positions:
            raise SystemExit("no open positions to close")

        position_id = positions[0]["id"]
        result = await close_position(
            position_id=position_id,
            exit_price=65600.0,
            conn=conn,
            close_reason="ADMIN",
        )
        print(f"  closed position_id={position_id}")
        print(f"  realized PnL={result}")

        closed_row = await fetch_position_by_id(conn, position_id)
        _print_record("closed row", closed_row)
        if closed_row is None or closed_row["closed_at"] is None:
            print("  FAIL: position not marked closed")
        elif closed_row["exit_price"] is None:
            print("  FAIL: exit_price not persisted")
        elif closed_row["close_reason"] != "ADMIN":
            print(f"  FAIL: close_reason={closed_row['close_reason']!r}")
        else:
            print(
                "  OK: position marked closed "
                "(closed_at, exit_price, close_reason set)"
            )

        still_open = await fetch_open_positions(conn)
        open_ids = {row["id"] for row in still_open}
        if position_id in open_ids:
            print("  FAIL: closed position still in open list")
        else:
            print(
                f"  OK: closed position removed from open list "
                f"({len(still_open)} open remaining)"
            )

        loss_hit = await check_daily_loss(conn)
        print(f"\ncheck_daily_loss() -> {loss_hit}")

        print("\nDaily stats (today, after close / ON CONFLICT upsert):")
        _print_daily(await fetch_daily_pnl(conn))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
