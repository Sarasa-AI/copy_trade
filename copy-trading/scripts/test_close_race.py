"""Concurrent close_position race test: only one close should succeed."""

from __future__ import annotations

import asyncio
import os
import sys

# Ensure place_order does not sleep during this test
os.environ["EXECUTION_DELAY_SEC"] = "0"

from mock_exchange import (  # noqa: E402
    PositionAlreadyClosed,
    close_pool,
    close_position,
    place_order,
)
from wallet_repository import get_or_create_wallet  # noqa: E402
import asyncpg  # noqa: E402


async def _open_position(conn: asyncpg.Connection) -> str:
    wallet = await get_or_create_wallet(conn, "race_test_wallet")
    wallet_uuid = wallet["id"]
    await place_order(
        "BTCUSDT",
        "BUY",
        0.01,
        65000.0,
        conn,
        wallet_id=wallet_uuid,
    )
    row = await conn.fetchrow(
        """
        SELECT id
        FROM positions
        WHERE closed_at IS NULL AND wallet_id = $1
        ORDER BY opened_at DESC
        LIMIT 1
        """,
        wallet_uuid,
    )
    if row is None:
        raise RuntimeError("failed to open position for race test")
    return str(row["id"])


async def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL environment variable is required")

    conn = await asyncpg.connect(database_url)
    try:
        position_id = await _open_position(conn)
        print(f"opened position_id={position_id}")

        # Two concurrent closes on separate pool connections (conn=None)
        results = await asyncio.gather(
            close_position(position_id, exit_price=65600.0, close_reason="ADMIN"),
            close_position(position_id, exit_price=65600.0, close_reason="ADMIN"),
            return_exceptions=True,
        )

        successes = [r for r in results if isinstance(r, float)]
        already_closed = [
            r for r in results if isinstance(r, PositionAlreadyClosed)
        ]
        other_errors = [
            r
            for r in results
            if isinstance(r, BaseException)
            and not isinstance(r, PositionAlreadyClosed)
        ]

        print(f"results={results!r}")
        for err in already_closed:
            print(f"already_closed payload={err.to_dict()}")

        if other_errors:
            print(f"FAIL: unexpected errors: {other_errors}")
            sys.exit(1)
        if len(successes) != 1:
            print(f"FAIL: expected exactly 1 success, got {len(successes)}")
            sys.exit(1)
        if len(already_closed) != 1:
            print(
                f"FAIL: expected exactly 1 position_already_closed, "
                f"got {len(already_closed)}"
            )
            sys.exit(1)

        print("PASS: exactly one close succeeded; other got position_already_closed")
    finally:
        await conn.close()
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
