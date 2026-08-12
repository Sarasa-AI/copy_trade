"""Phase 2 — wallet identity consistency (UUID canonical id)."""

from __future__ import annotations

import os
import uuid
from uuid import UUID

import asyncpg
import pytest

# Place orders immediately in tests
os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import ensure_funded_wallet  # noqa: E402
from mock_exchange import place_order  # noqa: E402
from wallet_repository import (  # noqa: E402
    InvalidWalletId,
    WalletNotFound,
    create_wallet,
    get_or_create_wallet,
    get_wallet_by_address,
    get_wallet_by_id,
    parse_wallet_id,
)
from db_isolation import reset_critical_db_state  # noqa: E402


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "copytrading")
    if host == "postgres":
        host = "localhost"
    if not user or not password:
        pytest.skip("DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD required")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@pytest.fixture
async def conn():
    db_url = _database_url()
    connection = await asyncpg.connect(db_url)
    try:
        await reset_critical_db_state(connection)
        yield connection
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_create_wallet_returns_uuid(conn: asyncpg.Connection):
    address = f"phase2_create_{uuid.uuid4().hex[:12]}"
    row = await create_wallet(conn, address, win_rate=0.55)
    assert isinstance(row["id"], UUID)
    assert row["address"] == address
    fetched = await get_wallet_by_id(conn, row["id"])
    assert fetched is not None
    assert fetched["id"] == row["id"]


@pytest.mark.asyncio
async def test_get_or_create_resolves_address_to_uuid(conn: asyncpg.Connection):
    address = f"phase2_resolve_{uuid.uuid4().hex[:12]}"
    first = await get_or_create_wallet(conn, address)
    second = await get_or_create_wallet(conn, address)
    assert first["id"] == second["id"]
    by_addr = await get_wallet_by_address(conn, address)
    assert by_addr is not None
    assert by_addr["id"] == first["id"]


@pytest.mark.asyncio
async def test_place_order_valid_wallet_uuid(conn: asyncpg.Connection):
    address = f"phase2_order_{uuid.uuid4().hex[:12]}"
    wallet = await create_wallet(conn, address)
    await ensure_funded_wallet(conn, wallet["id"], amount=1000.0)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    assert order_id is not None

    order = await conn.fetchrow(
        "SELECT wallet_id, pg_typeof(wallet_id)::text AS t FROM paper_orders WHERE id = $1",
        order_id,
    )
    assert order is not None
    assert order["wallet_id"] == wallet["id"]
    assert order["t"] == "uuid"

    pos = await conn.fetchrow(
        """
        SELECT wallet_id, pg_typeof(wallet_id)::text AS t
        FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC
        LIMIT 1
        """,
        wallet["id"],
    )
    assert pos is not None
    assert pos["wallet_id"] == wallet["id"]
    assert pos["t"] == "uuid"


@pytest.mark.asyncio
async def test_place_order_invalid_wallet_id_string(conn: asyncpg.Connection):
    with pytest.raises(InvalidWalletId):
        await place_order(
            "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id="not-a-uuid"
        )


@pytest.mark.asyncio
async def test_place_order_sentinel_unknown_rejected(conn: asyncpg.Connection):
    with pytest.raises(InvalidWalletId):
        await place_order(
            "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id="unknown"
        )


@pytest.mark.asyncio
async def test_place_order_nonexistent_uuid(conn: asyncpg.Connection):
    missing = uuid.uuid4()
    with pytest.raises(WalletNotFound) as exc_info:
        await place_order(
            "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=missing
        )
    assert exc_info.value.wallet_id == missing


@pytest.mark.asyncio
async def test_open_position_valid_wallet_uuid(conn: asyncpg.Connection):
    address = f"phase2_pos_{uuid.uuid4().hex[:12]}"
    wallet = await create_wallet(conn, address)
    await ensure_funded_wallet(conn, wallet["id"], amount=1000.0)
    await place_order(
        "BTCUSDT", "SELL", 0.005, 65100.0, conn, wallet_id=str(wallet["id"])
    )
    row = await conn.fetchrow(
        """
        SELECT id, wallet_id FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
        """,
        wallet["id"],
    )
    assert row is not None
    assert row["wallet_id"] == wallet["id"]


@pytest.mark.asyncio
async def test_fk_integrity_orphan_impossible_via_api(conn: asyncpg.Connection):
    orphan = uuid.uuid4()
    with pytest.raises(WalletNotFound):
        await place_order(
            "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=orphan
        )
    count = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE wallet_id = $1", orphan
    )
    assert count == 0
    count_o = await conn.fetchval(
        "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1", orphan
    )
    assert count_o == 0


@pytest.mark.asyncio
async def test_two_wallets_data_separated_by_uuid(conn: asyncpg.Connection):
    w1 = await create_wallet(conn, f"phase2_a_{uuid.uuid4().hex[:12]}")
    w2 = await create_wallet(conn, f"phase2_b_{uuid.uuid4().hex[:12]}")
    assert w1["id"] != w2["id"]
    await ensure_funded_wallet(conn, w1["id"], amount=5000.0)
    await ensure_funded_wallet(conn, w2["id"], amount=5000.0)

    await place_order(
        "BTCUSDT", "BUY", 0.01, 64000.0, conn, wallet_id=w1["id"]
    )
    await place_order(
        "BTCUSDT", "BUY", 0.02, 64100.0, conn, wallet_id=w2["id"]
    )

    p1 = await conn.fetchrow(
        """
        SELECT qty FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
        """,
        w1["id"],
    )
    p2 = await conn.fetchrow(
        """
        SELECT qty FROM positions
        WHERE wallet_id = $1 AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
        """,
        w2["id"],
    )
    assert p1 is not None and p2 is not None
    assert float(p1["qty"]) == pytest.approx(0.01)
    assert float(p2["qty"]) == pytest.approx(0.02)


@pytest.mark.asyncio
async def test_no_sentinel_string_in_wallet_id_column(conn: asyncpg.Connection):
    address = f"phase2_nosent_{uuid.uuid4().hex[:12]}"
    wallet = await create_wallet(conn, address)
    await ensure_funded_wallet(conn, wallet["id"], amount=1000.0)
    order_id = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=wallet["id"]
    )
    stored = await conn.fetchval(
        "SELECT wallet_id::text FROM paper_orders WHERE id = $1", order_id
    )
    assert stored == str(wallet["id"])
    assert stored not in ("unknown", "race_test_wallet", "test_wallet", address)
    # Column type remains uuid
    typ = await conn.fetchval(
        """
        SELECT data_type FROM information_schema.columns
        WHERE table_name = 'paper_orders' AND column_name = 'wallet_id'
        """
    )
    assert typ == "uuid"


def test_parse_wallet_id_accepts_uuid_and_str():
    u = uuid.uuid4()
    assert parse_wallet_id(u) == u
    assert parse_wallet_id(str(u)) == u
    with pytest.raises(InvalidWalletId):
        parse_wallet_id("race_test_wallet")
