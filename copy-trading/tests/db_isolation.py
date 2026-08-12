"""P7-001 — dedicated test-DB isolation helpers (no production bypasses).

All cleanup is TRUNCATE/reseed against the pytest session test database only.
Never targets the live paper database.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import asyncpg

# Mutable trading/capital/risk tables cleared between critical tests.
# Do NOT include risk_control_lock or kill_switch_state (singletons reseeded).
_TRUNCATE_TABLES = (
    "risk_denials",
    "kill_switch_events",
    "equity_sod_snapshots",
    "critical_events",
    "reconciliation_runs",
    "exchange_fills",
    "exchange_orders",
    "capital_ledger",
    "wallet_balances",
    "daily_stats",
    "phase5_legacy_unlinked_orders",
    "phase5_legacy_unlinked_positions",
    "positions",
    "paper_orders",
    "wallets",
    "master_pool",
)

_LIVE_FINGERPRINT_ENV = "P7_LIVE_PAPER_FINGERPRINT"
_LIVE_DB_NAME_ENV = "LIVE_POSTGRES_DB"


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def database_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    return (parsed.path or "").lstrip("/") or ""


def live_database_name() -> str:
    return (
        os.environ.get(_LIVE_DB_NAME_ENV, "").strip()
        or os.environ.get("POSTGRES_DB_LIVE", "").strip()
    )


def current_database_name() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return database_name_from_url(url)
    return os.environ.get("POSTGRES_DB", "").strip()


def live_db_escape_hatch_allowed() -> bool:
    """True only when both operator flags set and not under CI/verify."""
    if _env_truthy("CI") or _env_truthy("PHASE7_VERIFY"):
        return False
    return _env_truthy("PYTEST_USE_LIVE_DB") and _env_truthy("PYTEST_ALLOW_LIVE_DB")


def assert_using_dedicated_test_db() -> None:
    """Raise if the active DATABASE_URL/POSTGRES_DB looks like live paper."""
    live = live_database_name()
    current = current_database_name()
    if not current:
        raise RuntimeError("No DATABASE_URL / POSTGRES_DB configured for tests")
    if live and current == live:
        raise RuntimeError(
            f"Refusing to use live paper database {current!r}; "
            "pytest must use a dedicated *_test DB"
        )
    explicit_test = os.environ.get("TEST_DATABASE_URL", "").strip()
    if explicit_test:
        if database_name_from_url(explicit_test) == live:
            raise RuntimeError(
                f"TEST_DATABASE_URL must not target live paper DB {live!r}"
            )
        return
    if not current.endswith("_test"):
        raise RuntimeError(
            f"Active database {current!r} is not a dedicated test DB "
            "(expected name ending with '_test')"
        )


def assert_reset_allowed() -> None:
    """Destructive TRUNCATE is never allowed against live paper or live hatch."""
    if live_db_escape_hatch_allowed():
        raise RuntimeError(
            "reset_critical_db_state cannot run when PYTEST_USE_LIVE_DB is enabled"
        )
    assert_using_dedicated_test_db()


def fingerprint_from_counts(
    *, wallet_count: int, open_position_count: int, db_name: str
) -> dict[str, Any]:
    return {
        "db_name": db_name,
        "wallet_count": int(wallet_count),
        "open_position_count": int(open_position_count),
    }


def encode_fingerprint(fp: dict[str, Any]) -> str:
    return (
        f"{fp['db_name']}|{fp['wallet_count']}|{fp['open_position_count']}"
    )


def decode_fingerprint(raw: str) -> dict[str, Any]:
    parts = raw.split("|")
    if len(parts) != 3:
        raise ValueError(f"invalid fingerprint encoding: {raw!r}")
    return {
        "db_name": parts[0],
        "wallet_count": int(parts[1]),
        "open_position_count": int(parts[2]),
    }


def store_live_fingerprint(fp: dict[str, Any]) -> None:
    os.environ[_LIVE_FINGERPRINT_ENV] = encode_fingerprint(fp)


def get_stored_live_fingerprint() -> dict[str, Any] | None:
    raw = os.environ.get(_LIVE_FINGERPRINT_ENV, "").strip()
    if not raw:
        return None
    return decode_fingerprint(raw)


async def read_db_fingerprint(conn: asyncpg.Connection, db_name: str) -> dict[str, Any]:
    wallets = int(await conn.fetchval("SELECT COUNT(*) FROM wallets") or 0)
    opens = int(
        await conn.fetchval(
            "SELECT COUNT(*) FROM positions WHERE closed_at IS NULL"
        )
        or 0
    )
    return fingerprint_from_counts(
        wallet_count=wallets, open_position_count=opens, db_name=db_name
    )


async def reset_critical_db_state(conn: asyncpg.Connection) -> None:
    """TRUNCATE mutable state and reseed singletons / master pool.

    Safe only against the dedicated test database.
    """
    assert_reset_allowed()

    tables = ", ".join(_TRUNCATE_TABLES)
    await conn.execute(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE")

    # Preserve / restore control singletons (not truncated).
    await conn.execute(
        """
        INSERT INTO risk_control_lock (id) VALUES (1)
        ON CONFLICT (id) DO NOTHING
        """
    )
    await conn.execute(
        """
        UPDATE kill_switch_state
        SET active = false,
            reason = NULL,
            actor = NULL,
            activated_at = NULL,
            deactivated_at = NULL,
            updated_at = NOW()
        WHERE id = 1
        """
    )
    # If kill_switch_state row somehow missing, re-insert.
    exists = await conn.fetchval(
        "SELECT 1 FROM kill_switch_state WHERE id = 1"
    )
    if exists is None:
        await conn.execute(
            """
            INSERT INTO kill_switch_state (id, active)
            VALUES (1, false)
            """
        )

    # Recreate master pool via capital authority (insert-only path).
    from capital import ensure_master_pool

    await ensure_master_pool(conn)

    # P7-004: seed fresh paper marks so place_order is not denied by default.
    # Tests that need missing/stale marks clear or backdate explicitly.
    from price_feed import clear_all_marks, update_mark

    clear_all_marks()
    update_mark("BTCUSDT", 65000.0, source="pytest_reset")
    update_mark("ETHUSDT", 3500.0, source="pytest_reset")
