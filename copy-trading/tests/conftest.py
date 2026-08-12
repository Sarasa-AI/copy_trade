"""Pytest bootstrap: load .env and isolate integration tests from live paper DB.

P7-001 policy:
- Default: session uses a dedicated DB ``{POSTGRES_DB}_test`` (or
  ``TEST_DATABASE_URL`` / ``TEST_DATABASE_NAME`` when set), recreated each session.
- Function-scoped TRUNCATE reset via ``db_isolation.reset_critical_db_state``.
- Live paper DB is forbidden unless BOTH ``PYTEST_USE_LIVE_DB=1`` and
  ``PYTEST_ALLOW_LIVE_DB=1`` (and never under ``CI=1`` / ``PHASE7_VERIFY=1``).
- Migration tests keep creating their own ephemeral databases.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote_plus, urlparse, urlunparse

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

# Ensure ``import db_isolation`` works when pytest root is the project.
_TESTS_DIR = Path(__file__).resolve().parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

from db_isolation import (  # noqa: E402
    database_name_from_url,
    live_db_escape_hatch_allowed,
    store_live_fingerprint,
)


def _load_dotenv() -> None:
    if not ENV_PATH.is_file():
        return
    for raw in ENV_PATH.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)
    if os.environ.get("POSTGRES_HOST") == "postgres":
        os.environ["POSTGRES_HOST"] = "localhost"


def _host_port() -> tuple[str, str]:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    if host == "postgres":
        host = "localhost"
    return host, port


def _live_database_url() -> str | None:
    explicit = os.environ.get("DATABASE_URL", "").strip()
    if explicit:
        # During pytest_configure before isolation, DATABASE_URL may already
        # point at live; treat POSTGRES_* as the live name source of truth.
        pass
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    db = os.environ.get("POSTGRES_DB", "copytrading")
    # Prefer original live name if already stashed.
    live_name = os.environ.get("LIVE_POSTGRES_DB", "").strip() or db
    if not user or not password:
        if explicit:
            return explicit
        return None
    host, port = _host_port()
    return f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{live_name}"


def _replace_db_name(url: str, dbname: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{dbname}"))


def _admin_psycopg_connect():
    """Connect to admin DB with a role that can CREATE DATABASE."""
    import psycopg2

    host, port = _host_port()
    admin_db = os.environ.get("POSTGRES_ADMIN_DB", "postgres")
    try:
        conn = psycopg2.connect(dbname=admin_db, host=host, port=port)
        return conn
    except Exception:
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        if not user or not password:
            raise
        return psycopg2.connect(
            dbname=admin_db,
            host=host,
            port=port,
            user=user,
            password=password,
        )


def _ensure_test_database(dbname: str, owner: str | None) -> None:
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    conn = _admin_psycopg_connect()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (dbname,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
            if owner:
                cur.execute(
                    f'CREATE DATABASE "{dbname}" OWNER "{owner}"'
                )
            else:
                cur.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        conn.close()


def _alembic_upgrade(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["MIGRATE_ON_HOST"] = "1"
    # Prevent env.py from rebuilding a different URL from POSTGRES_*.
    env.pop("POSTGRES_HOST", None)
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "alembic upgrade head failed for test database:\n"
            f"{result.stdout}\n{result.stderr}"
        )


def _snapshot_live_paper_fingerprint(live_url: str, live_name: str) -> None:
    """Record live paper wallet/open counts before tests mutate the test DB."""
    import psycopg2

    try:
        conn = psycopg2.connect(live_url)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM wallets")
                wallets = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT COUNT(*) FROM positions WHERE closed_at IS NULL"
                )
                opens = int(cur.fetchone()[0])
            store_live_fingerprint(
                {
                    "db_name": live_name,
                    "wallet_count": wallets,
                    "open_position_count": opens,
                }
            )
            os.environ.pop("P7_LIVE_PAPER_FINGERPRINT_ERROR", None)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — fingerprint is best-effort gate aid
        os.environ["P7_LIVE_PAPER_FINGERPRINT_ERROR"] = str(exc)


def _isolate_to_test_database() -> str:
    """Create a fresh session test DB and redirect DATABASE_URL / POSTGRES_DB."""
    # Resolve live name from POSTGRES_DB before we overwrite it.
    base_db = os.environ.get("POSTGRES_DB", "copytrading")
    # If DATABASE_URL already set in env to live, prefer POSTGRES_DB as live name.
    os.environ.setdefault("LIVE_POSTGRES_DB", base_db)
    live_name = os.environ["LIVE_POSTGRES_DB"]

    if live_db_escape_hatch_allowed():
        live = _live_database_url()
        if not live:
            raise RuntimeError(
                "PYTEST_USE_LIVE_DB + PYTEST_ALLOW_LIVE_DB set but "
                "DATABASE_URL / POSTGRES_* missing"
            )
        # Point at live paper (operators only; forbidden under CI/verify).
        os.environ["DATABASE_URL"] = (
            live
            if database_name_from_url(live) == live_name
            else _replace_db_name(live, live_name)
        )
        os.environ["POSTGRES_DB"] = live_name
        return os.environ["DATABASE_URL"]

    # Any attempt to use live without the double confirmation fails hard.
    if os.environ.get("PYTEST_USE_LIVE_DB", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        raise RuntimeError(
            "PYTEST_USE_LIVE_DB=1 requires PYTEST_ALLOW_LIVE_DB=1 as a second "
            "explicit confirmation. Under CI=1 or PHASE7_VERIFY=1 live DB is "
            "always forbidden."
        )

    explicit_test = os.environ.get("TEST_DATABASE_URL", "").strip()
    if explicit_test:
        test_name = database_name_from_url(explicit_test) or "copytrading_test"
        if test_name == live_name:
            raise RuntimeError(
                f"TEST_DATABASE_URL must not target live paper DB {live_name!r}"
            )
        _alembic_upgrade(explicit_test)
        os.environ["DATABASE_URL"] = explicit_test
        os.environ["POSTGRES_DB"] = test_name
        live_url = _live_database_url()
        if live_url:
            _snapshot_live_paper_fingerprint(live_url, live_name)
        return explicit_test

    live = _live_database_url()
    if not live:
        # No DB configured — unit/migration skips handle themselves.
        return ""

    # Snapshot live fingerprint before redirecting env to test DB.
    _snapshot_live_paper_fingerprint(live, live_name)

    test_name = os.environ.get("TEST_DATABASE_NAME", f"{base_db}_test")
    if test_name == live_name:
        raise RuntimeError(
            f"TEST_DATABASE_NAME {test_name!r} collides with live paper DB"
        )
    owner = os.environ.get("POSTGRES_USER")
    _ensure_test_database(test_name, owner=owner)
    test_url = _replace_db_name(live, test_name)
    _alembic_upgrade(test_url)
    os.environ["DATABASE_URL"] = test_url
    os.environ["POSTGRES_DB"] = test_name
    return test_url


def pytest_configure() -> None:
    _load_dotenv()
    try:
        _isolate_to_test_database()
    except Exception as exc:  # noqa: BLE001 — surface clearly at session start
        # Do not silently fall back to live paper DB.
        raise RuntimeError(
            f"Failed to isolate pytest onto a dedicated test database: {exc}"
        ) from exc


def pytest_sessionfinish(session, exitstatus) -> None:
    """Gate: suite must not leave critical unlinked funded stubs."""
    if exitstatus != 0:
        return
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        return
    if live_db_escape_hatch_allowed():
        return

    import psycopg2

    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM positions p
                WHERE p.order_id IS NULL
                  AND p.exchange_order_id IS NULL
                  AND EXISTS (
                      SELECT 1 FROM capital_ledger cl
                      WHERE cl.position_id = p.id
                        AND cl.entry_type = 'RESERVE'
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM phase5_legacy_unlinked_positions l
                      WHERE l.position_id = p.id
                  )
                """
            )
            remaining = int(cur.fetchone()[0])
    finally:
        conn.close()

    if remaining != 0:
        session.exitstatus = 1
        print(
            f"\nERROR: {remaining} unregistered NULL-order_id funded "
            "position(s) remain after the test suite — stub cleanup regression",
            file=sys.stderr,
        )
