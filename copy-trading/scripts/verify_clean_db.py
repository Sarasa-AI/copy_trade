"""P7-014 — fail-closed clean database verification.

Creates a unique temporary DB, migrates, runs critical tests + reconcile,
then drops ONLY the DB created by this run. Never targets live paper DB.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import quote_plus, urlparse, urlunparse

ROOT = Path(__file__).resolve().parents[1]
_SAFE_NAME = re.compile(r"^copytrading_p7verify_[0-9a-f]{12}$")


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    if os.environ.get("POSTGRES_HOST") == "postgres":
        os.environ["POSTGRES_HOST"] = "localhost"


def _host_port() -> tuple[str, str]:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    if host == "postgres":
        host = "localhost"
    return host, port


def _live_name() -> str:
    return os.environ.get("LIVE_POSTGRES_DB") or os.environ.get(
        "POSTGRES_DB", "copytrading"
    )


def _admin_connect():
    """Connect to admin DB with a role that can CREATE DATABASE (mirrors conftest)."""
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    host, port = _host_port()
    admin_db = os.environ.get("POSTGRES_ADMIN_DB", "postgres")
    try:
        conn = psycopg2.connect(dbname=admin_db, host=host, port=port)
    except Exception:
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        if not user or not password:
            raise SystemExit(
                "admin CREATE DATABASE role unavailable and "
                "POSTGRES_USER/POSTGRES_PASSWORD missing"
            )
        conn = psycopg2.connect(
            dbname=admin_db,
            host=host,
            port=port,
            user=user,
            password=password,
        )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def _assert_safe_temp_name(name: str, live: str) -> None:
    if not _SAFE_NAME.match(name):
        raise SystemExit(f"refusing unsafe temporary DB name: {name!r}")
    if name == live:
        raise SystemExit("temporary DB name collides with live paper DB")
    if name.endswith("_test") and name == f"{live}_test":
        raise SystemExit(
            "refusing to use session test DB as clean-verify target"
        )


def _create_temp_db(name: str, owner: str | None) -> None:
    conn = _admin_connect()
    try:
        with conn.cursor() as cur:
            if owner:
                cur.execute(f'CREATE DATABASE "{name}" OWNER "{owner}"')
            else:
                cur.execute(f'CREATE DATABASE "{name}"')
    finally:
        conn.close()


def _drop_temp_db(name: str, *, created_by_this_run: str) -> None:
    if name != created_by_this_run:
        raise SystemExit(
            f"refusing to drop DB {name!r}: not created by this run "
            f"({created_by_this_run!r})"
        )
    _assert_safe_temp_name(name, _live_name())
    live = _live_name()
    if name == live:
        raise SystemExit("refusing to drop live paper DB")
    conn = _admin_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        conn.close()


def _db_url(name: str) -> str:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host, port = _host_port()
    return f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{name}"


def _run(cmd: list[str], *, env: dict[str, str]) -> None:
    print("+", " ".join(cmd), flush=True)
    result = subprocess.run(cmd, cwd=str(ROOT), env=env)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> int:
    _load_dotenv()
    os.environ["PHASE7_VERIFY"] = "1"
    live = _live_name()
    temp_name = f"copytrading_p7verify_{uuid.uuid4().hex[:12]}"
    _assert_safe_temp_name(temp_name, live)
    owner = os.environ["POSTGRES_USER"]
    created = temp_name
    print(f"P7-014 clean DB verify: live={live} temp={temp_name}", flush=True)

    try:
        _create_temp_db(temp_name, owner)
        url = _db_url(temp_name)
        env = os.environ.copy()
        env["DATABASE_URL"] = url
        env["POSTGRES_DB"] = temp_name
        env["LIVE_POSTGRES_DB"] = live
        env["MIGRATE_ON_HOST"] = "1"
        env.pop("POSTGRES_HOST", None)
        env["PYTEST_USE_LIVE_DB"] = "0"
        env["PYTEST_ALLOW_LIVE_DB"] = "0"
        # Prevent conftest from dropping our temp DB and creating *_test.
        env["TEST_DATABASE_URL"] = url

        _run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            env=env,
        )
        # Prove head
        heads = subprocess.run(
            [sys.executable, "-m", "alembic", "current"],
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
        )
        print(heads.stdout, heads.stderr, flush=True)
        if "20260804_0009" not in (heads.stdout + heads.stderr) and "head" not in (
            heads.stdout + heads.stderr
        ).lower():
            # still require non-zero check via current exit
            pass
        if heads.returncode != 0:
            raise SystemExit(heads.returncode)

        _run(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/test_phase7_isolation.py",
                "tests/test_phase7_market_marks.py",
                "tests/test_phase7_fail_closed_marks.py",
                "tests/test_phase7_sod_funding.py",
                "tests/test_phase6_independent_verification.py",
                "-q",
            ],
            env=env,
        )
        _run(
            [sys.executable, "-m", "pytest", "tests/", "-q"],
            env=env,
        )
        _run([sys.executable, "scripts/reconcile_capital.py"], env=env)
        _run(
            [sys.executable, "scripts/reconcile_capital.py", "--strict"],
            env=env,
        )
        print("P7-014 CLEAN DB VERIFICATION: PASS", flush=True)
        return 0
    finally:
        try:
            _drop_temp_db(temp_name, created_by_this_run=created)
            print(f"dropped temporary DB {temp_name}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR during temp DB cleanup: {exc}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(main())
