"""Phase 1 migration tests — fresh DB, legacy upgrade, FK integrity, downgrade."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

ROOT = Path(__file__).resolve().parents[1]
ADMIN_DB = os.environ.get("POSTGRES_ADMIN_DB", "postgres")
VERSIONS_DIR = ROOT / "migrations" / "versions"

_REVISION_RE = re.compile(r'^revision: str = "([^"]+)"', re.M)
_DOWN_REVISION_RE = re.compile(
    r'^down_revision[^=]*=\s*(?:"([^"]+)"|None)', re.M
)


def _revision_chain() -> list[str]:
    """Ordered revision ids from base to head, read from the migration files.

    Derived rather than hardcoded so adding a phase migration does not require
    editing revision literals in these tests. Also asserts a single linear
    chain, which is the property Phase 0 baseline depends on.
    """
    parents: dict[str, str | None] = {}
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        text = path.read_text()
        rev = _REVISION_RE.search(text)
        down = _DOWN_REVISION_RE.search(text)
        if rev is None or down is None:
            raise AssertionError(f"cannot parse revision ids from {path.name}")
        parents[rev.group(1)] = down.group(1)

    children = {down: rev for rev, down in parents.items()}
    bases = [rev for rev, down in parents.items() if down is None]
    heads = [rev for rev in parents if rev not in children]
    assert len(bases) == 1, f"expected one base revision, got {bases}"
    assert len(heads) == 1, f"expected one head revision, got {heads}"

    chain = [bases[0]]
    while chain[-1] in children:
        chain.append(children[chain[-1]])
    assert len(chain) == len(parents), "revision chain is not linear"
    return chain


def _host_port() -> tuple[str, str]:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    if host == "postgres":
        host = "localhost"
    return host, port


def _migrator_dsn(dbname: str) -> str:
    """Prefer local superuser/peer for CREATE DATABASE + migrations in tests.

    App role from .env (trading_user) often lacks CREATEDB; Phase 1 tests use
    the host OS DB role when available, falling back to POSTGRES_* credentials.
    """
    host, port = _host_port()
    explicit = os.environ.get("MIGRATION_TEST_DATABASE_URL")
    if explicit:
        # Replace DB name at end of URL path if needed — expect full URL incl db
        return explicit.rsplit("/", 1)[0] + f"/{dbname}"

    # Try passwordless local role (common on macOS Homebrew Postgres)
    try:
        conn = psycopg2.connect(dbname=ADMIN_DB, host=host, port=port)
        with conn.cursor() as cur:
            cur.execute("SELECT current_user, rolcreatedb FROM pg_roles WHERE rolname = current_user")
            user, createdb = cur.fetchone()
        conn.close()
        if createdb:
            return f"postgresql://{user}@{host}:{port}/{dbname}"
    except Exception:
        pass

    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    if not user or not password:
        pytest.skip("No migrator DB role with CREATEDB available")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def _admin_connect():
    dsn = _migrator_dsn(ADMIN_DB)
    return psycopg2.connect(dsn)


def _connect(dbname: str):
    return psycopg2.connect(_migrator_dsn(dbname))


def _drop_db(name: str) -> None:
    conn = _admin_connect()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
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


def _create_db(name: str) -> None:
    _drop_db(name)
    conn = _admin_connect()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{name}"')
    finally:
        conn.close()


def _alembic(dbname: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = _migrator_dsn(dbname)
    env["MIGRATE_ON_HOST"] = "1"
    # Prevent env.py from rebuilding a different URL from POSTGRES_* 
    env.pop("POSTGRES_HOST", None)
    env.pop("POSTGRES_DB", None)
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _fetchone(dbname: str, sql: str, params=None):
    conn = _connect(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def _execute(dbname: str, sql: str, params=None) -> None:
    conn = _connect(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _wallet_id_type(dbname: str, table: str) -> str:
    row = _fetchone(
        dbname,
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = 'wallet_id'
        """,
        (table,),
    )
    assert row is not None
    return row[0]


def _fk_exists(dbname: str, name: str) -> bool:
    row = _fetchone(
        dbname,
        "SELECT 1 FROM pg_constraint WHERE conname = %s",
        (name,),
    )
    return row is not None


def _index_exists(dbname: str, name: str) -> bool:
    row = _fetchone(
        dbname,
        "SELECT 1 FROM pg_indexes WHERE indexname = %s",
        (name,),
    )
    return row is not None


@pytest.fixture
def fresh_db():
    name = f"ct_mig_fresh_{uuid.uuid4().hex[:8]}"
    _create_db(name)
    yield name
    _drop_db(name)


@pytest.fixture
def legacy_db():
    name = f"ct_mig_legacy_{uuid.uuid4().hex[:8]}"
    _create_db(name)
    # Pre-Phase-1 schema (TEXT wallet_id), with sample rows and no wallets
    _execute(
        name,
        """
        CREATE TABLE wallets (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address TEXT NOT NULL UNIQUE,
            win_rate NUMERIC(7, 4) NOT NULL DEFAULT 0,
            total_trades INTEGER NOT NULL DEFAULT 0,
            last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE paper_orders (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
            qty NUMERIC(18, 8) NOT NULL,
            price NUMERIC(18, 8) NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            wallet_id TEXT NOT NULL DEFAULT 'unknown',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE positions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol TEXT NOT NULL,
            entry_price NUMERIC(18, 8) NOT NULL,
            qty NUMERIC(18, 8) NOT NULL,
            pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
            wallet_id TEXT NOT NULL DEFAULT 'unknown',
            opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            closed_at TIMESTAMPTZ
        );
        CREATE TABLE daily_stats (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            date DATE NOT NULL UNIQUE,
            total_pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
            trade_count INTEGER NOT NULL DEFAULT 0,
            max_drawdown NUMERIC(18, 8) NOT NULL DEFAULT 0
        );
        INSERT INTO paper_orders (symbol, side, qty, price, status, wallet_id)
        VALUES ('BTCUSDT', 'BUY', 0.01, 65000, 'FILLED', 'race_test_wallet');
        INSERT INTO positions (symbol, entry_price, qty, wallet_id)
        VALUES ('BTCUSDT', 65000, 0.01, 'race_test_wallet');
        """,
    )
    yield name
    _drop_db(name)


class TestFreshMigration:
    def test_upgrade_creates_uuid_fks_and_indexes(self, fresh_db):
        result = _alembic(fresh_db, "upgrade", "head")
        assert result.returncode == 0, result.stdout + result.stderr

        assert _wallet_id_type(fresh_db, "positions") == "uuid"
        assert _wallet_id_type(fresh_db, "paper_orders") == "uuid"
        assert _fk_exists(fresh_db, "positions_wallet_id_fkey")
        assert _fk_exists(fresh_db, "paper_orders_wallet_id_fkey")
        assert _index_exists(fresh_db, "idx_positions_wallet_id")
        assert _index_exists(fresh_db, "idx_paper_orders_wallet_id")
        assert _index_exists(fresh_db, "idx_positions_closed_at")

        head = _fetchone(fresh_db, "SELECT version_num FROM alembic_version")
        assert head is not None
        assert head[0] == _revision_chain()[-1]

        assert _fetchone(
            fresh_db,
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_name = 'positions' AND column_name = 'side'
            """,
        )[0] == "NO"
        assert _fk_exists(fresh_db, "positions_side_check")
        assert _fk_exists(fresh_db, "positions_close_reason_check")
        assert _fk_exists(fresh_db, "positions_lifecycle_check")
        assert _fk_exists(fresh_db, "paper_orders_status_check")
        assert _fetchone(
            fresh_db,
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'positions' AND column_name = 'order_id'
            """,
        ) is not None
        for table in (
            "risk_control_lock",
            "equity_sod_snapshots",
            "kill_switch_state",
            "kill_switch_events",
            "risk_denials",
            "reconciliation_runs",
            "critical_events",
        ):
            assert (
                _fetchone(
                    fresh_db,
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                    """,
                    (table,),
                )
                is not None
            ), table
        for col in ("actor", "source", "reason", "correlation_id", "order_id"):
            assert _fetchone(
                fresh_db,
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'capital_ledger' AND column_name = %s
                """,
                (col,),
            ) is not None

    def test_fk_rejects_orphan_wallet(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        orphan = str(uuid.uuid4())
        with pytest.raises(psycopg2.errors.ForeignKeyViolation):
            _execute(
                fresh_db,
                """
                INSERT INTO positions (symbol, entry_price, qty, wallet_id, side)
                VALUES ('BTCUSDT', 1, 1, %s, 'BUY')
                """,
                (orphan,),
            )

    def test_fk_accepts_valid_wallet(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        _execute(
            fresh_db,
            "INSERT INTO wallets (address) VALUES ('agent_a')",
        )
        wallet_id = _fetchone(
            fresh_db, "SELECT id FROM wallets WHERE address = 'agent_a'"
        )[0]
        _execute(
            fresh_db,
            """
            INSERT INTO positions (symbol, entry_price, qty, wallet_id, side)
            VALUES ('BTCUSDT', 1, 1, %s, 'BUY')
            """,
            (wallet_id,),
        )
        count = _fetchone(fresh_db, "SELECT COUNT(*) FROM positions")[0]
        assert count == 1

    def test_downgrade_empty_fresh_drops_or_legacy(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        for _ in range(len(_revision_chain())):
            result = _alembic(fresh_db, "downgrade", "-1")
            assert result.returncode == 0, result.stdout + result.stderr
        # Empty DB path drops tables after full downgrade chain to base
        row = _fetchone(
            fresh_db,
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('wallets', 'positions', 'paper_orders', 'daily_stats')
            """,
        )
        assert row[0] == 0


class TestPhase4Migration:
    def test_upgrade_creates_capital_tables(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        for table in ("master_pool", "wallet_balances", "capital_ledger"):
            row = _fetchone(
                fresh_db,
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            assert row is not None, table
        reserved = _fetchone(
            fresh_db,
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'positions' AND column_name = 'reserved_margin'
            """,
        )
        assert reserved is not None
        pool = _fetchone(
            fresh_db,
            "SELECT total_capital, allocated_capital, available_capital FROM master_pool WHERE id = 1",
        )
        assert pool is not None
        assert float(pool[0]) == pytest.approx(100000.0)
        assert float(pool[1]) == pytest.approx(0.0)
        assert float(pool[2]) == pytest.approx(100000.0)

    def test_migration_allocates_existing_wallets(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "20260728_0003").returncode == 0
        _execute(fresh_db, "INSERT INTO wallets (address) VALUES ('p4_seed_a')")
        _execute(fresh_db, "INSERT INTO wallets (address) VALUES ('p4_seed_b')")
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        count = _fetchone(fresh_db, "SELECT COUNT(*) FROM wallet_balances")[0]
        assert count == 2
        pool = _fetchone(
            fresh_db,
            "SELECT allocated_capital, available_capital FROM master_pool WHERE id = 1",
        )
        assert float(pool[0]) == pytest.approx(20000.0)
        assert float(pool[1]) == pytest.approx(80000.0)


class TestPhase45Migration:
    def test_upgrade_adds_integrity_constraints(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        for index in (
            "uq_capital_ledger_reserve_position",
            "uq_capital_ledger_release_position",
            "uq_capital_ledger_realize_position",
        ):
            row = _fetchone(
                fresh_db,
                """
                SELECT 1 FROM pg_indexes
                WHERE schemaname = 'public' AND indexname = %s
                """,
                (index,),
            )
            assert row is not None, index

        for constraint in (
            "wallet_balances_conservation",
            "wallet_balances_equity",
        ):
            row = _fetchone(
                fresh_db,
                """
                SELECT 1 FROM pg_constraint
                WHERE conname = %s
                """,
                (constraint,),
            )
            assert row is not None, constraint

    def test_conservation_check_rejects_invalid_row(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        _execute(fresh_db, "INSERT INTO wallets (address) VALUES ('p45_bad')")
        wallet_id = _fetchone(
            fresh_db, "SELECT id FROM wallets WHERE address = 'p45_bad'"
        )[0]
        with pytest.raises(psycopg2.errors.CheckViolation):
            _execute(
                fresh_db,
                """
                INSERT INTO wallet_balances (
                    wallet_id, initial_capital, current_equity,
                    available_balance, reserved_margin, unrealized_pnl, realized_pnl
                )
                VALUES (%s, 100, 100, 50, 0, 0, 0)
                """,
                (wallet_id,),
            )

    def test_duplicate_reserve_ledger_rejected(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        _execute(fresh_db, "INSERT INTO wallets (address) VALUES ('p45_dup')")
        wallet_id = _fetchone(
            fresh_db, "SELECT id FROM wallets WHERE address = 'p45_dup'"
        )[0]
        _execute(
            fresh_db,
            """
            INSERT INTO positions (symbol, entry_price, qty, wallet_id, side)
            VALUES ('BTCUSDT', 1, 1, %s, 'BUY')
            """,
            (wallet_id,),
        )
        position_id = _fetchone(
            fresh_db,
            "SELECT id FROM positions WHERE wallet_id = %s",
            (wallet_id,),
        )[0]
        _execute(
            fresh_db,
            """
            INSERT INTO capital_ledger (wallet_id, entry_type, amount, position_id)
            VALUES (%s, 'RESERVE', 1, %s)
            """,
            (wallet_id, position_id),
        )
        with pytest.raises(psycopg2.errors.UniqueViolation):
            _execute(
                fresh_db,
                """
                INSERT INTO capital_ledger
                    (wallet_id, entry_type, amount, position_id)
                VALUES (%s, 'RESERVE', 1, %s)
                """,
                (wallet_id, position_id),
            )

    def test_downgrade_removes_phase45_objects(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        assert _alembic(fresh_db, "downgrade", "20260728_0004").returncode == 0
        row = _fetchone(
            fresh_db,
            """
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'uq_capital_ledger_reserve_position'
            """,
        )
        assert row is None
        row = _fetchone(
            fresh_db,
            """
            SELECT 1 FROM pg_constraint
            WHERE conname = 'wallet_balances_conservation'
            """,
        )
        assert row is None


class TestLegacyMigration:
    def test_preserves_rows_and_maps_address(self, legacy_db):
        before_pos = _fetchone(legacy_db, "SELECT COUNT(*) FROM positions")[0]
        before_ord = _fetchone(legacy_db, "SELECT COUNT(*) FROM paper_orders")[0]
        assert before_pos == 1
        assert before_ord == 1
        assert _wallet_id_type(legacy_db, "positions") == "text"

        result = _alembic(legacy_db, "upgrade", "head")
        assert result.returncode == 0, result.stdout + result.stderr

        assert _wallet_id_type(legacy_db, "positions") == "uuid"
        assert _fk_exists(legacy_db, "positions_wallet_id_fkey")
        assert _fk_exists(legacy_db, "paper_orders_wallet_id_fkey")

        after_pos = _fetchone(legacy_db, "SELECT COUNT(*) FROM positions")[0]
        after_ord = _fetchone(legacy_db, "SELECT COUNT(*) FROM paper_orders")[0]
        assert after_pos == before_pos
        assert after_ord == before_ord

        mapped = _fetchone(
            legacy_db,
            """
            SELECT w.address
            FROM positions p
            JOIN wallets w ON w.id = p.wallet_id
            """,
        )
        assert mapped[0] == "race_test_wallet"

        # unknown wallet also ensured for future defaults path
        unknown = _fetchone(
            legacy_db,
            "SELECT COUNT(*) FROM wallets WHERE address = 'unknown'",
        )[0]
        assert unknown == 1

    def test_downgrade_restores_text_addresses(self, legacy_db):
        assert _alembic(legacy_db, "upgrade", "head").returncode == 0
        assert _alembic(legacy_db, "downgrade", "20260724_0001").returncode == 0
        result = _alembic(legacy_db, "downgrade", "-1")
        assert result.returncode == 0, result.stdout + result.stderr
        assert _wallet_id_type(legacy_db, "positions") == "text"
        addr = _fetchone(legacy_db, "SELECT wallet_id FROM positions")[0]
        assert addr == "race_test_wallet"
        assert not _fk_exists(legacy_db, "positions_wallet_id_fkey")


OPEN_WALLET = "11111111-1111-1111-1111-111111111111"
CLOSED_WALLET = "22222222-2222-2222-2222-222222222222"


@pytest.fixture
def phase3_legacy_db():
    """Phase 1 head DB with open + closed positions for side/metadata backfill."""
    name = f"ct_mig_p3_{uuid.uuid4().hex[:8]}"
    _create_db(name)
    assert _alembic(name, "upgrade", "20260724_0001").returncode == 0
    _execute(
        name,
        """
        INSERT INTO wallets (id, address) VALUES
            (%s, 'p3_open_wallet'),
            (%s, 'p3_closed_wallet');
        INSERT INTO paper_orders (symbol, side, qty, price, status, wallet_id)
        VALUES
            ('BTCUSDT', 'SELL', 0.01, 65000, 'FILLED', %s),
            ('ETHUSDT', 'BUY', 0.02, 3000, 'FILLED', %s);
        INSERT INTO positions (symbol, entry_price, qty, wallet_id, closed_at, pnl)
        VALUES
            ('BTCUSDT', 65000, 0.01, %s, NULL, 0),
            ('ETHUSDT', 3000, 0.02, %s, NOW(), 10);
        """,
        (OPEN_WALLET, CLOSED_WALLET, OPEN_WALLET, CLOSED_WALLET, OPEN_WALLET, CLOSED_WALLET),
    )
    yield name
    _drop_db(name)


@pytest.fixture
def phase3_ambiguous_db():
    """Phase 1 head DB where side cannot be mapped (more positions than orders)."""
    name = f"ct_mig_p3_amb_{uuid.uuid4().hex[:8]}"
    _create_db(name)
    assert _alembic(name, "upgrade", "20260724_0001").returncode == 0
    wallet = str(uuid.uuid4())
    _execute(
        name,
        """
        INSERT INTO wallets (id, address) VALUES (%s, 'p3_ambiguous');
        INSERT INTO paper_orders (symbol, side, qty, price, status, wallet_id)
        VALUES ('BTCUSDT', 'BUY', 0.01, 65000, 'FILLED', %s);
        INSERT INTO positions (symbol, entry_price, qty, wallet_id)
        VALUES
            ('BTCUSDT', 65000, 0.01, %s),
            ('BTCUSDT', 65000, 0.01, %s);
        """,
        (wallet, wallet, wallet, wallet),
    )
    yield name
    _drop_db(name)


class TestPhase3Migration:
    def test_upgrade_adds_lifecycle_columns(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        for col in (
            "side",
            "exit_price",
            "close_reason",
            "stop_loss_price",
            "take_profit_price",
        ):
            row = _fetchone(
                fresh_db,
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'positions' AND column_name = %s
                """,
                (col,),
            )
            assert row is not None

    def test_legacy_upgrade_preserves_and_backfills(self, phase3_legacy_db):
        before = _fetchone(phase3_legacy_db, "SELECT COUNT(*) FROM positions")[0]
        assert before == 2

        result = _alembic(phase3_legacy_db, "upgrade", "head")
        assert result.returncode == 0, result.stdout + result.stderr

        after = _fetchone(phase3_legacy_db, "SELECT COUNT(*) FROM positions")[0]
        assert after == before

        open_row = _fetchone(
            phase3_legacy_db,
            """
            SELECT side, exit_price, close_reason, closed_at
            FROM positions
            WHERE wallet_id = %s
            """,
            (OPEN_WALLET,),
        )
        assert open_row[0] == "SELL"
        assert open_row[1] is None
        assert open_row[2] is None
        assert open_row[3] is None

        closed_row = _fetchone(
            phase3_legacy_db,
            """
            SELECT side, exit_price, close_reason, closed_at, pnl
            FROM positions
            WHERE wallet_id = %s
            """,
            (CLOSED_WALLET,),
        )
        assert closed_row[0] == "BUY"
        assert closed_row[2] == "ADMIN"
        assert closed_row[3] is not None
        assert float(closed_row[1]) == pytest.approx(3500.0)
        assert float(closed_row[4]) == pytest.approx(10.0)

    def test_ambiguous_side_fails_migration(self, phase3_ambiguous_db):
        result = _alembic(phase3_ambiguous_db, "upgrade", "head")
        assert result.returncode != 0
        combined = result.stdout + result.stderr
        assert "side backfill" in combined.lower() or "runtimeerror" in combined.lower()

    def test_lifecycle_check_rejects_invalid_rows(self, fresh_db):
        assert _alembic(fresh_db, "upgrade", "head").returncode == 0
        _execute(fresh_db, "INSERT INTO wallets (address) VALUES ('lc_wallet')")
        wallet_id = _fetchone(
            fresh_db, "SELECT id FROM wallets WHERE address = 'lc_wallet'"
        )[0]

        with pytest.raises(psycopg2.errors.CheckViolation):
            _execute(
                fresh_db,
                """
                INSERT INTO positions
                    (symbol, entry_price, qty, wallet_id, side, exit_price)
                VALUES ('BTCUSDT', 1, 1, %s, 'BUY', 1.5)
                """,
                (wallet_id,),
            )

        with pytest.raises(psycopg2.errors.CheckViolation):
            _execute(
                fresh_db,
                """
                INSERT INTO positions
                    (symbol, entry_price, qty, wallet_id, side, closed_at)
                VALUES ('BTCUSDT', 1, 1, %s, 'BUY', NOW())
                """,
                (wallet_id,),
            )

    def test_full_chain_legacy_text_to_phase3(self, legacy_db):
        result = _alembic(legacy_db, "upgrade", "head")
        assert result.returncode == 0, result.stdout + result.stderr
        side = _fetchone(legacy_db, "SELECT side FROM positions")[0]
        assert side == "BUY"
