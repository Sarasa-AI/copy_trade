"""Phase 6 independent A1–A9 verification at Exit Gate thresholds.

Runs against the dedicated pytest test DB (conftest isolation).
Writes structured evidence JSON under VERIFICATION_EVIDENCE_DIR when set.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

os.environ["EXECUTION_DELAY_SEC"] = "0"

from capital import (  # noqa: E402
    ensure_funded_wallet,
    get_wallet_balance,
)
from emergency_control import (  # noqa: E402
    emergency_halt_and_flatten,
    flatten_position,
)
from kill_switch import (  # noqa: E402
    KillSwitchActive,
    activate_kill_switch,
    deactivate_kill_switch,
    get_kill_switch,
)
from mock_exchange import close_position, place_order  # noqa: E402
from reconcile_capital import reconcile  # noqa: E402
from risk_engine import (  # noqa: E402
    DAILY_EQUITY_LOSS_LIMIT_PCT,
    RiskDenied,
    _loss_pct,
    current_global_equity,
    utc_today,
)
from wallet_repository import create_wallet  # noqa: E402
from db_isolation import reset_critical_db_state  # noqa: E402

# Brief thresholds
A1_CONCURRENT = 20
A1_REPS = 20
A2_CONCURRENT = 50
A2_REPS = 5
A3_REPS = 20
A4_OPENS = 50
A4_REPS = 5
A5_REPS = 5
A6_REPS = 5


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


def _evidence_dir() -> Path | None:
    raw = os.environ.get("VERIFICATION_EVIDENCE_DIR", "").strip()
    if not raw:
        return None
    path = Path(raw)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_evidence(name: str, payload: dict[str, Any]) -> None:
    edir = _evidence_dir()
    if edir is None:
        return
    path = edir / name
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@pytest.fixture
async def conn():
    connection = await asyncpg.connect(_database_url())
    try:
        await reset_critical_db_state(connection)
        yield connection
    finally:
        await connection.close()


async def _funded_wallet(
    conn: asyncpg.Connection, *, amount: float = 1000.0
) -> asyncpg.Record:
    wallet = await create_wallet(conn, f"ind_{uuid.uuid4().hex[:12]}")
    await ensure_funded_wallet(conn, wallet["id"], amount=amount)
    return wallet


async def _force_global_drawdown(
    conn: asyncpg.Connection, *, drawdown_pct: float = 0.05
) -> tuple[float, float]:
    current = await current_global_equity(conn)
    if current <= 0:
        await _funded_wallet(conn, amount=1000.0)
        current = await current_global_equity(conn)
    sod = current / (1.0 - drawdown_pct)
    day = utc_today()
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = $1 AND scope = 'GLOBAL'",
        day,
    )
    await conn.execute(
        """
        INSERT INTO equity_sod_snapshots (as_of_date, scope, wallet_id, equity)
        VALUES ($1, 'GLOBAL', NULL, $2)
        """,
        day,
        sod,
    )
    return sod, current


async def _clear_today_sod(conn: asyncpg.Connection) -> None:
    await conn.execute(
        "DELETE FROM equity_sod_snapshots WHERE as_of_date = $1 AND scope = 'GLOBAL'",
        utc_today(),
    )


async def _snapshot_wallet_capital(
    conn: asyncpg.Connection, wallet_id
) -> dict[str, float | int]:
    bal = await get_wallet_balance(conn, wallet_id)
    orders = await conn.fetchval(
        "SELECT COUNT(*) FROM paper_orders WHERE wallet_id = $1", wallet_id
    )
    positions = await conn.fetchval(
        "SELECT COUNT(*) FROM positions WHERE wallet_id = $1", wallet_id
    )
    ledger = await conn.fetchval(
        "SELECT COUNT(*) FROM capital_ledger WHERE wallet_id = $1", wallet_id
    )
    denials = await conn.fetchval(
        "SELECT COUNT(*) FROM risk_denials WHERE wallet_id = $1", wallet_id
    )
    return {
        "available": float(bal["available_balance"]),
        "reserved": float(bal["reserved_margin"]),
        "equity": float(bal["current_equity"]),
        "orders": int(orders),
        "positions": int(positions),
        "ledger": int(ledger),
        "denials": int(denials),
    }


async def _assert_no_orphans_for_wallets(
    conn: asyncpg.Connection, wallet_ids: list
) -> None:
    for wid in wallet_ids:
        orphans = await conn.fetchval(
            """
            SELECT COUNT(*) FROM paper_orders o
            WHERE o.wallet_id = $1 AND o.status = 'FILLED'
              AND NOT EXISTS (
                SELECT 1 FROM positions p WHERE p.order_id = o.id
              )
            """,
            wid,
        )
        assert orphans == 0
        pending = await conn.fetchval(
            """
            SELECT COUNT(*) FROM paper_orders
            WHERE wallet_id = $1 AND status = 'PENDING'
            """,
            wid,
        )
        assert pending == 0
        filled = await conn.fetchval(
            """
            SELECT COUNT(*) FROM paper_orders
            WHERE wallet_id = $1 AND status = 'FILLED'
            """,
            wid,
        )
        linked = await conn.fetchval(
            """
            SELECT COUNT(*) FROM positions
            WHERE wallet_id = $1 AND order_id IS NOT NULL
            """,
            wid,
        )
        assert filled == linked


async def _attempt_open(url: str, wid) -> dict[str, Any]:
    t0 = _now()
    c = await asyncpg.connect(url)
    try:
        await place_order("BTCUSDT", "BUY", 0.001, 65000.0, c, wallet_id=wid)
        return {"result": "ok", "t_start": t0, "t_end": _now()}
    except KillSwitchActive as exc:
        return {
            "result": "kill",
            "error": type(exc).__name__,
            "t_start": t0,
            "t_end": _now(),
        }
    except RiskDenied as exc:
        return {
            "result": "denied",
            "error": type(exc).__name__,
            "reason_code": exc.reason_code,
            "t_start": t0,
            "t_end": _now(),
        }
    except Exception as exc:  # noqa: BLE001 — capture unexpected for evidence
        return {
            "result": "error",
            "error": f"{type(exc).__name__}:{exc}",
            "t_start": t0,
            "t_end": _now(),
        }
    finally:
        await c.close()


# --- A1 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a1_concurrent_sod_limit(conn: asyncpg.Connection):
    """A1 — ≥20 concurrent opens × ≥20 reps cannot violate SoD limit."""
    url = _database_url()
    limit = DAILY_EQUITY_LOSS_LIMIT_PCT
    rep_summaries: list[dict[str, Any]] = []

    for rep in range(A1_REPS):
        wallets = [
            await _funded_wallet(conn, amount=200.0) for _ in range(A1_CONCURRENT)
        ]
        # Force over-limit drawdown so risk boundary is active.
        sod, current = await _force_global_drawdown(conn, drawdown_pct=0.05)
        loss = _loss_pct(sod, current)
        assert loss > limit

        befores = {
            str(w["id"]): await _snapshot_wallet_capital(conn, w["id"])
            for w in wallets
        }
        outcomes = await asyncio.gather(
            *[_attempt_open(url, w["id"]) for w in wallets]
        )
        allowed = sum(1 for o in outcomes if o["result"] == "ok")
        denied = sum(1 for o in outcomes if o["result"] == "denied")
        other = [o for o in outcomes if o["result"] not in ("ok", "denied")]

        assert allowed == 0, f"rep={rep} allowed={allowed} violates SoD boundary"
        assert denied == A1_CONCURRENT
        assert not other

        for w in wallets:
            after = await _snapshot_wallet_capital(conn, w["id"])
            before = befores[str(w["id"])]
            # Denial telemetry may increment risk_denials only.
            assert after["orders"] == before["orders"]
            assert after["positions"] == before["positions"]
            assert after["reserved"] == before["reserved"]
            assert after["available"] == before["available"]
            assert after["ledger"] == before["ledger"]

        post_loss = _loss_pct(
            float(
                await conn.fetchval(
                    """
                    SELECT equity FROM equity_sod_snapshots
                    WHERE as_of_date = $1 AND scope = 'GLOBAL'
                    """,
                    utc_today(),
                )
            ),
            await current_global_equity(conn),
        )
        assert post_loss > limit

        await _assert_no_orphans_for_wallets(conn, [w["id"] for w in wallets])
        recon = await reconcile(conn)
        assert recon.ok

        rep_summaries.append(
            {
                "rep": rep,
                "sod_limit": limit,
                "sod": sod,
                "current": current,
                "loss_pct": loss,
                "attempts": A1_CONCURRENT,
                "allowed": allowed,
                "denied": denied,
                "timestamps": outcomes,
                "reconcile_ok": recon.ok,
            }
        )

    _write_evidence(
        "a1_concurrent_sod.json",
        {
            "test": "A1",
            "classification": "PASS",
            "config": {
                "EXECUTION_DELAY_SEC": 0,
                "DAILY_EQUITY_LOSS_LIMIT_PCT": limit,
                "concurrent": A1_CONCURRENT,
                "repetitions": A1_REPS,
                "drawdown_forced_pct": 0.05,
                "wallets_per_rep": A1_CONCURRENT,
            },
            "reps": rep_summaries,
        },
    )


# --- A2 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a2_concurrent_healthy_opens(conn: asyncpg.Connection):
    """A2 — 20–100 concurrent healthy opens: no capital/lifecycle corruption."""
    url = _database_url()
    rep_summaries: list[dict[str, Any]] = []

    for rep in range(A2_REPS):
        await _clear_today_sod(conn)
        # Ensure kill off and healthy SoD.
        state = await get_kill_switch(conn)
        if state["active"]:
            await deactivate_kill_switch(
                conn, reason="a2_clear", actor="ind_verify"
            )

        wallets = [
            await _funded_wallet(conn, amount=500.0) for _ in range(A2_CONCURRENT)
        ]
        outcomes = await asyncio.gather(
            *[_attempt_open(url, w["id"]) for w in wallets]
        )
        allowed = sum(1 for o in outcomes if o["result"] == "ok")
        errors = [o for o in outcomes if o["result"] == "error"]
        assert not errors, errors
        assert allowed == A2_CONCURRENT

        for w in wallets:
            bal = await get_wallet_balance(conn, w["id"])
            assert float(bal["reserved_margin"]) == pytest.approx(65.0)
            assert float(bal["available_balance"]) == pytest.approx(435.0)
            open_n = await conn.fetchval(
                """
                SELECT COUNT(*) FROM positions
                WHERE wallet_id = $1 AND closed_at IS NULL
                """,
                w["id"],
            )
            assert open_n == 1

        await _assert_no_orphans_for_wallets(conn, [w["id"] for w in wallets])
        recon = await reconcile(conn)
        assert recon.ok
        # Close positions to keep subsequent reps solvent / clean.
        open_rows = await conn.fetch(
            """
            SELECT id FROM positions
            WHERE wallet_id = ANY($1::uuid[]) AND closed_at IS NULL
            """,
            [w["id"] for w in wallets],
        )
        for row in open_rows:
            await close_position(row["id"], 65000.0, conn, close_reason="ADMIN")

        rep_summaries.append(
            {
                "rep": rep,
                "attempts": A2_CONCURRENT,
                "allowed": allowed,
                "denied": sum(1 for o in outcomes if o["result"] != "ok"),
                "reconcile_ok": recon.ok,
            }
        )

    _write_evidence(
        "a2_concurrent_healthy.json",
        {
            "test": "A2",
            "classification": "PASS",
            "config": {
                "EXECUTION_DELAY_SEC": 0,
                "concurrent": A2_CONCURRENT,
                "repetitions": A2_REPS,
            },
            "reps": rep_summaries,
        },
    )


# --- A3 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a3_kill_race(conn: asyncpg.Connection):
    """A3 — place_order vs activate_kill_switch: no post-kill successful open."""
    url = _database_url()
    race_log: list[dict[str, Any]] = []
    invalid = 0

    for rep in range(A3_REPS):
        await _clear_today_sod(conn)
        state = await get_kill_switch(conn)
        if state["active"]:
            await deactivate_kill_switch(
                conn, reason="a3_reset", actor="ind_verify"
            )

        wallet = await _funded_wallet(conn, amount=800.0)
        probe = await _funded_wallet(conn, amount=400.0)

        async def _open() -> dict[str, Any]:
            return await _attempt_open(url, wallet["id"])

        async def _kill() -> dict[str, Any]:
            t0 = _now()
            c = await asyncpg.connect(url)
            try:
                await activate_kill_switch(
                    c, reason=f"a3_race_{rep}", actor="ind_verify"
                )
                return {"result": "activated", "t_start": t0, "t_end": _now()}
            finally:
                await c.close()

        open_r, kill_r = await asyncio.gather(_open(), _kill())
        assert kill_r["result"] == "activated"
        assert open_r["result"] in ("ok", "kill")

        ks = await get_kill_switch(conn)
        assert ks["active"] is True
        activated_at = ks["activated_at"]

        order_created = await conn.fetchval(
            """
            SELECT created_at FROM paper_orders
            WHERE wallet_id = $1 AND status = 'FILLED'
            ORDER BY created_at DESC NULLS LAST
            LIMIT 1
            """,
            wallet["id"],
        )

        # INVALID: kill committed, then a NEW open succeeds.
        probe_r = await _attempt_open(url, probe["id"])
        if probe_r["result"] == "ok":
            invalid += 1

        race_log.append(
            {
                "rep": rep,
                "open": open_r,
                "kill": kill_r,
                "case": "A_open_then_kill"
                if open_r["result"] == "ok"
                else "B_kill_then_deny",
                "kill_activated_at": activated_at,
                "order_created_at": order_created,
                "post_kill_probe": probe_r,
            }
        )

        await deactivate_kill_switch(
            conn, reason="a3_clear", actor="ind_verify"
        )
        recon = await reconcile(conn)
        assert recon.ok

    assert invalid == 0
    _write_evidence(
        "a3_kill_race.json",
        {
            "test": "A3",
            "classification": "PASS",
            "config": {
                "EXECUTION_DELAY_SEC": 0,
                "repetitions": A3_REPS,
            },
            "invalid_post_kill_opens": invalid,
            "races": race_log,
        },
    )


# --- A4 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a4_combined_stress(conn: asyncpg.Connection):
    """A4 — ≥50 concurrent opens + kill activation; no orphans/deadlock."""
    url = _database_url()
    rep_summaries: list[dict[str, Any]] = []

    for rep in range(A4_REPS):
        await _clear_today_sod(conn)
        state = await get_kill_switch(conn)
        if state["active"]:
            await deactivate_kill_switch(
                conn, reason="a4_reset", actor="ind_verify"
            )

        wallets = [
            await _funded_wallet(conn, amount=600.0) for _ in range(A4_OPENS)
        ]

        async def _kill() -> dict[str, Any]:
            t0 = _now()
            c = await asyncpg.connect(url)
            try:
                await activate_kill_switch(
                    c, reason=f"a4_stress_{rep}", actor="ind_verify"
                )
                return {"result": "activated", "t_start": t0, "t_end": _now()}
            finally:
                await c.close()

        outcomes = await asyncio.gather(
            *[_attempt_open(url, w["id"]) for w in wallets],
            _kill(),
        )
        kill_outcome = outcomes[-1]
        open_outcomes = outcomes[:-1]
        assert kill_outcome["result"] == "activated"
        assert set(o["result"] for o in open_outcomes) <= {"ok", "kill", "denied"}
        assert await get_kill_switch(conn)
        assert (await get_kill_switch(conn))["active"] is True

        await _assert_no_orphans_for_wallets(conn, [w["id"] for w in wallets])
        for w in wallets:
            filled = await conn.fetchval(
                """
                SELECT COUNT(*) FROM paper_orders
                WHERE wallet_id = $1 AND status = 'FILLED'
                """,
                w["id"],
            )
            open_pos = await conn.fetchval(
                """
                SELECT COUNT(*) FROM positions
                WHERE wallet_id = $1 AND closed_at IS NULL
                """,
                w["id"],
            )
            assert filled == open_pos
            bal = await get_wallet_balance(conn, w["id"])
            assert float(bal["reserved_margin"]) == pytest.approx(65.0 * open_pos)

        # New open after kill commit must fail.
        probe = await _funded_wallet(conn, amount=400.0)
        probe_r = await _attempt_open(url, probe["id"])
        assert probe_r["result"] == "kill"

        recon = await reconcile(conn)
        assert recon.ok

        # Cleanup open positions then deactivate.
        open_rows = await conn.fetch(
            """
            SELECT id FROM positions
            WHERE wallet_id = ANY($1::uuid[]) AND closed_at IS NULL
            """,
            [w["id"] for w in wallets],
        )
        for row in open_rows:
            await close_position(row["id"], 65000.0, conn, close_reason="ADMIN")
        await deactivate_kill_switch(
            conn, reason="a4_clear", actor="ind_verify"
        )

        rep_summaries.append(
            {
                "rep": rep,
                "attempts": A4_OPENS,
                "allowed": sum(1 for o in open_outcomes if o["result"] == "ok"),
                "kill_denied": sum(
                    1 for o in open_outcomes if o["result"] == "kill"
                ),
                "risk_denied": sum(
                    1 for o in open_outcomes if o["result"] == "denied"
                ),
                "kill": kill_outcome,
                "post_kill_probe": probe_r,
                "reconcile_ok": recon.ok,
            }
        )

    _write_evidence(
        "a4_combined_stress.json",
        {
            "test": "A4",
            "classification": "PASS",
            "config": {
                "EXECUTION_DELAY_SEC": 0,
                "opens": A4_OPENS,
                "repetitions": A4_REPS,
            },
            "reps": rep_summaries,
        },
    )


# --- A5 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a5_risk_denial_freeze(conn: asyncpg.Connection):
    """A5 — Risk denial freezes capital/order/position except denial telemetry."""
    snapshots: list[dict[str, Any]] = []
    for rep in range(A5_REPS):
        wallet = await _funded_wallet(conn, amount=250.0)
        await _force_global_drawdown(conn, drawdown_pct=0.05)
        before = await _snapshot_wallet_capital(conn, wallet["id"])
        with pytest.raises(RiskDenied):
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
            )
        after = await _snapshot_wallet_capital(conn, wallet["id"])
        assert after["orders"] == before["orders"]
        assert after["positions"] == before["positions"]
        assert after["reserved"] == before["reserved"]
        assert after["available"] == before["available"]
        assert after["ledger"] == before["ledger"]
        # Denial telemetry may increase.
        assert after["denials"] >= before["denials"]
        snapshots.append({"rep": rep, "before": before, "after": after})

    recon = await reconcile(conn)
    assert recon.ok
    _write_evidence(
        "a5_risk_denial_freeze.json",
        {
            "test": "A5",
            "classification": "PASS",
            "repetitions": A5_REPS,
            "snapshots": snapshots,
            "reconcile_ok": recon.ok,
        },
    )


# --- A6 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a6_kill_denial_freeze(conn: asyncpg.Connection):
    """A6 — Kill denial via direct place_order; KillSwitchActive; freeze."""
    snapshots: list[dict[str, Any]] = []
    for rep in range(A6_REPS):
        await _clear_today_sod(conn)
        wallet = await _funded_wallet(conn, amount=250.0)
        await activate_kill_switch(
            conn, reason=f"a6_{rep}", actor="ind_verify"
        )
        try:
            before = await _snapshot_wallet_capital(conn, wallet["id"])
            with pytest.raises(KillSwitchActive):
                await place_order(
                    "BTCUSDT",
                    "BUY",
                    0.001,
                    65000.0,
                    conn,
                    wallet_id=wallet["id"],
                )
            after = await _snapshot_wallet_capital(conn, wallet["id"])
            assert after["orders"] == before["orders"]
            assert after["positions"] == before["positions"]
            assert after["reserved"] == before["reserved"]
            assert after["available"] == before["available"]
            assert after["ledger"] == before["ledger"]
            snapshots.append({"rep": rep, "before": before, "after": after})
        finally:
            await deactivate_kill_switch(
                conn, reason=f"a6_clear_{rep}", actor="ind_verify"
            )

    recon = await reconcile(conn)
    assert recon.ok
    _write_evidence(
        "a6_kill_denial_freeze.json",
        {
            "test": "A6",
            "classification": "PASS",
            "repetitions": A6_REPS,
            "expected_error": "KillSwitchActive",
            "snapshots": snapshots,
            "reconcile_ok": recon.ok,
        },
    )


# --- A7 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a7_post_risk_rollback(conn: asyncpg.Connection):
    """A7 — Failure after assert_open_allowed / during reserve: full rollback."""
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=1000.0)
    before = await _snapshot_wallet_capital(conn, wallet["id"])

    with patch(
        "mock_exchange.reserve_margin",
        new_callable=AsyncMock,
        side_effect=RuntimeError("injected_post_risk_failure"),
    ):
        with pytest.raises(RuntimeError, match="injected_post_risk_failure"):
            await place_order(
                "BTCUSDT", "BUY", 0.001, 65000.0, conn, wallet_id=wallet["id"]
            )

    after = await _snapshot_wallet_capital(conn, wallet["id"])
    assert after == before
    recon = await reconcile(conn)
    assert recon.ok
    _write_evidence(
        "a7_post_risk_rollback.json",
        {
            "test": "A7",
            "classification": "PASS",
            "injection": "monkeypatch reserve_margin RuntimeError",
            "before": before,
            "after": after,
            "reconcile_ok": recon.ok,
        },
    )


# --- A8 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a8_restart_persistence(conn: asyncpg.Connection):
    """A8 — Kill persists across new connection; direct place_order denied."""
    await _clear_today_sod(conn)
    wallet = await _funded_wallet(conn, amount=400.0)
    await activate_kill_switch(conn, reason="a8_persist", actor="ind_verify")

    url = _database_url()
    # New connection = process/restart equivalent (DB-backed state).
    new_conn = await asyncpg.connect(url)
    try:
        state = await get_kill_switch(new_conn)
        assert state["active"] is True
        with pytest.raises(KillSwitchActive):
            await place_order(
                "BTCUSDT",
                "BUY",
                0.001,
                65000.0,
                new_conn,
                wallet_id=wallet["id"],
            )
        await deactivate_kill_switch(
            new_conn, reason="a8_clear", actor="ind_verify"
        )
        recon = await reconcile(new_conn)
        assert recon.ok
        _write_evidence(
            "a8_restart_persistence.json",
            {
                "test": "A8",
                "classification": "PASS",
                "kill_active_on_new_connection": True,
                "error": "KillSwitchActive",
                "reconcile_ok": recon.ok,
            },
        )
    finally:
        await new_conn.close()


# --- A9 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a9_emergency_flatten(conn: asyncpg.Connection):
    """A9 — Mixed flatten: open/closed/missing-price; best-effort; reconcile."""
    await _clear_today_sod(conn)
    state = await get_kill_switch(conn)
    if state["active"]:
        await deactivate_kill_switch(
            conn, reason="a9_reset", actor="ind_verify"
        )

    w1 = await _funded_wallet(conn, amount=3000.0)
    w2 = await _funded_wallet(conn, amount=3000.0)

    o1 = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=w1["id"]
    )
    o2 = await place_order(
        "BTCUSDT", "BUY", 0.01, 65000.0, conn, wallet_id=w2["id"]
    )
    o3 = await place_order(
        "ETHUSDT", "BUY", 0.1, 3000.0, conn, wallet_id=w1["id"]
    )
    # Already-closed position in dataset.
    o4 = await place_order(
        "BTCUSDT", "BUY", 0.005, 65000.0, conn, wallet_id=w2["id"]
    )
    p1 = await conn.fetchval("SELECT id FROM positions WHERE order_id = $1", o1)
    p2 = await conn.fetchval("SELECT id FROM positions WHERE order_id = $1", o2)
    p3 = await conn.fetchval("SELECT id FROM positions WHERE order_id = $1", o3)
    p4 = await conn.fetchval("SELECT id FROM positions WHERE order_id = $1", o4)
    await close_position(p4, 65100.0, conn, close_reason="ADMIN")

    result = await emergency_halt_and_flatten(
        conn,
        actor="ind_verify",
        reason="a9_flatten",
        exit_prices={"BTCUSDT": 66000.0},
    )
    assert result["kill_switch_active"] is True
    statuses = {r["position_id"]: r["status"] for r in result["results"]}
    assert statuses[str(p1)] == "closed"
    assert statuses[str(p2)] == "closed"
    assert statuses[str(p3)] == "skipped_no_price"
    # Already closed should not appear in open list / must not double-settle.
    assert str(p4) not in statuses

    # Explicit already_closed path.
    again = await flatten_position(
        conn, p1, 66000.0, actor="ind_verify", reason="dup"
    )
    assert again["status"] == "already_closed"

    release_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'RELEASE'
        """,
        p1,
    )
    realize_n = await conn.fetchval(
        """
        SELECT COUNT(*) FROM capital_ledger
        WHERE position_id = $1 AND entry_type = 'REALIZE_PNL'
        """,
        p1,
    )
    # Exactly one RELEASE + one REALIZE_PNL; flatten-again must not double-settle.
    assert release_n == 1
    assert realize_n == 1

    recon = await reconcile(conn)
    assert recon.ok

    # Close remaining skipped ETH for cleanup.
    await close_position(p3, 3010.0, conn, close_reason="ADMIN")
    await deactivate_kill_switch(conn, reason="a9_done", actor="ind_verify")
    recon2 = await reconcile(conn)
    assert recon2.ok

    _write_evidence(
        "a9_emergency_flatten.json",
        {
            "test": "A9",
            "classification": "PASS",
            "semantics": "best-effort / per-position transactional (not all-or-nothing)",
            "results": result["results"],
            "already_closed_probe": again,
            "skipped": [r for r in result["results"] if r["status"] == "skipped_no_price"],
            "closed": [r for r in result["results"] if r["status"] == "closed"],
            "reconcile_ok": recon.ok,
            "reconcile_after_cleanup_ok": recon2.ok,
        },
    )
