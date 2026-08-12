"""Reconcile master pool, wallet balances, positions, and capital ledger.

Usage:
  python scripts/reconcile_capital.py
  python scripts/reconcile_capital.py --json
  python scripts/reconcile_capital.py --strict
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import asyncpg

from observability import (
    emit_event_async,
    inc_capital_mismatch,
    inc_reconcile_mismatch,
    new_correlation_id,
)

@dataclass
class Mismatch:
    reason: str
    expected: Any
    actual: Any
    wallet_id: str | None = None
    position_id: str | None = None


@dataclass
class ReconciliationResult:
    ok: bool
    mismatches: list[Mismatch]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "mismatches": [asdict(m) for m in self.mismatches],
        }


def _dec(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


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
        raise SystemExit(
            "DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD required"
        )
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


async def reconcile(conn: asyncpg.Connection) -> ReconciliationResult:
    """Inspect DB state and return PASS/FAIL with computed mismatches."""
    mismatches: list[Mismatch] = []

    master = await conn.fetchrow("SELECT * FROM master_pool WHERE id = 1")
    if master is None:
        mismatches.append(
            Mismatch(
                reason="master_pool_missing",
                expected="row id=1",
                actual=None,
            )
        )
        return ReconciliationResult(ok=False, mismatches=mismatches)

    total = _dec(master["total_capital"])
    allocated = _dec(master["allocated_capital"])
    available = _dec(master["available_capital"])
    if allocated + available != total:
        mismatches.append(
            Mismatch(
                reason="master_conservation",
                expected=str(total),
                actual=str(allocated + available),
            )
        )

    sum_initial = _dec(
        await conn.fetchval(
            "SELECT COALESCE(SUM(initial_capital), 0) FROM wallet_balances"
        )
    )
    if allocated != sum_initial:
        mismatches.append(
            Mismatch(
                reason="master_allocated_vs_wallet_initial",
                expected=str(sum_initial),
                actual=str(allocated),
            )
        )

    for field, label in (
        ("total_capital", "master_total_negative"),
        ("allocated_capital", "master_allocated_negative"),
        ("available_capital", "master_available_negative"),
    ):
        if _dec(master[field]) < 0:
            mismatches.append(
                Mismatch(
                    reason=label,
                    expected=">= 0",
                    actual=str(master[field]),
                )
            )

    wallets = await conn.fetch("SELECT * FROM wallet_balances")
    open_reserved_by_wallet = {
        row["wallet_id"]: _dec(row["open_reserved"])
        for row in await conn.fetch(
            """
            SELECT wallet_id,
                   COALESCE(SUM(COALESCE(reserved_margin, 0)), 0) AS open_reserved
            FROM positions
            WHERE closed_at IS NULL
            GROUP BY wallet_id
            """
        )
    }

    for bal in wallets:
        wid = bal["wallet_id"]
        wid_s = str(wid)
        available_b = _dec(bal["available_balance"])
        reserved_b = _dec(bal["reserved_margin"])
        initial_b = _dec(bal["initial_capital"])
        realized_b = _dec(bal["realized_pnl"])
        unrealized_b = _dec(bal["unrealized_pnl"])
        equity_b = _dec(bal["current_equity"])

        if available_b < 0:
            mismatches.append(
                Mismatch(
                    reason="wallet_available_negative",
                    expected=">= 0",
                    actual=str(available_b),
                    wallet_id=wid_s,
                )
            )
        if reserved_b < 0:
            mismatches.append(
                Mismatch(
                    reason="wallet_reserved_negative",
                    expected=">= 0",
                    actual=str(reserved_b),
                    wallet_id=wid_s,
                )
            )

        lhs = available_b + reserved_b
        rhs = initial_b + realized_b
        if lhs != rhs:
            mismatches.append(
                Mismatch(
                    reason="wallet_conservation",
                    expected=str(rhs),
                    actual=str(lhs),
                    wallet_id=wid_s,
                )
            )

        equity_expected = available_b + reserved_b + unrealized_b
        if equity_b != equity_expected:
            mismatches.append(
                Mismatch(
                    reason="wallet_equity",
                    expected=str(equity_expected),
                    actual=str(equity_b),
                    wallet_id=wid_s,
                )
            )

        open_reserved = open_reserved_by_wallet.get(wid, Decimal("0"))
        if reserved_b != open_reserved:
            mismatches.append(
                Mismatch(
                    reason="wallet_reserved_vs_open_positions",
                    expected=str(open_reserved),
                    actual=str(reserved_b),
                    wallet_id=wid_s,
                )
            )

    # Closed positions must not contribute to open reserved sums (already
    # excluded by WHERE closed_at IS NULL). Flag closed rows still marked
    # open-inconsistent only via ledger/settlement checks below.

    closed_with_balance = await conn.fetch(
        """
        SELECT p.id AS position_id,
               p.wallet_id,
               COALESCE(p.reserved_margin, 0) AS reserved_margin,
               EXISTS (
                   SELECT 1 FROM capital_ledger cl
                   WHERE cl.position_id = p.id AND cl.entry_type = 'RESERVE'
               ) AS had_reserve
        FROM positions p
        JOIN wallet_balances wb ON wb.wallet_id = p.wallet_id
        WHERE p.closed_at IS NOT NULL
        """
    )
    for pos in closed_with_balance:
        pid = pos["position_id"]
        pid_s = str(pid)
        wid_s = str(pos["wallet_id"])
        release_n = int(
            await conn.fetchval(
                """
                SELECT COUNT(*) FROM capital_ledger
                WHERE position_id = $1 AND entry_type = 'RELEASE'
                """,
                pid,
            )
        )
        realize_n = int(
            await conn.fetchval(
                """
                SELECT COUNT(*) FROM capital_ledger
                WHERE position_id = $1 AND entry_type = 'REALIZE_PNL'
                """,
                pid,
            )
        )
        reserved = _dec(pos["reserved_margin"])
        had_reserve = bool(pos["had_reserve"])
        # Capital-managed closes: RESERVE implies full settlement pair.
        if had_reserve:
            if release_n != 1:
                mismatches.append(
                    Mismatch(
                        reason="closed_position_release_cardinality",
                        expected=1,
                        actual=release_n,
                        wallet_id=wid_s,
                        position_id=pid_s,
                    )
                )
            if realize_n != 1:
                mismatches.append(
                    Mismatch(
                        reason="closed_position_realize_cardinality",
                        expected=1,
                        actual=realize_n,
                        wallet_id=wid_s,
                        position_id=pid_s,
                    )
                )
        elif reserved > 0 and release_n != 1:
            mismatches.append(
                Mismatch(
                    reason="closed_position_release_cardinality",
                    expected=1,
                    actual=release_n,
                    wallet_id=wid_s,
                    position_id=pid_s,
                )
            )
        elif realize_n not in (0, 1):
            mismatches.append(
                Mismatch(
                    reason="closed_position_realize_cardinality",
                    expected="0 or 1",
                    actual=realize_n,
                    wallet_id=wid_s,
                    position_id=pid_s,
                )
            )

    open_with_reserve = await conn.fetch(
        """
        SELECT p.id AS position_id,
               p.wallet_id,
               COALESCE(p.reserved_margin, 0) AS reserved_margin
        FROM positions p
        WHERE p.closed_at IS NULL
          AND COALESCE(p.reserved_margin, 0) > 0
        """
    )
    for pos in open_with_reserve:
        pid = pos["position_id"]
        pid_s = str(pid)
        wid_s = str(pos["wallet_id"])
        reserve_n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM capital_ledger
            WHERE position_id = $1 AND entry_type = 'RESERVE'
            """,
            pid,
        )
        if int(reserve_n) != 1:
            mismatches.append(
                Mismatch(
                    reason="open_position_reserve_cardinality",
                    expected=1,
                    actual=int(reserve_n),
                    wallet_id=wid_s,
                    position_id=pid_s,
                )
            )

    # --- Phase 5: order ↔ position execution linkage ---
    orphan_filled = await conn.fetch(
        """
        SELECT o.id AS order_id, o.wallet_id,
               EXISTS (
                   SELECT 1 FROM phase5_legacy_unlinked_orders l
                   WHERE l.order_id = o.id
               ) AS is_legacy
        FROM paper_orders o
        WHERE o.status = 'FILLED'
          AND NOT EXISTS (
              SELECT 1 FROM positions p WHERE p.order_id = o.id
          )
        """
    )
    for row in orphan_filled:
        mismatches.append(
            Mismatch(
                reason=(
                    "legacy_filled_order_without_position"
                    if row["is_legacy"]
                    else "filled_order_without_position"
                ),
                expected="exactly one position",
                actual=0,
                wallet_id=str(row["wallet_id"]),
                position_id=None,
            )
        )

    # Funded positions with RESERVE but NULL order_id: legacy ONLY when
    # explicitly registered in phase5_legacy_unlinked_positions. Never
    # classify NULL order_id as legacy by inference alone — a NEW funded
    # unlinked position is a critical Phase 5 violation.
    # Phase 8: venue-opened positions link through exchange_order_id instead.
    # Reconcile must still run on databases that predate that migration, so the
    # extra predicate is added only when the column exists.
    venue_link_clause = (
        "AND p.exchange_order_id IS NULL"
        if await _column_exists(conn, "positions", "exchange_order_id")
        else ""
    )
    missing_link = await conn.fetch(
        f"""
        SELECT p.id AS position_id, p.wallet_id,
               EXISTS (
                   SELECT 1 FROM phase5_legacy_unlinked_positions l
                   WHERE l.position_id = p.id
               ) AS is_legacy
        FROM positions p
        WHERE p.order_id IS NULL
          {venue_link_clause}
          AND EXISTS (
              SELECT 1 FROM capital_ledger cl
              WHERE cl.position_id = p.id AND cl.entry_type = 'RESERVE'
          )
        """
    )
    for row in missing_link:
        mismatches.append(
            Mismatch(
                reason=(
                    "legacy_position_missing_order_link"
                    if row["is_legacy"]
                    else "position_missing_order_link"
                ),
                expected="order_id set",
                actual=None,
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["position_id"]),
            )
        )

    multi_pos = await conn.fetch(
        """
        SELECT order_id, COUNT(*) AS n
        FROM positions
        WHERE order_id IS NOT NULL
        GROUP BY order_id
        HAVING COUNT(*) > 1
        """
    )
    for row in multi_pos:
        mismatches.append(
            Mismatch(
                reason="multiple_positions_for_order",
                expected=1,
                actual=int(row["n"]),
                position_id=str(row["order_id"]),
            )
        )

    dangling = await conn.fetch(
        """
        SELECT p.id AS position_id, p.wallet_id, p.order_id
        FROM positions p
        WHERE p.order_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM paper_orders o WHERE o.id = p.order_id
          )
        """
    )
    for row in dangling:
        mismatches.append(
            Mismatch(
                reason="position_order_fk_broken",
                expected="existing paper_orders.id",
                actual=str(row["order_id"]),
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["position_id"]),
            )
        )

    terminal_with_pos = await conn.fetch(
        """
        SELECT o.id AS order_id, o.wallet_id, o.status
        FROM paper_orders o
        WHERE o.status IN ('REJECTED', 'FAILED')
          AND EXISTS (
              SELECT 1 FROM positions p WHERE p.order_id = o.id
          )
        """
    )
    for row in terminal_with_pos:
        mismatches.append(
            Mismatch(
                reason="rejected_or_failed_order_has_position",
                expected="no position",
                actual=row["status"],
                wallet_id=str(row["wallet_id"]),
            )
        )

    mismatches.extend(await _reconcile_exchange_orders(conn))

    critical = [
        m
        for m in mismatches
        if not str(m.reason).startswith("legacy_")
    ]
    return ReconciliationResult(ok=not critical, mismatches=mismatches)


async def _table_exists(conn: asyncpg.Connection, table: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table))


async def _column_exists(
    conn: asyncpg.Connection, table: str, column: str
) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
              AND column_name = $2
            """,
            table,
            column,
        )
    )


async def _reconcile_exchange_orders(
    conn: asyncpg.Connection,
) -> list[Mismatch]:
    """Phase 8 — venue order ↔ position ↔ fill linkage.

    Skipped on databases that predate the Phase 8 migration.
    """
    if not await _table_exists(conn, "exchange_orders"):
        return []

    mismatches: list[Mismatch] = []

    unresolved = await conn.fetch(
        """
        SELECT id, wallet_id, symbol, status
        FROM exchange_orders
        WHERE status = 'UNKNOWN'
        """
    )
    for row in unresolved:
        mismatches.append(
            Mismatch(
                reason="exchange_order_state_unresolved",
                expected="resolved terminal status",
                actual=row["status"],
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["id"]),
            )
        )

    # Stale non-terminal intents (crash between intent commit and settlement).
    stale_pending = await conn.fetch(
        """
        SELECT id, wallet_id, symbol, status, intent
        FROM exchange_orders
        WHERE status IN ('PENDING', 'PARTIALLY_FILLED')
          AND created_at <= NOW() - INTERVAL '60 seconds'
        """
    )
    for row in stale_pending:
        mismatches.append(
            Mismatch(
                reason="exchange_order_nonterminal_stale",
                expected="terminal status within 60s",
                actual=f"{row['status']}/{row['intent']}",
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["id"]),
            )
        )

    # CLOSE filled but linked position still open — capital not settled.
    close_filled_open = await conn.fetch(
        """
        SELECT o.id, o.wallet_id, p.id AS position_id
        FROM exchange_orders o
        JOIN positions p ON p.id = o.position_id
        WHERE o.intent = 'CLOSE'
          AND o.qty_filled > 0
          AND o.status IN ('FILLED', 'CANCELLED', 'UNKNOWN')
          AND p.closed_at IS NULL
        """
    )
    for row in close_filled_open:
        mismatches.append(
            Mismatch(
                reason="exchange_close_filled_position_still_open",
                expected="position closed_at set",
                actual="open",
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["position_id"]),
            )
        )

    orphan_fills = await conn.fetch(
        """
        SELECT id, wallet_id, qty_filled
        FROM exchange_orders
        WHERE intent = 'OPEN' AND qty_filled > 0 AND position_id IS NULL
        """
    )
    for row in orphan_fills:
        mismatches.append(
            Mismatch(
                reason="exchange_order_filled_without_position",
                expected="linked position",
                actual=str(row["qty_filled"]),
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["id"]),
            )
        )

    rejected_with_position = await conn.fetch(
        """
        SELECT o.id, o.wallet_id, o.status
        FROM exchange_orders o
        WHERE o.status = 'REJECTED'
          AND EXISTS (
              SELECT 1 FROM positions p WHERE p.exchange_order_id = o.id
          )
        """
    )
    for row in rejected_with_position:
        mismatches.append(
            Mismatch(
                reason="rejected_exchange_order_has_position",
                expected="no position",
                actual=row["status"],
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["id"]),
            )
        )

    fill_sums = await conn.fetch(
        """
        SELECT o.id, o.wallet_id, o.qty_filled,
               COALESCE(SUM(f.qty_filled), 0) AS fills_qty
        FROM exchange_orders o
        LEFT JOIN exchange_fills f ON f.exchange_order_row_id = o.id
        GROUP BY o.id, o.wallet_id, o.qty_filled
        HAVING o.qty_filled <> COALESCE(SUM(f.qty_filled), 0)
        """
    )
    for row in fill_sums:
        mismatches.append(
            Mismatch(
                reason="exchange_order_qty_vs_fills",
                expected=str(_dec(row["qty_filled"])),
                actual=str(_dec(row["fills_qty"])),
                wallet_id=str(row["wallet_id"]),
                position_id=str(row["id"]),
            )
        )

    return mismatches


def format_human(result: ReconciliationResult) -> str:
    legacy = [m for m in result.mismatches if str(m.reason).startswith("legacy_")]
    critical = [m for m in result.mismatches if not str(m.reason).startswith("legacy_")]
    if result.ok and not legacy:
        return "CAPITAL RECONCILIATION: PASS"
    lines: list[str] = []
    if result.ok:
        lines.append("CAPITAL RECONCILIATION: PASS (legacy linkage notes present)")
    else:
        lines.append("CAPITAL RECONCILIATION: FAILED")
    if critical:
        lines.extend(["", "Critical mismatches:"])
        for m in critical:
            chunk = [
                f"wallet_id={m.wallet_id}" if m.wallet_id else None,
                f"position_id={m.position_id}" if m.position_id else None,
                f"expected={m.expected}",
                f"actual={m.actual}",
                f"reason={m.reason}",
            ]
            lines.append("* " + " ".join(c for c in chunk if c))
    if legacy:
        lines.extend(["", "Legacy linkage notes (non-fatal):"])
        for m in legacy:
            chunk = [
                f"wallet_id={m.wallet_id}" if m.wallet_id else None,
                f"position_id={m.position_id}" if m.position_id else None,
                f"expected={m.expected}",
                f"actual={m.actual}",
                f"reason={m.reason}",
            ]
            lines.append("* " + " ".join(c for c in chunk if c))
    return "\n".join(lines)


async def persist_reconciliation_run(
    conn: asyncpg.Connection,
    result: ReconciliationResult,
    *,
    correlation_id: UUID,
    started_at: datetime,
    strict: bool,
    notes: str | None = None,
) -> UUID:
    """Persist PASS or FAIL reconciliation history (truthful ok flag)."""
    mismatches = [asdict(m) for m in result.mismatches]
    run_id = await conn.fetchval(
        """
        INSERT INTO reconciliation_runs (
            correlation_id, started_at, finished_at, ok, strict,
            mismatch_count, mismatches, notes
        )
        VALUES ($1, $2, NOW(), $3, $4, $5, $6::jsonb, $7)
        RETURNING id
        """,
        correlation_id,
        started_at,
        result.ok,
        strict,
        len(result.mismatches),
        json.dumps(mismatches, default=str),
        notes,
    )
    return run_id


async def _async_main(args: argparse.Namespace) -> int:
    conn = await asyncpg.connect(_database_url())
    correlation_id = new_correlation_id()
    started = datetime.now(timezone.utc)
    result: ReconciliationResult | None = None
    try:
        await emit_event_async(
            conn,
            "reconcile_start",
            severity="info",
            correlation_id=correlation_id,
            actor="cli",
            source="reconcile_capital",
            detail={"strict": bool(args.strict)},
        )
        result = await reconcile(conn)
        if not result.ok:
            inc_reconcile_mismatch(float(len(result.mismatches)))
            inc_capital_mismatch()
        try:
            await persist_reconciliation_run(
                conn,
                result,
                correlation_id=correlation_id,
                started_at=started,
                strict=bool(args.strict),
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"WARNING: could not persist reconciliation_runs ({exc}); "
                "apply alembic head 20260804_0009",
                file=sys.stderr,
            )
        await emit_event_async(
            conn,
            "reconcile_result",
            severity="critical" if not result.ok else "info",
            correlation_id=correlation_id,
            actor="cli",
            source="reconcile_capital",
            reason="PASS" if result.ok else "FAIL",
            detail=result.to_dict(),
        )
    finally:
        await conn.close()

    if result is None:
        return 1

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, default=str))
    else:
        print(format_human(result))

    if args.strict and not result.ok:
        return 1
    return 0
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile capital accounting invariants"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when reconciliation fails",
    )
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    sys.exit(main())
