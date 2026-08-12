# Capital Integrity Contract (Phase 4.5)

This document defines the capital accounting contract for the paper trading
system. It is derived from the implemented schema and code in
`scripts/capital.py`, `scripts/mock_exchange.py`, and Alembic migrations —
not from aspirational design alone.

## 9.1 Capital Authority

```text
Capital Authority:
scripts/capital.py

Required policy:
No direct capital mutation outside capital.py.
```

| Component | Role |
|---|---|
| `master_pool` | **Global capital authority.** Singleton (`id = 1`). Holds `total_capital`, `allocated_capital`, `available_capital`. |
| `wallet_balances` | **Operational state.** One row per funded wallet. Spendable and reserved balances live here. |
| `capital_ledger` | **Audit / accounting history.** Append-only event log. Not the source of truth for balances. |
| `wallets` | Identity only — no capital columns. |
| `positions.reserved_margin` | Per-position record of margin locked at open; used on close to release the exact amount. |

**Balance source of truth:** `wallet_balances` (and `master_pool` for global capacity).  
**Audit trail:** `capital_ledger`. Reconstructing balances from ledger alone is not required for operational correctness; reconciliation compares operational state to derived invariants.

**Runtime writers:** only `scripts/capital.py` may `INSERT`/`UPDATE` `master_pool`, `wallet_balances`, or `capital_ledger` in application code. Callers (`mock_exchange.place_order` / `close_position`, `agent_trader`, `position_manager.mark_unrealized`) must go through capital APIs. Alembic migrations may seed/upgrade schema. Tests may use direct SQL only for intentional corruption fixtures or stub cleanup — never as a substitute production path.

**Module location note:** `scripts/capital.py` under `scripts/` is an accepted paper-phase layout. Relocating to a package is an architectural recommendation, not a current integrity blocker.

### Accounting equations

```
master:  allocated_capital + available_capital = total_capital
master:  allocated_capital = SUM(wallet_balances.initial_capital)
wallet:  available_balance + reserved_margin = initial_capital + realized_pnl
wallet:  current_equity = available_balance + reserved_margin + unrealized_pnl
open:    wallet.reserved_margin = SUM(positions.reserved_margin
                                      WHERE wallet_id = w AND closed_at IS NULL)
         (NULL reserved_margin treated as 0 — legacy opens)
```

## 9.2 Allowed Capital Mutations

All mutating capital APIs live in `scripts/capital.py`. Direct SQL writes to
capital tables outside migrations and this module are forbidden.

### Transaction / savepoint semantics (asyncpg)

Verified against asyncpg and this repository:

- Outer `async with conn.transaction()` issues `BEGIN`.
- Nested `async with conn.transaction()` issues `SAVEPOINT` / `RELEASE` /
  `ROLLBACK TO SAVEPOINT`.
- The repo already uses this pattern (`ensure_funded_wallet` →
  `allocate_to_wallet`).
- `reserve_margin`, `release_margin`, and `realize_pnl` open their own
  transaction block so standalone calls are atomic; when called inside
  `place_order` / `close_position` they become savepoints under the outer TX.

### `ensure_master_pool` / `MASTER_INIT`

| | |
|---|---|
| Preconditions | Pool missing or ops expanding total for paper funding |
| State changes | Insert/update `master_pool`; ledger `MASTER_INIT` |
| Transaction | Caller-dependent; `ensure_funded_wallet` wraps expansion |
| Failure | No partial pool row without ledger when inside a TX |

### `allocate_to_wallet`

| | |
|---|---|
| Preconditions | Wallet has no `wallet_balances` row; master available ≥ amount |
| State changes | Decrease master available / increase allocated; insert wallet balance; ledger `ALLOCATE` |
| Locks | `wallet_balances` row (absence) + `master_pool FOR UPDATE` |
| Idempotency | Rejects second allocation for same wallet |
| Failure | Full TX rollback |

### `reserve_margin`

| | |
|---|---|
| Preconditions | Funded wallet; `amount > 0`; available ≥ amount; if `position_id` set, no prior `RESERVE` for that position |
| State changes | `available → reserved`; ledger `RESERVE` |
| Locks | `wallet_balances FOR UPDATE` |
| Transaction | Own TX / nested savepoint |
| Failure | Raises `InsufficientAvailableBalance` or `DuplicateCapitalSettlement`; rolls back |

### `release_margin`

| | |
|---|---|
| Preconditions | Funded wallet; `amount ≥ 0`; reserved ≥ amount (no silent clamp); if `position_id` set, no prior `RELEASE` for that position |
| State changes | `reserved → available`; ledger `RELEASE` |
| Locks | `wallet_balances FOR UPDATE` |
| Idempotency | `amount == 0` is a no-op; duplicate position release is rejected |
| Failure | Raises `InsufficientReservedMargin` or `DuplicateCapitalSettlement` |

### `realize_pnl`

| | |
|---|---|
| Preconditions | Funded wallet; caller released margin first; PnL must not drive available below zero; if `position_id` set, no prior `REALIZE_PNL` |
| State changes | Adjust `available_balance` and `realized_pnl`; ledger `REALIZE_PNL` |
| Locks | `wallet_balances FOR UPDATE` |
| Failure | Raises `InsufficientAvailableBalance` or `DuplicateCapitalSettlement` |

### `mark_unrealized`

| | |
|---|---|
| Preconditions | Mark prices for open symbols |
| State changes | Recompute `unrealized_pnl` and `current_equity` per wallet; ledger `MARK_UNREALIZED` when changed |
| Locks | Per-wallet `FOR UPDATE` (no single outer TX across all wallets) |
| Notes | Soft recompute; closed positions drop out of the open set on next mark |

### Open / close orchestration

- **Open (`place_order`):** one DB transaction — insert order, insert position
  (with `reserved_margin`), `reserve_margin`.
- **Close (`close_position`):** one DB transaction — `SELECT position FOR UPDATE`
  where open, update close metadata, `release_margin`, `realize_pnl`, upsert
  `daily_stats`. Second close raises `PositionAlreadyClosed`.

## 9.3 Forbidden States

- Negative `available_balance`, `reserved_margin`, or master capital fields
- Master conservation violation
- Wallet conservation or equity equation violation
- Reserved margin greater than what open positions support (drift)
- Double `RESERVE` / `RELEASE` / `REALIZE_PNL` for the same `position_id`
- Closed position still counted in open reserved sums
- Partial settlement (position closed without matching capital settle when a
  capital account exists and reserved > 0)
- Cross-wallet mutation from another wallet’s operation

## 9.4 Transaction Guarantees

Must be atomic:

1. Open: order + position + reserve
2. Close: position close + release + realize + daily_stats
3. Allocate: master update + wallet insert + ledger
4. Standalone reserve / release / realize: balance update + ledger

A failed step rolls back the enclosing transaction (or savepoint).

## 9.5 Concurrency Guarantees

- Wallet isolation: `SELECT … FOR UPDATE` on `wallet_balances`
- Master allocation races: `SELECT … FOR UPDATE` on `master_pool`
- Double close: `SELECT … FOR UPDATE` on open position + `closed_at IS NULL`
- Double settlement ledger: partial unique indexes on
  `(position_id)` for each of `RESERVE`, `RELEASE`, `REALIZE_PNL`
- Isolation level: PostgreSQL default `READ COMMITTED` via asyncpg

## 9.6 Idempotency Policy

| Operation | Duplicate behavior |
|---|---|
| Close position | **Rejected** — `PositionAlreadyClosed` |
| Reserve / release / realize for same `position_id` | **Rejected** — `DuplicateCapitalSettlement` (app) + unique index (DB) |
| Allocate same wallet | **Rejected** — `ValueError` |
| `release_margin(amount=0)` | Safe no-op |
| `ensure_master_pool` / already-funded wallet | Safe no-op / return existing |

## 9.7 Reconciliation

Run:

```bash
python scripts/reconcile_capital.py
python scripts/reconcile_capital.py --json
python scripts/reconcile_capital.py --strict
```

`--strict` exits non-zero on any mismatch.

## 9.8 Ledger / Auditability Status

| Question | Current answer |
|---|---|
| Append-only by application design? | **Yes** — `capital.py` only `INSERT`s into `capital_ledger`. |
| Can application code UPDATE/DELETE ledger rows? | **No** — blocked by DB trigger `trg_capital_ledger_append_only` (P7-007). |
| DB permissions prevent modification? | **Trigger enforcement** on UPDATE/DELETE (no production GUC bypass). |
| Provenance per reserve/release/realize? | **Yes (P7-006)** — `actor`, `source`, `reason`, `correlation_id`, optional internal `order_id`. |
| Reconciliation history? | **Yes (P7-012)** — `reconciliation_runs` persists PASS/FAIL. |
| Sufficient for real-money ops? | **Not yet** — still paper; no exchange IDs. |

**Classification:** `HARDENED FOR PAPER PRE-TESTNET` · `NOT TESTNET` · `NOT MAINNET`


## 9.9 PnL Cost Label

Current paper PnL is **GROSS / PRE-COST PAPER PnL**. Fees, slippage, and funding are not modeled. No performance report may describe current PnL as net or real-world expected return until a cost model is approved.

## 9.10 Legacy Notes

- Pre-Phase-4 open positions may have `reserved_margin` NULL/0. Close releases 0
  and does not invent capital history.
- Position Manager / Phase 5 tests may insert positions directly without reserve;
  production open path always goes through `place_order` (always sets `order_id`).
- Historical Phase 4.5 test stubs with NULL `order_id` + RESERVE were incomplete
  cleanup (not a production defect). Suite now purges stubs and isolates onto
  `{POSTGRES_DB}_test`.
