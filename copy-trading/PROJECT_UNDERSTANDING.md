# PROJECT UNDERSTANDING

**Project:** Multi-Agent Algorithmic Copy-Trading / Paper Trading System  
**Document status:** Updated after Phase 6 Exit Gate verification (2026-08-04)  
**Source of truth:** Code + Alembic migrations + tests + reconciliation (not this file alone)  
**Rule:** No phase starts without Owner approval. Phase 6 verification does **not** authorize Phase 7 / Exchange / Testnet / Mainnet.

---

## 1. Executive Summary

This repository is a **paper-trading system** with completed capital integrity
(Phase 4.5), execution lifecycle integrity (Phase 5), and equity Risk Engine +
persistent Kill Switch + Emergency Flatten (Phase 6). It is **not**
Testnet/Mainnet-ready.

**Implemented today (verified):**

- Postgres + Alembic migrations (head `20260802_0008`) + pytest suite
- UUID wallet identity with FK isolation
- Master pool + per-wallet capital (`scripts/capital.py` is sole capital write authority)
- Full position lifecycle: open → SL/TP monitor → close (`position_manager` + `close_position`)
- Side-aware PnL (BUY/SELL), persisted SL/TP prices — **GROSS / PRE-COST / PAPER**
- Order state machine: PENDING → FILLED | REJECTED | FAILED
- 1:1 NEW order↔position linkage (`positions.order_id` UNIQUE FK)
- Capital reconciliation with explicit legacy registries (`phase5_legacy_unlinked_*`)
- Execution delay **before** DB connection acquire (paper fill price may be stale)
- Equity Risk Engine vs UTC SoD inside atomic `place_order` TX (`risk_engine.py`)
- Persistent Kill Switch hard-deny on opens (`kill_switch_state`)
- Emergency flatten via `close_position` (`emergency_control.py`; sync best-effort)

**Still deferred (Phase 7 / Later — not Phase 6):**

- Fees / slippage / funding, real exchange adapters, copy-trading, frontend
- Mark-price freshness fail-safe on risk decisions (stale/missing mark → deny opens)
- Atomic intent/reservation redesign beyond current open TX boundary
- Soft realized-only `check_daily_loss` remains compatibility-only (not authoritative)

Mainnet remains forbidden until paper metrics, risk, security, stress, and exchange-adapter readiness are proven.

---

## 2. Product Goal

Build a **Hybrid Multi-Agent Copy-Trading System** that:

1. Monitors multiple agents/wallets concurrently
2. Evaluates performance on real market data (paper first)
3. Selects top agents via statistical filters
4. Simulates copy-trading behavior in paper mode
5. Later mirrors real lead-wallet trades
6. Uses an Alpha Engine for ranking and capital allocation (not primary signal generation)

`random.choice(BUY, SELL)` is a **simulation placeholder only**, not final trading logic.

Target pipeline (future):

```
Signal Provider → Normalized Signal → Risk Engine → Kill-Switch
  → Execution Engine → Position Manager
```

---

## 3. Actual Current Architecture

Repository root: `copy-trading/` (Alembic, pytest, Docker Compose).

| Layer | Current reality | Key files |
|---|---|---|
| Entry / Agent | 60s loop; fund allocations; PM tick; soft daily-loss; `place_order` | `scripts/agent_trader.py` |
| Price | Binance public WS trade stream into in-memory dict | `scripts/price_feed.py` |
| Execution | Mock exchange: delay → atomic FILLED+position+reserve; transactional close | `scripts/mock_exchange.py` |
| Position Manager | Mark unrealized; evaluate SL/TP; close by `position_id` | `scripts/position_manager.py` |
| Capital | Master pool + wallet balances + ledger; reserve/release/realize/mark | `scripts/capital.py` |
| Reconcile | Read-only invariant checker (`--strict`) | `scripts/reconcile_capital.py` |
| DB | Alembic SoT; `db/schema.sql` human mirror of head | `migrations/`, `db/schema.sql` |
| Risk | Equity Risk Engine (realized+unrealized vs UTC SoD); soft `check_daily_loss` compatibility only | `risk_engine.py`, `mock_exchange.py` |
| Kill Switch | Persistent singleton; hard deny inside `place_order` open TX | `kill_switch.py` |
| Emergency Flatten | Sync best-effort closes via `close_position` (not all-or-nothing) | `emergency_control.py` |
| Orchestration | One Daily Report workflow JSON | `n8n/workflows/daily_report.json` |
| Deploy | Compose: postgres, migrate, n8n, python-agent | `docker-compose.yml` |
| Tests | pytest (Phase 0–6); session `{POSTGRES_DB}_test` isolation | `tests/`, `scripts/test_*.py` |

**Important behavioral facts (current code):**

- Agent calls `position_manager.tick` before opens; closes via SL/TP / `close_position`
- Positions have `side`, side-aware PnL, SL/TP absolute prices
- `wallet_id` is UUID FK to `wallets`
- `place_order` always sets `positions.order_id`; production inserts only via `mock_exchange`
- `EXECUTION_DELAY_SEC` sleeps **before** acquiring a DB connection
- Capital mutations go through `scripts/capital.py` only (runtime)

---

## 4. Phase status (audit numbering)

| Phase | Name | Status |
|---|---|---|
| 0–4 | Foundation → Capital model | Complete |
| 4.5 | Capital Integrity | Complete (`20260729_0005`) |
| **5** | **Execution & Position Lifecycle** | **Complete (`20260802_0007`)** |
| **6** | **Equity Risk Engine + Kill Switch + Flatten** | **Complete (`20260802_0008`)** |
| **7** | **Pre-Testnet Safety, Auditability & Evidence Hardening** | **In progress / Owner-gated (`20260804_0009`)** |
| **8** | **Exchange Adapter & Testnet Parity** | **Implemented, UNVERIFIED (`20260807_0011`)** — hermetic tests only; no live testnet evidence, no mainnet |
| **8A** | **Paper Exchange Adapter** | **Complete (`20260807_0011`, no new migration)** — simulated venue on `price_feed`; does **not** validate real Testnet |
| Next | Cost model (fees/slippage) / Copy / Alpha / later roadmap | **NOT AUTHORIZED** |

Phase 8 code exists but the real venue has never been contacted: the Bybit
adapter requires credentials that are unset, and CI runs against `NullAdapter`
or `PaperExchangeAdapter`. Live testnet operation stays unauthorized until the
Phase 8 exit gate in `docs/EXCHANGE_ADAPTER.md` §12 is satisfied. Phase 8A
proves execution-path correctness offline; it is **not** Testnet validation.

See `IMPLEMENTATION_ROADMAP.md`, `docs/RISK_CONTROLS.md`, `docs/EXECUTION_LIFECYCLE.md`, `docs/CAPITAL_INTEGRITY.md`, and `docs/EXCHANGE_ADAPTER.md`.

---

## 5. Contracts & authorities

| Concern | Authority |
|---|---|
| Open (legacy paper) | `mock_exchange.place_order` |
| Close (legacy paper) | `mock_exchange.close_position` |
| Open (venue / paper adapter) | `execution_engine.ExecutionEngine.open_position` |
| Close (venue / paper adapter) | `execution_engine.ExecutionEngine.close_position` (settles via `mock_exchange.close_position`) |
| Exchange I/O | `scripts/exchange_adapter/*` — the only outbound door (`paper` never calls Bybit) |
| Capital writes | `scripts/capital.py` |
| Fill price / margin basis | venue `avg_fill_price` (`signal_price` is estimate only) |
| Reconcile | `scripts/reconcile_capital.py`, `ExecutionEngine.reconcile_positions` |
| Schema evolution | Alembic (`db/schema.sql` is mirror only) |

Legacy unlinked FILLED orders / NULL-`order_id` funded positions are non-fatal **only** when listed in `phase5_legacy_unlinked_*`. New violations are critical under `--strict`. Venue-opened positions link through `positions.exchange_order_id` instead of `order_id`; a check constraint forbids both.

---

## 6. Deferred risk register (post–Phase 6)

| Risk | Classification |
|---|---|
| Soft daily-loss check (realized-only) | Compatibility pre-check only; authority is Phase 6 equity Risk Engine |
| Risk outside final atomic open TX | Addressed (`assert_open_allowed` inside `place_order` TX) |
| Persistent Kill Switch | Addressed (`kill_switch_state` / events; hard deny on opens) |
| Emergency Flatten | Addressed (sync best-effort; not all-or-nothing) |
| Equity mark vs concurrent `mark_unrealized` | Accepted for paper; risk reads DB equity snapshot under lock |
| Stale / missing mark price on risk decisions | **Addressed in Phase 7** — `MARK_MISSING` / `MARK_STALE` deny new exposure |
| Master capital expansion vs fixed UTC SoD | **Addressed in Phase 7 Policy B** — allocate into risk equity adjusts same-day SoD |
| No atomic intent/reservation architecture | Historical Phase 4–6 open path already transactional; future async exchange execution deferred to Exchange Integration phase |
| No fees / slippage / funding | **Partly addressed in Phase 8** — venue fees and slippage are *recorded* per order; no cost model applies them, so PnL remains GROSS / PRE-COST |
| No real exchange execution | **Implemented, unverified in Phase 8** — adapter + execution path exist; no live testnet evidence |
| Paper fill price stale after delay | Accepted for paper; venue path uses `avg_fill_price` instead |
| Risk gate not held across venue round-trip | Accepted and deliberate — holding `risk_control_lock` over network I/O would delay kill-switch activation; gate is authoritative at send time |
| Venue state unresolvable after retries | Fail-closed — order marked `UNKNOWN` and kill switch armed; capital never guessed |
| Partial close of a venue position | Refused — capital model settles a position once |

---

## 7. Test isolation note

Integration tests use dedicated `{POSTGRES_DB}_test`, recreated each pytest session,
with **function-scoped TRUNCATE reset** for capital/risk suites (P7-001). Live paper
DB must not be mutated. Double-confirm escape hatch:
`PYTEST_USE_LIVE_DB=1` **and** `PYTEST_ALLOW_LIVE_DB=1` (forbidden under `CI=1` /
`PHASE7_VERIFY=1`). Clean verify: `python scripts/verify_clean_db.py`.

---

## 8. Open decisions (Owner)

Testnet operation / Mainnet / Copy / Alpha require **explicit Owner approval**.
Phase 8 delivers the Exchange Adapter code only; it does **not** authorize
running against a live venue.

Open items for the Owner:

1. Final roadmap number for the cost model (fees/slippage), which previously
   held the "Phase 8" label. See the numbering note in
   `IMPLEMENTATION_ROADMAP.md`.
2. Approval to run the Phase 8 exit-gate integration session against Bybit
   testnet (`docs/EXCHANGE_ADAPTER.md` §12), which requires testnet credentials.
