# IMPLEMENTATION ROADMAP

**Project:** Multi-Agent Algorithmic Copy-Trading System  
**Companion doc:** `PROJECT_UNDERSTANDING.md`  
**Rule:** No phase starts without Owner approval. After each phase: run tests → report → summarize → regression check → wait for next approval.  
**First executable phase after approval:** Phase 0 only.

---

## Phase 0 — Repository & Safety Baseline

### Objective
Establish a safe, reproducible repo baseline before any domain changes: git, ignore rules, secret hygiene, documentation anchors, no public credential leakage.

### Dependencies
- None (first phase)
- Owner approval of this roadmap

### Files Affected
- `copy-trading/.gitignore` (verify/extend)
- `copy-trading/.env.example` (ensure complete placeholders, no secrets)
- `copy-trading/PROJECT_UNDERSTANDING.md` (already created)
- `copy-trading/IMPLEMENTATION_ROADMAP.md` (this file)
- Possibly root-level git init under `copy-trading/` or monorepo root (Owner choice at execution time)
- Optional: `SECRET_SCAN.md` or short `SECURITY_BASELINE.md` (only if needed for checklist; prefer minimal)

### Database Changes
- None

### API/Interface Changes
- None

### Implementation Tasks
1. Confirm `.env` is gitignored and not staged
2. `git init` in agreed root
3. Audit working tree for secrets (tokens, passwords, bot keys)
4. Ensure `.env.example` lists required keys with `CHANGE_ME` only (`POSTGRES_*`, `N8N_*`, future placeholders documented)
5. Record that current `.env` credentials should be treated as sensitive; rotate if history/exposure risk exists
6. Do **not** commit real `.env`
7. Initial commit of non-secret project files only (if Owner requests commit in Phase 0)

### Tests
- Manual: `git status` shows `.env` untracked/ignored
- Manual: grep/scan for high-entropy secrets in tracked files
- No runtime behavior change expected

### Acceptance Criteria
- [ ] Git repository exists
- [ ] `.env` ignored
- [ ] No secrets in tracked files
- [ ] Docs present: Understanding + Roadmap
- [ ] Compose still starts conceptually unchanged (no functional refactor required)

### Risks
- Accidental commit of `.env`
- Unclear git root (`copy_tr` vs `copy-trading`)

### Rollback Strategy
- Delete `.git` if init was wrong location (only if nothing pushed; Owner-confirmed)
- Revert doc-only commits

---

## Phase 1 — Database & Migration Foundation

### Objective
Introduce Alembic versioned migrations; keep `schema.sql` as bootstrap/dev aid only; establish migration workflow for all future schema changes.

### Dependencies
- Phase 0

### Files Affected
- New: `alembic.ini`, `alembic/env.py`, `alembic/versions/*`
- `db/schema.sql` (document as bootstrap-only; avoid dual-source drift)
- `scripts/Dockerfile` / `docker-compose.yml` (optional migrate service or startup note)
- `requirements` / dependency pin file (to be added)

### Database Changes
- Baseline migration matching current schema (wallets, paper_orders, positions, daily_stats, existing indexes)
- No behavioral domain redesign yet (that starts Phase 2) unless needed for Alembic bootstrap cleanliness

### API/Interface Changes
- None for trading APIs
- Ops: `alembic upgrade head` / `downgrade` commands documented

### Implementation Tasks
1. Add Python dependency management (`requirements.txt` or equivalent)
2. Wire Alembic to `DATABASE_URL` / Postgres env
3. Create baseline migration from current schema
4. Define rule: all future schema changes go through Alembic
5. Document bootstrap path: empty volume → migrate (preferred) vs `schema.sql` init

### Tests
- Migration up on empty DB
- Migration downgrade (as far as baseline allows)
- Idempotent re-run / second environment apply

### Acceptance Criteria
- [ ] Alembic installed and configured
- [ ] Fresh DB reachable via migrations alone
- [ ] Baseline revision checked in
- [ ] `schema.sql` role clarified (bootstrap/dev only)

### Risks
- Drift between `schema.sql` and Alembic
- Existing docker volumes created from old init

### Rollback Strategy
- `alembic downgrade` where supported
- Restore volume from backup if needed
- Keep Phase 0 git tag/commit as restore point

---

## Phase 2 — Correct Trading Domain Model

### Objective
Fix core domain correctness: position side/direction, wallet UUID FK SoT, long/short PnL, closed-trade-oriented stats foundations.

### Dependencies
- Phase 1

### Files Affected
- Alembic new revision(s)
- `scripts/mock_exchange.py` (PnL, inserts)
- `scripts/agent_trader.py` (wallet id types)
- `scripts/test_*.py` → migrate toward pytest in later phase; update interim scripts
- Possibly new `scripts/domain/` or `app/` package layout (minimal, only if needed)

### Database Changes
- `positions.side` (or `direction`) `CHECK IN ('BUY','SELL')` / LONG-SHORT mapping documented
- `positions.wallet_id` → `UUID FK wallets(id)` (with backfill strategy)
- `paper_orders.wallet_id` → `UUID FK wallets(id)`
- Indexes on `wallet_id`, `closed_at`, open-position lookups
- Optional: `exit_price`, `close_reason` stubs if cheap and useful for Phase 3

### API/Interface Changes
- `place_order` / `close_position` require valid wallet UUID and side-aware PnL
- Reject unknown wallets (FK / explicit validation)

### Implementation Tasks
1. Migration: add side; convert wallet_id to UUID FK with data migration plan
2. Implement long/short PnL formulas
3. Ensure `wallets` is SoT; seed strategy for paper wallets
4. Stop treating free-text wallet ids as valid
5. Align win-rate query to wallet UUID + net PnL when fees arrive (gross OK until Phase 8, with clear TODO)

### Tests
- Unit: long PnL, short PnL
- DB: FK reject orphan wallet_id
- Integration: open+close round trip both sides

### Acceptance Criteria
- [ ] Every position has side
- [ ] Short PnL correct
- [ ] Orphan wallet_id impossible
- [ ] Existing close race behavior still passes

### Risks
- Breaking existing paper rows / TEXT wallet ids
- Agent query breakage during transition

### Rollback Strategy
- Alembic downgrade
- Feature flag / dual-read only if absolutely required (prefer clean cut in paper)

---

## Phase 3 — Position Lifecycle

### Objective
Complete Open → Monitor → Close. Agent/system must not be open-only. Introduce Position Manager with paper close reasons.

### Dependencies
- Phase 2

### Files Affected
- New: position manager module
- `scripts/agent_trader.py` (orchestrate monitor/close)
- `scripts/mock_exchange.py` (close reasons, metadata)
- Alembic if `close_reason`, SL/TP columns needed

### Database Changes
- `close_reason` enum/text
- Optional SL/TP fields on positions
- Ensure schema allows future partial close (e.g. keep full qty; later `closed_qty`)

### API/Interface Changes
- `PositionManager.tick()` or equivalent
- Close API accepts reason: `STOP_LOSS | TAKE_PROFIT | RISK | KILL_SWITCH | ADMIN | LEAD_CLOSE` (lead may be stub)

### Implementation Tasks
1. Define paper close policy defaults (document chosen Open Decision #3)
2. Implement monitor loop using mark price
3. Wire SL/TP enforcement (params currently discarded must become real)
4. Ensure opens still go through execution path
5. Record close reason on position

### Tests
- SL triggers close
- TP triggers close
- Admin/manual close
- No orphan opens without manager attention in test harness

### Acceptance Criteria
- [ ] System can open and close without test-only callers
- [ ] Close reasons persisted
- [ ] Partial close still not implemented but not blocked by schema

### Risks
- Flapping closes on noisy WS prices
- Interaction with future kill-switch flatten policy

### Rollback Strategy
- Disable manager via config; revert to open-only only as emergency (not desired)
- Migration downgrade for new columns

### Phase 3 — Position Lifecycle / Paper Close Policy

**Status:** Task 1 policy documented; **Task 2 lifecycle foundation implemented** (Alembic `20260728_0002`); **Task 3 Position Manager implemented** (Alembic `20260728_0003`). Phase 4 capital model: see Phase 4 section.  
**Resolves:** Open Decision #3 (Position Manager close policy in paper without lead).  
**Authority:** Owner-approved defaults for Tasks 2+.

This section records **policy decisions** and **Task 2–3 implementation status**. Unless explicitly labeled *Current implementation*, policy engine behavior below is not live yet.

#### Current implementation (after Phase 3 Task 3)

| Fact | State today |
|---|---|
| Agent lifecycle | `agent_trader.tick()` calls `position_manager.tick()` **before** wallet ranking / open path |
| Position Manager | **`scripts/position_manager.py`** — monitors `closed_at IS NULL` positions; closes via `close_position()` |
| `positions.side` | **Persisted** (`CHECK IN ('BUY','SELL')`, NOT NULL) |
| Short PnL | **Side-aware** in `close_position` for new closes |
| `stop_loss_price` / `take_profit_price` | **Nullable columns**; BUY **and** SELL opens persist both from pct at open |
| SELL automatic SL/TP | **Supported** — side-aware formulas; manager evaluates BUY and SELL |
| `exit_price` / `close_reason` | **Persisted on close**; NULL while open; lifecycle CHECK enforced |
| Close path | `close_position(close_reason=…)` remains lifecycle authority (`SELECT … FOR UPDATE`) |
| Automatic close reasons | **STOP_LOSS**, **TAKE_PROFIT** (when persisted levels exist); **ADMIN** manual only |
| Reserved close reasons | **RISK**, **KILL_SWITCH**, **LEAD_CLOSE** — accepted by API/schema; **not auto-triggered** |
| SL/TP trigger priority | When both fire: **STOP_LOSS wins over TAKE_PROFIT** |
| Legacy open positions | No SL/TP backfill; NULL levels remain open unless manual ADMIN close |
| Mark price | `price_feed.py` in-memory latest trade price per symbol (read by Position Manager) |
| Partial close | Not implemented; no `closed_qty` column |

#### Migration backfill policy (Task 2)

- **Side:** join `positions` → `paper_orders` on `wallet_id`, `symbol`, `entry_price=price`, `qty`, `status='FILLED'`; when multiple orders match, assign each position to the temporally closest unused FILLED order (greedy, deterministic); fail if any position remains unmapped (no silent BUY/SELL guess).
- **Legacy closed rows:** `exit_price = entry_price + (pnl / qty)` (historical long-only formula); `close_reason = 'ADMIN'` (no recorded reason at close time).

#### Current implementation (baseline before Task 2 — historical reference)

| Fact | State before Task 2 |
|---|---|
| `positions.side` | Did not exist |
| Short PnL | Long-only formula in `close_position` |
| `close_reason` / `exit_price` | Did not exist on `positions` |

#### 1. Position monitoring order (policy)

During each agent cycle, the future Position Manager **must evaluate existing open positions before opening new positions**.

```text
price_feed
    ↓
agent cycle
    ↓
PositionManager.tick()          ← implemented (Task 3)
    ↓
monitor existing open positions
    ↓
evaluate close triggers
    ↓
close triggered positions
    ↓
existing open/order path        ← current agent behavior today
```

*Current implementation:* agent calls `position_manager.tick(conn)` before wallet ranking and `place_order`; open path unchanged otherwise.

#### 2. Close reason taxonomy

Supported close reasons (domain/API vocabulary):

```text
STOP_LOSS
TAKE_PROFIT
RISK
KILL_SWITCH
ADMIN
LEAD_CLOSE
```

#### 3. Close reason priority (policy)

When multiple reasons could apply, evaluation follows this **semantic priority** (highest first):

| Priority | Reason | Category |
|---:|---|---|
| 1 | `KILL_SWITCH` | Future system-level forced close |
| 2 | `RISK` | Future Risk Engine forced close |
| 3 | `ADMIN` | Manual / external control |
| 4 | `LEAD_CLOSE` | Future copy-trading lead-close event |
| 5 | `STOP_LOSS` | Market-based protective trigger |
| 6 | `TAKE_PROFIT` | Market-based profit-taking trigger |

`ADMIN` = explicit manual/admin close.  
`LEAD_CLOSE` = future lead-wallet close propagation (API/domain stub in Phase 3).

*Current implementation:* no `close_reason` field; no reason-based close from the agent.

#### 4. SL / TP trigger semantics (policy)

For a position with **valid persisted** `stop_loss_price` and `take_profit_price`, the future Position Manager compares the latest mark price.

**Long / BUY semantics** (policy target once direction is persisted):

```text
STOP_LOSS:   mark_price <= stop_loss_price
TAKE_PROFIT: mark_price >= take_profit_price
```

**Hard rule — missing `positions.side`:**

> Full directional SL/TP semantics for short positions are blocked until position direction (`side`) is persisted as part of the appropriate trading-domain phase.

- Do **not** invent or silently assume short-position SL/TP behavior in Phase 3 Tasks 2+ until `side` exists on `positions`.
- Phase 3 Task 1 does **not** add `positions.side`.
- Phase 3 market-based automation (`STOP_LOSS` / `TAKE_PROFIT`) applies only where persisted direction and SL/TP levels are unambiguous per the trading-domain work that introduces `side`.

*Current implementation:* `stop_loss_price` / `take_profit_price` columns exist; BUY `stop_loss_price` persisted at open from `stop_loss_pct`; automatic BUY SL/TP enforced when levels present; SELL auto SL/TP not implemented.

#### 5. Simultaneous SL + TP trigger (policy)

If both `STOP_LOSS` and `TAKE_PROFIT` would be considered triggered for the same position at the same observed mark price:

```text
STOP_LOSS wins over TAKE_PROFIT
```

Rationale: conservative, risk-first behavior.

*Current implementation:* Position Manager evaluates BUY positions; `STOP_LOSS` wins when both would trigger.

#### 6. Mark price source (policy)

- The future Position Manager reads the **latest available mark/current price** from the existing `price_feed` module (`scripts/price_feed.py`).
- Monitoring is point-in-time against the in-memory latest trade price; there is **no** separate historical candle engine in Phase 3.
- This task does **not** modify `price_feed.py` or add a new pricing service.

```text
Position Manager → read current mark price → evaluate open positions
```

*Current implementation:* Position Manager reads mark price via `get_mark_price()` from `price_feed.prices` for monitoring and closes.

#### 7. Close reason semantics (policy)

| Reason | Semantics | Phase 3 implementation path |
|---|---|---|
| `STOP_LOSS` | Automatic market-based protective close | **Active** (Tasks 2+) |
| `TAKE_PROFIT` | Automatic market-based profit-taking close | **Active** (Tasks 2+) |
| `ADMIN` | Explicit manual/admin close | **Active** (Tasks 2+) |
| `RISK` | Reserved: future Risk Engine forced close | **Reserved** — API/domain only |
| `KILL_SWITCH` | Reserved: future global/per-wallet kill-switch forced close | **Reserved** — API/domain only |
| `LEAD_CLOSE` | Reserved: future real lead/copy-trading close propagation | **Reserved** — API/domain only |

`RISK`, `KILL_SWITCH`, and `LEAD_CLOSE` must be representable in close APIs and schema, but **must not** have triggering logic in Phase 3.

#### 8. Existing open positions (policy)

When the Position Manager is introduced (Tasks 2+):

- Existing open positions **must not** be silently closed merely because they predate Phase 3.
- If an open position has **no** persisted SL/TP metadata, the Position Manager **must not** invent SL/TP levels for it.
- Such positions remain open until an explicitly supported close trigger occurs.
- They may be closed manually with reason `ADMIN`.
- Future `RISK` / `KILL_SWITCH` / `LEAD_CLOSE` mechanisms may close them once those phases are implemented.

*Current implementation:* Position Manager does not invent SL/TP for legacy opens; positions with NULL levels stay open.

#### 9. No-flapping rule (policy)

- A position that has already been closed (`closed_at IS NOT NULL`) must **never** be evaluated again as open.
- The future Position Manager monitors only rows where `closed_at IS NULL`.
- The existing transactional `close_position()` locking (`SELECT … FOR UPDATE`) remains the authoritative protection against concurrent/double close.

*Current implementation:* Position Manager queries only `closed_at IS NULL`; `close_position()` row lock remains authoritative.

#### 10. No partial close (policy)

```text
One position → one full close event
```

- Phase 3 does **not** implement partial close.
- Do **not** introduce a `closed_qty` column in Phase 3.
- Schema may remain compatible with future partial-close work (e.g. full `qty` on open; later phases may add `closed_qty` or close events), but that work belongs to a **later phase**.

*Current implementation:* every close is full-qty via `close_position`; no partial-close path.

#### 11–12. Active vs reserved reasons (summary)

**Active in Phase 3 implementation path:** `STOP_LOSS`, `TAKE_PROFIT`, `ADMIN`.

**Reserved for future phases (domain/API-ready only in Phase 3):** `RISK`, `KILL_SWITCH`, `LEAD_CLOSE`.

#### Task completion notes

- [x] Implementation Task 1: Define paper close policy defaults (Open Decision #3) — **documented above**
- [x] Implementation Task 2: Position lifecycle foundation — `positions.side`, `exit_price`, `close_reason`, lifecycle CHECKs, `close_position` API (Alembic `20260728_0002`)
- [x] Implementation Task 3: Position Manager — monitor loop, SL/TP wire, agent orchestration (Alembic `20260728_0003`, `scripts/position_manager.py`)

---

## Phase 4 — Capital & Balance Model

### Objective
Implement Master Pool + Isolated Agent Allocation with real available/reserved balances; prevent negative balance and over-allocation.

### Dependencies
- Phase 2 (FK wallets); Phase 3 strongly preferred so closes release margin correctly

### Files Affected
- Alembic revisions
- Execution engine / mock_exchange capital reservation
- New capital/ledger module
- Agent sizing path

### Database Changes
- Wallet capital fields and/or `wallet_balances` + ledger entries
- Master pool representation
- Constraints / transactional updates for available/reserved

### API/Interface Changes
- `reserve_margin`, `release_margin`, `realize_pnl` APIs
- Open path must reserve inside transaction

### Implementation Tasks
1. Choose schema shape (resolve Open Decision #4)
2. Initialize allocations for paper agents
3. Enforce spend ≤ allocation
4. Update equity fields on close / mark
5. Make negative balance impossible under concurrency

### Tests
- Cannot open beyond available
- Concurrent opens cannot double-spend margin
- Close restores/settles balances correctly

### Acceptance Criteria
- [x] Per-agent isolation proven by tests
- [x] Master + allocation model documented and enforced
- [x] Hardcoded EQUITY constant removed from authority path (config/DB instead)

### Risks
- Accounting bugs → silent insolvency in paper (teaches bad Mainnet habits)
- Lock contention at 50+ agents

### Rollback Strategy
- Migration downgrade; temporary read-only paper halt

### Phase 4 — Capital Schema / Master Pool + Isolated Allocation

**Status:** Task 1 schema shape documented; Tasks 2–5 implemented (Alembic `20260728_0004`).  
**Resolves:** Open Decision #4 (Capital schema shape).  
**Authority:** Owner-approved defaults for Tasks 2+.

#### Decision (Open Decision #4)

**Chosen shape:** separate capital tables — **not** capital columns on `wallets`.

| Table | Role |
|---|---|
| `master_pool` | Singleton (id=1): `total_capital`, `allocated_capital`, `available_capital` |
| `wallet_balances` | 1:1 with `wallets`: `initial_capital`, `current_equity`, `available_balance`, `reserved_margin`, `unrealized_pnl`, `realized_pnl` |
| `capital_ledger` | Append-only audit of ALLOCATE / RESERVE / RELEASE / REALIZE_PNL / MARK_UNREALIZED / MASTER_INIT |
| `positions.reserved_margin` | Margin locked at open so close releases the exact reserved amount |

**Rationale:** keeps `wallets` as identity SoT; enables row-level `FOR UPDATE` isolation per agent; ledger provides audit trail against silent insolvency; matches roadmap “new capital/ledger module.”

#### Current implementation (after Phase 4 Tasks 2–5)

| Fact | State today |
|---|---|
| Schema | Alembic `20260728_0004` — `master_pool`, `wallet_balances`, `capital_ledger`, `positions.reserved_margin` |
| Capital module | **`scripts/capital.py`** — `allocate_to_wallet`, `reserve_margin`, `release_margin`, `realize_pnl`, `mark_unrealized`, `initialize_paper_allocations` |
| Open path | `place_order` reserves margin `(qty * price) / leverage` inside the same DB transaction as order+position |
| Close path | `close_position` releases reserved margin then realizes PnL into `wallet_balances` |
| Mark path | `position_manager.tick` calls `mark_unrealized` before SL/TP evaluation |
| Authority equity | `check_daily_loss` / `daily_stats` drawdown use `master_pool.total_capital` (DB), not hardcoded EQUITY |
| Negative balance | CHECK `available_balance >= 0` + `SELECT … FOR UPDATE` on `wallet_balances`; overspend raises `InsufficientAvailableBalance` |
| Legacy open positions | No reserved-margin backfill; `reserved_margin` NULL/0 → release 0 on close; wallets get balances via migration seed / `initialize_paper_allocations` |

#### Paper allocation defaults

- Master pool total: `MASTER_POOL_USDT` env (default `100000`)
- Per-agent allocation: `AGENT_ALLOCATION_USDT` env (default `10000`), capped by master available
- Migration seeds singleton `master_pool` and allocates to existing `wallets` rows

#### Task completion notes

- [x] Implementation Task 1: Choose schema shape (Open Decision #4) — **documented above**
- [x] Implementation Task 2: Initialize allocations for paper agents — migration seed + `initialize_paper_allocations`
- [x] Implementation Task 3: Enforce spend ≤ allocation — `reserve_margin` in `place_order`
- [x] Implementation Task 4: Update equity fields on close / mark — `realize_pnl` + `mark_unrealized`
- [x] Implementation Task 5: Negative balance impossible under concurrency — CHECKs + row locks + concurrent tests

---

## Phase 4.5 — Capital Integrity & Reconciliation

**Status:** Complete (Alembic `20260729_0005`).  
**Goal:** Prove Phase 4 capital is atomic, concurrency-safe, idempotent where required, and reconcilable before Phase 5.

### Deliverables
- Contract: `docs/CAPITAL_INTEGRITY.md`
- Migration: partial unique ledger indexes + wallet conservation/equity CHECKs
- Hardened `scripts/capital.py` (TX/savepoint wrappers, reject over-release, duplicate settlement)
- Reconciliation: `scripts/reconcile_capital.py` (`--json`, `--strict`)
- Integrity / concurrency / rollback tests

### Explicitly out of scope
- Risk Engine, kill-switch, equity-based daily loss replacement (next authorized phases)
- Production/mainnet execution

---

## Phase 5 — Execution & Position Lifecycle Integrity

**Status:** Complete (Alembic head `20260802_0007`).  
**Contract:** `docs/EXECUTION_LIFECYCLE.md`  
**Goal:** Enforce paper open/close lifecycle integrity: order state machine, 1:1 order↔position linkage, wallet-scoped lookup, failure atomicity, concurrent settlement, legacy orphan registry, reconcile critical vs legacy split.

### Deliverables
- Migrations `20260802_0006` (order_id FK/UNIQUE, status CHECK, backfill) + `20260802_0007` (legacy registries)
- `place_order`: delay outside DB acquire; TX PENDING→FILLED + position.order_id + reserve
- REJECTED / FAILED terminal orders without positions
- `close_position`: FOR UPDATE, single settlement, `PositionAlreadyClosed`
- Reconcile Phase 5 checks; legacy only via explicit `phase5_legacy_unlinked_*` membership
- Tests: `tests/test_phase5_execution_lifecycle.py` (+ Phase 4.5 concurrency regression)

### Explicitly out of scope (deferred)
- Risk Engine / SoD / equity daily loss (next phase)
- Persistent Kill Switch
- Atomic Open redesign / fees / real exchange

---

## Phase numbering note (Pre-Phase-6)

**Implemented Phase 5** above is Execution Lifecycle — **COMPLETE**.  
**Phase 6** (Equity Risk Engine + Kill Switch + Emergency Flatten) is **COMPLETE** at Alembic `20260802_0008`. See `docs/RISK_CONTROLS.md`.

---

## Phase 5 (roadmap legacy label) — Risk Engine

**Status:** Superseded by **Phase 6 — Equity Risk Engine + Kill Switch** (Alembic `20260802_0008`). See `docs/RISK_CONTROLS.md`.

---

## Phase 6 — Persistent Kill-Switch (+ Equity Risk Engine)

**Status:** Complete (Alembic `20260802_0008`).  
**Contract:** `docs/RISK_CONTROLS.md`

Atomic open path (risk + kill + reserve + order + position in one TX; delay
outside connection hold) was completed in Phases 5–6 and is **not** reopened here.

---

## Phase 7 — Pre-Testnet Safety, Auditability & Evidence Hardening

**Status:** Implementation (Alembic head `20260804_0009`).  
**Objective:** Fail-closed marks, SoD Policy B, ledger provenance + append-only,
structured events/metrics/alerts, reconcile history, flatten residual NO-GO,
hermetic tests + clean DB verification. **PAPER ONLY.**

### Does NOT authorize
- Exchange Adapter / Testnet / Mainnet / real capital
- Fees / slippage / funding cost model
- Copy Trading / Alpha / Frontend / production deploy

### Key deliverables
1. Hermetic critical test isolation (`tests/db_isolation.py`)
2. Timestamped marks + `MAX_MARK_AGE_SEC` freshness (`price_feed.py`)
3. Fail-closed `MARK_MISSING` / `MARK_STALE` on `assert_open_allowed`
4. SoD Policy B: allocate into risk equity adjusts same-day SoD
5. Ledger provenance + DB append-only trigger
6. `observability.py` events / correlation IDs / metrics / critical alerts
7. `reconciliation_runs` persistence
8. Emergency flatten aggregate `SAFE` / `NOT_SAFE`
9. `scripts/verify_clean_db.py`
10. Documentation realignment (this section)

### Future exchange async execution
Deferred to a later **Exchange Integration** phase (not Phase 7).

---

## Phase 8 — Exchange Adapter & Testnet Parity

**Status:** Implementation complete, **unverified** (Alembic head `20260807_0011`).  
**Contract:** `docs/EXCHANGE_ADAPTER.md`

**Objective:** an abstract exchange boundary plus a Bybit testnet adapter, with
venue fills — not signal prices — as the authority for margin, entry price and
PnL. Legacy paper path (`mock_exchange`) unchanged.

### Delivered
1. `exchange_adapter/interface.py` — abstract adapter, transport models, errors
2. `exchange_adapter/null_adapter.py` — deterministic scriptable venue for CI
3. `exchange_adapter/reconnect.py` — capped backoff + silent-socket detection
4. `exchange_adapter/bybit_adapter.py` — Bybit V5 testnet REST + websockets
5. `execution_engine.py` — risk gate → adapter → capital, fail-closed recovery
6. `exchange_orders` + append-only `exchange_fills` + `positions.exchange_order_id`
7. Venue checks in `reconcile_capital.py`; venue-vs-DB position reconciliation
8. Hermetic Phase 8 tests (no sockets, no credentials)
9. Phase 8A-1: `stop_loss_pct` / `take_profit_pct` on `exchange_orders` (`20260807_0011`)

### Does NOT authorize
- Mainnet or real capital (`BYBIT_ALLOW_MAINNET` refused by default)
- Fees / slippage / funding **cost model** — venue fees are recorded, not applied;
  PnL stays GROSS / PRE-COST
- Copy Trading / Alpha / Frontend / production deploy

### Exit gate (open)
1. Live testnet run: network drop mid-order, partial fills, rejects, duplicate
   sends, websocket kill
2. Numeric paper-vs-testnet deviation report (PnL, slippage, fees)
3. Reduce-only / position-mode verified against a real testnet account

---

## Phase 8A — Paper Exchange Adapter

**Status:** Complete (no new migration; head remains `20260807_0011`).  
**Contract:** `docs/EXCHANGE_ADAPTER.md` §13

**Objective:** validate the execution engine, capital, risk, recovery and
reconciliation against a simulated venue that fills at `price_feed` marks —
without sending real orders or requiring exchange credentials.

### Delivered
1. `exchange_adapter/paper_adapter.py` — `PaperExchangeAdapter` (`name=paper`)
2. `exchange_adapter/factory.py` — `EXCHANGE_ADAPTER` / `PAPER_MODE` selection with
   hard safety (paper never reads Bybit credentials or instantiates Bybit)
3. Zero-fee / zero-slippage MARKET fills at authoritative `price_feed` mark
4. Deterministic `PaperFailureMode` injection exercising real engine recovery
5. Hermetic E2E suite `tests/test_phase8a_paper_adapter.py`
6. Opt-in live-mark and soak suites (skipped unless `PAPER_LIVE_MARK=1` /
   `PAPER_SOAK=1`)

### Does NOT claim
- Real Testnet validated
- Mainnet ready
- Fee / slippage / funding cost model

### Relationship
- Does **not** replace `NullAdapter` (CI scripting) or `mock_exchange` (legacy
  paper path).
- Uses the same `ExchangeAdapter` + `ExecutionEngine` path as Bybit would.

---

## Phase numbering note (Phase 8)

The owner's ground-truth plan assigns **Phase 8 = Exchange Adapter & Testnet
Parity** (implemented above). The section below kept the earlier label
"Phase 8 — Fees & Slippage" and is now the **cost-model phase**, unscheduled and
awaiting an owner decision on its final number. Following the Phase 5 precedent,
no sections were renumbered.

---

## Phase 8 (roadmap legacy label) — Fees & Slippage

**Status:** Not started. Superseded as "Phase 8" by Exchange Adapter above;
final numbering is an owner decision.

### Objective
Model trading fees and slippage in paper PnL (funding optional per Open Decision #10). Net PnL becomes win/risk/report authority.

### Dependencies
- Phase 2 (side-aware PnL), Phase 4–5 preferred

### Files Affected
- PnL module / mock exchange
- Config for bps
- daily_stats aggregation inputs

### Database Changes
- Store fee, slippage, (optional funding) on closes/orders
- Net vs gross fields if useful

### API/Interface Changes
- Quote/fill applies slippage
- Close returns net PnL breakdown

### Implementation Tasks
1. Choose paper fee/slippage schedule (Open Decision #2)
2. Apply on open/close as appropriate
3. Win definition = net > 0
4. Risk engine consumes net costs
5. Decide funding in/out for first 30-day paper

### Tests
- Unit fee/slip math long & short
- Win-rate uses net
- Risk includes costs

### Acceptance Criteria
- [ ] Gross-only wins no longer used for qualification
- [ ] Breakdown auditable per trade
- [ ] Documented parameter set

### Risks
- Over/under-realistic params skew 30-day results

### Rollback Strategy
- Config set fees/slip to 0 (explicit) if needed; prefer not to for paper honesty

---

## Phase 9 — Agent Architecture

### Objective
Logical multi-agent isolation inside one process; pluggable Signal Provider; replace random as default-but-swappable simulation provider; prepare scale-out seams.

### Dependencies
- Phases 3–7

### Files Affected
- Agent runtime refactor
- `signals/` providers: `RandomSimulationProvider`, future `LeadWalletProvider`
- Wiring in composition root

### Database Changes
- Possibly agent/provider config tables

### API/Interface Changes
- `SignalProvider` protocol → `NormalizedSignal`
- Trading core accepts only normalized signals

### Implementation Tasks
1. Define `NormalizedSignal` schema
2. Implement provider interface + random provider
3. Per-agent state isolation (capital, kill, positions)
4. Document scale path: process/worker/container per agent
5. Ensure core has zero hard dependency on random/lead

### Tests
- Provider swap smoke test
- Multi-agent isolation tests
- Core unit test with fake provider

### Acceptance Criteria
- [ ] Random is a provider, not core logic
- [ ] One process runs N logical agents cleanly
- [ ] Interface ready for external lead signals

### Risks
- Over-abstraction early; keep interfaces thin

### Rollback Strategy
- Keep random provider; revert wiring

---

## Phase 10 — n8n + Telegram

### Objective
Production-ready UTC daily report + kill + error alerts; git-versioned workflows; secrets via n8n; idempotent execution; deployable provisioning.

### Dependencies
- Phase 6 events; Phase 2+ stats correctness; Phase 8 preferred for net metrics

### Files Affected
- `n8n/workflows/*.json`
- `docker-compose.yml` (import/provision, network, env placeholders)
- Possibly small SQL views for report

### Database Changes
- Idempotency markers / report_runs table (Open Decision #6)
- Ensure `daily_stats` reflects closed trades; per-wallet stats as needed

### API/Interface Changes
- Workflow contracts for summary payload
- Alert payloads for kill/error

### Implementation Tasks
1. Fix report queries (`closed_at`, global + per-wallet, kill status, unrealized)
2. Activate workflow with UTC schedule
3. Retry + error handling + idempotency
4. Telegram daily + kill + error
5. Secrets only in n8n credential store; `.env.example` documents non-secret setup steps
6. Repeatable import/provision approach

### Tests
- Dry-run SQL fixtures
- Idempotent double-trigger does not duplicate notify (marker)
- Timezone boundary cases

### Acceptance Criteria
- [ ] Report active and correct basis
- [ ] Three alert classes exist
- [ ] No hardcoded tokens/chat ids in git
- [ ] Workflow JSON in git

### Risks
- Duplicate Telegram spam
- Credential misconfig on VPS

### Rollback Strategy
- Deactivate workflow in n8n; keep JSON in git

---

## Phase 11 — Structured Logging & Observability

### Objective
Centralized structured logging for ticks, orders, risk denials, kill events, errors — enough to operate 30-day paper safely.

### Dependencies
- Core paths from Phases 3–7; alerts in Phase 10 optional but complementary

### Files Affected
- Logging utility; agent/execution/risk modules
- Compose logging options / doc

### Database Changes
- Optional: none (prefer logs/metrics first)

### API/Interface Changes
- Standard log fields: ts, level, wallet_id, signal_id, order_id, position_id, event

### Implementation Tasks
1. Structured JSON or key=value logs
2. Correlation ids per signal/order
3. Error taxonomy for Telegram error alerts
4. Retention guidance for VPS

### Tests
- Log contract unit tests (required fields present)
- Failure path emits error event

### Acceptance Criteria
- [ ] No reliance on ad-hoc prints for critical path
- [ ] Kill/risk/order events queryable in logs

### Risks
- Log volume on 50+ agents

### Rollback Strategy
- Fall back to previous print logger via config

---

## Phase 12 — Automated Testing

### Objective
Pytest quality gate covering unit/integration/DB/concurrency/kill/PnL/risk; CI if remote exists.

### Dependencies
- Phases 1–8 ideally; can grow incrementally earlier but **gate** is here before stress/30-day

### Files Affected
- `tests/**`
- `pyproject.toml` / pytest config
- CI workflow file (Open Decision #7)
- Retire or wrap legacy scripts

### Database Changes
- Test DB fixtures / transactional tests

### API/Interface Changes
- None (test harness only)

### Implementation Tasks
1. Pytest suite layout
2. Migrate race/smoke into proper tests
3. Add missing unit/integration coverage
4. CI pipeline (if git remote ready)
5. Define make/invoke targets: `test`, `test-integration`

### Tests
- The suite itself is the deliverable; include markers for unit vs integration vs concurrency

### Acceptance Criteria
- [ ] `pytest` green on clean environment
- [ ] Categories required by Owner are present
- [ ] CI configured or explicitly deferred with Owner sign-off

### Risks
- Flaky concurrency tests
- Heavy DB tests slow CI

### Rollback Strategy
- Quarantine flaky tests; do not delete coverage silently

---

## Phase 13 — 50+ Agent Stress Testing

### Objective
Prove system stability and correctness under ≥50 concurrent logical agents.

### Dependencies
- Phase 7 + Phase 12
- Phases 4–6 for capital/risk/kill under load

### Files Affected
- Stress harness scripts/tests
- Pool/config tuning docs
- Possibly compose resource limits

### Database Changes
- None required (indexes already from earlier phases)

### API/Interface Changes
- Harness-only

### Implementation Tasks
1. Spawn 50+ logical agents
2. Measure open/close throughput, lock errors, pool exhaustion, kill correctness
3. Fix bottlenecks found
4. Document hardware sizing for VPS paper

### Tests
- Stress test as gated job (not necessarily every PR)
- Invariants: no negative balance, no double-close, kill honored

### Acceptance Criteria
- [ ] 50+ agents sustained for agreed duration
- [ ] Invariants hold
- [ ] Known limits documented

### Risks
- Masking bugs by weakening assertions
- WS rate limits if each agent hits network poorly

### Rollback Strategy
- Reduce concurrency; hotfix locks/pool; re-run

---

## Phase 14 — 30-Day Paper Trading

### Objective
Always-on paper run ≥30 days with monitoring, kill-switch live, daily reports, no Mainnet.

### Dependencies
- Phases 0–13 acceptance
- Hardened deploy (internal Postgres, secrets, VPS)

### Files Affected
- Deploy docs / compose prod overlay
- Runbooks (restart, override, rotate)

### Database Changes
- Operational only (stats accumulation)

### API/Interface Changes
- None

### Implementation Tasks
1. Deploy always-on paper
2. Verify reports/alerts
3. Monitor Sharpe/DD/win-rate dashboards (even if Alpha not built—compute offline OK)
4. Incident log for 30 days
5. No exchange Mainnet keys in this phase

### Tests
- Soak checks; weekly invariant audits; restart recovery drills

### Acceptance Criteria
- [ ] ≥30 days continuous (or Owner-approved continuity definition)
- [ ] Kill-switch exercised or simulation-drilled
- [ ] Metrics captured for Sharpe/DD gates

### Risks
- VPS downtime invalidating soak
- Silent accounting drift

### Rollback Strategy
- Halt trading; preserve DB snapshot for analysis

---

## Phase 15 — Alpha Engine

### Objective
Ranking + capital allocation + rebalancing engine using net performance and risk metrics. Not primary signal generation.

### Dependencies
- Phase 14 data (or rich paper history)
- Phase 9 provider architecture
- Phase 4 capital model

### Files Affected
- New `alpha_engine` package
- Allocation writer into capital model
- Reports/metrics

### Database Changes
- Allocation history / ranking snapshots

### API/Interface Changes
- `rank_agents()`, `propose_allocations()`, `rebalance()`

### Implementation Tasks
1. Metrics: net win rate, Sharpe, max DD, sample size, consistency
2. Ranking job
3. Allocation constrained by master pool + risk
4. Rebalance cadence + audit log
5. Integrate with signal admission (“Alpha-approved”) without owning signal creation

### Tests
- Deterministic ranking fixtures
- Allocation constraints
- Rebalance does not breach kill/risk

### Acceptance Criteria
- [ ] Rankings reproducible from DB fixtures
- [ ] Allocations sum ≤ master pool
- [ ] Documented metrics definitions

### Risks
- Overfitting to short paper history
- Allocation thrash

### Rollback Strategy
- Freeze allocations; disable rebalance job

---

## Phase 16 — Mainnet Readiness

### Objective
Abstract exchange adapter mature; security/restart/risk/checklists complete; Mainnet only if all gates pass. Concrete exchange still Owner-selected.

### Dependencies
- Phases 14–15 results meeting numeric gates
- Owner selects exchange (Open Decision #1)

### Files Affected
- `ExchangeInterface` + concrete adapter
- `MAINNET_CHECKLIST.md`
- Secrets/deploy overlays
- Integration tests against testnet/sandbox if available

### Database Changes
- Possibly venue order ids, fill reconciliation tables

### API/Interface Changes
- Full `ExchangeInterface` implementation
- Mock remains for paper

### Implementation Tasks
1. Finalize checklist: Sharpe>1.0, DD<15%, stress 50+, kill verified, risk verified, security, restart recovery, exchange tests
2. Implement adapter behind interface
3. Reconciliation & failure modes
4. Network/secret hardening review
5. Explicit Go/No-Go Owner sign-off

### Tests
- Exchange integration (testnet)
- Restart recovery with live order state machine
- Kill-switch on adapter path

### Acceptance Criteria
- [ ] All Mainnet gates green
- [ ] Checklist signed by Owner
- [ ] Paper path still works via Mock
- [ ] **No Mainnet capital until sign-off**

### Risks
- Exchange API quirks; partial fills; funding; liquidations
- Checklist pressure to skip gates

### Rollback Strategy
- Remain on paper/mock; disable adapter in config; no funds at risk if Go not signed

---

## Cross-Phase Execution Protocol

After **each** approved phase:

1. Implement only that phase’s scope  
2. Run that phase’s tests (+ regression of prior critical tests)  
3. Report: what changed, test results, residual risks  
4. Summarize files/migrations  
5. Regression check against Phase Acceptance Criteria of prior phases  
6. **Stop and wait** for Owner approval of the next phase  

**Explicitly forbidden without approval:** jumping ahead, drive-by refactors, Mainnet wiring, committing secrets.

---

## Suggested Dependency Graph (summary)

```
0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8
                ↘           ↘
                 9 ←─────────7
                 ↓
        10 ← 6 + stats
        11
        12 → 13 → 14 → 15 → 16
```

Phases 10–12 can partially overlap in planning, but execution still requires per-phase Owner approval.

---

## Waiting for Owner

**Status:** Phases 0–6 are implemented through Alembic head `20260802_0008`
(Capital Integrity, Execution Lifecycle, Equity Risk Engine, Kill Switch,
Emergency Flatten). See `docs/RISK_CONTROLS.md` and `docs/CAPITAL_INTEGRITY.md`.

Next authorized workstreams (require separate Owner approval): fees/slippage,
Atomic Open deepening, Copy Trading, Alpha Engine, exchange adapters.

Please review:

1. `PROJECT_UNDERSTANDING.md`
2. `IMPLEMENTATION_ROADMAP.md`
3. `docs/RISK_CONTROLS.md` / `docs/EXECUTION_LIFECYCLE.md` / `docs/CAPITAL_INTEGRITY.md`

