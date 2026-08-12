# Exchange Adapter & Venue Execution (Phase 8)

**Status:** Phase 8 hermetic implementation complete; Phase 8A Paper Adapter
complete. Alembic head `20260807_0011`. Real Bybit Testnet evidence is **not**
collected yet, so this phase is **not** signed off and **no** mainnet capability
exists. Paper Adapter validation is **not** Testnet validation.

**PnL remains GROSS / PRE-COST.** Fees returned by the venue are recorded but
not applied to capital — see [Fees](#fees-recorded-not-applied).

---

## 1. What this phase adds

| Component | File | Role |
| --- | --- | --- |
| Boundary contract | `scripts/exchange_adapter/interface.py` | Abstract adapter, transport models, error hierarchy |
| Offline venue (CI) | `scripts/exchange_adapter/null_adapter.py` | Deterministic, scriptable simulator for CI |
| Paper venue (8A) | `scripts/exchange_adapter/paper_adapter.py` | Simulated venue backed by `price_feed` marks |
| Adapter factory | `scripts/exchange_adapter/factory.py` | Explicit `EXCHANGE_ADAPTER` selection |
| Socket supervision | `scripts/exchange_adapter/reconnect.py` | Capped backoff, jitter, stall detection |
| Bybit V5 | `scripts/exchange_adapter/bybit_adapter.py` | Testnet REST + public/private websockets |
| Execution path | `scripts/execution_engine.py` | risk gate → adapter → capital |
| Persistence | `migrations/versions/20260806_0010_*.py` | `exchange_orders`, `exchange_fills`, `positions.exchange_order_id` |
| SL/TP on intents | `migrations/versions/20260807_0011_*.py` | `stop_loss_pct` / `take_profit_pct` on `exchange_orders` |

Untouched on purpose: `capital.py`, `risk_engine.py`, `kill_switch.py`,
`mock_exchange.py` (legacy paper path), `price_feed.py` (mark provider only).
The Paper Adapter consumes marks; it does not replace `mock_exchange`.

## 2. Layering

```
agent / strategy
      │
execution_engine        ← ordering, recovery, audit, fail-closed policy
      ├── risk_engine.assert_open_allowed   (kill switch + fresh mark + drawdown)
      ├── ExchangeAdapter                    (the only network I/O)
      └── capital.py                         (single writer of balances)
```

An adapter never imports `capital`, `risk_engine` or `kill_switch`, and never
writes to the database. That is what makes `NullAdapter` / `PaperExchangeAdapter`
complete substitutes for a venue in tests.

### Paper Adapter vs NullAdapter vs Bybit vs mock_exchange

| Path | Role | Real orders? | Mark source |
| --- | --- | --- | --- |
| `mock_exchange` | Legacy paper opens (`paper_orders`) | No | Caller price |
| `NullAdapter` | Hermetic CI scripting (`NULL-*` IDs) | No | Injected `mark_prices` |
| `PaperExchangeAdapter` | Simulated venue (`paper-*` IDs) for execution-path validation | No | `price_feed` (live or injected) |
| `BybitAdapter` | Real testnet/mainnet venue | Yes (when credentials set) | Bybit `markPrice` / REST |

**Paper Adapter validates** execution-engine correctness, capital, risk, order
lifecycle, recovery, reconciliation, failure handling, and long-running
stability. **It does not validate** real exchange behaviour, latency, reject
codes, REST/WS consistency, authentication, or outages.

## 3. Order lifecycle

1. **Validate** against the same bounds as the paper path (`MAX_LEVERAGE`,
   stop-loss and take-profit ranges are imported from `mock_exchange`, not
   re-declared, so paper and venue geometry cannot drift apart).
2. **Risk gate** — `assert_open_allowed` under `risk_control_lock`, in its own
   committed transaction.
3. **Affordability** — margin at `signal_price × (1 + EXECUTION_MARGIN_BUFFER_PCT)`
   must fit in `available_balance`.
4. **Quantize** — `adapter.quantize_order` snaps qty/price down to venue steps
   before anything is persisted, so the intent row equals what is submitted.
5. **Persist intent** — `exchange_orders` row (`status='PENDING'`) is
   **committed before the network call**. A crash after this point is always
   recoverable through `client_order_id`.
6. **Send** — `adapter.place_order`.
7. **Settle** — order row update, append-only `exchange_fills` row,
   `capital.reserve_margin` at `avg_fill_price`, `positions` insert linked by
   `exchange_order_id`.

### Why the risk lock is released before the network call

Holding `risk_control_lock` across a venue round-trip would serialize every
open globally and, worse, delay a kill-switch activation that needs the same
lock. The gate is therefore authoritative **at send time**; settlement is
unconditional accounting of what the venue actually did. A kill switch that
flips mid-flight blocks the *next* order — it never leaves a real fill off the
ledger.

## 4. Price authority

`avg_fill_price` from the venue is the authority for:

- margin reserved (`capital.reserve_margin`)
- `positions.entry_price`
- stop-loss / take-profit levels
- exit price and realized PnL on close

`signal_price` is a pre-trade estimate. It is stored in
`exchange_orders.signal_price` alongside `avg_fill_price` so slippage is
measurable per order:

```
slippage_bps = (avg_fill_price - signal_price) / signal_price × 10000
               × (+1 for BUY, -1 for SELL)      # positive = adverse
```

`ExecutionResult.slippage_bps` exposes this, and `observability` records the
`slippage_bps` gauge.

## 5. Idempotency

`client_order_id` is a UUID we generate, persisted in `exchange_orders`
(`UNIQUE`) and sent as Bybit's `orderLinkId`. Every retry of the same intent
reuses it, so a duplicate send cannot create a second venue order. Adapters must
return the existing order's state rather than raising on a duplicate.

## 6. Failure handling

| Situation | Behaviour |
| --- | --- |
| Order-scoped rejection | `OrderFill(status='REJECTED')` persisted; no capital movement; `OrderNotFilled` raised |
| Remainder left open (partial MARKET fill) | Remainder cancelled, then the filled quantity is settled — one position, exactly one `RESERVE` |
| Response lost, order was filled | Recovered via `get_order_status`; settled once; no second order |
| Response lost, venue never got it | Confirmed by `EXECUTION_NOT_FOUND_CONFIRMATIONS` consecutive not-found reads → order marked `CANCELLED` / `NEVER_SENT`; kill switch untouched |
| Create response fails / ambiguous | Treated as `NetworkTimeout` (never local `REJECTED` from a single not-found); engine recovers with the same confirmation count |
| State unresolvable | Order marked `UNKNOWN`, `execution_uncertain` critical event, **kill switch activated** (activation failure raises `KillSwitchActivationFailed` — never swallowed), `ExecutionUncertain` raised. Opens are also denied while any `UNKNOWN` order exists |
| Filled but wallet cannot cover margin | Fill/qty persisted first; then fail-closed + `SettlementCapitalShortfall`; reconciliation reports `exchange_order_filled_without_position` |
| Close filled, capital settle fails | Fill persisted; order `UNKNOWN` + kill switch; `ExecutionUncertain` — never leave venue flat / DB open silently |
| Partial close | Refused (`PartialCloseUnsupported`) after arming the kill switch — the capital model settles a position once |
| Crash after intent commit | `recover_pending_orders` (startup sweeper) + reconcile `exchange_order_nonterminal_stale` |
| Websocket dies or goes silent | Public tickers: reconnect with capped backoff; marks age out. Private order stream: no stall timeout (idle ≠ dead); auth ack required before subscribe |

One consecutive not-found read is deliberately not enough: a venue can lag
behind its own accept path, and treating that lag as "never sent" would hide
live exposure. The create-error path uses the same rule.

## 7. Persistence model

`exchange_orders` — one row per `client_order_id`, mutable status, holds both
`signal_price` and `avg_fill_price`, `raw_response` JSONB for audit.

`exchange_fills` — **append-only** (DB trigger, like `capital_ledger`). One row
per newly observed executed quantity. Because `OrderFill` carries *cumulative*
quantities, each row stores the delta since the last observation, priced at the
delta's own VWAP. `venue_fill_id` has a partial unique index so replayed
websocket events cannot double-count an execution. Venue fill state is written
**before** capital mutation so a settlement shortfall cannot erase the audit.

`positions.exchange_order_id` — venue-opened positions link here;
paper-opened positions keep using `order_id`. A check constraint forbids both.

Close path locks the position (`FOR UPDATE`) and refuses a second CLOSE while a
non-terminal CLOSE intent already exists (`CloseAlreadyInFlight`).

## 8. Fees recorded, not applied

`exchange_orders.fee_paid` and `exchange_fills.fee_paid` capture real venue
fees. They are **not** deducted from capital, because:

- `wallet_balances` has a conservation check
  (`available + reserved = initial + realized`); a fee can only enter through
  `realize_pnl`, which is once-per-position by design.
- A cost model (fees, slippage, funding) is a separate authorized phase.

So reported PnL stays gross and directly comparable to paper. The recorded fees
are what a later cost phase will consume.

## 9. Reconciliation

`ExecutionEngine.reconcile_positions` compares venue-opened DB positions with
`adapter.get_open_positions()` per `(symbol, side)`, counts orders stuck in
`UNKNOWN`, writes a `reconciliation_runs` row and emits
`exchange_reconcile_result`.

`ExecutionEngine.recover_pending_orders` resolves crash-orphaned
`PENDING` / `PARTIALLY_FILLED` intents (call on startup).

`scripts/reconcile_capital.py` gained venue checks:
`exchange_order_state_unresolved`, `exchange_order_nonterminal_stale`,
`exchange_close_filled_position_still_open`,
`exchange_order_filled_without_position`,
`rejected_exchange_order_has_position`, `exchange_order_qty_vs_fills`. They are
skipped on databases predating this migration.

**Note:** `reconcile_capital` is DB-only. Venue qty cross-check requires
`ExecutionEngine.reconcile_positions`.

## 10. Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `EXCHANGE_ADAPTER` | `null` (factory) | `paper` \| `null` \| `bybit` — explicit selection via `create_exchange_adapter()` |
| `PAPER_MODE` | unset | When `true`, forces paper; conflicts with non-paper `EXCHANGE_ADAPTER` |
| `PAPER_FAILURE_MODE` | `success` | Deterministic failure injection (see §13) |
| `PAPER_FEES_ENABLED` | `false` | Must stay false for Phase 8A (fee model deferred) |
| `PAPER_SLIPPAGE_BPS` | `0` | Zero-slippage contract for Phase 8A |
| `PAPER_PARTIAL_FILL_RATIO` | `0.5` | Used when `PAPER_FAILURE_MODE=partial_fill` |
| `PAPER_LIVE_MARK` | unset | Opt-in live mark E2E (`tests/test_phase8a_paper_live_mark.py`) |
| `PAPER_SOAK` | unset | Opt-in soak (`tests/test_phase8a_paper_soak.py`) |
| `BYBIT_API_KEY` / `BYBIT_API_SECRET` | unset | Required to construct Bybit only — **ignored in paper mode** |
| `BYBIT_TESTNET` | `1` | Testnet endpoints |
| `BYBIT_ALLOW_MAINNET` | unset | Mainnet is refused without it. Not an authorized phase. |
| `EXECUTION_MARGIN_BUFFER_PCT` | `0.01` | Head-room over signal-price margin |
| `EXECUTION_RESOLVE_ATTEMPTS` | `5` | Status reads before declaring state unknown |
| `EXECUTION_RESOLVE_BACKOFF_SEC` | `0.5` | Base delay between status reads |
| `EXECUTION_NOT_FOUND_CONFIRMATIONS` | `2` | Not-found reads needed to declare "never sent" (also used after create ambiguity) |

Bybit Phase 8 supports **one-way mode only** (`positionIdx=0`). Hedge-mode
accounts are refused by `ensure_one_way_mode`. Paper `ensure_one_way_mode` is a
deterministic no-op success.

**Hard safety:** `EXCHANGE_ADAPTER=paper` never reads Bybit credentials, never
imports `BybitAdapter`, and never opens exchange REST/private WebSocket
sessions. Presence of `BYBIT_*` env vars while in paper mode is ignored.

## 11. Test coverage

`tests/test_phase8_exchange_adapter.py` — interface invariants, adapter
idempotency, reserve at fill price, reject, partial fill, lost response
recovery, never-sent, unresolvable state, kill-switch and stale-mark gating
before any venue call, close settlement, partial-close refusal, append-only
fills, reconciliation, mark bridge, paper↔venue parity, settlement shortfall
fill persistence, UNKNOWN open gate, concurrent close claim, pending recovery,
close settlement fail-closed, SL/TP recovery (8A-1).

`tests/test_phase8_reconnect_bybit.py` — backoff maths, reconnect after
failure, silent-socket detection, signature vectors, status mapping, payload
parsing, instrument filters, mainnet refusal, create-error → NetworkTimeout,
retCode-aware status reads, hedge-mode refusal, and the full Bybit REST path
driven through `httpx.MockTransport`.

`tests/test_phase8a_paper_adapter.py` — Paper Adapter E2E through
`ExecutionEngine` + risk + capital + DB (hermetic).

`tests/test_phase8a_paper_live_mark.py` / `tests/test_phase8a_paper_soak.py` —
opt-in only (`PAPER_LIVE_MARK=1` / `PAPER_SOAK=1`); skipped in default CI.

Phase 8 / 8A hermetic suites: no credentials, no real venue orders.

## 12. Not done yet (Phase 8 exit gate)

1. Live Bybit testnet integration run: network drop mid-order, forced partial
   fills, forced rejections, duplicate sends, websocket kill.
2. Numeric paper-vs-testnet deviation report (PnL, slippage, fees) — the gate
   metric.
3. Reduce-only / position-mode verified against a real testnet account
   (code refuses hedge mode; live confirmation still required).
4. Funding-rate handling for perpetuals (out of scope until the cost phase).

Until 1–3 exist, this phase is implementation-complete but **unverified**, and
Testnet operation is not authorized. Phase 8A Paper Adapter success does **not**
satisfy this gate.

## 13. Phase 8A — Paper Exchange Adapter

**Purpose:** prove the execution engine, capital, risk, recovery and
reconciliation paths against a simulated venue that fills at the project's
authoritative `price_feed` mark — without sending real orders.

**Contract:** same `ExchangeAdapter` interface; same `ExecutionEngine` (no
Paper-specific execution branch). Venue identity: `adapter.name = "paper"`,
order IDs `paper-order-<uuid>`, fill event IDs `paper-fill-<uuid>`.

**Price:** fill at current `price_feed` mark; Phase 8A defaults are
`fee_paid = 0`, `slippage_bps = 0`. Stale/missing marks deny new exposure via
the existing risk gate (Paper does not manufacture fresh prices).

**Failure injection** (`PAPER_FAILURE_MODE`, default `success`):

`success`, `create_timeout`, `create_reject`, `response_lost_after_accept`,
`status_timeout`, `not_found_once`, `not_found_twice`, `partial_fill`,
`close_success`, `close_timeout`, `close_unknown`, `ws_disconnect`,
`duplicate_event`.

**Limitations:** no fee/slippage/funding model; no private-WS stall timeout;
does not claim Testnet or mainnet readiness. No new database migration for the
adapter itself (reuses `exchange_orders` / `exchange_fills`).