# Risk Controls Contract (Phase 6)

Paper trading risk, kill switch, and emergency flatten. Capital writes remain
in `scripts/capital.py` only.

## Authority

| Control | Module | Enforced where |
|---|---|---|
| Equity Risk Engine | `scripts/risk_engine.py` | Inside `place_order` open TX (before order/position/reserve) |
| Kill Switch | `scripts/kill_switch.py` | Same open TX via `assert_open_allowed` |
| Emergency flatten | `scripts/emergency_control.py` | Admin/manual; uses `close_position` |

`mock_exchange.check_daily_loss()` is **compatibility only** (realized
`daily_stats`). It is **not** authoritative for opens.

## Equity model

```text
current_equity = SUM(wallet_balances.current_equity)
               = available + reserved + unrealized   (per wallet, then summed)

PnL label: GROSS / PRE-COST PAPER PnL
(fees / slippage / funding not modeled)
```

SoD: first locked observation of the UTC calendar day is stored in
`equity_sod_snapshots` (`scope=GLOBAL` and per-wallet). Drawdown:

```text
loss_pct = max(0, (sod - current) / sod)   # sod > 0
deny when loss_pct > DAILY_EQUITY_LOSS_LIMIT_PCT (default 0.03)
```

## Atomic open sequence

```text
sleep(EXECUTION_DELAY_SEC)          # outside DB
BEGIN
  LOCK risk_control_lock (row FOR UPDATE, held until COMMIT)
  CHECK kill switch
  ENSURE SoD (UTC day)
  READ equity (realized + unrealized in balances)
  IF deny → ROLLBACK open writes; record denial in separate TX; raise
  INSERT paper_orders PENDING → FILLED
  INSERT positions (+ order_id)
  reserve_margin
COMMIT
```

No sleep while holding a DB connection. Concurrent opens and kill
activate/deactivate serialize on `risk_control_lock`.

## Kill switch

Persistent singleton `kill_switch_state` (id=1). Events in
`kill_switch_events` (actor, reason, timestamp, equity_at_event). Active
switch denies all `place_order` paths (agent and direct). Survives process
restart (DB-backed). Does **not** auto-flatten.

## Emergency flatten

`emergency_halt_and_flatten` optionally activates the kill switch, then
**synchronously** closes open positions through `close_position`
(`close_reason=KILL_SWITCH`). Semantics:

- **Best-effort / not all-or-nothing** — each close is its own transaction
- Missing/non-positive exit price → `skipped_no_price` (no capital mutation)
- Double close is safe (`already_closed`)
- Does not auto-liquidate on risk deny alone

## Mark price / freshness (Phase 7)

Marks are timestamped (`MarkQuote`: symbol, price, ts_utc, source) with
configurable ``MAX_MARK_AGE_SEC`` (default 30). Authoritative open path:

```text
STALE OR MISSING MARK → DENY NEW EXPOSURE
(reason_code MARK_STALE | MARK_MISSING)
```

SL/TP skips stale marks (does not treat them as fresh). Emergency flatten
uses caller-supplied prices; missing/stale classified per position.

## SoD / Funding Policy B (Phase 7)

- ``MASTER_INIT`` funds master pool only → does **not** adjust SoD
- ``allocate_to_wallet`` enters risk equity perimeter → adjusts existing
  same-day GLOBAL/WALLET SoD by the allocated delta
- Prevents silent same-day funding from wiping drawdown vs fixed SoD

## Emergency flatten (Phase 7 residual policy)

Kill first → best-effort per-position outcomes → aggregate:

```text
residual_count > 0 → status = NOT_SAFE (+ flatten_residual critical event)
residual_count = 0 → status = SAFE
```

## Readiness labels

```text
Verified Paper Capital Integrity
Verified Risk Controls (paper)
Verified Kill Switch (paper)
Verified Emergency Flatten (paper)
Hermetic critical tests (P7-001)
Mark freshness fail-closed (P7-002..004)
SoD Policy B (P7-005)
Ledger append-only + provenance (P7-006/007)
Structured events / metrics (P7-008..011)
Reconcile history (P7-012)
NOT Exchange Ready
NOT Testnet Ready
NOT Mainnet Ready
PnL is GROSS / PRE-COST / PAPER — NOT REAL RETURN
Phase 7 does not authorize exchange connectivity
```
