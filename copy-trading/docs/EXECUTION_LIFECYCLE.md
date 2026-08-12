# Execution Lifecycle Contract (Phase 5)

Paper-trading execution integrity for orders and positions. Capital mutations
remain exclusively in `scripts/capital.py` (see `docs/CAPITAL_INTEGRITY.md`).

## Authorities

| Concern | Module |
|---|---|
| Open | `scripts/mock_exchange.py` → `place_order` |
| Close | `scripts/mock_exchange.py` → `close_position` |
| Monitor SL/TP | `scripts/position_manager.py` |
| Capital | `scripts/capital.py` |
| Reconcile | `scripts/reconcile_capital.py` |

The Phase 8 venue path (`scripts/execution_engine.py` +
`scripts/exchange_adapter/`) is a **parallel** lifecycle documented in
`docs/EXCHANGE_ADAPTER.md`. It reuses the SL/TP geometry and close settlement
described here, but records orders in `exchange_orders` rather than
`paper_orders`. Nothing below changes for the paper path.

## Order state machine

```text
PENDING  →  FILLED | REJECTED | FAILED
```

- **PENDING:** inserted at the start of the open transaction (intent within TX).
- **FILLED:** successful paper fill; exactly one `positions` row with `order_id`.
- **REJECTED:** insufficient available balance after delay; no position, no reserve.
- **FAILED:** non-wallet FK failure during open; no position when recorded.

DB CHECK: `paper_orders_status_check`.

## Position state machine

```text
OPEN (closed_at IS NULL)  →  CLOSED (closed_at IS NOT NULL)
```

No OPENING/CLOSING states. All closes go through `close_position` with
`SELECT … FOR UPDATE` where `closed_at IS NULL`.

## Order ↔ position

- `positions.order_id` UUID UNIQUE NULL FK → `paper_orders.id`
- New `place_order` always sets `order_id`
- Legacy NULL `order_id` / unlinked FILLED orders snapshotted in
  `phase5_legacy_unlinked_*` tables (non-fatal in reconcile)

## Execution delay

```text
Intent → sleep(EXECUTION_DELAY_SEC) → acquire connection → TX → commit
```

- Delay runs **before** connection acquisition inside `place_order`.
- `agent_trader.tick` releases its pool connection before calling `place_order`.
- Capital check/reserve remains inside the final transaction (after delay).
- Paper fill price may be stale vs the pre-delay signal (accepted for paper).

## SL / TP

- BUY/SELL both persist `stop_loss_price` and `take_profit_price` at open.
- BUY: SL below entry, TP above entry.
- SELL: SL above entry, TP below entry.
- Dual trigger → `STOP_LOSS` wins.

## Position lookup

- Lifecycle ops: `position_id`
- Search: `get_position(symbol, *, wallet_id=…)` — wallet-scoped only

## Reconciliation

Critical (fail `--strict`): capital invariants, new FILLED without position,
**funded position with NULL `order_id` not present in
`phase5_legacy_unlinked_positions`**, multiple positions per order,
REJECTED/FAILED with position, broken FK.

Legacy notes (non-fatal): rows **explicitly registered** in
`phase5_legacy_unlinked_*` only. NULL `order_id` is never classified as
legacy by inference alone.

## Deferred risks (post–Phase 7 / Exchange Integration)

Phase 7 addressed mark freshness fail-closed, SoD Policy B, ledger append-only,
structured events, reconcile history, and flatten residual NO-GO. Still deferred:

| Risk | Status |
|---|---|
| Soft realized-only `check_daily_loss` | Compatibility only (not authoritative) |
| Concurrent `mark_unrealized` vs risk equity read | Accepted for paper |
| Fees / slippage / funding cost | Later — PnL = GROSS / PRE-COST / PAPER (Phase 8 records venue fees without applying them) |
| Real / testnet exchange adapters | Phase 8: implemented but **unverified**; live venue operation still **NOT AUTHORIZED** |
| Paper fill price may be stale after `EXECUTION_DELAY_SEC` | Accepted (paper) |

**Labels:** `PAPER ONLY` · `NOT TESTNET` · `NOT MAINNET`
