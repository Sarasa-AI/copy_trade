"""Phase 8 — venue execution path: risk gate → exchange adapter → capital.

Ordering contract
-----------------
1. ``risk_engine.assert_open_allowed`` (kill switch + fresh mark + drawdown)
   under ``risk_control_lock``, in its own committed transaction.
2. ``exchange_orders`` intent row committed **before** the order leaves the
   process, so a crash between send and response is always recoverable via
   ``client_order_id``.
3. Adapter call — the only network I/O.
4. Settlement transaction: order row update, append-only fill rows,
   ``capital.reserve_margin`` at ``avg_fill_price``, position insert.

``risk_control_lock`` is released before the network call on purpose: holding it
across a venue round-trip would serialize every open globally and, worse, delay
a kill-switch activation that needs the same lock. The gate is therefore
authoritative at send time, and settlement is unconditional accounting of what
the venue actually did.

Authority
---------
- ``avg_fill_price`` from the venue is the authority for margin and entry price.
  ``signal_price`` is pre-trade estimate only and is stored for audit.
- ``capital.py`` remains the single writer of balances; this module calls it.
- Fees are recorded (``exchange_orders.fee_paid``, ``exchange_fills.fee_paid``)
  but NOT applied to capital: PnL stays GROSS / PRE-COST, as in paper. A fee
  model is a separate phase.

Fail-closed rules
-----------------
- Unresolvable venue state → order marked ``UNKNOWN``, kill switch activated
  in the same failure path (activation failure aborts — never swallowed),
  ``ExecutionUncertain`` raised. Capital is never guessed.
- Venue fill state is persisted **before** capital mutation so a settlement
  shortfall cannot erase the audit trail that reconciliation needs.
- A remainder left open on a MARKET order is cancelled before settlement, so
  every position carries exactly one RESERVE ledger entry.
- Partial closes are refused (single RELEASE/REALIZE per position); they raise
  ``PartialCloseUnsupported`` after arming the kill switch.
- ``recover_pending_orders`` resolves crash-orphaned PENDING intents on startup.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from capital import (
    InsufficientAvailableBalance,
    get_wallet_balance,
    margin_required,
    reserve_margin,
)
from exchange_adapter.interface import (
    ExchangeAdapter,
    ExchangePosition,
    ExchangeRejected,
    NetworkTimeout,
    OrderFill,
    OrderNotFound,
    PlaceOrderRequest,
)
from kill_switch import KillSwitchActive, activate_kill_switch
from observability import (
    Timer,
    emit_event_async,
    inc_exchange_duplicate_send,
    inc_exchange_partial_fill,
    inc_exchange_reject,
    inc_execution_uncertain,
    inc_order_failure,
    inc_recovery_failure,
    inc_risk_denial,
    new_correlation_id,
    record_order_latency_ms,
    record_slippage_bps,
)
from risk_engine import RiskDenied, assert_open_allowed, record_denial
from wallet_repository import WalletNotFound, get_wallet_by_id, parse_wallet_id

# Imported so venue orders obey exactly the same bounds and SL/TP geometry as
# the paper path. Divergence here would invalidate paper-vs-testnet parity.
from mock_exchange import (  # noqa: E402
    MAX_LEVERAGE,
    STOP_LOSS_MAX,
    STOP_LOSS_MIN,
    TAKE_PROFIT_MAX,
    TAKE_PROFIT_MIN,
    _acquire,
    _sl_tp_prices,
    _validate_close_reason,
    close_position as settle_close_in_capital,
)

QUANT = Decimal("0.00000001")

UNKNOWN_STATUS = "UNKNOWN"


def _env_float(name: str, default: str) -> float:
    raw = os.getenv(name, default)
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _env_int(name: str, default: str) -> int:
    raw = os.getenv(name, default)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def margin_buffer_pct() -> float:
    """Head-room over the signal-price margin required before sending an order.

    Margin is reserved at ``avg_fill_price``, which is unknown pre-trade. This
    buffer keeps ordinary adverse slippage from turning into a post-fill
    capital shortfall.
    """
    return _env_float("EXECUTION_MARGIN_BUFFER_PCT", "0.01")


def resolve_attempts() -> int:
    return _env_int("EXECUTION_RESOLVE_ATTEMPTS", "5")


def resolve_backoff_sec() -> float:
    return _env_float("EXECUTION_RESOLVE_BACKOFF_SEC", "0.5")


def not_found_confirmations() -> int:
    """Consecutive ``not found`` reads needed to declare an order never placed.

    One read is not enough: a venue can lag behind its own accept path, and
    treating that lag as "never sent" would hide live exposure.
    """
    return _env_int("EXECUTION_NOT_FOUND_CONFIRMATIONS", "2")


class ExecutionError(RuntimeError):
    """Base class for venue execution failures."""

    def to_dict(self) -> dict[str, Any]:
        return {"error": "execution_error", "detail": str(self)}


class ExecutionUncertain(ExecutionError):
    """Venue state could not be resolved; kill switch armed, capital untouched."""

    def __init__(
        self,
        detail: str,
        *,
        client_order_id: UUID,
        order_row_id: UUID | None = None,
    ) -> None:
        self.client_order_id = client_order_id
        self.order_row_id = order_row_id
        super().__init__(f"execution_uncertain: {detail}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "execution_uncertain",
            "detail": str(self),
            "client_order_id": str(self.client_order_id),
            "order_row_id": (
                str(self.order_row_id) if self.order_row_id else None
            ),
        }


class OrderNotFilled(ExecutionError):
    """Order reached a terminal state with zero filled quantity."""

    def __init__(self, result: "ExecutionResult") -> None:
        self.result = result
        super().__init__(
            f"order_not_filled: status={result.status} "
            f"client_order_id={result.client_order_id} "
            f"reason={result.reject_reason}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {"error": "order_not_filled", **self.result.to_dict()}


class PartialCloseUnsupported(ExecutionError):
    """A close filled partially; capital model allows one settlement only."""

    def __init__(self, position_id: UUID, qty_filled: Decimal, qty: Decimal) -> None:
        self.position_id = position_id
        self.qty_filled = qty_filled
        self.qty = qty
        super().__init__(
            f"partial_close_unsupported: position={position_id} "
            f"filled={qty_filled} of {qty}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "partial_close_unsupported",
            "position_id": str(self.position_id),
            "qty_filled": str(self.qty_filled),
            "qty": str(self.qty),
        }


class SettlementCapitalShortfall(ExecutionError):
    """Venue filled but the wallet cannot cover margin at the fill price."""

    def __init__(
        self, wallet_id: UUID, required: float, available: float
    ) -> None:
        self.wallet_id = wallet_id
        self.required = required
        self.available = available
        super().__init__(
            f"settlement_capital_shortfall: wallet={wallet_id} "
            f"required={required} available={available}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "settlement_capital_shortfall",
            "wallet_id": str(self.wallet_id),
            "required": self.required,
            "available": self.available,
        }


class PositionNotExchangeOpened(ValueError):
    """Refuse to send a venue close for a position the venue never opened."""

    def __init__(self, position_id: UUID | str) -> None:
        self.position_id = position_id
        super().__init__(f"position_not_exchange_opened: {position_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "position_not_exchange_opened",
            "position_id": str(self.position_id),
        }


class KillSwitchActivationFailed(ExecutionError):
    """UNKNOWN was recorded but the kill switch could not be armed — hard fail."""

    def __init__(self, detail: str, *, order_row_id: UUID) -> None:
        self.order_row_id = order_row_id
        super().__init__(f"kill_switch_activation_failed: {detail}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "kill_switch_activation_failed",
            "detail": str(self),
            "order_row_id": str(self.order_row_id),
        }


class CloseAlreadyInFlight(ExecutionError):
    """Another CLOSE intent already owns this open position."""

    def __init__(self, position_id: UUID, order_row_id: UUID) -> None:
        self.position_id = position_id
        self.order_row_id = order_row_id
        super().__init__(
            f"close_already_in_flight: position={position_id} "
            f"order={order_row_id}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "close_already_in_flight",
            "position_id": str(self.position_id),
            "order_row_id": str(self.order_row_id),
        }


@dataclass
class ExecutionResult:
    """Outcome of one venue order, including audit fields for parity reports."""

    client_order_id: UUID
    order_row_id: UUID
    adapter_name: str
    intent: str
    symbol: str
    side: str
    status: str
    qty_requested: Decimal
    qty_filled: Decimal
    avg_fill_price: Decimal | None
    signal_price: Decimal | None
    fee_paid: Decimal
    fee_asset: str = "USDT"
    exchange_order_id: str | None = None
    position_id: UUID | None = None
    reserved_margin: float | None = None
    realized_pnl: float | None = None
    reject_code: str | None = None
    reject_reason: str | None = None
    correlation_id: UUID | None = None

    @property
    def slippage_bps(self) -> float | None:
        """Adverse slippage in basis points; positive means worse than signal."""
        if self.signal_price is None or self.signal_price <= 0:
            return None
        if self.avg_fill_price is None:
            return None
        drift = (self.avg_fill_price - self.signal_price) / self.signal_price
        signed = drift if self.side == "BUY" else -drift
        return float(signed * Decimal("10000"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_order_id": str(self.client_order_id),
            "order_row_id": str(self.order_row_id),
            "adapter_name": self.adapter_name,
            "intent": self.intent,
            "symbol": self.symbol,
            "side": self.side,
            "status": self.status,
            "qty_requested": str(self.qty_requested),
            "qty_filled": str(self.qty_filled),
            "avg_fill_price": (
                str(self.avg_fill_price)
                if self.avg_fill_price is not None
                else None
            ),
            "signal_price": (
                str(self.signal_price) if self.signal_price is not None else None
            ),
            "slippage_bps": self.slippage_bps,
            "fee_paid": str(self.fee_paid),
            "fee_asset": self.fee_asset,
            "exchange_order_id": self.exchange_order_id,
            "position_id": str(self.position_id) if self.position_id else None,
            "reserved_margin": self.reserved_margin,
            "realized_pnl": self.realized_pnl,
            "reject_code": self.reject_code,
            "reject_reason": self.reject_reason,
            "correlation_id": (
                str(self.correlation_id) if self.correlation_id else None
            ),
        }


def _q(value: Decimal) -> Decimal:
    return value.quantize(QUANT, rounding=ROUND_DOWN)


def _dec(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _opposite(side: str) -> str:
    return "SELL" if side == "BUY" else "BUY"


def _never_sent_fill(req: PlaceOrderRequest) -> OrderFill:
    """Synthetic terminal state for an intent the venue never accepted."""
    return OrderFill(
        client_order_id=req.client_order_id,
        exchange_order_id=None,
        symbol=req.symbol,
        side=req.side,
        status="CANCELLED",
        qty_requested=req.qty,
        qty_filled=Decimal("0"),
        avg_fill_price=None,
        reject_code="NEVER_SENT",
        reject_reason="venue has no record of this client_order_id",
    )


def _assert_own_transaction(conn: asyncpg.Connection) -> None:
    """Intent durability needs real commits, not savepoints."""
    if conn.is_in_transaction():
        raise ExecutionError(
            "execution_engine requires a connection with no open transaction "
            "(the order intent must be committed before the venue call)"
        )


class ExecutionEngine:
    """Drives one :class:`ExchangeAdapter` against the existing capital model."""

    def __init__(
        self, adapter: ExchangeAdapter, *, actor: str = "execution_engine"
    ) -> None:
        self.adapter = adapter
        self.actor = actor

    @property
    def source(self) -> str:
        return f"execution_engine[{self.adapter.name}]"

    # --- persistence helpers ------------------------------------------------

    async def _insert_intent(
        self,
        conn: asyncpg.Connection,
        *,
        client_order_id: UUID,
        wallet_id: UUID,
        intent: str,
        req: PlaceOrderRequest,
        signal_price: Decimal | None,
        leverage: float | None,
        position_id: UUID | None,
        correlation_id: UUID,
        stop_loss_pct: float | None = None,
        take_profit_pct: float | None = None,
    ) -> UUID:
        async with conn.transaction():
            row_id = await conn.fetchval(
                """
                INSERT INTO exchange_orders (
                    client_order_id, adapter_name, wallet_id, symbol, side,
                    order_type, intent, status, qty_requested, signal_price,
                    reduce_only, leverage, position_id, correlation_id,
                    stop_loss_pct, take_profit_pct
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8, $9,
                        $10, $11, $12, $13, $14, $15)
                RETURNING id
                """,
                client_order_id,
                self.adapter.name,
                wallet_id,
                req.symbol,
                req.side,
                req.order_type,
                intent,
                req.qty,
                signal_price,
                req.reduce_only,
                leverage,
                position_id,
                correlation_id,
                stop_loss_pct,
                take_profit_pct,
            )
        return row_id

    async def _record_state(
        self,
        conn: asyncpg.Connection,
        order_row_id: UUID,
        fill: OrderFill,
        *,
        status_override: str | None = None,
        venue_fill_id: str | None = None,
    ) -> asyncpg.Record:
        """Update the order row and append any newly observed executed quantity.

        ``OrderFill`` carries cumulative quantities, so the appended
        ``exchange_fills`` row is the delta since the last observation, priced
        at the delta's own VWAP. Caller must hold a transaction.
        """
        row = await conn.fetchrow(
            "SELECT * FROM exchange_orders WHERE id = $1 FOR UPDATE",
            order_row_id,
        )
        if row is None:
            raise ExecutionError(f"exchange_orders row missing: {order_row_id}")

        prior_qty = _dec(row["qty_filled"])
        prior_avg = _dec(row["avg_fill_price"])
        new_qty = _dec(fill.qty_filled)
        new_avg = _dec(fill.avg_fill_price)
        delta_qty = _q(new_qty - prior_qty)

        if delta_qty > 0:
            delta_notional = (new_qty * new_avg) - (prior_qty * prior_avg)
            delta_price = _q(delta_notional / delta_qty)
            delta_fee = _q(_dec(fill.fee_paid) - _dec(row["fee_paid"]))
            await conn.execute(
                """
                INSERT INTO exchange_fills (
                    exchange_order_row_id, venue_fill_id, qty_filled,
                    fill_price, fee_paid, fee_asset, timestamp_utc, raw_event
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                """,
                order_row_id,
                venue_fill_id,
                delta_qty,
                delta_price,
                max(Decimal("0"), delta_fee),
                fill.fee_asset,
                fill.timestamp_utc,
                json.dumps(fill.raw, default=str),
            )

        return await conn.fetchrow(
            """
            UPDATE exchange_orders
            SET exchange_order_id = COALESCE($2, exchange_order_id),
                status = $3,
                qty_filled = $4,
                avg_fill_price = $5,
                fee_paid = $6,
                fee_asset = $7,
                reject_code = COALESCE($8, reject_code),
                reject_reason = COALESCE($9, reject_reason),
                raw_response = $10::jsonb,
                updated_at = NOW()
            WHERE id = $1
            RETURNING *
            """,
            order_row_id,
            fill.exchange_order_id,
            status_override or fill.status,
            new_qty,
            fill.avg_fill_price,
            _dec(fill.fee_paid),
            fill.fee_asset,
            fill.reject_code,
            fill.reject_reason,
            json.dumps(fill.raw, default=str),
        )

    async def _mark_unknown(
        self,
        conn: asyncpg.Connection,
        order_row_id: UUID,
        *,
        detail: str,
        correlation_id: UUID,
        symbol: str,
        wallet_id: UUID | None,
    ) -> None:
        """Record unresolved venue state and arm the kill switch (fail-closed).

        UNKNOWN is committed first so forensics survive a subsequent kill-switch
        failure. Kill-switch activation is **not** swallowed: if it fails after
        UNKNOWN is persisted, ``KillSwitchActivationFailed`` is raised so the
        caller cannot continue trading as if the system were healthy.
        """
        inc_execution_uncertain()
        inc_recovery_failure()
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE exchange_orders
                SET status = $2,
                    reject_code = COALESCE(reject_code, 'STATE_UNRESOLVED'),
                    reject_reason = COALESCE(reject_reason, $3),
                    updated_at = NOW()
                WHERE id = $1
                """,
                order_row_id,
                UNKNOWN_STATUS,
                detail,
            )
            await emit_event_async(
                conn,
                "execution_uncertain",
                severity="critical",
                correlation_id=correlation_id,
                actor=self.actor,
                source=self.source,
                reason="STATE_UNRESOLVED",
                wallet_id=wallet_id,
                order_id=order_row_id,
                symbol=symbol,
                detail={"detail": detail, "adapter": self.adapter.name},
            )
        try:
            await activate_kill_switch(
                conn,
                reason=f"exchange state unresolved for order {order_row_id}",
                actor=self.actor,
            )
        except Exception as exc:  # noqa: BLE001 — hard-fail, do not swallow
            await emit_event_async(
                conn,
                "control_error",
                severity="critical",
                correlation_id=correlation_id,
                actor=self.actor,
                source=self.source,
                reason="KILL_SWITCH_ACTIVATION_FAILED",
                detail={"error": str(exc), "order_row_id": str(order_row_id)},
            )
            raise KillSwitchActivationFailed(
                str(exc), order_row_id=order_row_id
            ) from exc

    # --- venue state resolution -------------------------------------------

    async def _resolve_state(
        self, client_order_id: UUID, symbol: str
    ) -> OrderFill | None:
        """Read venue state, retrying transport failures.

        Returns ``None`` when the state stays unknown. Raises ``OrderNotFound``
        only after the venue has denied knowledge of the order
        ``not_found_confirmations()`` times.
        """
        attempts = resolve_attempts()
        backoff = resolve_backoff_sec()
        needed = not_found_confirmations()
        not_found_seen = 0
        for attempt in range(attempts):
            try:
                return await self.adapter.get_order_status(
                    client_order_id, symbol
                )
            except OrderNotFound:
                not_found_seen += 1
                if not_found_seen >= needed:
                    raise
            except NetworkTimeout:
                pass
            if attempt + 1 < attempts and backoff > 0:
                await asyncio.sleep(backoff * (attempt + 1))
        return None

    async def _flatten_remainder(self, fill: OrderFill) -> OrderFill:
        """Cancel any open remainder so one order yields one settlement."""
        try:
            return await self.adapter.cancel_order(
                fill.client_order_id, fill.symbol
            )
        except OrderNotFound:
            return fill
        except NetworkTimeout:
            try:
                resolved = await self._resolve_state(
                    fill.client_order_id, fill.symbol
                )
            except OrderNotFound:
                return fill
            return resolved if resolved is not None else fill

    # --- open ---------------------------------------------------------------

    async def open_position(
        self,
        conn: asyncpg.Connection | None = None,
        *,
        wallet_id: UUID | str,
        symbol: str,
        side: str,
        qty: Decimal | float | str,
        signal_price: Decimal | float | str,
        leverage: float = 1.0,
        stop_loss_pct: float = 0.03,
        take_profit_pct: float = 0.06,
        order_type: str = "MARKET",
        limit_price: Decimal | float | str | None = None,
        correlation_id: UUID | None = None,
    ) -> ExecutionResult:
        """Open exposure on the venue and reserve margin at the fill price."""
        side_u = str(side).upper()
        if side_u not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        if leverage <= 0 or leverage > MAX_LEVERAGE:
            raise ValueError(f"leverage must be in (0, {MAX_LEVERAGE}]")
        if not (STOP_LOSS_MIN <= stop_loss_pct <= STOP_LOSS_MAX):
            raise ValueError(
                f"stop_loss_pct must be between {STOP_LOSS_MIN} and {STOP_LOSS_MAX}"
            )
        if not (TAKE_PROFIT_MIN <= take_profit_pct <= TAKE_PROFIT_MAX):
            raise ValueError(
                f"take_profit_pct must be between {TAKE_PROFIT_MIN} and "
                f"{TAKE_PROFIT_MAX}"
            )

        qty_d = _q(_dec(qty))
        signal_d = _dec(signal_price)
        if qty_d <= 0 or signal_d <= 0:
            raise ValueError("qty and signal_price must be positive")

        wid = parse_wallet_id(wallet_id)
        corr = correlation_id or new_correlation_id()
        client_order_id = uuid4()
        timer = Timer()

        async with _acquire(conn) as db:
            _assert_own_transaction(db)

            wallet = await get_wallet_by_id(db, wid)
            if wallet is None:
                raise WalletNotFound(wid)

            await emit_event_async(
                db,
                "order_attempt",
                severity="info",
                correlation_id=corr,
                actor=self.actor,
                source=self.source,
                wallet_id=wid,
                symbol=symbol,
                detail={
                    "side": side_u,
                    "qty": str(qty_d),
                    "signal_price": str(signal_d),
                    "leverage": leverage,
                    "adapter": self.adapter.name,
                },
            )

            await self._gate_open(
                db, wallet_id=wid, symbol=symbol, correlation_id=corr
            )
            await self._assert_affordable(
                db,
                wallet_id=wid,
                qty=qty_d,
                signal_price=signal_d,
                leverage=leverage,
            )

            venue_qty, venue_price = await self.adapter.quantize_order(
                symbol,
                qty_d,
                _dec(limit_price) if limit_price is not None else None,
            )
            req = PlaceOrderRequest(
                client_order_id=client_order_id,
                symbol=symbol,
                side=side_u,  # type: ignore[arg-type]
                order_type=str(order_type).upper(),  # type: ignore[arg-type]
                qty=venue_qty,
                price=venue_price,
                reduce_only=False,
            )
            order_row_id = await self._insert_intent(
                db,
                client_order_id=client_order_id,
                wallet_id=wid,
                intent="OPEN",
                req=req,
                signal_price=signal_d,
                leverage=leverage,
                position_id=None,
                correlation_id=corr,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
            )

            fill = await self._send_and_resolve(
                db,
                req,
                order_row_id=order_row_id,
                correlation_id=corr,
                wallet_id=wid,
            )

            result = await self._settle_open(
                db,
                fill,
                order_row_id=order_row_id,
                wallet_id=wid,
                signal_price=signal_d,
                leverage=leverage,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                correlation_id=corr,
            )
            record_order_latency_ms(timer.ms())
            if result.slippage_bps is not None:
                record_slippage_bps(result.slippage_bps)
            if result.qty_filled <= 0:
                inc_order_failure()
                raise OrderNotFilled(result)
            return result

    async def _gate_open(
        self,
        db: asyncpg.Connection,
        *,
        wallet_id: UUID,
        symbol: str,
        correlation_id: UUID,
    ) -> None:
        """Authoritative Phase 6/7 gate; denials are persisted then re-raised."""
        # Deny new exposure while any order is stuck UNKNOWN — even if the kill
        # switch failed to arm (audit C3).
        unknown_count = int(
            await db.fetchval(
                "SELECT COUNT(*) FROM exchange_orders WHERE status = $1",
                UNKNOWN_STATUS,
            )
            or 0
        )
        if unknown_count > 0:
            raise ExecutionUncertain(
                f"{unknown_count} exchange_orders in UNKNOWN state; "
                "refusing new opens until resolved",
                client_order_id=uuid4(),
                order_row_id=None,
            )
        try:
            async with db.transaction():
                await assert_open_allowed(db, wallet_id=wallet_id, symbol=symbol)
        except RiskDenied as exc:
            inc_risk_denial()
            if exc.reason_code == "MARK_STALE":
                from observability import inc_stale_mark

                inc_stale_mark()
            async with db.transaction():
                await record_denial(
                    db,
                    reason_code=exc.reason_code,
                    detail=exc.detail,
                    wallet_id=exc.wallet_id,
                    sod_equity=exc.sod_equity,
                    current_equity=exc.current_equity,
                    loss_pct=exc.loss_pct,
                    limit_pct=exc.limit_pct,
                )
                await emit_event_async(
                    db,
                    "risk_deny",
                    severity="critical"
                    if exc.reason_code in ("MARK_MISSING", "MARK_STALE")
                    else "warning",
                    correlation_id=correlation_id,
                    actor=self.actor,
                    source=self.source,
                    reason=exc.reason_code,
                    wallet_id=wallet_id,
                    symbol=symbol,
                    detail=exc.to_dict(),
                )
            raise
        except KillSwitchActive as exc:
            inc_risk_denial()
            async with db.transaction():
                await record_denial(
                    db,
                    reason_code="KILL_SWITCH_ACTIVE",
                    detail=str(exc.reason or "kill_switch_active"),
                    wallet_id=wallet_id,
                )
                await emit_event_async(
                    db,
                    "risk_deny",
                    severity="warning",
                    correlation_id=correlation_id,
                    actor=self.actor,
                    source=self.source,
                    reason="KILL_SWITCH_ACTIVE",
                    wallet_id=wallet_id,
                    symbol=symbol,
                )
            raise

    async def _assert_affordable(
        self,
        db: asyncpg.Connection,
        *,
        wallet_id: UUID,
        qty: Decimal,
        signal_price: Decimal,
        leverage: float,
    ) -> None:
        """Pre-trade check at signal price plus buffer.

        Margin is reserved later at ``avg_fill_price``; refusing to send when
        there is no head-room keeps a filled order from failing settlement.
        """
        balance = await get_wallet_balance(db, wallet_id)
        if balance is None:
            from capital import CapitalAccountMissing

            raise CapitalAccountMissing(wallet_id)
        estimate = margin_required(
            float(qty), float(signal_price), leverage
        ) * (1.0 + margin_buffer_pct())
        available = float(balance["available_balance"])
        if estimate > available:
            raise InsufficientAvailableBalance(wallet_id, estimate, available)

    async def _send_and_resolve(
        self,
        db: asyncpg.Connection,
        req: PlaceOrderRequest,
        *,
        order_row_id: UUID,
        correlation_id: UUID,
        wallet_id: UUID | None,
    ) -> OrderFill:
        """Send the order and return a state with no open remainder."""
        await emit_event_async(
            db,
            "exchange_order_sent",
            severity="info",
            correlation_id=correlation_id,
            actor=self.actor,
            source=self.source,
            wallet_id=wallet_id,
            order_id=order_row_id,
            symbol=req.symbol,
            detail={
                "client_order_id": str(req.client_order_id),
                "adapter": self.adapter.name,
                "qty": str(req.qty),
                "order_type": req.order_type,
                "reduce_only": req.reduce_only,
            },
        )
        try:
            fill = await self.adapter.place_order(req)
        except (NetworkTimeout, ExchangeRejected) as exc:
            try:
                resolved = await self._resolve_state(
                    req.client_order_id, req.symbol
                )
            except OrderNotFound:
                # Venue consistently denies the order: no exposure was created.
                return _never_sent_fill(req)
            if resolved is None:
                await self._mark_unknown(
                    db,
                    order_row_id,
                    detail=str(exc),
                    correlation_id=correlation_id,
                    symbol=req.symbol,
                    wallet_id=wallet_id,
                )
                raise ExecutionUncertain(
                    str(exc),
                    client_order_id=req.client_order_id,
                    order_row_id=order_row_id,
                ) from exc
            # The intent did reach the venue; only the response was lost.
            inc_exchange_duplicate_send()
            fill = resolved

        if not fill.is_terminal:
            if fill.status == "PARTIALLY_FILLED":
                inc_exchange_partial_fill()
            fill = await self._flatten_remainder(fill)
            if not fill.is_terminal:
                await self._mark_unknown(
                    db,
                    order_row_id,
                    detail=(
                        f"order stuck in {fill.status} after cancel attempt"
                    ),
                    correlation_id=correlation_id,
                    symbol=req.symbol,
                    wallet_id=wallet_id,
                )
                raise ExecutionUncertain(
                    f"non-terminal status {fill.status}",
                    client_order_id=req.client_order_id,
                    order_row_id=order_row_id,
                )
        if fill.status == "REJECTED":
            inc_exchange_reject()
        return fill

    async def _settle_open(
        self,
        db: asyncpg.Connection,
        fill: OrderFill,
        *,
        order_row_id: UUID,
        wallet_id: UUID,
        signal_price: Decimal,
        leverage: float,
        stop_loss_pct: float,
        take_profit_pct: float,
        correlation_id: UUID,
    ) -> ExecutionResult:
        """Persist venue truth, then reserve margin at ``avg_fill_price``.

        Runs unconditionally once quantity is filled: a kill switch flipped
        mid-flight must not stop the ledger from reflecting real exposure.
        """
        result = ExecutionResult(
            client_order_id=fill.client_order_id,
            order_row_id=order_row_id,
            adapter_name=self.adapter.name,
            intent="OPEN",
            symbol=fill.symbol,
            side=fill.side,
            status=fill.status,
            qty_requested=fill.qty_requested,
            qty_filled=fill.qty_filled,
            avg_fill_price=fill.avg_fill_price,
            signal_price=signal_price,
            fee_paid=fill.fee_paid,
            fee_asset=fill.fee_asset,
            exchange_order_id=fill.exchange_order_id,
            reject_code=fill.reject_code,
            reject_reason=fill.reject_reason,
            correlation_id=correlation_id,
        )

        if fill.qty_filled <= 0:
            async with db.transaction():
                await self._record_state(db, order_row_id, fill)
                await emit_event_async(
                    db,
                    "exchange_order_result",
                    severity="warning",
                    correlation_id=correlation_id,
                    actor=self.actor,
                    source=self.source,
                    reason=fill.status,
                    wallet_id=wallet_id,
                    order_id=order_row_id,
                    symbol=fill.symbol,
                    detail=fill.to_dict(),
                )
            return result

        assert fill.avg_fill_price is not None
        entry = float(fill.avg_fill_price)
        qty_filled = float(fill.qty_filled)
        margin = margin_required(qty_filled, entry, leverage)
        stop_loss_price, take_profit_price = _sl_tp_prices(
            fill.side, entry, stop_loss_pct, take_profit_pct
        )
        position_id: UUID | None = None

        # Persist venue truth BEFORE capital mutation so a shortfall / crash
        # cannot erase qty_filled (reconcile needs filled-without-position).
        async with db.transaction():
            await self._record_state(db, order_row_id, fill)

        try:
            async with db.transaction():
                position_id = await db.fetchval(
                    """
                    INSERT INTO positions (
                        symbol, entry_price, qty, wallet_id, side,
                        stop_loss_price, take_profit_price,
                        reserved_margin, exchange_order_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id
                    """,
                    fill.symbol,
                    entry,
                    qty_filled,
                    wallet_id,
                    fill.side,
                    stop_loss_price,
                    take_profit_price,
                    margin,
                    order_row_id,
                )
                await reserve_margin(
                    db,
                    wallet_id,
                    margin,
                    position_id=position_id,
                    note="execution_engine.open_position",
                    actor=self.actor,
                    source=self.source,
                    reason="open_reserve_at_fill_price",
                    correlation_id=correlation_id,
                    order_id=order_row_id,
                )
                await db.execute(
                    """
                    UPDATE exchange_orders
                    SET position_id = $2, updated_at = NOW()
                    WHERE id = $1
                    """,
                    order_row_id,
                    position_id,
                )
                await emit_event_async(
                    db,
                    "exchange_order_result",
                    severity="info",
                    correlation_id=correlation_id,
                    actor=self.actor,
                    source=self.source,
                    reason=fill.status,
                    wallet_id=wallet_id,
                    order_id=order_row_id,
                    position_id=position_id,
                    symbol=fill.symbol,
                    detail={
                        **fill.to_dict(),
                        "signal_price": str(signal_price),
                        "reserved_margin": margin,
                    },
                )
        except InsufficientAvailableBalance as exc:
            await self._mark_unknown(
                db,
                order_row_id,
                detail=(
                    "filled order could not be margined at fill price: "
                    f"required={exc.required} available={exc.available}"
                ),
                correlation_id=correlation_id,
                symbol=fill.symbol,
                wallet_id=wallet_id,
            )
            raise SettlementCapitalShortfall(
                wallet_id, exc.required, exc.available
            ) from exc
        except Exception as exc:
            # Any other settlement failure after a venue fill is fail-closed.
            await self._mark_unknown(
                db,
                order_row_id,
                detail=f"open settlement failed after venue fill: {exc}",
                correlation_id=correlation_id,
                symbol=fill.symbol,
                wallet_id=wallet_id,
            )
            raise ExecutionUncertain(
                f"open settlement failed: {exc}",
                client_order_id=fill.client_order_id,
                order_row_id=order_row_id,
            ) from exc

        result.position_id = position_id
        result.reserved_margin = margin
        return result

    # --- close --------------------------------------------------------------

    async def close_position(
        self,
        position_id: UUID | str,
        conn: asyncpg.Connection | None = None,
        *,
        close_reason: str,
        signal_price: Decimal | float | str | None = None,
        correlation_id: UUID | None = None,
    ) -> ExecutionResult:
        """Flatten a venue-opened position with a reduce-only market order.

        Settlement reuses ``mock_exchange.close_position`` so release, PnL
        realization and ``daily_stats`` follow exactly the audited paper path,
        with ``avg_fill_price`` as the exit price.

        The open position row is locked (``FOR UPDATE``) while the CLOSE intent
        is inserted so concurrent closers cannot both send reduce-only orders.
        """
        reason_u = _validate_close_reason(close_reason)
        corr = correlation_id or new_correlation_id()
        client_order_id = uuid4()

        async with _acquire(conn) as db:
            _assert_own_transaction(db)

            pid = (
                position_id
                if isinstance(position_id, UUID)
                else UUID(str(position_id))
            )

            # Claim the position under row lock before any venue I/O.
            # Quantize outside the lock — Bybit instrument lookup is network I/O.
            peek = await db.fetchrow(
                """
                SELECT id, wallet_id, symbol, side, qty, entry_price,
                       exchange_order_id
                FROM positions
                WHERE id = $1 AND closed_at IS NULL
                """,
                pid,
            )
            if peek is None:
                from mock_exchange import PositionAlreadyClosed

                raise PositionAlreadyClosed(position_id)
            if peek["exchange_order_id"] is None:
                raise PositionNotExchangeOpened(position_id)

            qty_d = _q(_dec(peek["qty"]))
            venue_qty, _ = await self.adapter.quantize_order(
                peek["symbol"], qty_d
            )
            close_side = _opposite(peek["side"])
            signal_d = (
                _dec(signal_price)
                if signal_price is not None
                else _dec(peek["entry_price"])
            )
            req = PlaceOrderRequest(
                client_order_id=client_order_id,
                symbol=peek["symbol"],
                side=close_side,  # type: ignore[arg-type]
                order_type="MARKET",
                qty=venue_qty,
                price=None,
                reduce_only=True,
            )

            async with db.transaction():
                position = await db.fetchrow(
                    """
                    SELECT id, wallet_id, symbol, side, qty, entry_price,
                           exchange_order_id
                    FROM positions
                    WHERE id = $1 AND closed_at IS NULL
                    FOR UPDATE
                    """,
                    pid,
                )
                if position is None:
                    from mock_exchange import PositionAlreadyClosed

                    raise PositionAlreadyClosed(position_id)
                if position["exchange_order_id"] is None:
                    raise PositionNotExchangeOpened(position_id)

                inflight = await db.fetchrow(
                    """
                    SELECT id FROM exchange_orders
                    WHERE position_id = $1
                      AND intent = 'CLOSE'
                      AND status IN ('PENDING', 'PARTIALLY_FILLED', 'UNKNOWN')
                    LIMIT 1
                    """,
                    position["id"],
                )
                if inflight is not None:
                    raise CloseAlreadyInFlight(position["id"], inflight["id"])

                # Qty may have changed under us; refuse rather than undersize.
                if _q(_dec(position["qty"])) != qty_d:
                    raise ExecutionError(
                        f"position qty changed during close claim: "
                        f"{position['qty']} vs {qty_d}"
                    )

                wid = position["wallet_id"]
                order_row_id = await db.fetchval(
                    """
                    INSERT INTO exchange_orders (
                        client_order_id, adapter_name, wallet_id, symbol, side,
                        order_type, intent, status, qty_requested, signal_price,
                        reduce_only, leverage, position_id, correlation_id
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, 'CLOSE', 'PENDING', $7, $8,
                        true, NULL, $9, $10
                    )
                    RETURNING id
                    """,
                    client_order_id,
                    self.adapter.name,
                    wid,
                    req.symbol,
                    req.side,
                    req.order_type,
                    req.qty,
                    signal_d,
                    position["id"],
                    corr,
                )

            fill = await self._send_and_resolve(
                db,
                req,
                order_row_id=order_row_id,
                correlation_id=corr,
                wallet_id=wid,
            )

            result = ExecutionResult(
                client_order_id=client_order_id,
                order_row_id=order_row_id,
                adapter_name=self.adapter.name,
                intent="CLOSE",
                symbol=req.symbol,
                side=close_side,
                status=fill.status,
                qty_requested=qty_d,
                qty_filled=fill.qty_filled,
                avg_fill_price=fill.avg_fill_price,
                signal_price=signal_d,
                fee_paid=fill.fee_paid,
                fee_asset=fill.fee_asset,
                exchange_order_id=fill.exchange_order_id,
                position_id=position["id"],
                reject_code=fill.reject_code,
                reject_reason=fill.reject_reason,
                correlation_id=corr,
            )

            if fill.qty_filled <= 0:
                async with db.transaction():
                    await self._record_state(db, order_row_id, fill)
                    await emit_event_async(
                        db,
                        "exchange_order_result",
                        severity="warning",
                        correlation_id=corr,
                        actor=self.actor,
                        source=self.source,
                        reason=fill.status,
                        wallet_id=wid,
                        order_id=order_row_id,
                        position_id=position["id"],
                        symbol=req.symbol,
                        detail=fill.to_dict(),
                    )
                inc_order_failure()
                raise OrderNotFilled(result)

            if _q(_dec(fill.qty_filled)) != qty_d:
                async with db.transaction():
                    await self._record_state(db, order_row_id, fill)
                await self._mark_unknown(
                    db,
                    order_row_id,
                    detail=(
                        f"partial close {fill.qty_filled} of {qty_d}; "
                        "capital model settles a position once"
                    ),
                    correlation_id=corr,
                    symbol=req.symbol,
                    wallet_id=wid,
                )
                raise PartialCloseUnsupported(
                    position["id"], fill.qty_filled, qty_d
                )

            assert fill.avg_fill_price is not None
            exit_price = float(fill.avg_fill_price)
            # Persist fill before capital settle so a settle failure retains
            # venue truth for reconciliation.
            async with db.transaction():
                await self._record_state(db, order_row_id, fill)

            try:
                async with db.transaction():
                    pnl = await settle_close_in_capital(
                        position["id"],
                        exit_price,
                        db,
                        close_reason=reason_u,
                    )
                    await emit_event_async(
                        db,
                        "exchange_order_result",
                        severity="info",
                        correlation_id=corr,
                        actor=self.actor,
                        source=self.source,
                        reason=reason_u,
                        wallet_id=wid,
                        order_id=order_row_id,
                        position_id=position["id"],
                        symbol=req.symbol,
                        detail={
                            **fill.to_dict(),
                            "exit_price": exit_price,
                            "realized_pnl_gross": pnl,
                        },
                    )
            except Exception as exc:
                await self._mark_unknown(
                    db,
                    order_row_id,
                    detail=(
                        f"close settlement failed after venue fill: {exc}"
                    ),
                    correlation_id=corr,
                    symbol=req.symbol,
                    wallet_id=wid,
                )
                raise ExecutionUncertain(
                    f"close settlement failed: {exc}",
                    client_order_id=client_order_id,
                    order_row_id=order_row_id,
                ) from exc

            result.realized_pnl = pnl
            if result.slippage_bps is not None:
                record_slippage_bps(result.slippage_bps)
            return result

    async def recover_pending_orders(
        self,
        conn: asyncpg.Connection | None = None,
        *,
        min_age_sec: float = 60.0,
    ) -> list[dict[str, Any]]:
        """Resolve crash-orphaned PENDING / PARTIALLY_FILLED intents.

        Call on process startup (and periodically). For each stale intent,
        polls venue state and either settles, marks NEVER_SENT, or arms the
        kill switch via ``_mark_unknown``.
        """
        outcomes: list[dict[str, Any]] = []
        async with _acquire(conn) as db:
            _assert_own_transaction(db)
            rows = await db.fetch(
                """
                SELECT id, client_order_id, wallet_id, symbol, side, intent,
                       status, qty_requested, signal_price, leverage,
                       position_id, correlation_id, reduce_only,
                       stop_loss_pct, take_profit_pct
                FROM exchange_orders
                WHERE status IN ('PENDING', 'PARTIALLY_FILLED')
                  AND adapter_name = $1
                  AND created_at <= NOW() - ($2 * INTERVAL '1 second')
                ORDER BY created_at ASC
                """,
                self.adapter.name,
                float(min_age_sec),
            )
            for row in rows:
                corr = row["correlation_id"] or new_correlation_id()
                cid = row["client_order_id"]
                try:
                    try:
                        fill = await self._resolve_state(cid, row["symbol"])
                    except OrderNotFound:
                        never = OrderFill(
                            client_order_id=cid,
                            exchange_order_id=None,
                            symbol=row["symbol"],
                            side=row["side"],
                            status="CANCELLED",
                            qty_requested=_dec(row["qty_requested"]),
                            qty_filled=Decimal("0"),
                            avg_fill_price=None,
                            reject_code="NEVER_SENT",
                            reject_reason=(
                                "startup recovery: venue has no record"
                            ),
                        )
                        async with db.transaction():
                            await self._record_state(
                                db, row["id"], never
                            )
                        outcomes.append(
                            {
                                "order_row_id": str(row["id"]),
                                "outcome": "never_sent",
                            }
                        )
                        continue

                    if fill is None:
                        await self._mark_unknown(
                            db,
                            row["id"],
                            detail="startup recovery: unresolvable state",
                            correlation_id=corr,
                            symbol=row["symbol"],
                            wallet_id=row["wallet_id"],
                        )
                        outcomes.append(
                            {
                                "order_row_id": str(row["id"]),
                                "outcome": "unknown",
                            }
                        )
                        continue

                    if not fill.is_terminal and fill.status == "PARTIALLY_FILLED":
                        fill = await self._flatten_remainder(fill)

                    if row["intent"] == "OPEN" and fill.qty_filled > 0:
                        # Only settle if no position linked yet.
                        if row["position_id"] is None:
                            await self._settle_open(
                                db,
                                fill,
                                order_row_id=row["id"],
                                wallet_id=row["wallet_id"],
                                signal_price=_dec(row["signal_price"])
                                or _dec(fill.avg_fill_price),
                                leverage=float(row["leverage"] or 1.0),
                                stop_loss_pct=float(row["stop_loss_pct"] or 0.03),
                                take_profit_pct=float(row["take_profit_pct"] or 0.06),
                                correlation_id=corr,
                            )
                            outcomes.append(
                                {
                                    "order_row_id": str(row["id"]),
                                    "outcome": "settled_open",
                                }
                            )
                        else:
                            async with db.transaction():
                                await self._record_state(db, row["id"], fill)
                            outcomes.append(
                                {
                                    "order_row_id": str(row["id"]),
                                    "outcome": "state_synced",
                                }
                            )
                    elif row["intent"] == "CLOSE" and fill.qty_filled > 0:
                        # Close settlement requires the live position; if still
                        # open, leave fill recorded and arm kill for operator.
                        async with db.transaction():
                            await self._record_state(db, row["id"], fill)
                        still_open = await db.fetchval(
                            """
                            SELECT id FROM positions
                            WHERE id = $1 AND closed_at IS NULL
                            """,
                            row["position_id"],
                        )
                        if still_open is not None and _q(
                            _dec(fill.qty_filled)
                        ) == _q(_dec(row["qty_requested"])):
                            # Venue flattened; capital not settled — fail-closed.
                            await self._mark_unknown(
                                db,
                                row["id"],
                                detail=(
                                    "startup recovery: CLOSE filled but "
                                    "position still open in DB"
                                ),
                                correlation_id=corr,
                                symbol=row["symbol"],
                                wallet_id=row["wallet_id"],
                            )
                            outcomes.append(
                                {
                                    "order_row_id": str(row["id"]),
                                    "outcome": "close_unsettled_unknown",
                                }
                            )
                        else:
                            outcomes.append(
                                {
                                    "order_row_id": str(row["id"]),
                                    "outcome": "close_state_synced",
                                }
                            )
                    else:
                        async with db.transaction():
                            await self._record_state(db, row["id"], fill)
                        outcomes.append(
                            {
                                "order_row_id": str(row["id"]),
                                "outcome": "terminal_recorded",
                                "status": fill.status,
                            }
                        )
                except Exception as exc:  # noqa: BLE001 — per-row isolation
                    outcomes.append(
                        {
                            "order_row_id": str(row["id"]),
                            "outcome": "error",
                            "error": str(exc),
                        }
                    )
            return outcomes

    # --- reconciliation -----------------------------------------------------

    async def reconcile_positions(
        self, conn: asyncpg.Connection | None = None
    ) -> dict[str, Any]:
        """Compare venue-opened DB positions against the venue's own view."""
        from reconcile_capital import Mismatch, ReconciliationResult, persist_reconciliation_run

        corr = new_correlation_id()
        started = datetime.now(timezone.utc)
        async with _acquire(conn) as db:
            rows = await db.fetch(
                """
                SELECT symbol, side, SUM(qty) AS qty
                FROM positions
                WHERE closed_at IS NULL AND exchange_order_id IS NOT NULL
                GROUP BY symbol, side
                """
            )
            local: dict[tuple[str, str], Decimal] = {
                (r["symbol"], r["side"]): _q(_dec(r["qty"])) for r in rows
            }
            venue_positions: list[ExchangePosition] = (
                await self.adapter.get_open_positions()
            )
            venue: dict[tuple[str, str], Decimal] = {
                (p.symbol, p.side): _q(p.qty) for p in venue_positions
            }

            mismatches: list[Mismatch] = []
            for key in sorted(set(local) | set(venue)):
                want = venue.get(key, Decimal("0"))
                have = local.get(key, Decimal("0"))
                if want != have:
                    mismatches.append(
                        Mismatch(
                            reason="exchange_position_qty_mismatch",
                            expected=str(want),
                            actual=str(have),
                            position_id=f"{key[0]}:{key[1]}",
                        )
                    )

            unresolved = int(
                await db.fetchval(
                    """
                    SELECT COUNT(*) FROM exchange_orders
                    WHERE status = $1 AND adapter_name = $2
                    """,
                    UNKNOWN_STATUS,
                    self.adapter.name,
                )
            )
            if unresolved:
                mismatches.append(
                    Mismatch(
                        reason="exchange_orders_unknown_state",
                        expected=0,
                        actual=unresolved,
                    )
                )

            result = ReconciliationResult(ok=not mismatches, mismatches=mismatches)
            if not result.ok:
                from observability import inc_reconcile_mismatch

                inc_reconcile_mismatch(float(len(mismatches)))
            await persist_reconciliation_run(
                db,
                result,
                correlation_id=corr,
                started_at=started,
                strict=False,
                notes=f"exchange_adapter={self.adapter.name}",
            )
            await emit_event_async(
                db,
                "exchange_reconcile_result",
                severity="info" if result.ok else "critical",
                correlation_id=corr,
                actor=self.actor,
                source=self.source,
                reason="PASS" if result.ok else "FAIL",
                detail=result.to_dict(),
            )
            return result.to_dict()


async def bridge_marks(
    adapter: ExchangeAdapter, symbols: list[str]
) -> None:
    """Feed venue marks into ``price_feed`` and let freshness policy do the rest.

    No separate liveness flag is introduced: if the stream dies, marks age past
    ``MAX_MARK_AGE_SEC`` and ``assert_open_allowed`` denies new exposure.
    """
    import price_feed
    from observability import record_mark_age

    async def on_mark(update: Any) -> None:
        quote = price_feed.update_mark(
            update.symbol,
            float(update.price),
            source=f"{adapter.name}_ws",
            ts_utc=update.timestamp_utc,
        )
        record_mark_age(quote.age_sec())

    await adapter.subscribe_marks(list(symbols), on_mark)


__all__ = (
    "CloseAlreadyInFlight",
    "ExecutionEngine",
    "ExecutionError",
    "ExecutionResult",
    "ExecutionUncertain",
    "KillSwitchActivationFailed",
    "OrderNotFilled",
    "PartialCloseUnsupported",
    "PositionNotExchangeOpened",
    "SettlementCapitalShortfall",
    "bridge_marks",
    "margin_buffer_pct",
)
