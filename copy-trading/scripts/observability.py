"""P7-008..011 — structured critical events, correlation IDs, metrics, alerts.

Internal paper observability only. No external monitoring SaaS.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

# --- Correlation ---


def new_correlation_id() -> UUID:
    """Internal operation/workflow correlation ID (not an exchange client id)."""
    return uuid4()


# --- Metrics ---

_metrics_lock = threading.Lock()
_counters: dict[str, float] = {}
_gauges: dict[str, float] = {}


def _inc(name: str, value: float = 1.0) -> None:
    with _metrics_lock:
        _counters[name] = _counters.get(name, 0.0) + value


def _set_gauge(name: str, value: float) -> None:
    with _metrics_lock:
        _gauges[name] = float(value)


def record_mark_age(age_sec: float) -> None:
    _set_gauge("mark_age_sec", age_sec)


def inc_stale_mark() -> None:
    _inc("stale_mark_count")


def inc_risk_denial() -> None:
    _inc("risk_denial_count")


def inc_kill_activation() -> None:
    _inc("kill_activation_count")


def record_order_latency_ms(latency_ms: float) -> None:
    _set_gauge("order_latency_ms", latency_ms)


def inc_order_failure() -> None:
    _inc("order_failure_count")


def inc_reconcile_mismatch(count: float = 1.0) -> None:
    _inc("reconcile_mismatch_count", count)


def set_flatten_residual(count: float) -> None:
    _set_gauge("flatten_residual_count", count)
    if count > 0:
        _inc("flatten_residual_events")


def set_open_exposure(count: float) -> None:
    _set_gauge("open_exposure", count)


def inc_capital_mismatch() -> None:
    _inc("capital_mismatch")


def inc_recovery_failure() -> None:
    _inc("recovery_failure_count")


# --- Phase 8: exchange boundary ---


def inc_exchange_reject() -> None:
    _inc("exchange_reject_count")


def inc_exchange_partial_fill() -> None:
    _inc("exchange_partial_fill_count")


def inc_exchange_duplicate_send() -> None:
    _inc("exchange_duplicate_send_count")


def inc_execution_uncertain() -> None:
    _inc("execution_uncertain_count")


def record_slippage_bps(slippage_bps: float) -> None:
    """Adverse slippage of the last fill, signed so positive = worse than signal."""
    _set_gauge("slippage_bps", slippage_bps)


# --- Phase 8A: paper adapter (additive; do not duplicate generic execution) ---


def inc_paper_order() -> None:
    _inc("paper_orders_total")


def inc_paper_fill() -> None:
    _inc("paper_fills_total")


def inc_paper_partial_fill() -> None:
    _inc("paper_partial_fills_total")


def inc_paper_failure(mode: str | None = None) -> None:
    _inc("paper_failures_total")
    if mode:
        _inc(f"paper_failure_{mode}")


def inc_paper_unknown() -> None:
    _inc("paper_unknown_orders_total")


def inc_paper_recovery() -> None:
    _inc("paper_recoveries_total")


def record_paper_fill_latency_seconds(latency_sec: float) -> None:
    _set_gauge("paper_fill_latency_seconds", float(latency_sec))


def metrics_snapshot() -> dict[str, float]:
    with _metrics_lock:
        out = {f"counter.{k}": v for k, v in _counters.items()}
        out.update({f"gauge.{k}": v for k, v in _gauges.items()})
        return out


def reset_metrics_for_tests() -> None:
    with _metrics_lock:
        _counters.clear()
        _gauges.clear()


# --- Structured events ---

EVENT_TYPES = frozenset(
    {
        "order_attempt",
        "risk_allow",
        "risk_deny",
        "kill_activate",
        "kill_deactivate",
        "flatten_start",
        "flatten_complete",
        "flatten_residual",
        "reconcile_start",
        "reconcile_result",
        "mark_stale",
        "mark_recovery",
        "control_error",
        # Phase 8 — exchange boundary
        "exchange_order_sent",
        "exchange_order_result",
        "exchange_fill",
        "execution_uncertain",
        "exchange_reconcile_result",
    }
)

CRITICAL_ALERT_TYPES = frozenset(
    {
        "mark_stale",
        "risk_deny",  # only when MARK_* — callers set severity
        "reconcile_result",
        "flatten_residual",
        "kill_activate",
        "control_error",
        "execution_uncertain",
        "exchange_reconcile_result",
    }
)

_event_buffer: list[dict[str, Any]] = []
_event_lock = threading.Lock()


@dataclass
class CriticalEvent:
    event_type: str
    severity: str
    correlation_id: UUID | None = None
    actor: str | None = None
    source: str | None = None
    reason: str | None = None
    wallet_id: UUID | None = None
    order_id: UUID | None = None
    position_id: UUID | None = None
    symbol: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts.isoformat(),
            "event_type": self.event_type,
            "severity": self.severity,
            "correlation_id": str(self.correlation_id)
            if self.correlation_id
            else None,
            "actor": self.actor,
            "source": self.source,
            "reason": self.reason,
            "wallet_id": str(self.wallet_id) if self.wallet_id else None,
            "order_id": str(self.order_id) if self.order_id else None,
            "position_id": str(self.position_id) if self.position_id else None,
            "symbol": self.symbol,
            "detail": self.detail,
        }


def emit_event(
    event_type: str,
    *,
    severity: str = "info",
    correlation_id: UUID | None = None,
    actor: str | None = None,
    source: str | None = None,
    reason: str | None = None,
    wallet_id: UUID | None = None,
    order_id: UUID | None = None,
    position_id: UUID | None = None,
    symbol: str | None = None,
    detail: dict[str, Any] | None = None,
    conn: Any = None,
) -> CriticalEvent:
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event_type: {event_type}")
    ev = CriticalEvent(
        event_type=event_type,
        severity=severity,
        correlation_id=correlation_id,
        actor=actor,
        source=source,
        reason=reason,
        wallet_id=wallet_id,
        order_id=order_id,
        position_id=position_id,
        symbol=symbol,
        detail=detail or {},
    )
    payload = ev.to_dict()
    with _event_lock:
        _event_buffer.append(payload)
    # Structured JSON line for operators (no SaaS).
    print(json.dumps({"critical_event": payload}, default=str), flush=True)

    if conn is not None:
        # Best-effort persist; caller may be outside a TX.
        try:
            import asyncio

            if asyncio.iscoroutinefunction(getattr(conn, "execute", None)):
                pass  # async path via emit_event_async
        except Exception:
            pass
    return ev


async def emit_event_async(
    conn: Any,
    event_type: str,
    *,
    severity: str = "info",
    correlation_id: UUID | None = None,
    actor: str | None = None,
    source: str | None = None,
    reason: str | None = None,
    wallet_id: UUID | None = None,
    order_id: UUID | None = None,
    position_id: UUID | None = None,
    symbol: str | None = None,
    detail: dict[str, Any] | None = None,
) -> CriticalEvent:
    ev = emit_event(
        event_type,
        severity=severity,
        correlation_id=correlation_id,
        actor=actor,
        source=source,
        reason=reason,
        wallet_id=wallet_id,
        order_id=order_id,
        position_id=position_id,
        symbol=symbol,
        detail=detail,
    )
    if conn is not None:
        try:
            await conn.execute(
                """
                INSERT INTO critical_events (
                    ts, event_type, severity, correlation_id, actor, source, reason,
                    wallet_id, order_id, position_id, symbol, detail
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
                """,
                ev.ts,
                ev.event_type,
                ev.severity,
                ev.correlation_id,
                ev.actor,
                ev.source,
                ev.reason,
                ev.wallet_id,
                ev.order_id,
                ev.position_id,
                ev.symbol,
                json.dumps(ev.detail),
            )
        except Exception as exc:  # noqa: BLE001 — table may be absent pre-migrate
            # Still emitted to stdout; persistence is best-effort.
            if "critical_events" not in str(exc):
                raise
    return ev


def recent_events(*, limit: int = 100) -> list[dict[str, Any]]:
    with _event_lock:
        return list(_event_buffer[-limit:])


def clear_events_for_tests() -> None:
    with _event_lock:
        _event_buffer.clear()


class Timer:
    def __init__(self) -> None:
        self._start = time.perf_counter()

    def ms(self) -> float:
        return (time.perf_counter() - self._start) * 1000.0
