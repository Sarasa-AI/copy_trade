"""WebSocket supervision: capped exponential backoff and stall detection.

A silently dead socket is more dangerous than a closed one, because marks keep
their last value while the market moves. This module never fakes liveness: on a
stall it tears the connection down and reconnects, letting
``price_feed`` marks age out so the Phase 7 freshness gate denies new exposure.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncContextManager, Awaitable, Callable

StateCallback = Callable[[str, dict[str, Any]], Awaitable[None]]


class StreamStalled(RuntimeError):
    """No message arrived within the stall timeout."""

    def __init__(self, name: str, idle_sec: float, timeout_sec: float) -> None:
        self.name = name
        self.idle_sec = idle_sec
        self.timeout_sec = timeout_sec
        super().__init__(
            f"stream_stalled: {name} idle_sec={idle_sec:.3f} "
            f"timeout_sec={timeout_sec:.3f}"
        )


@dataclass(frozen=True)
class BackoffPolicy:
    """Capped exponential backoff with jitter to avoid reconnect storms."""

    initial_sec: float = 1.0
    max_sec: float = 30.0
    multiplier: float = 2.0
    jitter: float = 0.2

    def __post_init__(self) -> None:
        if self.initial_sec <= 0:
            raise ValueError("initial_sec must be positive")
        if self.max_sec < self.initial_sec:
            raise ValueError("max_sec must be >= initial_sec")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if not 0 <= self.jitter < 1:
            raise ValueError("jitter must be in [0, 1)")

    def delay_for(self, attempt: int, *, rand: float | None = None) -> float:
        """Delay before retry ``attempt`` (1-based). ``rand`` injects jitter."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        raw = self.initial_sec * (self.multiplier ** (attempt - 1))
        capped = min(raw, self.max_sec)
        if self.jitter == 0:
            return capped
        r = random.random() if rand is None else rand
        # Symmetric jitter around the capped delay, clamped to the cap.
        return min(self.max_sec, capped * (1 + self.jitter * (2 * r - 1)))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ReconnectingStream:
    """Runs ``handler`` against a connection, restoring it after any failure.

    ``connect`` yields a live connection; ``handler(connection, stream)``
    consumes it until the connection ends. The handler must call
    ``stream.note_message()`` for each frame it processes, which is what stall
    detection watches.
    """

    def __init__(
        self,
        *,
        name: str,
        connect: Callable[[], AsyncContextManager[Any]],
        handler: Callable[[Any, "ReconnectingStream"], Awaitable[None]],
        policy: BackoffPolicy | None = None,
        stall_timeout_sec: float | None = None,
        on_state: StateCallback | None = None,
        max_attempts: int | None = None,
    ) -> None:
        self.name = name
        self._connect = connect
        self._handler = handler
        self.policy = policy or BackoffPolicy()
        self.stall_timeout_sec = stall_timeout_sec
        self._on_state = on_state
        self.max_attempts = max_attempts
        self._stop = asyncio.Event()
        self.connected = False
        self.attempt = 0
        self.reconnect_count = 0
        self.message_count = 0
        self.last_message_utc: datetime | None = None

    # --- liveness ---------------------------------------------------------

    def note_message(self) -> None:
        self.last_message_utc = _utcnow()
        self.message_count += 1

    def idle_sec(self, *, now: datetime | None = None) -> float | None:
        if self.last_message_utc is None:
            return None
        wall = now or _utcnow()
        return max(0.0, (wall - self.last_message_utc).total_seconds())

    # --- lifecycle --------------------------------------------------------

    def stop(self) -> None:
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    async def _notify(self, state: str, **detail: Any) -> None:
        if self._on_state is None:
            return
        try:
            await self._on_state(state, dict(detail))
        except Exception:  # noqa: BLE001 — telemetry must not kill the stream
            pass

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return

    async def _pump(self, connection: Any) -> None:
        """Consume the connection, aborting if it goes quiet."""
        if self.stall_timeout_sec is None:
            await self._handler(connection, self)
            return

        # Seed the idle clock without counting a frame we did not receive.
        self.last_message_utc = _utcnow()
        task = asyncio.create_task(self._handler(connection, self))
        tick = max(0.05, min(1.0, self.stall_timeout_sec / 4))
        try:
            while not task.done():
                await asyncio.wait({task}, timeout=tick)
                if task.done():
                    break
                idle = self.idle_sec() or 0.0
                if idle > self.stall_timeout_sec:
                    raise StreamStalled(
                        self.name, idle, self.stall_timeout_sec
                    )
                if self._stop.is_set():
                    break
        finally:
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        if task.done() and not task.cancelled():
            # Surface a handler crash as this iteration's failure.
            exc = task.exception()
            if exc is not None:
                raise exc

    async def run(self) -> None:
        """Connect/consume/reconnect until :meth:`stop` or ``max_attempts``."""
        while not self._stop.is_set():
            messages_before = self.message_count
            try:
                async with self._connect() as connection:
                    self.connected = True
                    await self._notify("connected")
                    await self._pump(connection)
                await self._notify("closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — any failure is retryable
                await self._notify(
                    "error", error=str(exc), error_type=type(exc).__name__
                )
            finally:
                self.connected = False

            if self._stop.is_set():
                return

            if self.message_count > messages_before:
                # The connection actually delivered data, so start over from
                # the shortest delay instead of inheriting an old backoff.
                self.attempt = 0
            self.attempt += 1
            self.reconnect_count += 1
            if self.max_attempts is not None and self.attempt > self.max_attempts:
                await self._notify("give_up", attempts=self.attempt)
                return
            delay = self.policy.delay_for(self.attempt)
            await self._notify("reconnecting", attempt=self.attempt, delay=delay)
            await self._sleep_or_stop(delay)


__all__ = (
    "BackoffPolicy",
    "ReconnectingStream",
    "StreamStalled",
)
