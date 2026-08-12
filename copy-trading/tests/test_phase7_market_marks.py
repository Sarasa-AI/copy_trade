"""P7-002 / P7-003 — timestamped marks and freshness policy."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

os.environ.setdefault("EXECUTION_DELAY_SEC", "0")
os.environ["MAX_MARK_AGE_SEC"] = "30"

import price_feed  # noqa: E402
from position_manager import (  # noqa: E402
    evaluate_close_trigger,
    get_fresh_mark_price,
    get_mark_price,
)


@pytest.fixture(autouse=True)
def _clear_marks():
    price_feed.clear_all_marks()
    yield
    price_feed.clear_all_marks()


def test_update_mark_stores_timestamp_source_symbol():
    before = datetime.now(timezone.utc)
    quote = price_feed.update_mark(
        "BTCUSDT", 65000.0, source="test_fixture"
    )
    after = datetime.now(timezone.utc)
    assert quote.symbol == "BTCUSDT"
    assert quote.price == 65000.0
    assert quote.source == "test_fixture"
    assert before <= quote.ts_utc <= after
    assert price_feed.get_mark("BTCUSDT") == quote
    assert price_feed.prices["BTCUSDT"] == 65000.0


def test_mark_age_and_freshness():
    now = datetime(2026, 8, 4, 12, 0, 0, tzinfo=timezone.utc)
    price_feed.update_mark(
        "BTCUSDT",
        100.0,
        source="test",
        ts_utc=now - timedelta(seconds=10),
    )
    assert price_feed.mark_age_sec("BTCUSDT", now=now) == pytest.approx(10.0)
    assert price_feed.is_mark_fresh("BTCUSDT", now=now, max_age_sec=30) is True
    fresh = price_feed.require_fresh_mark("BTCUSDT", now=now, max_age_sec=30)
    assert fresh.price == 100.0


def test_missing_mark_raises():
    with pytest.raises(price_feed.MarkMissing) as ei:
        price_feed.require_fresh_mark("ETHUSDT")
    assert ei.value.symbol == "ETHUSDT"
    assert price_feed.mark_age_sec("ETHUSDT") is None
    assert price_feed.is_mark_fresh("ETHUSDT") is False
    assert get_mark_price("ETHUSDT") is None
    assert get_fresh_mark_price("ETHUSDT") is None


def test_stale_mark_raises_and_is_not_treated_as_fresh():
    now = datetime(2026, 8, 4, 12, 0, 0, tzinfo=timezone.utc)
    price_feed.update_mark(
        "BTCUSDT",
        200.0,
        source="test",
        ts_utc=now - timedelta(seconds=45),
    )
    assert price_feed.get_mark("BTCUSDT") is not None
    assert price_feed.is_mark_fresh("BTCUSDT", now=now, max_age_sec=30) is False
    with pytest.raises(price_feed.MarkStale) as ei:
        price_feed.require_fresh_mark("BTCUSDT", now=now, max_age_sec=30)
    assert ei.value.age_sec == pytest.approx(45.0)
    assert ei.value.max_age_sec == 30.0
    # Raw price still readable; fresh helpers hide it.
    assert get_mark_price("BTCUSDT") == 200.0
    assert get_fresh_mark_price("BTCUSDT") is None
    assert price_feed.fresh_mark_prices(now=now, max_age_sec=30) == {}


def test_fresh_update_recovers_from_stale():
    now = datetime(2026, 8, 4, 12, 0, 0, tzinfo=timezone.utc)
    price_feed.update_mark(
        "BTCUSDT",
        200.0,
        source="test",
        ts_utc=now - timedelta(seconds=60),
    )
    with pytest.raises(price_feed.MarkStale):
        price_feed.require_fresh_mark("BTCUSDT", now=now, max_age_sec=30)

    price_feed.update_mark(
        "BTCUSDT",
        201.0,
        source="test_recovery",
        ts_utc=now - timedelta(seconds=1),
    )
    quote = price_feed.require_fresh_mark("BTCUSDT", now=now, max_age_sec=30)
    assert quote.price == 201.0
    assert quote.source == "test_recovery"
    assert price_feed.fresh_mark_prices(now=now, max_age_sec=30) == {
        "BTCUSDT": 201.0
    }


def test_heartbeat_tracks_last_update():
    assert price_feed.heartbeat_age_sec() is None
    assert price_feed.last_update() is None
    ts = datetime(2026, 8, 4, 12, 0, 0, tzinfo=timezone.utc)
    price_feed.update_mark("BTCUSDT", 1.0, source="hb", ts_utc=ts)
    assert price_feed.last_update() == ts
    assert price_feed.last_update("BTCUSDT") == ts
    later = ts + timedelta(seconds=5)
    assert price_feed.heartbeat_age_sec(now=later) == pytest.approx(5.0)


def test_max_mark_age_sec_from_env_not_hardcoded():
    with mock.patch.dict(os.environ, {"MAX_MARK_AGE_SEC": "12"}):
        assert price_feed.max_mark_age_sec() == 12.0
        now = datetime(2026, 8, 4, 12, 0, 0, tzinfo=timezone.utc)
        price_feed.update_mark(
            "BTCUSDT",
            50.0,
            source="env",
            ts_utc=now - timedelta(seconds=15),
        )
        with pytest.raises(price_feed.MarkStale) as ei:
            price_feed.require_fresh_mark("BTCUSDT", now=now)
        assert ei.value.max_age_sec == 12.0


def test_legacy_prices_write_creates_timestamped_mark():
    with price_feed.lock:
        price_feed.prices["ETHUSDT"] = 3000.0
    quote = price_feed.get_mark("ETHUSDT")
    assert quote is not None
    assert quote.price == 3000.0
    assert quote.source == "legacy_prices_write"
    with price_feed.lock:
        price_feed.prices.pop("ETHUSDT", None)
    assert price_feed.get_mark("ETHUSDT") is None


def test_evaluate_close_trigger_unchanged_for_fresh_float():
    row = {
        "side": "BUY",
        "stop_loss_price": 64000.0,
        "take_profit_price": 66000.0,
    }
    assert evaluate_close_trigger(row, 63000.0) == "STOP_LOSS"
    assert evaluate_close_trigger(row, None) is None
