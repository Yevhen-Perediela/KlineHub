from __future__ import annotations

import pytest

from app.services.candle_service import CandleService


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str) -> None:
        self.values[key] = value

    async def get(self, key: str):
        return self.values.get(key)


@pytest.mark.asyncio
async def test_open_keys_are_isolated_by_basis(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr("app.services.candle_service.get_redis", lambda: redis)
    common = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "open_time": 1,
        "close_time": 2,
        "open": "1",
        "high": "2",
        "low": "1",
        "close": "2",
        "volume": "0",
        "is_closed": False,
        "source": "rest",
    }
    await CandleService.write_open_candle_to_redis(
        exchange="bybit",
        market="futures",
        symbol="BTCUSDT",
        interval="1d",
        price_basis="mark",
        event={**common, "price_basis": "mark", "close": "100"},
    )
    await CandleService.write_open_candle_to_redis(
        exchange="bybit",
        market="futures",
        symbol="BTCUSDT",
        interval="1d",
        price_basis="trade",
        event={**common, "price_basis": "trade", "close": "101"},
    )

    mark_key = CandleService._open_key("bybit", "futures", "BTCUSDT", "1d", "mark")
    trade_key = CandleService._open_key("bybit", "futures", "BTCUSDT", "1d", "trade")
    assert mark_key != trade_key
    assert '"close": "100"' in redis.values[mark_key]
    assert '"close": "101"' in redis.values[trade_key]


def test_all_cache_key_families_include_basis():
    for builder in (CandleService._open_key, CandleService._last_key, CandleService._recent_key):
        assert builder("bybit", "futures", "BTCUSDT", "1d", "mark") != builder(
            "bybit", "futures", "BTCUSDT", "1d", "trade"
        )
