from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from app.api import klines as klines_api


class FakeAdapter:
    def get_history_backfill_interval(self, interval: str) -> str:
        return interval

    async def resolve_symbol(self, *, market: str, symbol: str) -> str:
        return symbol


class FakeDb:
    async def rollback(self) -> None:
        return None


class FakeBackfill:
    async def validate_pair(self, **kwargs) -> None:
        return None

    async def ensure_range_loaded(self, **kwargs) -> None:
        return None


class FakeOnDemand:
    def __init__(self) -> None:
        self.calls = []

    async def ensure_pair_tracked(self, **kwargs) -> None:
        self.calls.append(kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("profiling", [False, True])
@pytest.mark.parametrize(
    ("requested_basis", "expected_basis", "expected_close"),
    [(None, "mark", 100.0), ("mark", "mark", 100.0), ("trade", "trade", 200.0)],
)
async def test_bybit_futures_http_default_and_explicit_basis(
    monkeypatch, caplog, profiling, requested_basis, expected_basis, expected_close
):
    monkeypatch.setattr(klines_api.settings, "klines_profiling_enabled", profiling)
    caplog.set_level(logging.INFO, logger=klines_api.__name__)
    on_demand = FakeOnDemand()
    fake_backfill = FakeBackfill()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                backfill_service=fake_backfill,
                on_demand_tracking_service=on_demand,
            )
        )
    )

    async def pick_best_source_interval(**kwargs):
        return "1d"

    async def get_bars_from_interval(**kwargs):
        return [{
            "time": 1_700_000_000_000,
            "open": expected_close,
            "high": expected_close,
            "low": expected_close,
            "close": expected_close,
            "volume": 0.0,
        }]

    async def no_open_bar(**kwargs):
        return None

    monkeypatch.setattr(klines_api, "get_adapter", lambda **kwargs: FakeAdapter())
    monkeypatch.setattr(
        klines_api.AggregationService,
        "pick_best_source_interval",
        pick_best_source_interval,
    )
    monkeypatch.setattr(
        klines_api.AggregationService,
        "get_bars_from_interval",
        get_bars_from_interval,
    )
    monkeypatch.setattr(klines_api.OpenCandleService, "get_open_bar", no_open_bar)
    monkeypatch.setattr(klines_api.backfill_service, "ensure_range_loaded", fake_backfill.ensure_range_loaded)

    response = await klines_api.get_klines(
        request=request,  # type: ignore[arg-type]
        exchange="bybit",
        market="futures",
        symbol="BTCUSDT",
        interval="1d",
        price_basis=requested_basis,
        from_ts=1_700_000_000_000,
        to_ts=1_700_000_000_000,
        limit=10,
        db=FakeDb(),  # type: ignore[arg-type]
    )

    assert response.price_basis == expected_basis
    assert response.bars[0].close == expected_close
    assert on_demand.calls[0]["price_basis"] == expected_basis

    summaries = [r for r in caplog.records if r.name == klines_api.__name__]
    assert len(summaries) == int(profiling)
    if profiling:
        summary = json.loads(summaries[0].getMessage())
        assert summary["event"] == "klines_timing"
        assert summary["total_ms"] >= sum(summary["stages_ms"].values()) - 0.01
        assert set(summary["stages_ms"]) == {
            "symbol_resolution", "available_interval_lookup", "pair_validation",
            "on_demand_tracking", "missing_range_backfill", "database_candle_retrieval",
            "aggregation", "open_candle_retrieval", "response_preparation",
        }
        assert summary["stages_ms"]["aggregation"] == 0
        assert "BTCUSDT" not in summaries[0].getMessage()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_basis", [False, True])
async def test_profiling_logs_early_returns_and_errors(monkeypatch, caplog, invalid_basis):
    from fastapi import HTTPException
    monkeypatch.setattr(klines_api.settings, "klines_profiling_enabled", True)
    caplog.set_level(logging.INFO, logger=klines_api.__name__)
    monkeypatch.setattr(klines_api, "get_adapter", lambda **kwargs: FakeAdapter())
    kwargs = dict(request=SimpleNamespace(), exchange="bybit", market="futures",
                  symbol="PRIVATE_SYMBOL", interval="invalid", price_basis="invalid" if invalid_basis else "trade",
                  from_ts=None, to_ts=None, limit=10, db=FakeDb())
    if invalid_basis:
        with pytest.raises(HTTPException) as exc:
            await klines_api.get_klines(**kwargs)
        assert exc.value.status_code == 400
    else:
        response = await klines_api.get_klines(**kwargs)
        assert response.noData
    assert len(caplog.records) == 1
    summary = json.loads(caplog.records[0].getMessage())
    assert summary["total_ms"] >= 0
    assert summary["stages_ms"]["database_candle_retrieval"] == 0
    assert "PRIVATE_SYMBOL" not in caplog.records[0].getMessage()
