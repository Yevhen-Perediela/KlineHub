from __future__ import annotations

from decimal import Decimal

import pytest

from app.models import Candle
from app.services.aggregation_service import AggregationService


class FakeScalars:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return FakeScalars(self.rows)


class BasisSession:
    def __init__(self, rows_by_basis):
        self.rows_by_basis = rows_by_basis
        self.seen_bases: list[str] = []

    async def execute(self, statement):
        params = statement.compile().params
        basis = next(value for key, value in params.items() if key.startswith("price_basis"))
        self.seen_bases.append(basis)
        return FakeResult(self.rows_by_basis[basis])


def _minute_rows(price_basis: str, close: str) -> list[Candle]:
    rows = []
    for minute in range(60):
        value = Decimal(close)
        rows.append(
            Candle(
                id=minute + (0 if price_basis == "mark" else 100),
                exchange="bybit",
                market="futures",
                symbol="BTCUSDT",
                interval="1m",
                price_basis=price_basis,
                open_time=minute * 60_000,
                close_time=(minute + 1) * 60_000 - 1,
                open=value,
                high=value,
                low=value,
                close=value,
                volume=Decimal("1"),
                is_closed=True,
                source="rest",
            )
        )
    return rows


@pytest.mark.asyncio
async def test_aggregation_uses_only_requested_basis():
    db = BasisSession({
        "mark": _minute_rows("mark", "100"),
        "trade": _minute_rows("trade", "200"),
    })
    common = dict(
        db=db,
        exchange="bybit",
        market="futures",
        symbol="BTCUSDT",
        source_interval="1m",
        target_interval="1h",
        from_ts=0,
        to_ts=0,
        limit=10,
    )
    mark = await AggregationService.get_aggregated_bars(price_basis="mark", **common)
    trade = await AggregationService.get_aggregated_bars(price_basis="trade", **common)

    assert mark[0]["close"] == 100.0
    assert trade[0]["close"] == 200.0
    assert db.seen_bases == ["mark", "trade"]
