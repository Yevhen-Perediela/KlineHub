from __future__ import annotations

import pytest

from app.exchanges.bybit import BybitAdapter


class FakeResponse:
    status_code = 200
    headers = {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "retCode": 0,
            "result": {
                "list": [["1700000000000", "100", "110", "90", "105", "12", "1250"]]
            },
        }


class FakeClient:
    calls: list[tuple[str, dict]] = []

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def get(self, url: str, params: dict):
        self.calls.append((url, params))
        return FakeResponse()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("market", "basis", "endpoint", "category", "volume"),
    [
        ("futures", "mark", "mark-price-kline", "linear", "0"),
        ("futures", "trade", "kline", "linear", "12"),
        ("spot", "trade", "kline", "spot", "12"),
    ],
)
async def test_rest_endpoint_selection(monkeypatch, market, basis, endpoint, category, volume):
    FakeClient.calls.clear()
    adapter = BybitAdapter()

    async def resolve_symbol(*, market: str, symbol: str) -> str:
        return symbol

    monkeypatch.setattr(adapter, "resolve_symbol", resolve_symbol)
    monkeypatch.setattr("app.exchanges.bybit.httpx.AsyncClient", FakeClient)
    events = await adapter.fetch_klines(
        market=market,
        symbol="BTCUSDT",
        interval="1d",
        price_basis=basis,
    )

    url, params = FakeClient.calls[0]
    assert url.endswith(f"/v5/market/{endpoint}")
    assert params["category"] == category
    assert events[0]["price_basis"] == basis
    assert events[0]["source"] == "rest"
    assert events[0]["volume"] == volume
