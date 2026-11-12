from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from app.api import instruments
from app.exchanges.oanda import OandaConfigurationError


@pytest.mark.asyncio
async def test_instruments_normalize_and_filter(monkeypatch):
    rows = {
        ("binance", "spot"): [{"symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT", "status": "TRADING", "is_spot_trading_allowed": False}],
        ("bybit", "spot"): [{"symbol": "ETHUSDT", "base_coin": "ETH", "quote_coin": "USDT", "status": "Trading"}],
        ("okx", "spot"): [{"symbol": "BTC-USDT", "base_coin": "BTC", "quote_coin": "USDT", "status": "live"}],
    }
    class Adapter:
        def __init__(self, key):
            self.list_instruments = AsyncMock(return_value=rows[key])
    monkeypatch.setattr(instruments, "get_adapter", lambda *, exchange, market: Adapter((exchange, market)))
    app = FastAPI()
    app.include_router(instruments.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/instruments", params={"market": "spot"})
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3
        assert [item["available"] for item in data["items"]] == [False, True, True]
        assert data["items"][0] == {"exchange": "binance", "market": "spot", "symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT", "available": False}
        assert (await client.get("/api/instruments?exchange=okx&market=forex")).status_code == 400


@pytest.mark.asyncio
async def test_oanda_markets_share_fetch_and_report_errors(monkeypatch):
    adapter = type("Adapter", (), {})()
    adapter.list_instruments = AsyncMock(return_value=[
        {"symbol": "EUR_USD", "market": "forex", "tradeable": True},
        {"symbol": "SPX500_USD", "market": "stocks", "tradeable": False},
    ])
    monkeypatch.setattr(instruments, "get_adapter", lambda **kwargs: adapter)
    app = FastAPI()
    app.include_router(instruments.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/instruments?exchange=oanda")
        assert response.status_code == 200
        assert [(i["market"], i["base_asset"], i["quote_asset"], i["available"]) for i in response.json()["items"]] == [("forex", "EUR", "USD", True), ("stocks", "SPX500", "USD", False)]
        adapter.list_instruments.assert_awaited_once()
        adapter.list_instruments.side_effect = OandaConfigurationError("missing")
        assert (await client.get("/api/instruments?exchange=oanda")).status_code == 503
        adapter.list_instruments.side_effect = httpx.ConnectError("offline")
        assert (await client.get("/api/instruments?exchange=oanda&market=forex")).status_code == 502
