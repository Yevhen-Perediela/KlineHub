from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from app.api import oanda as oanda_api
from app.exchanges.oanda import OandaConfigurationError


@pytest.mark.asyncio
async def test_account_instruments_and_market_filter(monkeypatch):
    instruments = [
        dict(symbol="EUR_USD", display_name="EUR/USD", market="forex", type="CURRENCY", tradeable=True),
        dict(symbol="XAU_USD", display_name="Gold/USD", market="metals", type="METAL", tradeable=True),
    ]
    adapter = SimpleNamespace(list_instruments=AsyncMock(return_value=instruments))
    monkeypatch.setattr(oanda_api, "get_adapter", lambda **kwargs: adapter)
    app = FastAPI()
    app.include_router(oanda_api.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/oanda/instruments")
        assert response.status_code == 200
        assert response.json()["count"] == 2
        assert [item["symbol"] for item in response.json()["items"]] == ["EUR_USD", "XAU_USD"]
        response = await client.get("/api/oanda/instruments?market=metals")
        assert response.status_code == 200
        assert response.json()["count"] == 1
        assert response.json()["items"][0]["symbol"] == "XAU_USD"
        response = await client.get("/api/oanda/instruments?market=stocks")
        assert response.json() == {"items": [], "count": 0}
        calls_before = adapter.list_instruments.await_count
        response = await client.get("/api/oanda/instruments?market=invalid")
        assert response.status_code == 422
        assert adapter.list_instruments.await_count == calls_before


@pytest.mark.asyncio
@pytest.mark.parametrize("error,status", [
    (OandaConfigurationError("OANDA support requires configuration"), 503),
    (httpx.ConnectError("connection failed"), 502),
    (httpx.HTTPStatusError("secret upstream detail", request=httpx.Request("GET", "https://example.com"), response=httpx.Response(401)), 502),
])
async def test_instrument_fetch_errors(monkeypatch, error, status):
    adapter = SimpleNamespace(list_instruments=AsyncMock(side_effect=error))
    monkeypatch.setattr(oanda_api, "get_adapter", lambda **kwargs: adapter)
    app = FastAPI()
    app.include_router(oanda_api.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/oanda/instruments")
    assert response.status_code == status
    assert "secret upstream detail" not in response.text
