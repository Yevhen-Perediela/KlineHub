from __future__ import annotations

import json

import pytest

from app.services.realtime_service import RealtimeService


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send_text(self, payload: str) -> None:
        self.messages.append(json.loads(payload))


@pytest.mark.asyncio
async def test_market_channels_and_delivery_are_basis_isolated():
    service = RealtimeService()
    mark_ws = FakeWebSocket()
    trade_ws = FakeWebSocket()
    common = dict(exchange="bybit", market="futures", symbol="BTCUSDT", interval="1d")

    mark_channel = await service.subscribe(mark_ws, price_basis="mark", **common)  # type: ignore[arg-type]
    trade_channel = await service.subscribe(trade_ws, price_basis="trade", **common)  # type: ignore[arg-type]
    assert mark_channel != trade_channel

    await service.publish_kline(
        price_basis="trade",
        event={
            "open_time": 1,
            "open": "200",
            "high": "200",
            "low": "200",
            "close": "200",
            "volume": "1",
            "is_closed": False,
        },
        **common,
    )

    assert mark_ws.messages == []
    assert trade_ws.messages[0]["price_basis"] == "trade"
    assert trade_ws.messages[0]["channel"].endswith(":trade")


def test_chart_sequence_channel_identity_is_basis_aware():
    mark = RealtimeService.make_channel(
        exchange="bybit", market="futures", symbol="BTCUSDT", interval="1d", price_basis="mark"
    )
    trade = RealtimeService.make_channel(
        exchange="bybit", market="futures", symbol="BTCUSDT", interval="1d", price_basis="trade"
    )
    assert mark == "kline:bybit:futures:BTCUSDT:1d:mark"
    assert trade == "kline:bybit:futures:BTCUSDT:1d:trade"
    assert mark != trade
