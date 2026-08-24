from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.exchanges.base import StreamSubscription
from app.services.stream_manager import StreamManager


class FakeBybitAdapter:
    native_kline_stream = True

    def parse_message(self, raw_message):
        return {}

    def extract_kline_event(self, message):
        return {
            "symbol": "BTCUSDT",
            "interval": "1d",
            "open_time": 1,
            "close_time": 2,
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100",
            "volume": "10",
            "is_closed": True,
        }


def _sub(basis: str) -> StreamSubscription:
    return StreamSubscription(
        exchange="bybit",
        market="futures",
        symbol="BTCUSDT",
        interval="1d",
        price_basis=basis,
        stream_key="kline.D.BTCUSDT",
    )


@pytest.mark.asyncio
async def test_bybit_native_futures_kline_routes_only_to_trade():
    manager = StreamManager(
        session_factory=None,  # type: ignore[arg-type]
        backfill_service=None,  # type: ignore[arg-type]
        realtime_service=None,  # type: ignore[arg-type]
    )
    manager._publish_native_kline = AsyncMock()  # type: ignore[method-assign]

    await manager._handle_stream_message(
        adapter=FakeBybitAdapter(),  # type: ignore[arg-type]
        exchange="bybit",
        market="futures",
        subscriptions=[_sub("mark"), _sub("trade")],
        raw_message="{}",
    )

    manager._publish_native_kline.assert_awaited_once()
    kwargs = manager._publish_native_kline.await_args.kwargs
    assert kwargs["price_basis"] == "trade"
    assert kwargs["event"]["price_basis"] == "trade"
    assert kwargs["event"]["source"] == "ws"


@pytest.mark.asyncio
async def test_bybit_native_futures_kline_cannot_mutate_mark_only_state():
    manager = StreamManager(
        session_factory=None,  # type: ignore[arg-type]
        backfill_service=None,  # type: ignore[arg-type]
        realtime_service=None,  # type: ignore[arg-type]
    )
    manager._publish_native_kline = AsyncMock()  # type: ignore[method-assign]

    await manager._handle_stream_message(
        adapter=FakeBybitAdapter(),  # type: ignore[arg-type]
        exchange="bybit",
        market="futures",
        subscriptions=[_sub("mark")],
        raw_message="{}",
    )

    manager._publish_native_kline.assert_not_awaited()
