from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any

from fastapi import WebSocket

from ..state import runtime_state

logger = logging.getLogger(__name__)


class RealtimeService:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._subscriptions_by_channel: dict[str, set[WebSocket]] = defaultdict(set)
        self._channels_by_connection: dict[WebSocket, set[str]] = defaultdict(set)
        self._chart_subscriptions_by_channel: dict[str, set[Any]] = defaultdict(set)
        self._chart_channels_by_session: dict[Any, set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()

    @staticmethod
    def make_channel(
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
    ) -> str:
        return f"kline:{exchange}:{market}:{symbol.upper()}:{interval}"

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
            runtime_state.internal_ws_clients = len(self._connections)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            channels = self._channels_by_connection.pop(websocket, set())

            for channel in channels:
                subscribers = self._subscriptions_by_channel.get(channel)
                if subscribers:
                    subscribers.discard(websocket)
                    if not subscribers:
                        self._subscriptions_by_channel.pop(channel, None)

            self._connections.discard(websocket)
            runtime_state.internal_ws_clients = len(self._connections)
            runtime_state.internal_ws_subscriptions = sum(
                len(v) for v in self._subscriptions_by_channel.values()
            )

    async def subscribe(
        self,
        websocket: WebSocket,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
    ) -> str:
        channel = self.make_channel(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
        )

        async with self._lock:
            self._subscriptions_by_channel[channel].add(websocket)
            self._channels_by_connection[websocket].add(channel)
            runtime_state.internal_ws_subscriptions = sum(
                len(v) for v in self._subscriptions_by_channel.values()
            )

        return channel

    async def unsubscribe(
        self,
        websocket: WebSocket,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
    ) -> str:
        channel = self.make_channel(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
        )

        async with self._lock:
            subscribers = self._subscriptions_by_channel.get(channel)
            if subscribers:
                subscribers.discard(websocket)
                if not subscribers:
                    self._subscriptions_by_channel.pop(channel, None)

            conn_channels = self._channels_by_connection.get(websocket)
            if conn_channels:
                conn_channels.discard(channel)
                if not conn_channels:
                    self._channels_by_connection.pop(websocket, None)

            runtime_state.internal_ws_subscriptions = sum(
                len(v) for v in self._subscriptions_by_channel.values()
            )

        return channel

    async def publish_kline(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        event: dict[str, Any],
    ) -> None:
        channel = self.make_channel(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
        )

        async with self._lock:
            subscribers = list(self._subscriptions_by_channel.get(channel, set()))
            chart_subscribers = list(self._chart_subscriptions_by_channel.get(channel, set()))

        if not subscribers and not chart_subscribers:
            return

        payload = {
            "type": "kline",
            "channel": channel,
            "exchange": exchange,
            "market": market,
            "symbol": symbol.upper(),
            "interval": interval,
            "is_closed": bool(event["is_closed"]),
            "bar": {
                "time": int(event["open_time"]),
                "open": float(event["open"]),
                "high": float(event["high"]),
                "low": float(event["low"]),
                "close": float(event["close"]),
                "volume": float(event["volume"]),
            },
        }

        dead_connections: list[WebSocket] = []

        for websocket in subscribers:
            try:
                await websocket.send_text(json.dumps(payload))
            except Exception:
                dead_connections.append(websocket)

        for websocket in dead_connections:
            await self.disconnect(websocket)

        for session in chart_subscribers:
            session.enqueue_kline(
                channel=channel,
                stream={
                    "exchange": exchange,
                    "market": market,
                    "symbol": symbol.upper(),
                    "interval": interval,
                },
                event=event,
            )

    async def subscribe_chart(self, session: Any, channel: str) -> None:
        async with self._lock:
            self._chart_subscriptions_by_channel[channel].add(session)
            self._chart_channels_by_session[session].add(channel)
            runtime_state.chart_ws_subscriptions_current = sum(
                len(v) for v in self._chart_subscriptions_by_channel.values()
            )

    async def unsubscribe_chart(self, session: Any, channel: str) -> None:
        async with self._lock:
            subscribers = self._chart_subscriptions_by_channel.get(channel)
            if subscribers:
                subscribers.discard(session)
                if not subscribers:
                    self._chart_subscriptions_by_channel.pop(channel, None)

            channels = self._chart_channels_by_session.get(session)
            if channels:
                channels.discard(channel)
                if not channels:
                    self._chart_channels_by_session.pop(session, None)

            runtime_state.chart_ws_subscriptions_current = sum(
                len(v) for v in self._chart_subscriptions_by_channel.values()
            )

    async def disconnect_chart(self, session: Any) -> None:
        async with self._lock:
            channels = self._chart_channels_by_session.pop(session, set())
            for channel in channels:
                subscribers = self._chart_subscriptions_by_channel.get(channel)
                if subscribers:
                    subscribers.discard(session)
                    if not subscribers:
                        self._chart_subscriptions_by_channel.pop(channel, None)
            runtime_state.chart_ws_subscriptions_current = sum(
                len(v) for v in self._chart_subscriptions_by_channel.values()
            )

    async def stats(self) -> dict[str, int]:
        async with self._lock:
            return {
                "clients": len(self._connections),
                "subscriptions": sum(len(v) for v in self._subscriptions_by_channel.values()),
                "channels": len(self._subscriptions_by_channel),
            }
