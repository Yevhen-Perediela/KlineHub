from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..exchanges.binance_spot import InvalidSpotSymbolError
from ..exchanges.bybit import InvalidBybitSymbolError
from ..exchanges.oanda import InvalidOandaInstrumentError
from ..exchanges.okx import InvalidOkxSymbolError
from ..exchanges.registry import get_adapter, get_canonical_interval
from ..models import TrackedPair
from ..price_basis import resolve_price_basis
from ..state import runtime_state
from ..utils.intervals import is_supported_interval
from .backfill_service import BackfillService
from .candle_service import CandleService
from .realtime_service import RealtimeService

logger = logging.getLogger(__name__)


INVALID_SYMBOL_ERRORS = (
    InvalidSpotSymbolError,
    InvalidBybitSymbolError,
    InvalidOandaInstrumentError,
    InvalidOkxSymbolError,
)


@dataclass(frozen=True, slots=True)
class StreamKey:
    exchange: str
    market: str
    symbol: str
    interval: str
    price_basis: str

    @property
    def channel(self) -> str:
        return RealtimeService.make_channel(
            exchange=self.exchange,
            market=self.market,
            symbol=self.symbol,
            interval=self.interval,
            price_basis=self.price_basis,
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "exchange": self.exchange,
            "market": self.market,
            "symbol": self.symbol,
            "interval": self.interval,
            "price_basis": self.price_basis,
        }

    def as_response(self) -> dict[str, str]:
        return {**self.as_dict(), "channel": self.channel}


class ChartProtocolError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


class ChartWebSocketService:
    protocol = "chart-v1"

    def __init__(
        self,
        *,
        realtime_service: RealtimeService,
        session_factory: async_sessionmaker[AsyncSession],
        backfill_service: BackfillService,
    ) -> None:
        self.realtime_service = realtime_service
        self.session_factory = session_factory
        self.backfill_service = backfill_service
        self.on_demand_tracking_service: Any | None = None
        self._sequence_by_channel: dict[str, int] = {}
        self._sequence_lock = asyncio.Lock()
        self._activation_tasks: dict[StreamKey, asyncio.Task[None]] = {}
        self._activation_lock = asyncio.Lock()

    async def handle(self, websocket: WebSocket) -> None:
        session = ChartConnectionSession(service=self, websocket=websocket)
        await session.run()

    async def normalize_stream(self, raw: Any) -> StreamKey:
        if not isinstance(raw, dict):
            raise ChartProtocolError("INVALID_STREAM", "stream must be an object")

        exchange_raw = raw.get("exchange")
        market_raw = raw.get("market")
        symbol_raw = raw.get("symbol")
        interval_raw = raw.get("interval")
        price_basis_raw = raw.get("price_basis")

        if not isinstance(exchange_raw, str) or not exchange_raw.strip():
            raise ChartProtocolError("INVALID_EXCHANGE", "exchange is required", details=raw)
        if not isinstance(market_raw, str) or not market_raw.strip():
            raise ChartProtocolError("INVALID_MARKET", "market is required", details=raw)
        if not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ChartProtocolError("INVALID_SYMBOL", "symbol is required", details=raw)
        if not isinstance(interval_raw, str) or not interval_raw.strip():
            raise ChartProtocolError("INVALID_INTERVAL", "interval is required", details=raw)

        exchange = exchange_raw.lower().strip()
        market = market_raw.lower().strip()
        symbol = symbol_raw.upper().strip()
        interval = interval_raw.strip()

        try:
            price_basis = resolve_price_basis(
                exchange=exchange,
                market=market,
                requested_price_basis=price_basis_raw,
            ).value
        except ValueError as exc:
            raise ChartProtocolError("INVALID_PRICE_BASIS", str(exc), details=raw) from exc

        if not is_supported_interval(interval):
            raise ChartProtocolError(
                "INVALID_INTERVAL",
                f"Unsupported interval: {interval}",
                details={"exchange": exchange, "market": market, "symbol": symbol, "interval": interval},
            )

        try:
            adapter = get_adapter(exchange=exchange, market=market)
        except ValueError as exc:
            if exchange not in {"binance", "bybit", "okx", "oanda"}:
                raise ChartProtocolError("INVALID_EXCHANGE", str(exc), details=raw) from exc
            raise ChartProtocolError("INVALID_MARKET", str(exc), details=raw) from exc

        canonical_interval = get_canonical_interval(
            exchange=exchange,
            market=market,
            requested_interval=interval,
        )

        resolver = getattr(adapter, "resolve_symbol", None)
        if callable(resolver):
            try:
                symbol = await resolver(market=market, symbol=symbol)
            except INVALID_SYMBOL_ERRORS as exc:
                raise ChartProtocolError(
                    "INVALID_SYMBOL",
                    str(exc),
                    details={"exchange": exchange, "market": market, "symbol": symbol},
                ) from exc

        try:
            await self.backfill_service.validate_pair(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=canonical_interval,
                price_basis=price_basis,
            )
        except INVALID_SYMBOL_ERRORS as exc:
            raise ChartProtocolError(
                "INVALID_SYMBOL",
                str(exc),
                details={"exchange": exchange, "market": market, "symbol": symbol},
            ) from exc
        except ValueError as exc:
            message = str(exc)
            code = "INVALID_INTERVAL" if "interval" in message.lower() else "INVALID_STREAM"
            raise ChartProtocolError(
                code,
                message,
                details={
                    "exchange": exchange,
                    "market": market,
                    "symbol": symbol,
                    "interval": canonical_interval,
                },
            ) from exc

        return StreamKey(
            exchange=exchange,
            market=market,
            symbol=symbol.upper(),
            interval=canonical_interval,
            price_basis=price_basis,
        )

    async def normalize_streams(self, raw_streams: Any) -> list[StreamKey]:
        if not isinstance(raw_streams, list) or not raw_streams:
            raise ChartProtocolError("INVALID_MESSAGE", "streams must be a non-empty array")
        if len(raw_streams) > settings.chart_ws_max_streams_per_request:
            raise ChartProtocolError(
                "SUBSCRIPTION_LIMIT_EXCEEDED",
                f"maximum streams per request is {settings.chart_ws_max_streams_per_request}",
            )

        normalized: list[StreamKey] = []
        seen: set[StreamKey] = set()
        for item in raw_streams:
            stream = await self.normalize_stream(item)
            if stream not in seen:
                normalized.append(stream)
                seen.add(stream)
        return normalized

    async def next_sequence(self, channel: str) -> int:
        async with self._sequence_lock:
            value = self._sequence_by_channel.get(channel, 0) + 1
            self._sequence_by_channel[channel] = value
            return value

    async def is_stream_active(self, stream: StreamKey) -> bool:
        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(
                    TrackedPair.exchange == stream.exchange,
                    TrackedPair.market == stream.market,
                    TrackedPair.symbol == stream.symbol,
                    TrackedPair.interval == stream.interval,
                    TrackedPair.price_basis == stream.price_basis,
                    TrackedPair.status == "active",
                )
            )
            return result.scalar_one_or_none() is not None

    async def ensure_stream_active(self, stream: StreamKey) -> None:
        if self.on_demand_tracking_service is None:
            raise RuntimeError("on-demand tracking service is not configured")

        async with self._activation_lock:
            task = self._activation_tasks.get(stream)
            if task is None or task.done():
                task = asyncio.create_task(self._activate_stream(stream))
                self._activation_tasks[stream] = task

        try:
            await task
        finally:
            async with self._activation_lock:
                if self._activation_tasks.get(stream) is task and task.done():
                    self._activation_tasks.pop(stream, None)

    async def _activate_stream(self, stream: StreamKey) -> None:
        await self.on_demand_tracking_service.ensure_pair_tracked(
            exchange=stream.exchange,
            market=stream.market,
            symbol=stream.symbol,
            interval=stream.interval,
            price_basis=stream.price_basis,
        )

    async def get_snapshot(self, stream: StreamKey) -> dict[str, Any] | None:
        return await CandleService.get_latest_cached_candle(
            exchange=stream.exchange,
            market=stream.market,
            symbol=stream.symbol,
            interval=stream.interval,
            price_basis=stream.price_basis,
        )


class ChartConnectionSession:
    def __init__(self, *, service: ChartWebSocketService, websocket: WebSocket) -> None:
        self.service = service
        self.websocket = websocket
        self.subscriptions: set[StreamKey] = set()
        self.generation = 0
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=settings.chart_ws_outbound_queue_size
        )
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False
        self._last_received_at = time.monotonic()

    async def run(self) -> None:
        await self.websocket.accept()
        runtime_state.chart_ws_connections_current += 1
        runtime_state.chart_ws_connections_total += 1

        writer = self._track_task(self._writer_loop())
        heartbeat = self._track_task(self._heartbeat_loop())

        try:
            await self._send_control(
                {
                    "type": "connected",
                    "protocol": self.service.protocol,
                    "message": "chart realtime websocket connected",
                }
            )
            while not self._closed:
                try:
                    raw = await asyncio.wait_for(
                        self.websocket.receive_text(),
                        timeout=settings.chart_ws_idle_timeout_sec,
                    )
                except asyncio.TimeoutError:
                    await self.close()
                    break

                self._last_received_at = time.monotonic()
                await self._handle_raw_message(raw)
        except WebSocketDisconnect:
            pass
        except Exception:
            logger.exception("chart websocket session failed")
        finally:
            self._closed = True
            writer.cancel()
            heartbeat.cancel()
            for task in list(self._tasks):
                task.cancel()
            await self.service.realtime_service.disconnect_chart(self)
            runtime_state.chart_ws_connections_current = max(
                0,
                runtime_state.chart_ws_connections_current - 1,
            )

    def _track_task(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self.websocket.close()
        except Exception:
            pass

    async def _handle_raw_message(self, raw: str) -> None:
        request_id: Any = None
        if len(raw.encode("utf-8")) > settings.chart_ws_max_message_bytes:
            await self._send_error(
                request_id=None,
                code="INVALID_MESSAGE",
                message="message is too large",
            )
            return

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            await self._send_error(request_id=None, code="INVALID_JSON", message="invalid json")
            return

        if not isinstance(data, dict):
            await self._send_error(request_id=None, code="INVALID_MESSAGE", message="message must be an object")
            return

        request_id = data.get("request_id")
        action = data.get("action")
        request_received_at = time.monotonic()

        try:
            if action == "subscribe":
                await self._handle_subscribe(data, request_id=request_id, request_received_at=request_received_at)
            elif action == "unsubscribe":
                await self._handle_unsubscribe(data, request_id=request_id)
            elif action == "switch":
                await self._handle_switch(data, request_id=request_id, request_received_at=request_received_at)
            elif action == "pong":
                return
            else:
                raise ChartProtocolError("UNSUPPORTED_ACTION", "unsupported action")
        except ChartProtocolError as exc:
            await self._send_error(
                request_id=request_id,
                code=exc.code,
                message=exc.message,
                details=exc.details,
            )
        except Exception:
            logger.exception("chart websocket request failed")
            await self._send_error(
                request_id=request_id,
                code="INTERNAL_ERROR",
                message="internal error",
            )

    async def _handle_subscribe(
        self,
        data: dict[str, Any],
        *,
        request_id: Any,
        request_received_at: float,
    ) -> None:
        streams = await self.service.normalize_streams(data.get("streams"))
        next_count = len(self.subscriptions | set(streams))
        if next_count > settings.chart_ws_max_subscriptions:
            raise ChartProtocolError(
                "SUBSCRIPTION_LIMIT_EXCEEDED",
                f"maximum subscriptions per connection is {settings.chart_ws_max_subscriptions}",
            )

        added = [stream for stream in streams if stream not in self.subscriptions]
        for stream in added:
            self.subscriptions.add(stream)
            await self.service.realtime_service.subscribe_chart(self, stream.channel)

        runtime_state.chart_ws_subscribe_total += 1
        await self._send_control(
            {
                "type": "subscribed",
                "request_id": request_id,
                "streams": [stream.as_response() for stream in streams],
            }
        )
        self._after_subscription_change(
            request_id=request_id,
            streams=streams,
            generation=self.generation,
            request_received_at=request_received_at,
        )

    async def _handle_unsubscribe(self, data: dict[str, Any], *, request_id: Any) -> None:
        streams = await self.service.normalize_streams(data.get("streams"))
        for stream in streams:
            if stream in self.subscriptions:
                self.subscriptions.remove(stream)
                await self.service.realtime_service.unsubscribe_chart(self, stream.channel)

        runtime_state.chart_ws_unsubscribe_total += 1
        await self._send_control(
            {
                "type": "unsubscribed",
                "request_id": request_id,
                "streams": [stream.as_response() for stream in streams],
            }
        )

    async def _handle_switch(
        self,
        data: dict[str, Any],
        *,
        request_id: Any,
        request_received_at: float,
    ) -> None:
        unsubscribe = await self.service.normalize_streams(data.get("unsubscribe"))
        subscribe = await self.service.normalize_streams(data.get("subscribe"))
        next_subscriptions = (self.subscriptions - set(unsubscribe)) | set(subscribe)
        if len(next_subscriptions) > settings.chart_ws_max_subscriptions:
            raise ChartProtocolError(
                "SUBSCRIPTION_LIMIT_EXCEEDED",
                f"maximum subscriptions per connection is {settings.chart_ws_max_subscriptions}",
            )

        self.generation += 1
        generation = self.generation

        for stream in self.subscriptions - next_subscriptions:
            await self.service.realtime_service.unsubscribe_chart(self, stream.channel)
        for stream in next_subscriptions - self.subscriptions:
            await self.service.realtime_service.subscribe_chart(self, stream.channel)
        self.subscriptions = next_subscriptions

        runtime_state.chart_ws_switch_total += 1
        await self._send_control(
            {
                "type": "switched",
                "request_id": request_id,
                "unsubscribed": [stream.as_response() for stream in unsubscribe],
                "subscribed": [stream.as_response() for stream in subscribe],
            }
        )
        self._after_subscription_change(
            request_id=request_id,
            streams=subscribe,
            generation=generation,
            request_received_at=request_received_at,
        )

    def _after_subscription_change(
        self,
        *,
        request_id: Any,
        streams: list[StreamKey],
        generation: int,
        request_received_at: float,
    ) -> None:
        for stream in streams:
            self._track_task(
                self._send_snapshot_if_available(
                    request_id=request_id,
                    stream=stream,
                    generation=generation,
                    request_received_at=request_received_at,
                )
            )
            self._track_task(
                self._warm_stream_if_needed(
                    request_id=request_id,
                    stream=stream,
                    generation=generation,
                    request_received_at=request_received_at,
                )
            )

    async def _send_snapshot_if_available(
        self,
        *,
        request_id: Any,
        stream: StreamKey,
        generation: int,
        request_received_at: float,
    ) -> None:
        snapshot = await self.service.get_snapshot(stream)
        if snapshot is None:
            return
        if self.generation != generation or stream not in self.subscriptions:
            return
        await self._send_control(
            {
                "type": "snapshot",
                "request_id": request_id,
                "stream": stream.as_dict(),
                "data": snapshot,
            }
        )
        logger.info(
            "chart_ws snapshot_sent stream=%s request_id=%s latency_ms=%s",
            stream.channel,
            request_id,
            int((time.monotonic() - request_received_at) * 1000),
        )

    async def _warm_stream_if_needed(
        self,
        *,
        request_id: Any,
        stream: StreamKey,
        generation: int,
        request_received_at: float,
    ) -> None:
        if await self.service.is_stream_active(stream):
            return
        if self.generation != generation or stream not in self.subscriptions:
            return

        runtime_state.chart_ws_warmup_total += 1
        await self._send_control(
            {
                "type": "warming_up",
                "request_id": request_id,
                "stream": stream.as_dict(),
            }
        )

        try:
            await self.service.ensure_stream_active(stream)
        except Exception:
            runtime_state.chart_ws_warmup_failed_total += 1
            logger.exception("chart_ws warmup failed stream=%s request_id=%s", stream.channel, request_id)
            if self.generation == generation and stream in self.subscriptions:
                await self._send_error(
                    request_id=request_id,
                    code="INTERNAL_ERROR",
                    message="stream warmup failed",
                    details=stream.as_dict(),
                )
            return

        if self.generation != generation or stream not in self.subscriptions:
            return

        await self._send_control(
            {
                "type": "stream_ready",
                "request_id": request_id,
                "stream": stream.as_dict(),
            }
        )
        logger.info(
            "chart_ws subscription_ready stream=%s request_id=%s latency_ms=%s",
            stream.channel,
            request_id,
            int((time.monotonic() - request_received_at) * 1000),
        )

    def enqueue_kline(self, *, channel: str, stream: dict[str, str], event: dict[str, Any]) -> None:
        if self._closed:
            return
        if channel not in {item.channel for item in self.subscriptions}:
            return

        try:
            data = {
                "time": int(event["open_time"]),
                "open": float(event["open"]),
                "high": float(event["high"]),
                "low": float(event["low"]),
                "close": float(event["close"]),
                "volume": float(event["volume"]),
            }
        except (KeyError, TypeError, ValueError):
            return

        item = {
            "kind": "kline",
            "channel": channel,
            "stream": stream,
            "data": data,
        }
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            runtime_state.chart_ws_dropped_updates_total += 1

    async def _send_control(self, payload: dict[str, Any]) -> None:
        await self._queue.put({"kind": "control", "payload": payload})

    async def _send_error(
        self,
        *,
        request_id: Any,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        runtime_state.chart_ws_errors_total += 1
        payload: dict[str, Any] = {
            "type": "error",
            "request_id": request_id,
            "code": code,
            "message": message,
        }
        if details is not None:
            payload["details"] = details
        await self._send_control(payload)

    async def _writer_loop(self) -> None:
        while not self._closed:
            item = await self._queue.get()
            payload = item.get("payload")
            if item.get("kind") == "kline":
                channel = item["channel"]
                if channel not in {stream.channel for stream in self.subscriptions}:
                    continue
                sequence = await self.service.next_sequence(channel)
                payload = {
                    "type": "kline",
                    "stream": item["stream"],
                    "channel": channel,
                    "sequence": sequence,
                    "data": item["data"],
                }
            try:
                await self.websocket.send_text(json.dumps(payload))
                runtime_state.chart_ws_messages_sent_total += 1
            except Exception:
                await self.close()
                break

    async def _heartbeat_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(settings.chart_ws_ping_interval_sec)
            if self._closed:
                break
            if (time.monotonic() - self._last_received_at) > settings.chart_ws_idle_timeout_sec:
                await self.close()
                break
            await self._send_control(
                {
                    "type": "ping",
                    "timestamp": int(time.time() * 1000),
                }
            )
