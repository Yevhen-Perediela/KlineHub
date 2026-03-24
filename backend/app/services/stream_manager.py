from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import websockets
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..exchanges.binance_futures import BinanceFuturesAdapter
from ..models import TrackedPair
from ..services.backfill_service import BackfillService
from ..services.candle_service import CandleService
from ..services.realtime_service import RealtimeService
from ..state import runtime_state

logger = logging.getLogger(__name__)


class StreamManager:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        backfill_service: BackfillService,
        realtime_service: RealtimeService,
    ) -> None:
        self.session_factory = session_factory
        self.backfill_service = backfill_service
        self.realtime_service = realtime_service
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._adapter = BinanceFuturesAdapter()
        self._reload_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._task and not self._task.done():
            logger.info("StreamManager already running")
            return

        logger.info("Starting StreamManager")
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        logger.info("Stopping StreamManager")
        self._stop_event.set()

        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except asyncio.TimeoutError:
                logger.warning("StreamManager stop timeout, cancelling task")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            finally:
                self._task = None

    async def reload(self) -> None:
        async with self._reload_lock:
            logger.info("Reloading StreamManager")
            await self.stop()
            await self.start()

    async def _run_forever(self) -> None:
        backoff_seconds = settings.ws_reconnect_min_sec

        while not self._stop_event.is_set():
            try:
                runtime_state.ws_connecting = True
                runtime_state.ws_last_error = None

                streams = await self._load_active_streams()
                runtime_state.active_streams = streams
                runtime_state.active_streams_count = len(streams)

                logger.info("Active streams loaded: %s", streams)

                if not streams:
                    runtime_state.ws_connected = False
                    runtime_state.ws_connecting = False
                    logger.info("No active streams configured, sleeping")
                    await asyncio.sleep(5)
                    continue

                ws_url = self._adapter.build_combined_url(streams)
                logger.info("Connecting to Binance futures WS with %s streams", len(streams))

                async with websockets.connect(
                    ws_url,
                    ping_interval=settings.ws_ping_interval_sec,
                    ping_timeout=settings.ws_ping_timeout_sec,
                    close_timeout=10,
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    runtime_state.ws_connected = True
                    runtime_state.ws_connecting = False
                    runtime_state.ws_connected_at = datetime.now(timezone.utc)
                    logger.info("Binance futures WS connected")

                    asyncio.create_task(self.backfill_service.repair_all_active_pairs())

                    backoff_seconds = settings.ws_reconnect_min_sec

                    while not self._stop_event.is_set():
                        raw_message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=settings.ws_receive_timeout_sec,
                        )
                        await self._handle_message(raw_message)

            except asyncio.TimeoutError:
                runtime_state.ws_connected = False
                runtime_state.ws_connecting = False
                runtime_state.ws_last_error = "WebSocket receive timeout"
                runtime_state.ws_reconnect_count += 1
                logger.warning("WebSocket receive timeout, reconnecting in %ss", backoff_seconds)

            except Exception as exc:
                runtime_state.ws_connected = False
                runtime_state.ws_connecting = False
                runtime_state.ws_last_error = str(exc)
                runtime_state.ws_reconnect_count += 1
                logger.exception("StreamManager loop error: %s", exc)

            if self._stop_event.is_set():
                break

            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, settings.ws_reconnect_max_sec)

        runtime_state.ws_connected = False
        runtime_state.ws_connecting = False
        logger.info("StreamManager loop stopped")

    async def _load_active_streams(self) -> list[str]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(
                    TrackedPair.exchange == "binance",
                    TrackedPair.market == "futures",
                    TrackedPair.status == "active",
                )
            )
            items = result.scalars().all()

        runtime_state.tracked_pairs_total = len(items)
        runtime_state.tracked_pairs_active = len(items)

        streams: list[str] = []
        for item in items:
            streams.append(
                self._adapter.build_stream_name(
                    symbol=item.symbol,
                    interval=item.interval,
                )
            )

        return sorted(set(streams))

    async def _handle_message(self, raw_message: Any) -> None:
        runtime_state.ws_last_message_at = datetime.now(timezone.utc)

        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")

        message = self._adapter.parse_message(raw_message)
        kline_event = self._adapter.extract_kline_event(message)

        if not kline_event:
            logger.debug("Non-kline event received: %s", message)
            return

        runtime_state.last_kline_event = kline_event

        exchange = "binance"
        market = "futures"
        symbol = str(kline_event["symbol"]).upper()
        interval = str(kline_event["interval"])

        await CandleService.write_open_candle_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=kline_event,
        )

        await self.realtime_service.publish_kline(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=kline_event,
        )

        if bool(kline_event["is_closed"]):
            async with self.session_factory() as session:
                await CandleService.upsert_closed_candles(
                    db=session,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=interval,
                    events=[kline_event],
                )

            await CandleService.write_closed_candle_to_redis(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                event=kline_event,
            )

            logger.info(
                "CLOSED %s %s o=%s h=%s l=%s c=%s v=%s",
                symbol,
                interval,
                kline_event["open"],
                kline_event["high"],
                kline_event["low"],
                kline_event["close"],
                kline_event["volume"],
            )
        else:
            logger.debug(
                "OPEN %s %s c=%s",
                symbol,
                interval,
                kline_event["close"],
            )