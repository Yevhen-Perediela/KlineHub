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


def chunk_list(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


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
        self._adapter = BinanceFuturesAdapter()

        self._stop_event = asyncio.Event()
        self._reload_lock = asyncio.Lock()

        self._supervisor_task: asyncio.Task | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._backfill_task: asyncio.Task | None = None

        # safer than putting all 609 into one socket
        self._streams_per_connection = getattr(settings, "ws_streams_per_connection", 200)

    async def start(self) -> None:
        if self._supervisor_task and not self._supervisor_task.done():
            logger.info("StreamManager already running")
            return

        logger.info("Starting StreamManager")
        self._stop_event.clear()
        self._supervisor_task = asyncio.create_task(self._run_supervisor())

    async def stop(self) -> None:
        logger.info("Stopping StreamManager")
        self._stop_event.set()

        # stop workers
        for task in self._worker_tasks:
            task.cancel()

        for task in self._worker_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Worker task stop error")

        self._worker_tasks.clear()

        # stop backfill
        if self._backfill_task:
            self._backfill_task.cancel()
            try:
                await self._backfill_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Backfill task stop error")
            finally:
                self._backfill_task = None

        if self._supervisor_task:
            try:
                await asyncio.wait_for(self._supervisor_task, timeout=10)
            except asyncio.TimeoutError:
                self._supervisor_task.cancel()
                try:
                    await self._supervisor_task
                except asyncio.CancelledError:
                    pass
            finally:
                self._supervisor_task = None

        runtime_state.ws_connected = False
        runtime_state.ws_connecting = False
        runtime_state.active_streams = []
        runtime_state.active_streams_count = 0

    async def reload(self) -> None:
        async with self._reload_lock:
            logger.info("Reloading StreamManager")
            await self.stop()
            await self.start()

    async def _run_supervisor(self) -> None:
        while not self._stop_event.is_set():
            try:
                runtime_state.ws_connecting = True
                runtime_state.ws_last_error = None

                streams = await self._load_active_streams()
                runtime_state.active_streams = streams
                runtime_state.active_streams_count = len(streams)

                logger.info("Loaded %s unique active streams", len(streams))

                if not streams:
                    runtime_state.ws_connected = False
                    runtime_state.ws_connecting = False
                    logger.info("No active streams configured, sleeping")
                    await asyncio.sleep(5)
                    continue

                # start backfill for all active pairs
                if not self._backfill_task or self._backfill_task.done():
                    self._backfill_task = asyncio.create_task(
                        self.backfill_service.repair_all_active_pairs()
                    )

                stream_chunks = chunk_list(streams, self._streams_per_connection)
                logger.info(
                    "Starting %s WS worker(s) for %s streams",
                    len(stream_chunks),
                    len(streams),
                )

                self._worker_tasks = [
                    asyncio.create_task(self._run_worker(idx, chunk))
                    for idx, chunk in enumerate(stream_chunks, start=1)
                ]

                runtime_state.ws_connecting = False

                done, pending = await asyncio.wait(
                    self._worker_tasks,
                    return_when=asyncio.FIRST_EXCEPTION,
                )

                for task in done:
                    exc = task.exception()
                    if exc:
                        raise exc

                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                self._worker_tasks.clear()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                runtime_state.ws_connected = False
                runtime_state.ws_connecting = False
                runtime_state.ws_last_error = str(exc)
                runtime_state.ws_reconnect_count += 1
                logger.exception("StreamManager supervisor error: %s", exc)
                await asyncio.sleep(settings.ws_reconnect_min_sec)

        logger.info("StreamManager supervisor stopped")

    async def _run_worker(self, worker_id: int, streams: list[str]) -> None:
        backoff_seconds = settings.ws_reconnect_min_sec

        while not self._stop_event.is_set():
            try:
                ws_url = self._adapter.build_combined_url(streams)
                logger.info(
                    "Worker %s connecting with %s streams",
                    worker_id,
                    len(streams),
                )

                async with websockets.connect(
                    ws_url,
                    ping_interval=settings.ws_ping_interval_sec,
                    ping_timeout=settings.ws_ping_timeout_sec,
                    close_timeout=10,
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    runtime_state.ws_connected = True
                    runtime_state.ws_connected_at = datetime.now(timezone.utc)
                    logger.info("Worker %s connected", worker_id)

                    backoff_seconds = settings.ws_reconnect_min_sec

                    while not self._stop_event.is_set():
                        raw_message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=settings.ws_receive_timeout_sec,
                        )
                        await self._handle_message(raw_message)

            except asyncio.TimeoutError:
                runtime_state.ws_last_error = f"Worker {worker_id}: receive timeout"
                runtime_state.ws_reconnect_count += 1
                logger.warning(
                    "Worker %s timeout, reconnecting in %ss",
                    worker_id,
                    backoff_seconds,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                runtime_state.ws_last_error = f"Worker {worker_id}: {exc}"
                runtime_state.ws_reconnect_count += 1
                logger.exception("Worker %s error: %s", worker_id, exc)

            if self._stop_event.is_set():
                break

            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, settings.ws_reconnect_max_sec)

        logger.info("Worker %s stopped", worker_id)

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

        logger.info("TrackedPair rows selected: %s", len(items))

        streams: list[str] = []
        for item in items:
            stream_name = self._adapter.build_stream_name(
                symbol=item.symbol,
                interval=item.interval,
            )
            streams.append(stream_name)

        unique_streams = sorted(set(streams))

        logger.info(
            "Unique streams after dedup: %s (from %s rows)",
            len(unique_streams),
            len(streams),
        )

        return unique_streams

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