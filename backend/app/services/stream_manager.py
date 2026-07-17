from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import aiohttp
import websockets
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..exchanges.base import ProviderAdapter, StreamSubscription
from ..exchanges.registry import get_adapter
from ..models import TrackedPair
from ..services.backfill_service import BackfillService
from ..services.candle_service import CandleService
from ..services.realtime_service import RealtimeService
from ..state import runtime_state
from ..utils.intervals import floor_to_interval_open, next_interval_open

logger = logging.getLogger(__name__)


def chunk_list(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


@dataclass
class WorkerSpec:
    adapter: ProviderAdapter
    exchange: str
    market: str
    subscriptions: list[StreamSubscription]


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

        self._stop_event = asyncio.Event()
        self._reload_lock = asyncio.Lock()
        self._reload_requested = False

        self._supervisor_task: asyncio.Task | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._backfill_task: asyncio.Task | None = None
        self._reconcile_task: asyncio.Task | None = None

        self._streams_per_connection = getattr(settings, "ws_streams_per_connection", 200)
        self._open_price_candles: dict[tuple[str, str, str, str], dict[str, Any]] = {}

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

        if self._reconcile_task:
            self._reconcile_task.cancel()
            try:
                await self._reconcile_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Reconcile task stop error")
            finally:
                self._reconcile_task = None

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

        self._open_price_candles.clear()
        runtime_state.ws_connected = False
        runtime_state.ws_connecting = False
        runtime_state.active_streams = []
        runtime_state.active_streams_count = 0

    async def reload(self) -> None:
        if self._reload_lock.locked():
            self._reload_requested = True
            logger.info("StreamManager reload coalesced while another reload is running")
            return

        async with self._reload_lock:
            while True:
                self._reload_requested = False
                logger.info("Reloading StreamManager")
                await self.stop()
                await self.start()
                if not self._reload_requested:
                    break

    async def _run_supervisor(self) -> None:
        while not self._stop_event.is_set():
            try:
                runtime_state.ws_connecting = True
                runtime_state.ws_last_error = None

                workers = await self._load_worker_specs()
                runtime_state.stream_workers = [
                    {
                        "worker_id": idx,
                        "exchange": worker.exchange,
                        "market": worker.market,
                        "provider": worker.adapter.provider_id,
                        "transport": worker.adapter.stream_transport,
                        "stream_count": len(worker.subscriptions),
                        "status": "configured",
                    }
                    for idx, worker in enumerate(workers, start=1)
                ]
                runtime_state.active_streams = [
                    f"{item.exchange}:{item.market}:{item.symbol}:{item.interval}"
                    for worker in workers
                    for item in worker.subscriptions
                ]
                runtime_state.active_streams_count = len(runtime_state.active_streams)

                if not workers:
                    runtime_state.ws_connected = False
                    runtime_state.ws_connecting = False
                    runtime_state.stream_workers = []
                    logger.info("No active streams configured, sleeping")
                    await asyncio.sleep(5)
                    continue

                if not self._backfill_task or self._backfill_task.done():
                    self._backfill_task = asyncio.create_task(
                        self.backfill_service.repair_all_active_pairs()
                    )

                if not self._reconcile_task or self._reconcile_task.done():
                    self._reconcile_task = asyncio.create_task(self._run_reconcile_loop())

                self._worker_tasks = [
                    asyncio.create_task(self._run_worker(idx, worker))
                    for idx, worker in enumerate(workers, start=1)
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

    async def _run_worker(self, worker_id: int, worker: WorkerSpec) -> None:
        backoff_seconds = settings.ws_reconnect_min_sec

        while not self._stop_event.is_set():
            try:
                streams = [item.stream_key for item in worker.subscriptions]
                logger.info(
                    "Worker %s connecting adapter=%s streams=%s",
                    worker_id,
                    worker.adapter.provider_id,
                    len(streams),
                )

                if worker.adapter.stream_transport == "websocket":
                    await self._run_websocket_worker(worker_id, worker.adapter, worker.subscriptions)
                elif worker.adapter.stream_transport == "http_stream":
                    await self._run_http_stream_worker(worker_id, worker.adapter, worker.subscriptions)
                else:
                    raise ValueError(f"Unsupported stream transport: {worker.adapter.stream_transport}")

                backoff_seconds = settings.ws_reconnect_min_sec

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

            runtime_state.ws_connected = False
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, settings.ws_reconnect_max_sec)

        logger.info("Worker %s stopped", worker_id)

    async def _run_websocket_worker(
        self,
        worker_id: int,
        adapter: ProviderAdapter,
        subscriptions: list[StreamSubscription],
    ) -> None:
        ws_url_builder = getattr(adapter, "build_market_ws_url", None)
        if callable(ws_url_builder):
            ws_url = ws_url_builder(subscriptions[0].market)
        else:
            ws_url = adapter.build_combined_url([item.stream_key for item in subscriptions])
        async with websockets.connect(
            ws_url,
            ping_interval=settings.ws_ping_interval_sec,
            ping_timeout=settings.ws_ping_timeout_sec,
            close_timeout=10,
            max_size=4 * 1024 * 1024,
        ) as websocket:
            for message in adapter.build_subscribe_messages([item.stream_key for item in subscriptions]):
                await websocket.send(message)
            runtime_state.ws_connected = True
            runtime_state.ws_connected_at = datetime.now(timezone.utc)
            logger.info("Worker %s connected via websocket", worker_id)

            while not self._stop_event.is_set():
                raw_message = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=settings.ws_receive_timeout_sec,
                )
                await self._handle_stream_message(
                    adapter=adapter,
                    exchange=subscriptions[0].exchange,
                    market=subscriptions[0].market,
                    raw_message=raw_message,
                )

    async def _run_http_stream_worker(
        self,
        worker_id: int,
        adapter: ProviderAdapter,
        subscriptions: list[StreamSubscription],
    ) -> None:
        url = adapter.build_combined_url([item.stream_key for item in subscriptions])
        headers = adapter.build_headers() if hasattr(adapter, "build_headers") else {}

        timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=30,
            sock_read=settings.ws_receive_timeout_sec,
        )

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                runtime_state.ws_connected = True
                runtime_state.ws_connected_at = datetime.now(timezone.utc)
                logger.info("Worker %s connected via http stream", worker_id)

                async for raw_chunk in response.content:
                    if self._stop_event.is_set():
                        break

                    raw_message = raw_chunk.decode("utf-8").strip()
                    if not raw_message:
                        continue

                    await self._handle_stream_message(
                        adapter=adapter,
                        exchange=subscriptions[0].exchange,
                        market=subscriptions[0].market,
                        raw_message=raw_message,
                    )

    async def _load_worker_specs(self) -> list[WorkerSpec]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(TrackedPair.status == "active")
            )
            items = result.scalars().all()

        runtime_state.tracked_pairs_total = len(items)
        runtime_state.tracked_pairs_active = len(items)

        grouped: dict[tuple[str, str, str], list[StreamSubscription]] = defaultdict(list)

        for item in items:
            adapter = get_adapter(exchange=item.exchange, market=item.market)
            stream_key = adapter.build_stream_name(
                symbol=item.symbol,
                interval=item.interval,
            )
            grouped[(adapter.provider_id, item.exchange, item.market)].append(
                StreamSubscription(
                    exchange=item.exchange,
                    market=item.market,
                    symbol=item.symbol,
                    interval=item.interval,
                    stream_key=stream_key,
                )
            )

        workers: list[WorkerSpec] = []
        for (_, exchange, market), subscriptions in grouped.items():
            adapter = get_adapter(
                exchange=exchange,
                market=market,
            )
            unique_by_key = {
                (sub.exchange, sub.market, sub.symbol, sub.interval, sub.stream_key): sub
                for sub in subscriptions
            }
            unique_subscriptions = sorted(
                unique_by_key.values(),
                key=lambda item: (item.exchange, item.market, item.symbol, item.interval),
            )

            if adapter.stream_transport == "websocket":
                stream_keys = sorted({item.stream_key for item in unique_subscriptions})
                chunks = chunk_list(stream_keys, self._streams_per_connection)
                for chunk in chunks:
                    chunk_set = set(chunk)
                    chunk_items = [item for item in unique_subscriptions if item.stream_key in chunk_set]
                    workers.append(
                        WorkerSpec(
                            adapter=adapter,
                            exchange=exchange,
                            market=market,
                            subscriptions=chunk_items,
                        )
                    )
            else:
                workers.append(
                    WorkerSpec(
                        adapter=adapter,
                        exchange=exchange,
                        market=market,
                        subscriptions=unique_subscriptions,
                    )
                )

        return workers

    async def _handle_stream_message(
        self,
        *,
        adapter: ProviderAdapter,
        exchange: str,
        market: str,
        raw_message: Any,
    ) -> None:
        runtime_state.ws_last_message_at = datetime.now(timezone.utc)

        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")

        message = adapter.parse_message(raw_message)

        if exchange == "bybit" and market == "futures":
            return

        if adapter.native_kline_stream:
            kline_event = adapter.extract_kline_event(message)
            if not kline_event:
                return
            runtime_state.last_kline_event = kline_event
            await self._publish_native_kline(
                exchange=exchange,
                market=market,
                symbol=str(kline_event["symbol"]).upper(),
                interval=str(kline_event["interval"]),
                event=kline_event,
            )
            return

        price_event = adapter.extract_price_event(message)
        if not price_event:
            return

        runtime_state.last_kline_event = price_event
        await self._handle_price_event(
            exchange=exchange,
            symbol=str(price_event["symbol"]).upper(),
            price_event=price_event,
        )

    async def _publish_native_kline(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        event: dict[str, Any],
    ) -> None:
        await CandleService.write_open_candle_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=event,
        )

        await self.realtime_service.publish_kline(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=event,
        )

        if bool(event["is_closed"]):
            async with self.session_factory() as session:
                await CandleService.upsert_closed_candles(
                    db=session,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=interval,
                    events=[event],
                )

            await CandleService.write_closed_candle_to_redis(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                event=event,
            )

    async def _handle_price_event(
        self,
        *,
        exchange: str,
        symbol: str,
        price_event: dict[str, Any],
    ) -> None:
        market = await self._resolve_oanda_market(symbol)
        interval = "1m"
        key = (exchange, market, symbol, interval)
        event_ts = int(price_event["timestamp_ms"])
        open_time = floor_to_interval_open(event_ts, interval)
        close_time = next_interval_open(open_time, interval) - 1
        price = Decimal(str(price_event["price"]))

        existing = self._open_price_candles.get(key)
        if existing is not None and int(existing["open_time"]) < open_time:
            closed_event = dict(existing)
            closed_event["is_closed"] = True
            closed_event["source"] = "ws"
            await self._persist_closed_price_candle(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                event=closed_event,
            )
            existing = None

        if existing is None or int(existing["open_time"]) != open_time:
            existing = {
                "symbol": symbol,
                "interval": interval,
                "open_time": open_time,
                "close_time": close_time,
                "open": str(price),
                "high": str(price),
                "low": str(price),
                "close": str(price),
                "volume": "0",
                "is_closed": False,
                "trades_count": None,
                "source": "ws",
            }
            self._open_price_candles[key] = existing
        else:
            existing["high"] = str(max(Decimal(str(existing["high"])), price))
            existing["low"] = str(min(Decimal(str(existing["low"])), price))
            existing["close"] = str(price)

        await CandleService.write_open_candle_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=existing,
        )
        await self.realtime_service.publish_kline(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=existing,
        )

    async def _persist_closed_price_candle(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        event: dict[str, Any],
    ) -> None:
        await self.realtime_service.publish_kline(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=event,
        )

        async with self.session_factory() as session:
            await CandleService.upsert_closed_candles(
                db=session,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                events=[event],
            )

        await CandleService.write_closed_candle_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            event=event,
        )

    async def _run_reconcile_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(settings.oanda_reconcile_interval_sec)
                if self._stop_event.is_set():
                    break

                async with self.session_factory() as session:
                    result = await session.execute(
                        select(TrackedPair).where(
                            TrackedPair.exchange == "oanda",
                            TrackedPair.status == "active",
                        )
                    )
                    pairs = result.scalars().all()

                for pair in pairs:
                    try:
                        await self.backfill_service.reconcile_recent_pair(
                            exchange=pair.exchange,
                            market=pair.market,
                            symbol=pair.symbol,
                            interval=pair.interval,
                        )
                    except Exception:
                        logger.exception(
                            "Failed OANDA reconciliation for %s %s",
                            pair.symbol,
                            pair.interval,
                        )
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("OANDA reconciliation loop failed")

    async def _resolve_oanda_market(self, symbol: str) -> str:
        adapter = get_adapter(exchange="oanda", market="forex")
        instruments = await adapter.list_instruments()
        instrument = next((item for item in instruments if item["symbol"] == symbol.upper()), None)
        if instrument is None:
            raise ValueError(f"Unknown OANDA instrument in stream: {symbol}")
        return str(instrument["market"])
