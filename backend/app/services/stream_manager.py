from __future__ import annotations

import asyncio
import logging
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import aiohttp
import websockets
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..exchanges.base import ProviderAdapter, StreamSubscription, SubscriptionError
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


@dataclass
class PendingSubscription:
    request: dict
    remaining: set[str]
    future: asyncio.Future


@dataclass
class ActiveWorker:
    worker_id: int
    spec: WorkerSpec
    price_basis: str = "trade"
    task: asyncio.Task | None = None
    websocket: Any = None
    ready: bool = False
    confirmed: set[StreamSubscription] = field(default_factory=set)
    control_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending: PendingSubscription | None = None
    control_times: deque = field(default_factory=deque)
    last_close_ms: float = 0.0
    ever_connected: bool = False

    @property
    def group(self) -> tuple[str, str, str, str]:
        return (self.spec.adapter.provider_id, self.spec.exchange, self.spec.market,
                self.price_basis)


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
        self._subscription_event = asyncio.Event()
        self._backfill_event = asyncio.Event()
        self._supervisor_task: asyncio.Task | None = None
        self._backfill_task: asyncio.Task | None = None
        self._reconcile_task: asyncio.Task | None = None
        self._workers: dict[int, ActiveWorker] = {}
        self._next_worker_id = 0
        self._request_sequence = 0
        self._loaded_pair_keys: set[tuple[str, str, str, str, str]] = set()
        self._known_pairs: set[tuple[str, str, str, str, str]] = set()
        self._backfill_initialized = False
        self._connection_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._connection_times: dict[str, float] = {}
        self._streams_per_connection = settings.ws_streams_per_connection
        self._open_price_candles: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}

    async def start(self) -> None:
        if self._supervisor_task and not self._supervisor_task.done():
            return
        self._stop_event.clear()
        self._backfill_event.clear()
        self._backfill_initialized = False
        self._supervisor_task = asyncio.create_task(self._run_supervisor(), name="stream-supervisor")
        self._backfill_task = asyncio.create_task(self._run_backfill_loop(), name="stream-history-repair")
        self._reconcile_task = asyncio.create_task(self._run_reconcile_loop(), name="stream-oanda-reconcile")

    async def stop(self) -> None:
        self._stop_event.set()
        # Cancel all owners first so maintenance isn't left running during WS close.
        tasks = [task for task in (
            self._supervisor_task, self._backfill_task, self._reconcile_task,
            *(worker.task for worker in self._workers.values()),
        ) if task is not None]
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error("Stream task shutdown failed: %s", type(result).__name__)
        self._workers.clear()
        self._supervisor_task = self._backfill_task = self._reconcile_task = None
        self._known_pairs.clear()
        self._open_price_candles.clear()
        self._publish_worker_state()

    async def reload(self, *, force: bool = False) -> None:
        """Reconcile subscriptions; force=True is the explicit global fallback."""
        async with self._reload_lock:
            if force:
                started = time.perf_counter()
                stopped = started
                try:
                    await self.stop()
                    stopped = time.perf_counter()
                    if asyncio.current_task().cancelling():
                        raise asyncio.CancelledError
                    await self.start()
                finally:
                    if settings.klines_profiling_enabled:
                        ended = time.perf_counter()
                        logger.info("%s", json.dumps({
                            "event": "stream_reload_timing", "force": True,
                            "total_ms": round((ended - started) * 1000, 3),
                            "stop_ms": round(((stopped if stopped != started else ended) - started) * 1000, 3),
                            "start_ms": round((ended - stopped) * 1000, 3) if stopped != started else 0.0,
                        }))
            elif not self._stop_event.is_set():
                await self._reconcile_subscriptions()

    async def _run_supervisor(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._subscription_event.clear()
                await self.reload()
                await asyncio.wait_for(self._subscription_event.wait(),
                                       timeout=settings.stream_reconcile_interval_sec)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
            except Exception as exc:
                runtime_state.ws_last_error = type(exc).__name__
                logger.exception("Subscription reconciliation failed; existing workers retained")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=settings.ws_reconnect_min_sec)
                except asyncio.TimeoutError:
                    pass

    async def _run_backfill_loop(self) -> None:
        # Preserve repair_all's validation, invalid-pair pausing and concurrency.
        while not self._stop_event.is_set():
            await self._backfill_event.wait()
            self._backfill_event.clear()
            try:
                await self.backfill_service.repair_all_active_pairs()
                self._subscription_event.set()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Stream history repair failed; retrying")
                self._backfill_event.set()
                await asyncio.sleep(settings.ws_reconnect_min_sec)

    @staticmethod
    def _group(spec: WorkerSpec) -> tuple[str, str, str, str]:
        return (spec.adapter.provider_id, spec.exchange, spec.market,
                spec.subscriptions[0].price_basis)

    def _publish_worker_state(self) -> None:
        running = list(self._workers.values()) if not self._stop_event.is_set() else []
        runtime_state.stream_workers = [{
            "worker_id": w.worker_id, "exchange": w.spec.exchange, "market": w.spec.market,
            "provider": w.spec.adapter.provider_id, "transport": w.spec.adapter.stream_transport,
            "stream_count": len(w.spec.subscriptions),
            "status": "connected" if w.ready else "connecting",
        } for w in running]
        runtime_state.active_streams = [
            f"{s.exchange}:{s.market}:{s.symbol}:{s.interval}:{s.price_basis}"
            for w in running for s in w.spec.subscriptions
        ]
        runtime_state.active_streams_count = len(runtime_state.active_streams)
        runtime_state.ws_connected = any(w.ready for w in running)
        runtime_state.ws_connecting = any(not w.ready for w in running)

    async def _reconcile_subscriptions(self) -> None:
        started = time.perf_counter()
        metrics = dict(subscription_diff_ms=0.0, affected_workers=0,
                       added_subscriptions=0, removed_subscriptions=0,
                       worker_restart_ms=0.0, websocket_close_ms=0.0,
                       dynamic_subscribe_ms=0.0)
        outcome = "ok"
        try:
            specs = await self._load_worker_specs()
            if self._stop_event.is_set():
                return
            desired = {sub for spec in specs for sub in spec.subscriptions}
            current = {sub for w in self._workers.values() for sub in w.spec.subscriptions}
            metrics["added_subscriptions"] = len(desired - current)
            metrics["removed_subscriptions"] = len(current - desired)
            plans = {w.worker_id: [s for s in w.spec.subscriptions if s in desired]
                     for w in self._workers.values()}
            groups = {w.worker_id: w.group for w in self._workers.values()}
            adapters = {self._group(spec): spec.adapter for spec in specs}
            for sub in sorted(desired - current, key=lambda s: (s.exchange, s.market, s.price_basis, s.symbol, s.interval)):
                adapter = get_adapter(exchange=sub.exchange, market=sub.market)
                group = (adapter.provider_id, sub.exchange, sub.market, sub.price_basis)
                capacity = min(self._streams_per_connection, adapter.max_streams_per_connection)
                # HTTP pricing streams have a URL-defined instrument set: one per market/basis.
                candidates = [wid for wid, g in groups.items() if g == group and (
                    adapter.stream_transport == "http_stream"
                    or len({s.stream_key for s in plans[wid]}) < capacity)]
                if candidates:
                    wid = candidates[0]
                else:
                    self._next_worker_id += 1
                    wid = self._next_worker_id
                    self._workers[wid] = ActiveWorker(wid, WorkerSpec(adapters[group], sub.exchange, sub.market, []), price_basis=sub.price_basis)
                    groups[wid] = group
                    plans[wid] = []
                plans[wid].append(sub)

            affected = []
            for wid, subscriptions in plans.items():
                worker = self._workers[wid]
                changed = set(worker.spec.subscriptions) != set(subscriptions)
                # Update routing before waiting for any upstream ACK or close.
                worker.spec.subscriptions[:] = subscriptions
                if (changed or worker.task is None or worker.task.done()
                        or (worker.ready and worker.confirmed != set(subscriptions))):
                    affected.append((worker, changed))
            metrics["subscription_diff_ms"] = (time.perf_counter() - started) * 1000
            metrics["affected_workers"] = len(affected)
            for worker, changed in affected:
                if self._stop_event.is_set():
                    break
                if not worker.spec.subscriptions:
                    metrics["websocket_close_ms"] += await self._stop_worker(worker)
                    self._workers.pop(worker.worker_id, None)
                elif worker.task is None or worker.task.done():
                    worker.task = asyncio.create_task(self._run_worker(worker), name=f"stream-worker-{worker.worker_id}")
                elif worker.spec.adapter.dynamic_subscriptions:
                    if worker.ready:
                        phase = time.perf_counter()
                        reconnect = False
                        try:
                            await self._sync_dynamic(worker)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            logger.warning("Worker %s subscription update failed (%s); reconnecting only this worker",
                                           worker.worker_id, type(exc).__name__)
                            # A disconnected worker already retries its latest desired set.
                            reconnect = worker.ready
                        finally:
                            metrics["dynamic_subscribe_ms"] += (time.perf_counter() - phase) * 1000
                        if reconnect:
                            await self._restart_worker(worker, metrics)
                elif changed or worker.confirmed != set(worker.spec.subscriptions):
                    await self._restart_worker(worker, metrics)
            removed_pairs = self._known_pairs - self._loaded_pair_keys
            for key in removed_pairs:
                self._open_price_candles.pop(key, None)
            if not self._backfill_initialized or self._loaded_pair_keys - self._known_pairs:
                self._backfill_event.set()
                self._backfill_initialized = True
            self._known_pairs = self._loaded_pair_keys.copy()
            self._publish_worker_state()
        except BaseException as exc:
            outcome = type(exc).__name__
            raise
        finally:
            if settings.klines_profiling_enabled:
                logger.info("%s", json.dumps({
                    "event": "subscription_reconcile_timing", "outcome": outcome,
                    "total_ms": round((time.perf_counter() - started) * 1000, 3),
                    **{k: round(v, 3) if isinstance(v, float) else v for k, v in metrics.items()},
                }))

    async def _stop_worker(self, worker: ActiveWorker) -> float:
        worker.last_close_ms = 0.0
        if worker.task is not None:
            worker.task.cancel()
            await asyncio.gather(worker.task, return_exceptions=True)
            worker.task = None
        return worker.last_close_ms

    async def _restart_worker(self, worker: ActiveWorker, metrics: dict) -> None:
        started = time.perf_counter()
        metrics["websocket_close_ms"] += await self._stop_worker(worker)
        if not self._stop_event.is_set() and worker.spec.subscriptions:
            worker.task = asyncio.create_task(self._run_worker(worker), name=f"stream-worker-{worker.worker_id}")
        metrics["worker_restart_ms"] += (time.perf_counter() - started) * 1000

    async def _throttle_connection(self, adapter: ProviderAdapter) -> None:
        # Provider-wide within this process, including markets sharing the same IP budget.
        key = "binance" if adapter.provider_id.startswith("binance") else adapter.provider_id
        async with self._connection_locks[key]:
            delay = self._connection_times.get(key, 0.0) + adapter.connection_interval_sec - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._connection_times[key] = time.monotonic()

    async def _run_worker(self, worker: ActiveWorker) -> None:
        backoff = settings.ws_reconnect_min_sec
        while not self._stop_event.is_set() and worker.spec.subscriptions:
            try:
                await self._throttle_connection(worker.spec.adapter)
                if worker.spec.adapter.stream_transport == "websocket":
                    await self._run_websocket_worker(worker)
                else:
                    await self._run_http_stream_worker(worker)
                backoff = settings.ws_reconnect_min_sec
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                runtime_state.ws_last_error = f"Worker {worker.worker_id}: {type(exc).__name__}"
                runtime_state.ws_reconnect_count += 1
                logger.warning("Worker %s failed (%s); retrying this worker", worker.worker_id, type(exc).__name__)
            if not self._stop_event.is_set() and worker.spec.subscriptions:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, settings.ws_reconnect_max_sec)

    def _connected(self, worker: ActiveWorker) -> None:
        worker.ready = True
        runtime_state.ws_connected_at = datetime.now(timezone.utc)
        if worker.ever_connected:
            self._backfill_event.set()
        worker.ever_connected = True
        self._subscription_event.set()
        self._publish_worker_state()

    async def _sync_dynamic(self, worker: ActiveWorker) -> None:
        async with worker.control_lock:
            if not worker.ready or worker.websocket is None:
                return
            desired = set(worker.spec.subscriptions)
            removed = worker.confirmed - desired
            added = desired - worker.confirmed
            await self._send_subscription(worker, "unsubscribe", {s.stream_key for s in removed})
            worker.confirmed.difference_update(removed)
            await self._send_subscription(worker, "subscribe", {s.stream_key for s in added})
            worker.confirmed.update(added)

    async def _send_subscription(self, worker: ActiveWorker, operation: str, streams: set[str]) -> None:
        adapter = worker.spec.adapter
        for batch in chunk_list(sorted(streams), adapter.subscription_batch_size(worker.spec.market)):
            now = time.monotonic()
            while worker.control_times and worker.control_times[0] <= now - 3600:
                worker.control_times.popleft()
            delay = (worker.control_times[-1] + adapter.control_interval_sec - now) if worker.control_times else 0
            if adapter.control_requests_per_hour and len(worker.control_times) >= adapter.control_requests_per_hour:
                raise SubscriptionError("Subscription control budget exhausted")
            if delay > 0:
                await asyncio.sleep(delay)
            socket = worker.websocket
            if socket is None:
                raise ConnectionError("Subscription connection was lost")
            self._request_sequence += 1
            request = adapter.build_subscription_message(operation, batch, str(self._request_sequence))
            future = asyncio.get_running_loop().create_future()
            pending = PendingSubscription(request, set(batch), future)
            worker.pending = pending
            try:
                # Bound both sends and acknowledgments; only the worker reader calls recv().
                async with asyncio.timeout(settings.ws_subscription_ack_timeout_sec):
                    await socket.send(json.dumps(request))
                    worker.control_times.append(time.monotonic())
                    if not adapter.control_requests_per_hour:
                        while len(worker.control_times) > 1:
                            worker.control_times.popleft()
                    await future
            finally:
                if worker.pending is pending:
                    worker.pending = None
                if not future.done():
                    future.cancel()
                elif not future.cancelled():
                    future.exception()  # A failed send can race the reader's disconnect notification.

    async def _receive_websocket(self, worker: ActiveWorker, socket) -> None:
        try:
            while not self._stop_event.is_set():
                raw = await asyncio.wait_for(socket.recv(), timeout=settings.ws_receive_timeout_sec)
                message = worker.spec.adapter.parse_message(raw)
                pending = worker.pending
                if pending is not None and worker.websocket is socket:
                    try:
                        ack = worker.spec.adapter.subscription_ack(message, pending.request)
                        if ack is not None:
                            pending.remaining.difference_update(ack)
                            if not pending.remaining and not pending.future.done():
                                pending.future.set_result(None)
                            continue
                    except SubscriptionError as exc:
                        if not pending.future.done():
                            pending.future.set_exception(exc)
                        continue
                await self._handle_stream_message(
                    adapter=worker.spec.adapter, exchange=worker.spec.exchange, market=worker.spec.market,
                    subscriptions=worker.spec.subscriptions, raw_message=raw,
                )
        finally:
            worker.ready = False
            self._publish_worker_state()
            pending = worker.pending
            if pending is not None and not pending.future.done():
                pending.future.set_exception(ConnectionError("Subscription reader stopped"))

    async def _run_websocket_worker(self, worker: ActiveWorker) -> None:
        spec = worker.spec
        initial = set(spec.subscriptions)
        if not initial:
            return
        streams = sorted({s.stream_key for s in initial})
        builder = getattr(spec.adapter, "build_market_ws_url", None)
        url = builder(spec.market) if callable(builder) else spec.adapter.build_combined_url(streams)
        socket = await websockets.connect(url, ping_interval=settings.ws_ping_interval_sec,
                                          ping_timeout=settings.ws_ping_timeout_sec,
                                          close_timeout=10, max_size=4 * 1024 * 1024)
        worker.websocket = socket
        worker.control_times.clear()
        reader = asyncio.create_task(self._receive_websocket(worker, socket), name=f"stream-reader-{worker.worker_id}")
        try:
            async with worker.control_lock:
                if callable(builder):
                    await self._send_subscription(worker, "subscribe", set(streams))
                # Binance combined URLs already subscribe to this exact initial set.
                worker.confirmed = initial
                self._connected(worker)
            await reader
        finally:
            worker.ready = False
            worker.websocket = None
            worker.confirmed.clear()
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
            closed = time.perf_counter()
            try:
                await socket.close()
            finally:
                worker.last_close_ms = (time.perf_counter() - closed) * 1000
                self._publish_worker_state()
                if settings.klines_profiling_enabled:
                    logger.info("%s", json.dumps({"event": "websocket_close_timing",
                        "worker_id": worker.worker_id, "exchange": spec.exchange, "market": spec.market,
                        "websocket_close_ms": round(worker.last_close_ms, 3)}))

    async def _run_http_stream_worker(self, worker: ActiveWorker) -> None:
        spec = worker.spec
        initial = set(spec.subscriptions)
        if not initial:
            return
        url = spec.adapter.build_combined_url(sorted({s.stream_key for s in initial}))
        headers = spec.adapter.build_headers() if hasattr(spec.adapter, "build_headers") else {}
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=settings.ws_receive_timeout_sec)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    worker.confirmed = initial
                    self._connected(worker)
                    async for raw_chunk in response.content:
                        if self._stop_event.is_set():
                            break
                        raw = raw_chunk.decode("utf-8").strip()
                        if raw:
                            await self._handle_stream_message(adapter=spec.adapter, exchange=spec.exchange,
                                market=spec.market, subscriptions=spec.subscriptions, raw_message=raw)
        finally:
            worker.ready = False
            worker.confirmed.clear()
            self._publish_worker_state()

    async def _load_worker_specs(self) -> list[WorkerSpec]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(TrackedPair.status == "active")
            )
            items = result.scalars().all()

        runtime_state.tracked_pairs_total = len(items)
        runtime_state.tracked_pairs_active = len(items)

        self._loaded_pair_keys = {
            (item.exchange.lower(), item.market.lower(), item.symbol.upper(), item.interval, item.price_basis)
            for item in items
        }
        grouped: dict[tuple[str, str, str, str], list[StreamSubscription]] = defaultdict(list)

        for item in items:
            exchange, market, symbol = item.exchange.lower(), item.market.lower(), item.symbol.upper()
            # Bybit's public linear kline topic is traded-price data. MARK pairs
            # remain REST-backed and must never own this upstream subscription.
            if exchange == "bybit" and market == "futures" and item.price_basis == "mark":
                continue
            adapter = get_adapter(exchange=exchange, market=market)
            stream_key = adapter.build_stream_name(
                symbol=symbol,
                interval=item.interval,
            )
            grouped[(adapter.provider_id, exchange, market, item.price_basis)].append(
                StreamSubscription(
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=item.interval,
                    price_basis=item.price_basis,
                    stream_key=stream_key,
                )
            )

        workers: list[WorkerSpec] = []
        for (_, exchange, market, basis), subscriptions in grouped.items():
            adapter = get_adapter(
                exchange=exchange,
                market=market,
            )
            unique_by_key = {
                (sub.exchange, sub.market, sub.symbol, sub.interval, sub.price_basis, sub.stream_key): sub
                for sub in subscriptions
            }
            unique_subscriptions = sorted(
                unique_by_key.values(),
                key=lambda item: (item.exchange, item.market, item.symbol, item.interval, item.price_basis),
            )

            if adapter.stream_transport == "websocket":
                stream_keys = sorted({item.stream_key for item in unique_subscriptions})
                chunks = chunk_list(stream_keys, min(self._streams_per_connection, adapter.max_streams_per_connection))
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
        subscriptions: list[StreamSubscription],
        raw_message: Any,
    ) -> None:
        runtime_state.ws_last_message_at = datetime.now(timezone.utc)

        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")

        message = adapter.parse_message(raw_message)

        if adapter.native_kline_stream:
            kline_event = adapter.extract_kline_event(message)
            if not kline_event:
                return
            symbol = str(kline_event["symbol"]).upper()
            interval = str(kline_event["interval"])
            matching_bases = {
                item.price_basis
                for item in subscriptions
                if item.symbol.upper() == symbol and item.interval == interval
            }
            if exchange == "bybit" and market == "futures":
                matching_bases &= {"trade"}
            for price_basis in matching_bases:
                event = {**kline_event, "price_basis": price_basis, "source": "ws"}
                runtime_state.last_kline_event = {
                    **event,
                    "exchange": exchange,
                    "market": market,
                }
                await self._publish_native_kline(
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    interval=interval,
                    price_basis=price_basis,
                    event=event,
                )
            return

        price_event = adapter.extract_price_event(message)
        if not price_event:
            return
        if not any(item.symbol.upper() == str(price_event["symbol"]).upper()
                   and item.price_basis == "mid" for item in subscriptions):
            return

        runtime_state.last_kline_event = {
            **price_event,
            "exchange": exchange,
            "market": market,
            "price_basis": "mid",
        }
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
        price_basis: str,
        event: dict[str, Any],
    ) -> None:
        await CandleService.write_open_candle_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            price_basis=price_basis,
            event=event,
        )

        await self.realtime_service.publish_kline(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            price_basis=price_basis,
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
                    price_basis=price_basis,
                    events=[event],
                )

            await CandleService.write_closed_candle_to_redis(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                price_basis=price_basis,
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
        price_basis = "mid"
        key = (exchange, market, symbol, interval, price_basis)
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
                price_basis=price_basis,
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
                "price_basis": price_basis,
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
            price_basis=price_basis,
            event=existing,
        )
        await self.realtime_service.publish_kline(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            price_basis=price_basis,
            event=existing,
        )

    async def _persist_closed_price_candle(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str,
        event: dict[str, Any],
    ) -> None:
        await self.realtime_service.publish_kline(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            price_basis=price_basis,
            event=event,
        )

        async with self.session_factory() as session:
            await CandleService.upsert_closed_candles(
                db=session,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                price_basis=price_basis,
                events=[event],
            )

        await CandleService.write_closed_candle_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            price_basis=price_basis,
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
                            price_basis=pair.price_basis,
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
