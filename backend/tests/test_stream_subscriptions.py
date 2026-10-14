from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.config import settings
from app.exchanges.base import StreamSubscription, SubscriptionError
from app.exchanges.registry import get_adapter
from app.services.stream_manager import StreamManager, WorkerSpec
from app.services import stream_manager as stream_module


def subscription(exchange="binance", market="spot", symbol="BTCUSDT", basis="trade"):
    adapter = get_adapter(exchange=exchange, market=market)
    return StreamSubscription(exchange, market, symbol, "1m", basis,
                              adapter.build_stream_name(symbol=symbol, interval="1m"))


async def eventually(predicate):
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(0)


class Socket:
    def __init__(self, url):
        self.url = url
        self.messages = asyncio.Queue()
        self.sent = []
        self.closed = False
        self.reject_next = False
        self.drop_ack = False

    async def send(self, raw):
        request = json.loads(raw)
        self.sent.append(request)
        if self.drop_ack:
            return
        if "method" in request:
            replies = [{"id": request["id"], **(
                {"code": 2} if self.reject_next else {"result": None})}]
        elif "req_id" in request:
            replies = [{"req_id": request["req_id"], "op": request["op"],
                        "success": not self.reject_next}]
        else:
            replies = ([{"id": request["id"], "event": "error", "code": "60012"}]
                       if self.reject_next else [
                           {"id": request["id"], "event": request["op"], "arg": arg}
                           for arg in request["args"]])
        self.reject_next = False
        for reply in replies:
            self.messages.put_nowait(json.dumps(reply))

    async def recv(self):
        message = await self.messages.get()
        if isinstance(message, Exception):
            raise message
        return message

    async def close(self):
        self.closed = True


@pytest.fixture
def harness(monkeypatch):
    sockets = []
    async def connect(url, **kwargs):
        socket = Socket(url)
        sockets.append(socket)
        return socket
    monkeypatch.setattr("app.services.stream_manager.websockets.connect", connect)
    monkeypatch.setattr(settings, "ws_reconnect_min_sec", 0.001)
    monkeypatch.setattr(settings, "ws_subscription_ack_timeout_sec", 0.03)
    for exchange, market in [("binance", "spot"), ("binance", "futures"),
                             ("bybit", "spot"), ("okx", "spot")]:
        adapter = get_adapter(exchange=exchange, market=market)
        monkeypatch.setattr(adapter, "connection_interval_sec", 0)
        monkeypatch.setattr(adapter, "control_interval_sec", 0)
    manager = StreamManager(None, SimpleNamespace(repair_all_active_pairs=AsyncMock()), None)
    desired = []
    async def load():
        manager._loaded_pair_keys = {(s.exchange, s.market, s.symbol, s.interval, s.price_basis)
                                     for s in desired}
        grouped = {}
        for sub in desired:
            if sub.exchange == "bybit" and sub.market == "futures" and sub.price_basis == "mark":
                continue
            grouped.setdefault((sub.exchange, sub.market, sub.price_basis), []).append(sub)
        return [WorkerSpec(get_adapter(exchange=e, market=m), e, m, subs.copy())
                for (e, m, _), subs in grouped.items()]
    manager._load_worker_specs = load
    return manager, desired, sockets


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange,market,symbol,second", [
    ("binance", "spot", "BTCUSDT", "ETHUSDT"),
    ("binance", "futures", "BTCUSDT", "ETHUSDT"),
    ("bybit", "spot", "BTCUSDT", "ETHUSDT"),
    ("bybit", "futures", "BTCUSDT", "ETHUSDT"),
    ("okx", "spot", "BTC-USDT", "ETH-USDT"),
    ("okx", "futures", "BTC-USDT-SWAP", "ETH-USDT-SWAP"),
])
async def test_dynamic_add_remove_and_noop_keep_unrelated_workers(harness, exchange, market, symbol, second):
    manager, desired, sockets = harness
    original = subscription(exchange, market, symbol)
    unrelated = subscription("okx", "spot", "SOL-USDT") if exchange != "okx" else subscription()
    desired[:] = [original, unrelated]
    try:
        await manager.reload()
        await eventually(lambda: all(w.ready for w in manager._workers.values()))
        worker = next(w for w in manager._workers.values() if original in w.spec.subscriptions)
        other = next(w for w in manager._workers.values() if unrelated in w.spec.subscriptions)
        socket, other_socket, task, other_task = worker.websocket, other.websocket, worker.task, other.task
        added = subscription(exchange, market, second)
        desired.extend([added, added])  # Concurrent activations have the same identity.
        await asyncio.gather(manager.reload(), manager.reload(), manager.reload())
        assert worker.confirmed == {original, added}
        assert len(worker.spec.subscriptions) == 2
        assert worker.websocket is socket and worker.task is task
        assert other.websocket is other_socket and other.task is other_task
        assert not socket.closed and not other_socket.closed
        sends = len(socket.sent)
        await manager.reload()
        assert len(socket.sent) == sends
        desired[:] = [added, unrelated]
        await manager.reload()
        assert worker.confirmed == {added}
        assert socket.sent[-1].get("op", socket.sent[-1].get("method")).lower() == "unsubscribe"
        assert other.websocket is other_socket and not other_socket.closed
        desired[:] = [unrelated]
        await manager.reload()
        assert socket.closed and worker.worker_id not in manager._workers
        assert other.websocket is other_socket and not other_socket.closed
    finally:
        await manager.stop()
    assert all(s.closed for s in sockets)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rejected", "timeout", "disconnect"])
async def test_failure_reconnects_only_affected_worker_and_restores_latest_set(harness, failure):
    manager, desired, sockets = harness
    original, unrelated = subscription(), subscription("bybit", "futures")
    desired[:] = [original, unrelated]
    try:
        await manager.reload()
        await eventually(lambda: all(w.ready for w in manager._workers.values()))
        worker = next(w for w in manager._workers.values() if original in w.confirmed)
        other = next(w for w in manager._workers.values() if unrelated in w.confirmed)
        socket, other_socket = worker.websocket, other.websocket
        desired.append(subscription(symbol="ETHUSDT"))
        if failure == "rejected":
            socket.reject_next = True
        elif failure == "timeout":
            socket.drop_ack = True
        await manager.reload()
        if failure == "disconnect":
            socket.messages.put_nowait(ConnectionError("test disconnect"))
        await eventually(lambda: worker.ready and worker.websocket is not socket)
        assert socket.closed
        assert worker.confirmed == set(desired) - {unrelated}
        assert "ethusdt@kline_1m" in worker.websocket.url
        assert other.websocket is other_socket and not other_socket.closed
        assert manager._backfill_event.is_set()
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_capacity_is_bounded_and_existing_slots_are_reused(harness):
    manager, desired, sockets = harness
    manager._streams_per_connection = 2
    desired[:] = [subscription(symbol=s) for s in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT"]]
    try:
        await manager.reload()
        await eventually(lambda: all(w.ready for w in manager._workers.values()))
        assert len(manager._workers) == 3
        retained = {w.worker_id: w.websocket for w in manager._workers.values()}
        desired.pop(0)
        await manager.reload()
        desired.append(subscription(symbol="DOGEUSDT"))
        await manager.reload()
        assert len(manager._workers) == 3
        assert all(w.websocket is retained[w.worker_id] for w in manager._workers.values())
        active = [s for w in manager._workers.values() for s in w.spec.subscriptions]
        assert len(active) == len(set(active)) == 5
        assert all(len(w.spec.subscriptions) <= 2 for w in manager._workers.values())
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_shutdown_during_pending_ack_cleans_reader_and_does_not_respawn(harness):
    manager, desired, sockets = harness
    desired.append(subscription())
    await manager.reload()
    await eventually(lambda: next(iter(manager._workers.values())).ready)
    worker = next(iter(manager._workers.values()))
    task = worker.task
    worker.websocket.drop_ack = True
    desired.append(subscription(symbol="ETHUSDT"))
    reload_task = asyncio.create_task(manager.reload())
    await eventually(lambda: worker.pending is not None)
    await manager.stop()
    await asyncio.wait_for(reload_task, 1)
    assert task.done() and worker.pending is None
    assert not manager._workers
    assert len(sockets) == 1 and sockets[0].closed


@pytest.mark.asyncio
async def test_mark_addition_requests_history_without_websocket_or_maintenance_cancellation(harness):
    manager, desired, sockets = harness
    desired.append(subscription("bybit", "futures", basis="mark"))
    entered, release = asyncio.Event(), asyncio.Event()
    async def repair():
        entered.set()
        await release.wait()
    manager.backfill_service.repair_all_active_pairs = AsyncMock(side_effect=repair)
    await manager.start()
    try:
        await asyncio.wait_for(entered.wait(), 1)
        backfill, reconciliation = manager._backfill_task, manager._reconcile_task
        desired.append(subscription("bybit", "futures", symbol="ETHUSDT", basis="mark"))
        await manager.reload()
        assert manager._backfill_task is backfill and not backfill.done()
        assert manager._reconcile_task is reconciliation and not reconciliation.done()
        assert not manager._workers and not sockets
        release.set()
        await eventually(lambda: manager.backfill_service.repair_all_active_pairs.await_count == 2)
        await manager.reload()
        assert manager.backfill_service.repair_all_active_pairs.await_count == 2
    finally:
        await manager.stop()
    assert backfill.done() and reconciliation.done()


@pytest.mark.asyncio
async def test_bybit_spot_subscriptions_are_batched_in_tens(harness):
    manager, desired, sockets = harness
    desired[:] = [subscription("bybit", "spot", symbol=f"COIN{i}USDT") for i in range(23)]
    try:
        await manager.reload()
        await eventually(lambda: next(iter(manager._workers.values())).ready)
        assert [len(r["args"]) for r in sockets[0].sent] == [10, 10, 3]
    finally:
        await manager.stop()


@pytest.mark.parametrize("exchange,market", [("binance", "spot"), ("binance", "futures"),
                                           ("bybit", "spot"), ("okx", "spot")])
def test_adapter_ack_correlation_and_rejection(exchange, market):
    adapter = get_adapter(exchange=exchange, market=market)
    sub = subscription(exchange, market, "BTC-USDT" if exchange == "okx" else "BTCUSDT")
    request = adapter.build_subscription_message("subscribe", [sub.stream_key], "7")
    if exchange == "binance":
        assert isinstance(request["id"], int)
        assert adapter.subscription_ack({"id": 8, "result": None}, request) is None
        assert adapter.subscription_ack({"id": 7, "result": None}, request) == {sub.stream_key}
        error = {"id": 7, "code": 2}
    elif exchange == "bybit":
        assert adapter.subscription_ack({"req_id": "8", "success": True, "op": "subscribe"}, request) is None
        assert adapter.subscription_ack({"req_id": "7", "success": True, "op": "subscribe"}, request) == {sub.stream_key}
        error = {"req_id": "7", "success": False, "op": "subscribe"}
    else:
        assert adapter.subscription_ack({"id": "8", "event": "subscribe", "arg": request["args"][0]}, request) is None
        assert adapter.subscription_ack({"id": "7", "event": "subscribe", "arg": request["args"][0]}, request) == {sub.stream_key}
        error = {"id": "7", "event": "error"}
    with pytest.raises(SubscriptionError):
        adapter.subscription_ack(error, request)


@pytest.mark.asyncio
async def test_closed_and_open_events_only_route_current_subscription(harness):
    manager, desired, sockets = harness
    original, added = subscription(), subscription(symbol="ETHUSDT")
    desired.append(original)
    manager._publish_native_kline = AsyncMock()
    def event(symbol, closed):
        return json.dumps({"e": "kline", "s": symbol, "k": {
            "s": symbol, "i": "1m", "t": 0, "T": 59999, "o": "1", "h": "2", "l": "1",
            "c": "2", "v": "3", "x": closed, "n": 4}})
    try:
        await manager.reload()
        await eventually(lambda: next(iter(manager._workers.values())).ready)
        worker = next(iter(manager._workers.values()))
        desired.append(added)
        await manager.reload()
        worker.websocket.messages.put_nowait(event("ETHUSDT", False))
        await eventually(lambda: manager._publish_native_kline.await_count == 1)
        desired.remove(original)
        await manager.reload()
        # Once removal completes, buffered frames from that topic must be ignored.
        worker.websocket.messages.put_nowait(event("BTCUSDT", True))
        worker.websocket.messages.put_nowait(event("ETHUSDT", True))
        await eventually(lambda: manager._publish_native_kline.await_count == 2)
        assert [c.kwargs["symbol"] for c in manager._publish_native_kline.await_args_list] == ["ETHUSDT", "ETHUSDT"]
        assert manager._publish_native_kline.await_args.kwargs["event"]["is_closed"] is True
    finally:
        await manager.stop()


class HttpStream:
    def __init__(self, url):
        self.url = url
        self.content = self
        self.messages = asyncio.Queue()
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    def raise_for_status(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.messages.get()


@pytest.mark.asyncio
async def test_oanda_restarts_only_affected_market_and_keeps_open_candle_state(harness, monkeypatch):
    manager, desired, sockets = harness
    streams = []
    class Session:
        def __init__(self, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        def get(self, url, **kwargs):
            stream = HttpStream(url)
            streams.append(stream)
            return stream
    monkeypatch.setattr("app.services.stream_manager.aiohttp.ClientSession", Session)
    monkeypatch.setattr(get_adapter(exchange="oanda", market="forex"), "build_headers", lambda: {})
    manager._handle_price_event = AsyncMock()
    original = subscription("oanda", "forex", "EUR_USD", "mid")
    metal = subscription("oanda", "metals", "XAU_USD", "mid")
    desired[:] = [original, metal, subscription()]
    key = ("oanda", "forex", "EUR_USD", "1m", "mid")
    candle = {"open_time": 0, "open": "1", "high": "1", "low": "1", "close": "1"}
    manager._open_price_candles[key] = candle
    try:
        await manager.reload()
        await eventually(lambda: all(w.ready for w in manager._workers.values()))
        forex = next(w for w in manager._workers.values() if w.spec.market == "forex")
        metals = next(w for w in manager._workers.values() if w.spec.market == "metals")
        old_task, metal_task = forex.task, metals.task
        old_forex = next(s for s in streams if "EUR_USD" in s.url)
        metal_stream = next(s for s in streams if "XAU_USD" in s.url)
        desired.append(subscription("oanda", "forex", "GBP_USD", "mid"))
        await manager.reload()
        await eventually(lambda: forex.ready)
        assert old_forex.closed and old_task.done()
        assert metals.task is metal_task and not metal_stream.closed
        assert not sockets[0].closed
        assert manager._open_price_candles[key] is candle
        current = next(s for s in streams if "GBP_USD" in s.url)
        desired.remove(original)
        await manager.reload()
        await eventually(lambda: forex.ready)
        assert current.closed and key not in manager._open_price_candles
        current = streams[-1]
        def price(symbol):
            return json.dumps({"type": "PRICE", "instrument": symbol, "time": "60",
                               "bids": [{"price": "1.1"}], "asks": [{"price": "1.2"}]}).encode()
        current.messages.put_nowait(price("EUR_USD"))
        current.messages.put_nowait(price("GBP_USD"))
        await eventually(lambda: manager._handle_price_event.await_count == 1)
        assert manager._handle_price_event.await_args.kwargs["symbol"] == "GBP_USD"
        assert not metal_stream.closed and not sockets[0].closed
    finally:
        await manager.stop()
    assert all(s.closed for s in streams)


@pytest.mark.asyncio
async def test_updates_during_connection_are_reconciled_after_initial_ack(harness, monkeypatch):
    manager, desired, sockets = harness
    desired.append(subscription("bybit", "futures"))
    original_connect = stream_module.websockets.connect
    connecting, release = asyncio.Event(), asyncio.Event()
    async def connect(url, **kwargs):
        connecting.set()
        await release.wait()
        return await original_connect(url, **kwargs)
    monkeypatch.setattr("app.services.stream_manager.websockets.connect", connect)
    await manager.start()
    try:
        await asyncio.wait_for(connecting.wait(), 1)
        desired.append(subscription("bybit", "futures", "ETHUSDT"))
        await manager.reload()
        release.set()
        await eventually(lambda: all(w.confirmed == set(desired) for w in manager._workers.values()))
        assert len(sockets) == 1
        assert [len(r["args"]) for r in sockets[0].sent] == [1, 1]
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_load_specs_normalizes_keys_and_keeps_mark_out_of_ws(monkeypatch):
    items = [SimpleNamespace(exchange="BYBIT", market="FUTURES", symbol="btcusdt", interval="1m", price_basis=b)
             for b in ("mark", "trade")]
    class Session:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def execute(self, query):
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: items))
    manager = StreamManager(Session, None, None)
    specs = await manager._load_worker_specs()
    assert len(specs) == 1
    assert specs[0].exchange == "bybit" and specs[0].market == "futures"
    assert specs[0].subscriptions == [subscription("bybit", "futures")]
    assert manager._loaded_pair_keys == {("bybit", "futures", "BTCUSDT", "1m", b) for b in ("mark", "trade")}


@pytest.mark.asyncio
async def test_okx_control_limit_fails_fast_instead_of_waiting_an_hour(harness):
    import time
    manager, desired, sockets = harness
    desired.append(subscription("okx", "spot", "BTC-USDT"))
    try:
        await manager.reload()
        await eventually(lambda: next(iter(manager._workers.values())).ready)
        worker = next(iter(manager._workers.values()))
        worker.control_times.extend([time.monotonic()] * 480)
        with pytest.raises(SubscriptionError, match="budget"):
            await asyncio.wait_for(manager._send_subscription(worker, "subscribe", {"candle1m|ETH-USDT"}), .1)
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_profiling_contains_subscription_counts_and_close_duration(harness, monkeypatch, caplog):
    manager, desired, sockets = harness
    monkeypatch.setattr(settings, "klines_profiling_enabled", True)
    caplog.set_level("INFO", logger="app.services.stream_manager")
    desired.append(subscription())
    await manager.reload()
    await eventually(lambda: next(iter(manager._workers.values())).ready)
    desired.append(subscription(symbol="ETHUSDT"))
    await manager.reload()
    await manager.stop()
    records = [json.loads(r.message) for r in caplog.records if r.message.startswith("{")]
    diff = [r for r in records if r["event"] == "subscription_reconcile_timing"][-1]
    assert diff["added_subscriptions"] == diff["affected_workers"] == 1
    assert diff["removed_subscriptions"] == 0
    assert diff["worker_restart_ms"] == diff["websocket_close_ms"] == 0
    assert diff["dynamic_subscribe_ms"] >= 0 and diff["subscription_diff_ms"] >= 0
    assert any(r["event"] == "websocket_close_timing" for r in records)


def test_bybit_partial_command_ack_reports_topics_and_rejects_failures():
    adapter = get_adapter(exchange="bybit", market="futures")
    request = adapter.build_subscription_message("subscribe", ["kline.1.BTCUSDT", "kline.1.ETHUSDT"], "7")
    ack = {"req_id": "7", "type": "COMMAND_RESP", "successTopics": ["kline.1.BTCUSDT"], "failTopics": []}
    assert adapter.subscription_ack(ack, request) == {"kline.1.BTCUSDT"}
    ack["failTopics"] = ["kline.1.ETHUSDT"]
    with pytest.raises(SubscriptionError):
        adapter.subscription_ack(ack, request)


@pytest.mark.asyncio
async def test_supervisor_picks_up_expiration_without_notification_and_shutdown_cleans_owned_tasks(harness, monkeypatch):
    manager, desired, sockets = harness
    monkeypatch.setattr(settings, "stream_reconcile_interval_sec", .005)
    original, unrelated = subscription(), subscription("bybit", "futures")
    desired[:] = [original, unrelated]
    await manager.start()
    try:
        await eventually(lambda: len(manager._workers) == 2 and all(w.ready for w in manager._workers.values()))
        other = next(w for w in manager._workers.values() if unrelated in w.confirmed)
        other_socket = other.websocket
        tasks = [manager._supervisor_task, manager._backfill_task, manager._reconcile_task,
                 *(w.task for w in manager._workers.values())]
        desired.remove(original)
        await eventually(lambda: len(manager._workers) == 1)
        assert other.websocket is other_socket and not other_socket.closed
    finally:
        await manager.stop()
    assert all(t.done() for t in tasks)
    assert all(s.closed for s in sockets)
    assert not any(t.get_name().startswith("stream-reader-") for t in asyncio.all_tasks())
