from __future__ import annotations

import asyncio
import json

import pytest

from app.services.chart_ws_service import ChartConnectionSession, ChartProtocolError, ChartWebSocketService, StreamKey


class FakeRealtimeService:
    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.disconnected = 0

    async def subscribe_chart(self, session, channel: str) -> None:
        self.subscribed.append(channel)

    async def unsubscribe_chart(self, session, channel: str) -> None:
        self.unsubscribed.append(channel)

    async def disconnect_chart(self, session) -> None:
        self.disconnected += 1


class FakeChartService:
    protocol = "chart-v1"

    def __init__(self) -> None:
        self.realtime_service = FakeRealtimeService()
        self.snapshot_by_stream: dict[StreamKey, dict | None] = {}
        self.active: set[StreamKey] = set()
        self.activated: list[StreamKey] = []
        self.sequence = 0
        self.fail_normalize = False

    async def normalize_streams(self, raw_streams):
        if self.fail_normalize:
            raise ChartProtocolError("INVALID_SYMBOL", "bad symbol", details={"symbol": "BAD"})
        streams = []
        for item in raw_streams:
            streams.append(
                StreamKey(
                    exchange=item["exchange"].lower(),
                    market=item["market"].lower(),
                    symbol=item["symbol"].upper(),
                    interval=item["interval"],
                    price_basis=item.get("price_basis", "trade"),
                )
            )
        seen = set()
        unique = []
        for stream in streams:
            if stream not in seen:
                unique.append(stream)
                seen.add(stream)
        return unique

    async def get_snapshot(self, stream: StreamKey):
        return self.snapshot_by_stream.get(stream)

    async def is_stream_active(self, stream: StreamKey) -> bool:
        return stream in self.active

    async def ensure_stream_active(self, stream: StreamKey) -> None:
        self.activated.append(stream)
        self.active.add(stream)

    async def next_sequence(self, channel: str) -> int:
        self.sequence += 1
        return self.sequence


class FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.closed = False

    async def send_text(self, text: str) -> None:
        self.sent.append(json.loads(text))

    async def close(self) -> None:
        self.closed = True


def make_session() -> tuple[ChartConnectionSession, FakeChartService, FakeWebSocket]:
    service = FakeChartService()
    websocket = FakeWebSocket()
    session = ChartConnectionSession(service=service, websocket=websocket)  # type: ignore[arg-type]
    return session, service, websocket


async def drain_control(session: ChartConnectionSession) -> dict:
    item = await asyncio.wait_for(session._queue.get(), timeout=1)
    assert item["kind"] == "control"
    return item["payload"]


BTC = {"exchange": "bybit", "market": "spot", "symbol": "BTCUSDT", "interval": "1m"}
ETH = {"exchange": "bybit", "market": "spot", "symbol": "ETHUSDT", "interval": "1m"}
SOL = {"exchange": "bybit", "market": "spot", "symbol": "SOLUSDT", "interval": "1m"}


@pytest.mark.asyncio
async def test_subscribe_single_stream_ack_and_registration():
    session, service, _ = make_session()

    await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC]}))
    payload = await drain_control(session)

    assert payload["type"] == "subscribed"
    assert payload["request_id"] == "r1"
    assert payload["streams"][0]["channel"] == "kline:bybit:spot:BTCUSDT:1m:trade"
    assert service.realtime_service.subscribed == ["kline:bybit:spot:BTCUSDT:1m:trade"]


@pytest.mark.asyncio
async def test_subscribe_multiple_and_duplicate_is_idempotent():
    session, service, _ = make_session()

    await session._handle_raw_message(
        json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC, ETH, BTC]})
    )
    payload = await drain_control(session)

    assert len(payload["streams"]) == 2
    assert len(session.subscriptions) == 2
    assert service.realtime_service.subscribed == [
        "kline:bybit:spot:BTCUSDT:1m:trade",
        "kline:bybit:spot:ETHUSDT:1m:trade",
    ]


@pytest.mark.asyncio
async def test_unsubscribe_inactive_stream_does_not_fail():
    session, service, _ = make_session()

    await session._handle_raw_message(json.dumps({"action": "unsubscribe", "request_id": "r2", "streams": [BTC]}))
    payload = await drain_control(session)

    assert payload["type"] == "unsubscribed"
    assert payload["request_id"] == "r2"
    assert service.realtime_service.unsubscribed == []


@pytest.mark.asyncio
async def test_atomic_switch_replaces_subscriptions():
    session, service, _ = make_session()
    await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC]}))
    await drain_control(session)

    await session._handle_raw_message(
        json.dumps({"action": "switch", "request_id": "r2", "unsubscribe": [BTC], "subscribe": [ETH]})
    )
    payload = await drain_control(session)

    assert payload["type"] == "switched"
    assert payload["request_id"] == "r2"
    assert StreamKey("bybit", "spot", "BTCUSDT", "1m", "trade") not in session.subscriptions
    assert StreamKey("bybit", "spot", "ETHUSDT", "1m", "trade") in session.subscriptions
    assert service.realtime_service.unsubscribed[-1] == "kline:bybit:spot:BTCUSDT:1m:trade"
    assert service.realtime_service.subscribed[-1] == "kline:bybit:spot:ETHUSDT:1m:trade"


@pytest.mark.asyncio
async def test_invalid_switch_does_not_change_existing_subscriptions():
    session, service, _ = make_session()
    await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC]}))
    await drain_control(session)
    before = set(session.subscriptions)

    service.fail_normalize = True
    await session._handle_raw_message(
        json.dumps({"action": "switch", "request_id": "r2", "unsubscribe": [BTC], "subscribe": [ETH]})
    )
    payload = await drain_control(session)

    assert payload["type"] == "error"
    assert payload["request_id"] == "r2"
    assert payload["code"] == "INVALID_SYMBOL"
    assert session.subscriptions == before


@pytest.mark.asyncio
async def test_snapshot_present_is_sent_after_ack_and_absent_is_silent():
    session, service, _ = make_session()
    stream = StreamKey("bybit", "spot", "BTCUSDT", "1m", "trade")
    service.active.add(stream)
    service.snapshot_by_stream[stream] = {"time": 1, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 3}

    await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC]}))
    assert (await drain_control(session))["type"] == "subscribed"
    snapshot = await drain_control(session)

    assert snapshot["type"] == "snapshot"
    assert snapshot["request_id"] == "r1"
    assert snapshot["data"]["close"] == 2


@pytest.mark.asyncio
async def test_cold_stream_sends_warming_up_and_ready():
    session, service, _ = make_session()

    await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC]}))
    assert (await drain_control(session))["type"] == "subscribed"
    warming = await drain_control(session)
    ready = await drain_control(session)

    assert warming["type"] == "warming_up"
    assert ready["type"] == "stream_ready"
    assert service.activated == [StreamKey("bybit", "spot", "BTCUSDT", "1m", "trade")]


@pytest.mark.asyncio
async def test_fast_switch_does_not_emit_stale_ready():
    session, service, _ = make_session()

    await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC]}))
    await drain_control(session)
    await session._handle_raw_message(
        json.dumps({"action": "switch", "request_id": "r2", "unsubscribe": [BTC], "subscribe": [ETH]})
    )
    await drain_control(session)
    await session._handle_raw_message(
        json.dumps({"action": "switch", "request_id": "r3", "unsubscribe": [ETH], "subscribe": [SOL]})
    )
    await drain_control(session)
    await asyncio.sleep(0.05)

    stale_payloads = []
    while not session._queue.empty():
        stale_payloads.append((await drain_control(session)))
    assert all(payload.get("stream", {}).get("symbol") != "BTCUSDT" for payload in stale_payloads)
    assert all(payload.get("stream", {}).get("symbol") != "ETHUSDT" for payload in stale_payloads)


@pytest.mark.asyncio
async def test_old_stream_update_is_dropped_after_switch():
    session, _, websocket = make_session()
    writer = asyncio.create_task(session._writer_loop())
    try:
        await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC]}))
        await asyncio.sleep(0.01)
        await session._handle_raw_message(
            json.dumps({"action": "switch", "request_id": "r2", "unsubscribe": [BTC], "subscribe": [ETH]})
        )
        await asyncio.sleep(0.01)

        session.enqueue_kline(
            channel="kline:bybit:spot:BTCUSDT:1m:trade",
            stream={"exchange": "bybit", "market": "spot", "symbol": "BTCUSDT", "interval": "1m", "price_basis": "trade"},
            event={"open_time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        )
        await asyncio.sleep(0.01)
    finally:
        await session.close()
        writer.cancel()

    assert not any(message.get("type") == "kline" and message.get("stream", {}).get("symbol") == "BTCUSDT" for message in websocket.sent)


@pytest.mark.asyncio
async def test_request_id_on_error_invalid_json_and_unsupported_action():
    session, _, _ = make_session()

    await session._handle_raw_message("{")
    invalid_json = await drain_control(session)
    await session._handle_raw_message(json.dumps({"action": "wat", "request_id": "r9"}))
    unsupported = await drain_control(session)

    assert invalid_json["code"] == "INVALID_JSON"
    assert unsupported["request_id"] == "r9"
    assert unsupported["code"] == "UNSUPPORTED_ACTION"


@pytest.mark.asyncio
async def test_subscription_limit(monkeypatch):
    monkeypatch.setattr("app.services.chart_ws_service.settings.chart_ws_max_subscriptions", 1)
    session, _, _ = make_session()

    await session._handle_raw_message(json.dumps({"action": "subscribe", "request_id": "r1", "streams": [BTC, ETH]}))
    payload = await drain_control(session)

    assert payload["type"] == "error"
    assert payload["code"] == "SUBSCRIPTION_LIMIT_EXCEEDED"


@pytest.mark.asyncio
async def test_slow_consumer_queue_overflow_drops_kline(monkeypatch):
    monkeypatch.setattr("app.services.chart_ws_service.settings.chart_ws_outbound_queue_size", 1)
    session, _, _ = make_session()
    stream = StreamKey("bybit", "spot", "BTCUSDT", "1m", "trade")
    session.subscriptions.add(stream)
    await session._queue.put({"kind": "control", "payload": {"type": "blocked"}})

    session.enqueue_kline(
        channel=stream.channel,
        stream=stream.as_dict(),
        event={"open_time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
    )

    assert session._queue.qsize() == 1


@pytest.mark.asyncio
async def test_activation_single_flight():
    calls = 0

    class OnDemand:
        async def ensure_pair_tracked(self, **kwargs):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)

    service = ChartWebSocketService(
        realtime_service=FakeRealtimeService(),  # type: ignore[arg-type]
        session_factory=None,  # type: ignore[arg-type]
        backfill_service=None,  # type: ignore[arg-type]
    )
    service.on_demand_tracking_service = OnDemand()
    stream = StreamKey("bybit", "spot", "BTCUSDT", "1m", "trade")

    await asyncio.gather(service.ensure_stream_active(stream), service.ensure_stream_active(stream))

    assert calls == 1


@pytest.mark.asyncio
async def test_normalize_bybit_futures_default_and_trade_channels(monkeypatch):
    class Adapter:
        async def resolve_symbol(self, *, market, symbol):
            return symbol

        async def validate_symbol(self, **kwargs):
            return None

    class Backfill:
        async def validate_pair(self, **kwargs):
            return None

    monkeypatch.setattr("app.services.chart_ws_service.get_adapter", lambda **kwargs: Adapter())
    service = ChartWebSocketService(
        realtime_service=FakeRealtimeService(),  # type: ignore[arg-type]
        session_factory=None,  # type: ignore[arg-type]
        backfill_service=Backfill(),  # type: ignore[arg-type]
    )
    raw = {"exchange": "bybit", "market": "futures", "symbol": "BTCUSDT", "interval": "1d"}

    mark = await service.normalize_stream(raw)
    trade = await service.normalize_stream({**raw, "price_basis": "trade"})

    assert mark.price_basis == "mark"
    assert mark.channel.endswith(":mark")
    assert trade.price_basis == "trade"
    assert trade.channel.endswith(":trade")
    assert mark != trade
