import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import Integer, MetaData, create_engine, select
from sqlalchemy.orm import Session

from app.api import binance as binance_api
from app.config import settings
from app.db import Base
from app.exchanges.binance_futures import BinanceMetalsAdapter, InvalidBinanceMetalSymbolError
from app.exchanges.binance_futures import BinanceFuturesAdapter
from app.exchanges.binance_spot import BinanceSpotAdapter
from app.exchanges.registry import get_adapter
from app.models import Candle, TrackedPair
from app.price_basis import PriceBasis, resolve_price_basis
from app.schemas import TrackedPairCreate, TrackedPairResponse
from app.services import backfill_service as backfill_module
from app.services.backfill_service import BackfillService
from app.services.candle_service import CandleService
from app.services.open_candle_service import OpenCandleService
from app.services.stream_manager import ActiveWorker, StreamManager, WorkerSpec
from app.exchanges.base import StreamSubscription
from app.utils.intervals import SUPPORTED_INTERVALS, next_interval_open

HTTP_CLIENT = httpx.AsyncClient


def raw_candle(open_time, interval="1m", close="105"):
    return [open_time, "100", "110", "90", close, "999",
            next_interval_open(open_time, interval) - 1, "999", 60, "999", "999", "0"]


@pytest.fixture
def index_http(monkeypatch):
    requests = []
    reply = {"payload": [raw_candle(0), raw_candle(60_000)]}

    def handle(request):
        requests.append(request)
        if request.url.path.endswith("exchangeInfo"):
            return httpx.Response(200, json={"symbols": [
                {"symbol": symbol, "status": status, "baseAsset": symbol[:-4], "quoteAsset": "USDT"}
                for symbol, status in reply.get("symbols", [(s, "TRADING") for s in BinanceMetalsAdapter.METALS])
            ]})
        payload = reply["payload"]
        if callable(payload):
            payload = payload(request)
        if isinstance(payload, httpx.Response):
            return payload
        return httpx.Response(200, json=payload)

    client_class = httpx.AsyncClient
    monkeypatch.setattr("app.exchanges.binance_futures.httpx.AsyncClient",
                        lambda **kwargs: client_class(transport=httpx.MockTransport(handle), **kwargs))
    adapter = BinanceMetalsAdapter()
    monkeypatch.setattr(adapter, "_now_ms", lambda: 90_000)
    return adapter, requests, reply


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", BinanceMetalsAdapter.METALS)
async def test_index_history_and_live_normalization(index_http, symbol):
    adapter, requests, _ = index_http
    args = dict(market="metals", symbol=symbol, interval="1m", start_time=0, end_time=90_000, limit=5000)
    history = await adapter.fetch_klines(**args)
    live = await adapter.fetch_klines(**args, include_open=True)
    assert len(history) == 1 and history[0]["is_closed"]
    assert len(live) == 2 and not live[1]["is_closed"]
    assert all(e["volume"] == "0" and e["trades_count"] is None and e["price_basis"] == "index" for e in live)
    assert all(e["source"] == "rest" for e in live)
    assert requests[0].url.path == "/fapi/v1/indexPriceKlines"
    assert dict(requests[0].url.params) == dict(pair=symbol, interval="1m", startTime="0", endTime="90000", limit="1500")


@pytest.mark.asyncio
async def test_discovery_checks_index_support_and_caches(index_http):
    adapter, requests, reply = index_http
    reply["symbols"] = [("XAUUSDT", "TRADING"), ("XAGUSDT", "TRADING"),
                        ("XPTUSDT", "PENDING_TRADING"), ("BTCUSDT", "TRADING")]
    reply["payload"] = lambda r: (httpx.Response(400, json={"code": -1121})
                                  if r.url.params["pair"] == "XAGUSDT" else [raw_candle(0)])
    items = await adapter.list_instruments()
    assert [i["symbol"] for i in items] == ["XAUUSDT"]
    before = len(requests)
    assert await adapter.list_instruments() == items
    await adapter.validate_symbol(market="metals", symbol="xauusdt", interval="1h")
    assert len(requests) == before
    with pytest.raises(InvalidBinanceMetalSymbolError):
        await adapter.validate_symbol(market="metals", symbol="XPTUSDT", interval="1m")
    with pytest.raises(InvalidBinanceMetalSymbolError):
        await adapter.fetch_klines(market="metals", symbol="BTCUSDT", interval="1m")


@pytest.mark.asyncio
async def test_discovery_failure_is_not_cached_as_empty(index_http):
    adapter, _, reply = index_http
    reply["payload"] = httpx.Response(403, json={"msg": "blocked"})
    with pytest.raises(httpx.HTTPStatusError):
        await adapter.list_instruments()
    assert adapter._metals_cache is None


@pytest.mark.asyncio
async def test_concurrent_discovery_shares_requests(index_http):
    adapter, requests, _ = index_http
    results = await asyncio.gather(adapter.list_instruments(), adapter.list_instruments(), adapter.list_instruments())
    assert all(len(items) == 4 for items in results)
    assert len(requests) == 5  # one exchangeInfo plus one probe per metal


@pytest.mark.asyncio
async def test_rate_limit_retry_honors_cooldown(index_http, monkeypatch):
    adapter, requests, reply = index_http
    responses = [httpx.Response(429, headers={"Retry-After": "7"}, json={}), [raw_candle(0)]]
    reply["payload"] = lambda r: responses.pop(0)
    sleeps = AsyncMock()
    monkeypatch.setattr("app.exchanges.binance_futures.asyncio.sleep", sleeps)
    events = await adapter.fetch_klines(market="metals", symbol="XAUUSDT", interval="1m")
    assert len(events) == 1 and len(requests) == 2
    assert sleeps.await_args.args[0] >= 6.9


@pytest.mark.asyncio
@pytest.mark.parametrize("interval", SUPPORTED_INTERVALS)
async def test_all_configured_intervals(index_http, interval):
    adapter, requests, reply = index_http
    reply["payload"] = [raw_candle(0, interval)]
    events = await adapter.fetch_klines(market="metals", symbol="XAUUSDT", interval=interval, include_open=True)
    assert requests[0].url.params["interval"] == interval
    assert events[0]["close_time"] == next_interval_open(0, interval) - 1


@pytest.fixture
def database(monkeypatch):
    engine = create_engine("sqlite://")
    metadata = MetaData()
    for table in Base.metadata.tables.values():
        copied = table.to_metadata(metadata)
        if table.name == "candles":
            copied.c.id.type = Integer()  # SQLite's auto-increment spelling.
    metadata.create_all(engine)
    session = Session(engine)
    db = SimpleNamespace(execute=AsyncMock(side_effect=session.execute),
                         commit=AsyncMock(side_effect=session.commit),
                         rollback=AsyncMock(side_effect=session.rollback))
    @asynccontextmanager
    async def factory():
        yield db
    monkeypatch.setattr(CandleService, "write_many_closed_candles_to_redis", AsyncMock())
    yield session, db, factory
    session.close()
    engine.dispose()


ARGS = dict(exchange="binance", market="metals", symbol="XAUUSDT", interval="1m", price_basis="index")


@pytest.mark.asyncio
async def test_backfill_pagination_persistence_duplicates_and_gaps(index_http, database, monkeypatch):
    adapter, requests, reply = index_http
    session, db, factory = database
    monkeypatch.setattr(backfill_module, "get_adapter", lambda **kwargs: adapter)
    monkeypatch.setattr(settings, "max_backfill_limit", 5000)
    monkeypatch.setattr(adapter, "max_kline_limit", 2)
    monkeypatch.setattr(adapter, "_now_ms", lambda: 600_000)
    monkeypatch.setattr(backfill_module.time, "time", lambda: 600)
    reply["payload"] = lambda r: [raw_candle(t) for t in range(int(r.url.params["startTime"]), int(r.url.params["endTime"]) + 1, 60_000)]
    service = BackfillService(factory)
    assert await service.ensure_range_loaded(db=db, from_ts=0, to_ts=240_000, **ARGS) == 5
    assert [(r.url.params["startTime"], r.url.params["endTime"], r.url.params["limit"]) for r in requests] == [
        ("0", "119999", "2"), ("120000", "239999", "2"), ("240000", "299999", "1")]
    rows = session.scalars(select(Candle).order_by(Candle.open_time)).all()
    assert [r.open_time for r in rows] == [0, 60_000, 120_000, 180_000, 240_000]
    assert all(r.price_basis == "index" and r.volume == Decimal(0) and r.source == "rest" for r in rows)
    assert await service.ensure_range_loaded(db=db, from_ts=0, to_ts=240_000, **ARGS) == 0
    assert len(requests) == 3
    # Execute the production ON CONFLICT upsert twice against the same identity.
    events = await adapter.fetch_klines(market="metals", symbol="XAUUSDT", interval="1m", start_time=0, end_time=59_999)
    await CandleService.upsert_closed_candles(db=db, events=events, **ARGS)
    assert len(session.scalars(select(Candle)).all()) == 5
    session.delete(rows[2])
    session.commit()
    assert await service.ensure_range_loaded(db=db, from_ts=0, to_ts=240_000, **ARGS) == 1
    assert requests[-1].url.params["startTime"] == "120000"
    assert len(session.scalars(select(Candle)).all()) == 5
    # The existing tail repair uses the same index endpoint and storage.
    assert await service.repair_missing_for_pair(**ARGS) == 5
    assert len(session.scalars(select(Candle)).all()) == 10


@pytest.mark.asyncio
async def test_poll_updates_closes_and_repairs_before_advancing_tail(database, monkeypatch):
    session, _, factory = database
    sub = StreamSubscription(**ARGS, stream_key="XAUUSDT:index:1m")
    events = [dict(open_time=t, close_time=t + 59_999, open="100", high="110", low="90", close="105",
                   volume="0", trades_count=None, price_basis="index", source="rest", is_closed=closed)
              for t, closed in [(0, True), (60_000, False), (60_000, True), (180_000, False)]]
    pages = [events[:2], events[:2], events[2:]]
    calls = []
    async def fetch(**kwargs):
        calls.append(kwargs)
        return pages.pop(0)
    adapter = SimpleNamespace(provider_id="binance_metals", stream_transport="rest_poll", fetch_klines=fetch)
    backfill = SimpleNamespace(repair_missing_for_pair=AsyncMock(), ensure_range_loaded=AsyncMock())
    realtime = SimpleNamespace(publish_kline=AsyncMock())
    manager = StreamManager(factory, backfill, realtime)
    worker = ActiveWorker(1, WorkerSpec(adapter, "binance", "metals", [sub]), price_basis="index")
    monkeypatch.setattr(settings, "binance_metals_poll_interval_sec", 0.001)
    monkeypatch.setattr(CandleService, "write_open_candle_to_redis", AsyncMock())
    monkeypatch.setattr(CandleService, "write_closed_candle_to_redis", AsyncMock())
    async def publish(**kwargs):
        if kwargs["event"]["open_time"] == 180_000:
            manager._stop_event.set()
    realtime.publish_kline.side_effect = publish
    await manager._run_rest_poll_worker(worker)
    backfill.repair_missing_for_pair.assert_awaited_once_with(**ARGS)
    assert len(calls) == 3 and all(c["include_open"] for c in calls)
    assert len(session.scalars(select(Candle)).all()) == 2
    assert CandleService.write_closed_candle_to_redis.await_count == 2
    assert realtime.publish_kline.await_count == 5  # repeated closed candle skipped
    assert backfill.ensure_range_loaded.await_args.kwargs["from_ts"] == 120_000
    assert backfill.ensure_range_loaded.await_args.kwargs["to_ts"] == 179_999
    assert not worker.ready


@pytest.mark.asyncio
async def test_discovery_api_and_open_bar(index_http, monkeypatch):
    adapter, _, _ = index_http
    monkeypatch.setattr(binance_api, "get_adapter", lambda **kwargs: adapter)
    app = FastAPI()
    app.include_router(binance_api.router)
    # Retain the real ASGI client; index_http replaces the shared httpx class.
    async with HTTP_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/binance/metals")
    assert response.status_code == 200 and response.json()["count"] == 4
    assert all(i["price_basis"] == "index" for i in response.json()["items"])
    monkeypatch.setattr("app.services.open_candle_service.get_adapter", lambda **kwargs: adapter)
    bar = await OpenCandleService._get_rest_open_bar(current_open_ts=60_000, now_ms=90_000, **ARGS)
    assert bar["time"] == 60_000 and bar["volume"] == 0


def test_index_basis_and_existing_defaults():
    assert resolve_price_basis(exchange="binance", market="metals") is PriceBasis.INDEX
    for basis in ("trade", "mark", "mid"):
        with pytest.raises(ValueError):
            resolve_price_basis(exchange="binance", market="metals", requested_price_basis=basis)
    assert resolve_price_basis(exchange="binance", market="spot") is PriceBasis.TRADE
    assert resolve_price_basis(exchange="binance", market="futures") is PriceBasis.TRADE
    assert resolve_price_basis(exchange="oanda", market="metals") is PriceBasis.MID
    assert get_adapter(exchange="binance", market="metals").stream_transport == "rest_poll"
    assert TrackedPairCreate(**ARGS).price_basis == "index"


@pytest.mark.asyncio
async def test_existing_binance_trade_endpoints_and_volumes(index_http, monkeypatch):
    _, requests, reply = index_http
    reply["payload"] = [raw_candle(0)]
    spot = await BinanceSpotAdapter().fetch_klines(market="spot", symbol="BTCUSDT", interval="1m", limit=1)
    assert requests[-1].url.path == "/api/v3/klines"
    assert requests[-1].url.params["symbol"] == "BTCUSDT"
    assert spot[0]["price_basis"] == "trade" and spot[0]["volume"] == "999"
    futures_requests = []
    @asynccontextmanager
    async def get(url, *, params, timeout):
        futures_requests.append((url, params))
        yield SimpleNamespace(status=200, headers={}, raise_for_status=lambda: None,
                              json=AsyncMock(return_value=[raw_candle(0), raw_candle(60_000)]))
    @asynccontextmanager
    async def client():
        yield SimpleNamespace(get=get)
    monkeypatch.setattr("app.exchanges.binance_futures.aiohttp.ClientSession", client)
    adapter = BinanceFuturesAdapter()
    monkeypatch.setattr(adapter, "_now_ms", lambda: 90_000)
    futures = await adapter.fetch_klines(market="futures", symbol="BTCUSDT", interval="1m", limit=2)
    assert futures_requests[0][0].endswith("/fapi/v1/klines")
    assert futures_requests[0][1] == dict(symbol="BTCUSDT", interval="1m", limit=2)
    assert len(futures) == 1  # retain the existing unfinished-candle exclusion
    assert futures[0]["price_basis"] == "trade" and futures[0]["volume"] == "999"
    assert futures[0]["trades_count"] == 60


@pytest.mark.asyncio
async def test_binance_and_oanda_metal_storage_coexist(database):
    session, db, _ = database
    event = dict(open_time=0, close_time=59_999, open="100", high="110", low="90", close="105",
                 volume="0", is_closed=True, price_basis="index", source="rest")
    await CandleService.upsert_closed_candles(db=db, events=[event], **ARGS)
    await CandleService.upsert_closed_candles(db=db, events=[{**event, "price_basis": "mid"}],
                                             exchange="oanda", market="metals", symbol="XAU_USD",
                                             interval="1m", price_basis="mid")
    session.add_all([
        TrackedPair(**ARGS, status="active"),
        TrackedPair(exchange="oanda", market="metals", symbol="XAU_USD", interval="1m", price_basis="mid"),
    ])
    session.commit()
    assert len(session.scalars(select(Candle)).all()) == 2
    pairs = session.scalars(select(TrackedPair)).all()
    assert len(pairs) == 2
    assert TrackedPairResponse.model_validate(pairs[0]).price_basis == "index"
