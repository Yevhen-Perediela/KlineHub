from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import aiohttp
import httpx

from ..config import settings
from ..services.exchange_limit_service import record_http_error, record_http_response
from ..state import runtime_state
from ..utils.intervals import is_supported_interval, latest_closed_open_time
from .base import ProviderAdapter
from .binance_spot import BinanceSubscriptionMixin
from ..price_basis import PriceBasis, resolve_price_basis


class InvalidBinanceMetalSymbolError(ValueError):
    pass


class BinanceFuturesAdapter(BinanceSubscriptionMixin, ProviderAdapter):
    provider_id = "binance_futures"
    BASE_WS_URL = "wss://fstream.binance.com/stream"
    _instrument_cache: tuple[float, list[dict[str, Any]]] | None = None

    @staticmethod
    def build_stream_name(symbol: str, interval: str) -> str:
        return f"{symbol.lower()}@kline_{interval}"

    @classmethod
    def build_combined_url(cls, streams: list[str]) -> str:
        stream_path = "/".join(streams)
        return f"{cls.BASE_WS_URL}?streams={stream_path}"

    @staticmethod
    def parse_message(raw_message: str) -> dict[str, Any]:
        return json.loads(raw_message)

    @staticmethod
    def extract_kline_event(message: dict[str, Any]) -> dict[str, Any] | None:
        data = message.get("data")
        if not isinstance(data, dict):
            return None

        if data.get("e") != "kline":
            return None

        k = data.get("k")
        if not isinstance(k, dict):
            return None

        return {
            "event_type": data.get("e"),
            "event_time": data.get("E"),
            "symbol": data.get("s"),
            "interval": k.get("i"),
            "open_time": k.get("t"),
            "close_time": k.get("T"),
            "open": k.get("o"),
            "high": k.get("h"),
            "low": k.get("l"),
            "close": k.get("c"),
            "volume": k.get("v"),
            "is_closed": k.get("x"),
            "trades_count": k.get("n"),
            "stream": message.get("stream"),
        }

    async def fetch_klines(
        self,
        *,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str | None = None,
        limit: int = 300,
        start_time: int | None = None,
        end_time: int | None = None,
        include_open: bool = False,
    ) -> list[dict[str, Any]]:
        basis = resolve_price_basis(
            exchange="binance", market=market, requested_price_basis=price_basis
        )
        is_index = basis is PriceBasis.INDEX
        params: dict[str, Any] = {
            "pair" if is_index else "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(limit or 300, 1500) if is_index else limit,
        }

        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        endpoint = "indexPriceKlines" if is_index else "klines"
        url = f"{settings.binance_futures_rest_url}/fapi/v1/{endpoint}"

        if is_index:
            payload = await self._fetch_index_payload(url, params)
        else:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, params=params, timeout=30) as response:
                        runtime_state.record_exchange_http_response(
                            exchange="binance",
                            status_code=int(response.status),
                            headers=dict(response.headers),
                        )
                        response.raise_for_status()
                        payload = await response.json()
                except Exception as exc:
                    record_http_error(exchange="binance", error=exc)
                    raise

        if not isinstance(payload, list):
            return []

        safe_latest_closed_open = latest_closed_open_time(
            now_ms=self._now_ms(),
            interval=interval,
        )

        result: list[dict[str, Any]] = []

        for item in payload:
            if not isinstance(item, list) or len(item) < (7 if is_index else 11):
                continue

            open_time = int(item[0])
            close_time = int(item[6])
            is_closed = close_time < self._now_ms() if is_index else True

            if is_index and not include_open and not is_closed:
                continue

            if not is_index and open_time > safe_latest_closed_open:
                continue

            result.append(
                {
                    "event_type": "kline",
                    "event_time": None,
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "open_time": open_time,
                    "close_time": close_time,
                    "open": str(item[1]),
                    "high": str(item[2]),
                    "low": str(item[3]),
                    "close": str(item[4]),
                    "volume": "0" if is_index else str(item[5]),
                    "is_closed": is_closed,
                    "trades_count": None if is_index else (int(item[8]) if item[8] is not None else None),
                    "stream": None,
                    "source": "rest",
                    "price_basis": basis.value,
                }
            )

        return result

    @staticmethod
    def _now_ms() -> int:
        import time
        return int(time.time() * 1000)

    async def list_instruments(self) -> list[dict[str, Any]]:
        now = time.time()
        if self._instrument_cache is not None:
            cached_at, items = self._instrument_cache
            if (now - cached_at) < 900:
                return items

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(f"{settings.binance_futures_rest_url}/fapi/v1/exchangeInfo")
                record_http_response(exchange="binance", response=response)
            except Exception as exc:
                record_http_error(exchange="binance", error=exc)
                raise

        response.raise_for_status()
        payload = response.json()
        symbols = payload.get("symbols")
        if not isinstance(symbols, list):
            return []

        items = [
            {
                "symbol": str(item.get("symbol", "")).upper(),
                "status": str(item.get("status", "")),
                "base_asset": str(item.get("baseAsset", "")).upper(),
                "quote_asset": str(item.get("quoteAsset", "")).upper(),
                "contract_type": str(item.get("contractType", "")),
            }
            for item in symbols
            if isinstance(item, dict)
        ]
        self._instrument_cache = (now, items)
        return items

    async def validate_symbol(self, *, market: str, symbol: str, interval: str) -> None:
        instruments = await self.list_instruments()
        instrument = next((item for item in instruments if item["symbol"] == symbol.upper()), None)
        if instrument is None:
            raise ValueError(f"Unknown Binance futures symbol: {symbol}")
        if instrument["status"] != "TRADING":
            raise ValueError(f"Binance futures instrument {symbol.upper()} is not tradable")


class BinanceMetalsAdapter(BinanceFuturesAdapter):
    """Metal reference indices; no futures trade or mark-price candles."""

    provider_id = "binance_metals"
    stream_transport = "rest_poll"
    native_kline_stream = False
    dynamic_subscriptions = False
    max_streams_per_connection = 1
    max_kline_limit = 1500
    METALS = {
        "XAUUSDT": "Gold",
        "XAGUSDT": "Silver",
        "XPTUSDT": "Platinum",
        "XPDUSDT": "Palladium",
    }

    def __init__(self) -> None:
        self._index_request_lock = asyncio.Lock()
        self._discovery_lock = asyncio.Lock()
        self._next_index_request_at = 0.0
        self._metals_cache: tuple[float, list[dict[str, Any]]] | None = None

    @staticmethod
    def build_stream_name(symbol: str, interval: str) -> str:
        return f"{symbol.upper()}:index:{interval}"

    async def _fetch_index_payload(self, url: str, params: dict) -> Any:
        # Share pacing across polling, discovery and backfills on this adapter.
        async with self._index_request_lock:
            async with httpx.AsyncClient(timeout=30.0) as client:
                for attempt in range(3):
                    delay = self._next_index_request_at - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    limit = int(params["limit"])
                    weight = 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
                    self._next_index_request_at = time.monotonic() + max(0.2, weight / 20)
                    try:
                        response = await client.get(url, params=params)
                    except httpx.RequestError as exc:
                        record_http_error(exchange="binance", error=exc)
                        if attempt == 2:
                            raise
                        self._next_index_request_at = time.monotonic() + 2 ** attempt
                        continue
                    record_http_response(exchange="binance", response=response)
                    if response.status_code == 400 and response.json().get("code") == -1121:
                        raise InvalidBinanceMetalSymbolError(f"Binance metal index is unavailable: {params['pair']}")
                    if response.status_code in {418, 429} or response.status_code >= 500:
                        retry_after = response.headers.get("Retry-After")
                        try:
                            delay = max(float(retry_after or 0), 2 ** attempt)
                        except ValueError:
                            delay = 2 ** attempt
                        self._next_index_request_at = time.monotonic() + delay
                        if attempt < 2:
                            continue
                    response.raise_for_status()
                    return response.json()

    async def fetch_klines(self, **kwargs) -> list[dict[str, Any]]:
        if kwargs["market"] != "metals":
            raise ValueError("Binance metal indices require market=metals")
        if kwargs["symbol"].upper() not in self.METALS:
            raise InvalidBinanceMetalSymbolError(f"Unknown Binance metal index: {kwargs['symbol']}")
        if not is_supported_interval(kwargs["interval"]):
            raise ValueError(f"Unsupported Binance metals interval: {kwargs['interval']}")
        return await super().fetch_klines(**kwargs)

    async def list_instruments(self) -> list[dict[str, Any]]:
        async with self._discovery_lock:
            now = time.time()
            if self._metals_cache is not None and now - self._metals_cache[0] < 900:
                return self._metals_cache[1]
            instruments = await super().list_instruments()
            supported = []
            for item in instruments:
                symbol = item["symbol"]
                if symbol not in self.METALS or item["status"] != "TRADING":
                    continue
                try:
                    candles = await self.fetch_klines(market="metals", symbol=symbol, interval="1m",
                                                     limit=1, include_open=True)
                except InvalidBinanceMetalSymbolError:
                    continue
                if candles:
                    supported.append({**item, "display_name": self.METALS[symbol],
                                      "market": "metals", "price_basis": "index"})
            self._metals_cache = (now, supported)
            return supported

    async def validate_symbol(self, *, market: str, symbol: str, interval: str) -> None:
        if not is_supported_interval(interval):
            raise ValueError(f"Unsupported Binance metals interval: {interval}")
        if symbol.upper() not in self.METALS:
            raise InvalidBinanceMetalSymbolError(f"Unknown Binance metal index: {symbol}")
        instruments = await self.list_instruments()
        if not any(item["symbol"] == symbol.upper() for item in instruments):
            raise InvalidBinanceMetalSymbolError(f"Binance metal index is unavailable: {symbol}")
