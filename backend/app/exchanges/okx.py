from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
from typing import Any

import httpx

from ..config import settings
from ..services.exchange_limit_service import record_http_error, record_http_response
from ..utils.intervals import latest_closed_open_time, next_interval_open
from .base import ProviderAdapter


class InvalidOkxSymbolError(ValueError):
    pass


class OkxRateLimitError(ValueError):
    pass


class OkxAdapter(ProviderAdapter):
    provider_id = "okx"
    logger = logging.getLogger(__name__)

    INTERVAL_MAP = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "12h": "12H",
        "1d": "1Dutc",
        "3d": "3Dutc",
        "1w": "1Wutc",
        "1M": "1Mutc",
    }
    REVERSE_INTERVAL_MAP = {value: key for key, value in INTERVAL_MAP.items()}
    MARKET_INST_TYPE_MAP = {
        "spot": "SPOT",
        "futures": "SWAP",
    }

    _instrument_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
    _rest_lock: asyncio.Lock | None = None
    _cache_lock: asyncio.Lock | None = None
    _last_rest_request_at: float = 0.0
    _response_cache: dict[tuple[str, tuple[tuple[str, Any], ...]], tuple[float, dict[str, Any]]] = {}
    _inflight_requests: dict[tuple[str, tuple[tuple[str, Any], ...]], asyncio.Task[dict[str, Any]]] = {}

    def build_stream_name(self, *, symbol: str, interval: str) -> str:
        channel = f"candle{self._to_okx_interval(interval)}"
        return f"{channel}|{symbol.upper()}"

    def build_combined_url(self, streams: list[str]) -> str:
        raise NotImplementedError("Use build_market_ws_url for OKX")

    def build_market_ws_url(self, market: str) -> str:
        self._market_to_inst_type(market)
        return settings.okx_ws_business_url

    def build_subscribe_messages(self, streams: list[str]) -> list[str]:
        args = []
        for stream in sorted(set(streams)):
            channel, inst_id = self._parse_stream_key(stream)
            args.append({"channel": channel, "instId": inst_id})
        if not args:
            return []
        return [json.dumps({"op": "subscribe", "args": args})]

    def parse_message(self, raw_message: str) -> dict[str, Any]:
        if raw_message == "pong":
            return {"event": "pong"}
        return json.loads(raw_message)

    def extract_kline_event(self, message: dict[str, Any]) -> dict[str, Any] | None:
        arg = message.get("arg")
        if not isinstance(arg, dict):
            return None

        channel = str(arg.get("channel") or "")
        if not channel.startswith("candle"):
            return None

        interval = self.REVERSE_INTERVAL_MAP.get(channel.removeprefix("candle"))
        inst_id = str(arg.get("instId") or "").upper()
        data = message.get("data")
        if not interval or not inst_id or not isinstance(data, list) or not data:
            return None

        return self._row_to_event(
            row=data[-1],
            symbol=inst_id,
            interval=interval,
            source="ws",
        )

    async def fetch_klines(
        self,
        *,
        market: str,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        inst_id = await self.resolve_symbol(market=market, symbol=symbol)
        bar = self._to_okx_interval(interval)
        target_limit = max(1, int(limit or 300))
        page_limit = min(target_limit, 100)
        rows: list[Any] = []
        cursor_end = end_time
        latest_closed_open = latest_closed_open_time(now_ms=self._now_ms(), interval=interval)
        endpoint = "history-candles"
        if end_time is None or int(end_time) > latest_closed_open:
            endpoint = "candles"

        async with httpx.AsyncClient(timeout=30.0) as client:
            while len(rows) < target_limit:
                params: dict[str, Any] = {
                    "instId": inst_id,
                    "bar": bar,
                    "limit": min(page_limit, target_limit - len(rows)),
                }
                if cursor_end is not None:
                    params["after"] = int(cursor_end) + 1
                if start_time is not None:
                    params["before"] = max(0, int(start_time) - 1)

                payload = await self._get_json(
                    path=f"/api/v5/market/{endpoint}",
                    params=params,
                    cache_ttl_sec=settings.okx_klines_cache_ttl_sec,
                    symbol=inst_id,
                )
                page = payload.get("data") or []
                if not isinstance(page, list) or not page:
                    break

                rows.extend(page)
                oldest_open = self._oldest_open_time(page)
                if oldest_open is None:
                    break
                if start_time is not None and oldest_open <= start_time:
                    break
                if len(page) < page_limit:
                    break
                cursor_end = oldest_open - 1

        events: list[dict[str, Any]] = []
        seen_open_times: set[int] = set()

        for row in sorted(rows, key=lambda item: int(item[0])):
            event = self._row_to_event(
                row=row,
                symbol=inst_id,
                interval=interval,
                source="rest",
                latest_closed_open=latest_closed_open,
            )
            if event is None:
                continue
            open_time = int(event["open_time"])
            if start_time is not None and open_time < int(start_time):
                continue
            if end_time is not None and open_time > int(end_time):
                continue
            if open_time in seen_open_times:
                continue
            seen_open_times.add(open_time)
            events.append(event)

        return events[:target_limit]

    async def list_instruments(self, *, market: str = "futures") -> list[dict[str, Any]]:
        inst_type = self._market_to_inst_type(market)
        cache_key = f"okx:{market}"
        cached = self._instrument_cache.get(cache_key)
        now = time.time()
        if cached is not None:
            cached_at, items = cached
            if (now - cached_at) < settings.okx_instruments_cache_ttl_sec:
                return items

        payload = await self._get_json(
            path="/api/v5/public/instruments",
            params={"instType": inst_type},
            cache_ttl_sec=settings.okx_instruments_cache_ttl_sec,
        )
        rows = payload.get("data") or []
        if not isinstance(rows, list):
            return []

        items = [
            self._normalize_instrument(item=item, market=market)
            for item in rows
            if isinstance(item, dict)
        ]
        self._instrument_cache[cache_key] = (now, items)
        return items

    async def validate_symbol(self, *, market: str, symbol: str, interval: str) -> None:
        self._to_okx_interval(interval)
        inst_id = await self.resolve_symbol(market=market, symbol=symbol)
        instruments = await self.list_instruments(market=market)
        instrument = next((item for item in instruments if item["symbol"] == inst_id), None)
        if instrument is None:
            raise InvalidOkxSymbolError(f"Unknown OKX {market} symbol: {symbol}")
        if instrument["status"] != "live":
            raise ValueError(f"OKX instrument {inst_id} is not live")

    async def resolve_symbol(self, *, market: str, symbol: str) -> str:
        normalized = symbol.upper()
        compact = normalized.replace("-", "")
        instruments = await self.list_instruments(market=market)

        for item in instruments:
            if item["symbol"] == normalized:
                return str(item["symbol"])
        for item in instruments:
            aliases = {
                str(item.get("symbol", "")).replace("-", "").upper(),
                f"{item.get('base_coin', '')}{item.get('quote_coin', '')}".upper(),
            }
            if compact in aliases:
                return str(item["symbol"])

        raise InvalidOkxSymbolError(f"Unknown OKX {market} symbol: {symbol}")

    @classmethod
    def _market_to_inst_type(cls, market: str) -> str:
        normalized = market.lower()
        if normalized not in cls.MARKET_INST_TYPE_MAP:
            raise ValueError(f"Unsupported OKX market: {market}")
        return cls.MARKET_INST_TYPE_MAP[normalized]

    @classmethod
    def _to_okx_interval(cls, interval: str) -> str:
        mapped = cls.INTERVAL_MAP.get(interval)
        if mapped is None:
            raise ValueError(f"Unsupported OKX interval: {interval}")
        return mapped

    @staticmethod
    def _parse_stream_key(stream: str) -> tuple[str, str]:
        try:
            channel, inst_id = stream.split("|", 1)
        except ValueError as exc:
            raise ValueError(f"Invalid OKX stream key: {stream}") from exc
        return channel, inst_id

    @staticmethod
    def _normalize_instrument(*, item: dict[str, Any], market: str) -> dict[str, Any]:
        inst_id = str(item.get("instId", "")).upper()
        base_coin = str(item.get("baseCcy") or "").upper()
        quote_coin = str(item.get("quoteCcy") or "").upper()

        if market.lower() == "futures" and inst_id.endswith("-SWAP"):
            parts = inst_id.removesuffix("-SWAP").split("-")
            if len(parts) >= 2:
                base_coin = base_coin or parts[0]
                quote_coin = quote_coin or parts[1]
            quote_coin = quote_coin or str(item.get("settleCcy") or "").upper()

        return {
            "symbol": inst_id,
            "market": market,
            "status": str(item.get("state", "")),
            "inst_type": str(item.get("instType", "")),
            "base_coin": base_coin,
            "quote_coin": quote_coin,
            "settle_coin": str(item.get("settleCcy") or "").upper(),
        }

    @staticmethod
    def _row_to_event(
        *,
        row: Any,
        symbol: str,
        interval: str,
        source: str,
        latest_closed_open: int | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(row, list) or len(row) < 6:
            return None

        open_time = int(row[0])
        close_time = next_interval_open(open_time, interval) - 1
        confirm = str(row[8]) if len(row) > 8 else "1"
        is_closed = confirm == "1"
        if latest_closed_open is not None:
            is_closed = is_closed and open_time <= latest_closed_open

        return {
            "symbol": symbol.upper(),
            "interval": interval,
            "open_time": open_time,
            "close_time": close_time,
            "open": str(row[1]),
            "high": str(row[2]),
            "low": str(row[3]),
            "close": str(row[4]),
            "volume": str(row[5]),
            "is_closed": is_closed,
            "source": source,
        }

    @staticmethod
    def _oldest_open_time(rows: list[Any]) -> int | None:
        values: list[int] = []
        for row in rows:
            if isinstance(row, list) and row:
                try:
                    values.append(int(row[0]))
                except (TypeError, ValueError):
                    continue
        return min(values) if values else None

    @staticmethod
    def _raise_on_error(payload: dict[str, Any], symbol: str | None = None) -> None:
        code = payload.get("code")
        if code in ("0", 0, None):
            return
        msg = str(payload.get("msg", "OKX request failed"))
        if str(code) == "50011" or "rate limit" in msg.lower() or "too many requests" in msg.lower():
            raise OkxRateLimitError(f"OKX rate limited: {msg}")
        normalized_msg = msg.lower()
        if symbol and ("instrument" in normalized_msg or "instid" in normalized_msg or "symbol" in normalized_msg):
            raise InvalidOkxSymbolError(f"Invalid OKX symbol: {symbol}")
        raise ValueError(f"OKX API error {code}: {msg}")

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @classmethod
    def _get_rest_lock(cls) -> asyncio.Lock:
        if cls._rest_lock is None:
            cls._rest_lock = asyncio.Lock()
        return cls._rest_lock

    @classmethod
    def _get_cache_lock(cls) -> asyncio.Lock:
        if cls._cache_lock is None:
            cls._cache_lock = asyncio.Lock()
        return cls._cache_lock

    @staticmethod
    def _cache_key(path: str, params: dict[str, Any]) -> tuple[str, tuple[tuple[str, Any], ...]]:
        normalized: dict[str, Any] = dict(params)
        if path.endswith("/candles") and normalized.get("after") is not None:
            try:
                bucket_ms = max(1, int(settings.okx_klines_cache_ttl_sec)) * 1000
                normalized["after"] = (int(normalized["after"]) // bucket_ms) * bucket_ms
            except (TypeError, ValueError):
                pass
        return path, tuple(sorted((str(key), value) for key, value in normalized.items()))

    async def _get_json(
        self,
        *,
        path: str,
        params: dict[str, Any],
        cache_ttl_sec: float,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        key = self._cache_key(path, params)
        now = time.monotonic()
        async with self._get_cache_lock():
            cached = self._response_cache.get(key)
            if cached is not None:
                cached_at, payload = cached
                if now - cached_at <= cache_ttl_sec:
                    return copy.deepcopy(payload)

            task = self._inflight_requests.get(key)
            if task is None or task.done():
                task = asyncio.create_task(
                    self._fetch_json_uncached(path=path, params=params, symbol=symbol)
                )
                self._inflight_requests[key] = task

        try:
            payload = await task
        finally:
            async with self._get_cache_lock():
                if self._inflight_requests.get(key) is task and task.done():
                    self._inflight_requests.pop(key, None)

        async with self._get_cache_lock():
            self._response_cache[key] = (time.monotonic(), copy.deepcopy(payload))
        return copy.deepcopy(payload)

    async def _fetch_json_uncached(
        self,
        *,
        path: str,
        params: dict[str, Any],
        symbol: str | None,
    ) -> dict[str, Any]:
        retries = max(0, settings.okx_rest_max_retries)
        for attempt in range(retries + 1):
            await self._throttle_rest_request()
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(
                        f"{settings.okx_rest_url}{path}",
                        params=params,
                    )
                record_http_response(exchange="okx", response=response)

                if response.status_code == 429:
                    raise OkxRateLimitError("OKX HTTP 429 rate limit")

                response.raise_for_status()
                payload = response.json()
                self._raise_on_error(payload=payload, symbol=symbol)
                return payload
            except OkxRateLimitError as exc:
                record_http_error(exchange="okx", error=exc)
                if attempt >= retries:
                    raise
                delay = settings.okx_rest_retry_after_sec * (attempt + 1)
                self.logger.warning("OKX rate limited, retrying in %.2fs", delay)
                await asyncio.sleep(delay)
            except Exception as exc:
                record_http_error(exchange="okx", error=exc)
                raise

        raise OkxRateLimitError("OKX rate limit retries exhausted")

    async def _throttle_rest_request(self) -> None:
        async with self._get_rest_lock():
            min_interval = max(0, settings.okx_rest_min_interval_ms) / 1000
            now = time.monotonic()
            wait_for = min_interval - (now - self._last_rest_request_at)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self.__class__._last_rest_request_at = time.monotonic()
