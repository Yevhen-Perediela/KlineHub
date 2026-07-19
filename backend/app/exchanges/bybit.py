from __future__ import annotations

import json
import time
from typing import Any

import httpx

from ..config import settings
from ..services.exchange_limit_service import record_http_error, record_http_response
from ..utils.intervals import latest_closed_open_time, next_interval_open
from .base import ProviderAdapter


class InvalidBybitSymbolError(ValueError):
    pass


class BybitAdapter(ProviderAdapter):
    provider_id = "bybit"

    INTERVAL_MAP = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "6h": "360",
        "12h": "720",
        "1d": "D",
        "1w": "W",
        "1M": "M",
    }

    REVERSE_INTERVAL_MAP = {value: key for key, value in INTERVAL_MAP.items()}
    MARKET_CATEGORY_MAP = {
        "spot": "spot",
        "futures": "linear",
    }
    WS_URL_MAP = {
        "spot": "/v5/public/spot",
        "futures": "/v5/public/linear",
    }

    _instrument_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}

    def build_stream_name(self, *, symbol: str, interval: str) -> str:
        return f"kline.{self._to_bybit_interval(interval)}.{symbol.upper()}"

    def build_combined_url(self, streams: list[str]) -> str:
        raise NotImplementedError("Use build_market_ws_url for Bybit")

    def build_market_ws_url(self, market: str) -> str:
        normalized_market = market.lower()
        if normalized_market not in self.WS_URL_MAP:
            raise ValueError(f"Unsupported Bybit market: {market}")
        return f"{settings.bybit_ws_url}{self.WS_URL_MAP[normalized_market]}"

    def build_subscribe_messages(self, streams: list[str]) -> list[str]:
        return [json.dumps({"op": "subscribe", "args": sorted(set(streams))})]

    def parse_message(self, raw_message: str) -> dict[str, Any]:
        return json.loads(raw_message)

    def extract_kline_event(self, message: dict[str, Any]) -> dict[str, Any] | None:
        topic = message.get("topic")
        if not isinstance(topic, str) or not topic.startswith("kline."):
            return None

        data = message.get("data")
        if not isinstance(data, list) or not data:
            return None

        kline = data[-1]
        if not isinstance(kline, dict):
            return None

        symbol = kline.get("symbol")
        interval = self.REVERSE_INTERVAL_MAP.get(str(kline.get("interval")))
        start = kline.get("start")
        end = kline.get("end")

        if not symbol or not interval or start is None or end is None:
            return None

        return {
            "symbol": str(symbol).upper(),
            "interval": interval,
            "open_time": int(start),
            "close_time": int(end),
            "open": str(kline.get("open")),
            "high": str(kline.get("high")),
            "low": str(kline.get("low")),
            "close": str(kline.get("close")),
            "volume": str(kline.get("volume", "0")),
            "is_closed": bool(kline.get("confirm")),
            "turnover": str(kline.get("turnover", "0")),
            "timestamp": kline.get("timestamp"),
        }

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
        category = self._market_to_category(market)
        symbol = await self.resolve_symbol(market=market, symbol=symbol)
        params: dict[str, Any] = {
            "category": category,
            "symbol": symbol.upper(),
            "interval": self._to_bybit_interval(interval),
            "limit": min(int(limit or 300), 1000),
        }
        if start_time is not None:
            params["start"] = int(start_time)
        if end_time is not None:
            params["end"] = int(end_time)

        is_mark_price = market.lower() == "futures"
        endpoint = "mark-price-kline" if is_mark_price else "kline"

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{settings.bybit_rest_url}/v5/market/{endpoint}",
                    params=params,
                )
                record_http_response(exchange="bybit", response=response)
            except Exception as exc:
                record_http_error(exchange="bybit", error=exc)
                raise

        response.raise_for_status()
        payload = response.json()
        self._raise_on_error(payload=payload, symbol=symbol)
        rows = ((payload.get("result") or {}).get("list")) or []
        if not isinstance(rows, list):
            return []

        events: list[dict[str, Any]] = []
        latest_closed_open = latest_closed_open_time(now_ms=self._now_ms(), interval=interval)

        for row in sorted(rows, key=lambda item: int(item[0])):
            min_row_length = 5 if is_mark_price else 7
            if not isinstance(row, list) or len(row) < min_row_length:
                continue
            open_time = int(row[0])
            close_time = next_interval_open(open_time, interval) - 1
            volume = "0" if is_mark_price else str(row[5])
            turnover = "0" if is_mark_price else str(row[6])
            events.append(
                {
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "open_time": open_time,
                    "close_time": close_time,
                    "open": str(row[1]),
                    "high": str(row[2]),
                    "low": str(row[3]),
                    "close": str(row[4]),
                    "volume": volume,
                    "turnover": turnover,
                    "is_closed": open_time <= latest_closed_open,
                    "source": "bybit_mark" if is_mark_price else "rest",
                }
            )

        return events

    async def list_instruments(self, *, market: str = "futures") -> list[dict[str, Any]]:
        category = self._market_to_category(market)
        cache_key = f"bybit:{market}"
        cached = self._instrument_cache.get(cache_key)
        now = time.time()
        if cached is not None:
            cached_at, items = cached
            if (now - cached_at) < settings.bybit_instruments_cache_ttl_sec:
                return items

        items: list[dict[str, Any]] = []
        cursor: str | None = None

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict[str, Any] = {
                    "category": category,
                    "limit": 1000,
                }
                if cursor:
                    params["cursor"] = cursor

                try:
                    response = await client.get(
                        f"{settings.bybit_rest_url}/v5/market/instruments-info",
                        params=params,
                    )
                    record_http_response(exchange="bybit", response=response)
                except Exception as exc:
                    record_http_error(exchange="bybit", error=exc)
                    raise
                response.raise_for_status()
                payload = response.json()
                self._raise_on_error(payload=payload)

                result = payload.get("result") or {}
                page = result.get("list") or []
                if isinstance(page, list):
                    items.extend(
                        self._normalize_instrument(item=item, market=market)
                        for item in page
                        if isinstance(item, dict)
                    )

                cursor = result.get("nextPageCursor")
                if not cursor:
                    break

        self._instrument_cache[cache_key] = (now, items)
        return items

    async def validate_symbol(self, *, market: str, symbol: str, interval: str) -> None:
        self._to_bybit_interval(interval)
        symbol = await self.resolve_symbol(market=market, symbol=symbol)
        instruments = await self.list_instruments(market=market)
        instrument = next((item for item in instruments if item["symbol"] == symbol.upper()), None)
        if instrument is None:
            raise InvalidBybitSymbolError(f"Unknown Bybit {market} symbol: {symbol}")
        if instrument["status"] != "Trading":
            raise ValueError(f"Bybit instrument {symbol.upper()} is not Trading")

    async def resolve_symbol(self, *, market: str, symbol: str) -> str:
        normalized = symbol.upper()
        instruments = await self.list_instruments(market=market)
        for item in instruments:
            if item["symbol"] == normalized:
                return str(item["symbol"])
        for item in instruments:
            if str(item.get("display_name") or "").upper() == normalized:
                return str(item["symbol"])
        raise InvalidBybitSymbolError(f"Unknown Bybit {market} symbol: {symbol}")

    @classmethod
    def _market_to_category(cls, market: str) -> str:
        normalized = market.lower()
        if normalized not in cls.MARKET_CATEGORY_MAP:
            raise ValueError(f"Unsupported Bybit market: {market}")
        return cls.MARKET_CATEGORY_MAP[normalized]

    @classmethod
    def _to_bybit_interval(cls, interval: str) -> str:
        mapped = cls.INTERVAL_MAP.get(interval)
        if mapped is None:
            raise ValueError(f"Unsupported Bybit interval: {interval}")
        return mapped

    @staticmethod
    def _normalize_instrument(*, item: dict[str, Any], market: str) -> dict[str, Any]:
        return {
            "symbol": str(item.get("symbol", "")).upper(),
            "market": market,
            "status": str(item.get("status", "")),
            "base_coin": item.get("baseCoin"),
            "quote_coin": item.get("quoteCoin"),
            "display_name": str(item.get("displayName") or "").upper(),
        }

    @staticmethod
    def _raise_on_error(payload: dict[str, Any], symbol: str | None = None) -> None:
        ret_code = payload.get("retCode")
        if ret_code in (0, "0", None):
            return
        ret_msg = str(payload.get("retMsg", "Bybit request failed"))
        normalized_msg = ret_msg.lower()
        if symbol and ("symbol" in normalized_msg or "invalid" in normalized_msg):
            raise InvalidBybitSymbolError(f"Invalid Bybit symbol: {symbol}")
        raise ValueError(f"Bybit API error {ret_code}: {ret_msg}")

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)
