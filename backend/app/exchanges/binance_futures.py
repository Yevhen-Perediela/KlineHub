from __future__ import annotations

import json
import time
from typing import Any

import aiohttp
import httpx

from ..config import settings
from ..utils.intervals import latest_closed_open_time
from .base import ProviderAdapter


class BinanceFuturesAdapter(ProviderAdapter):
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
        limit: int = 300,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }

        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        url = f"{settings.binance_futures_rest_url}/fapi/v1/klines"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=30) as response:
                response.raise_for_status()
                payload = await response.json()

        if not isinstance(payload, list):
            return []

        safe_latest_closed_open = latest_closed_open_time(
            now_ms=self._now_ms(),
            interval=interval,
        )

        result: list[dict[str, Any]] = []

        for item in payload:
            if not isinstance(item, list) or len(item) < 11:
                continue

            open_time = int(item[0])
            close_time = int(item[6])

            if open_time > safe_latest_closed_open:
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
                    "volume": str(item[5]),
                    "is_closed": True,
                    "trades_count": int(item[8]) if item[8] is not None else None,
                    "stream": None,
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
            response = await client.get(f"{settings.binance_futures_rest_url}/fapi/v1/exchangeInfo")

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
