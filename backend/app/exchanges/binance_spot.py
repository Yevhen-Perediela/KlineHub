from __future__ import annotations

import json
import time
from typing import Any

import httpx

from ..services.exchange_limit_service import record_http_error, record_http_response
from .base import ProviderAdapter
from ..price_basis import resolve_price_basis


class InvalidSpotSymbolError(ValueError):
    pass


class BinanceSpotAdapter(ProviderAdapter):
    provider_id = "binance_spot"

    REST_BASE_URL = "https://api.binance.com"
    WS_BASE_URL = "wss://stream.binance.com:9443"
    _instrument_cache: tuple[float, list[dict[str, Any]]] | None = None

    def build_stream_name(self, *, symbol: str, interval: str) -> str:
        return f"{symbol.lower()}@kline_{interval}"

    def build_combined_url(self, streams: list[str]) -> str:
        joined = "/".join(streams)
        return f"{self.WS_BASE_URL}/stream?streams={joined}"

    def parse_message(self, raw_message: str) -> dict[str, Any]:
        return json.loads(raw_message)

    def extract_kline_event(self, message: dict[str, Any]) -> dict[str, Any] | None:
        data = message.get("data") or message
        if data.get("e") != "kline":
            return None

        k = data.get("k") or {}
        symbol = data.get("s") or k.get("s")
        interval = k.get("i")

        if not symbol or not interval:
            return None

        return {
            "symbol": str(symbol).upper(),
            "interval": str(interval),
            "open_time": int(k["t"]),
            "close_time": int(k["T"]),
            "open": k["o"],
            "high": k["h"],
            "low": k["l"],
            "close": k["c"],
            "volume": k["v"],
            "is_closed": bool(k["x"]),
        }

    async def fetch_klines(
        self,
        *,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        basis = resolve_price_basis(
            exchange="binance", market=market, requested_price_basis=price_basis
        )
        symbol = symbol.upper()

        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
        }
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)
        if limit is not None:
            params["limit"] = int(limit)

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.REST_BASE_URL}/api/v3/klines",
                    params=params,
                )
                record_http_response(exchange="binance", response=response)
            except Exception as exc:
                record_http_error(exchange="binance", error=exc)
                raise

        if response.status_code == 400:
            try:
                payload = response.json()
            except Exception:
                payload = {}

            msg = str(payload.get("msg", "")).lower()

            if "invalid symbol" in msg:
                raise InvalidSpotSymbolError(f"Invalid Binance spot symbol: {symbol}")

            raise httpx.HTTPStatusError(
                f"Binance spot returned 400 for {symbol}: {payload}",
                request=response.request,
                response=response,
            )

        response.raise_for_status()
        rows = response.json()

        if not isinstance(rows, list):
            return []

        events: list[dict[str, Any]] = []
        for row in rows:
            events.append(
                {
                    "open_time": int(row[0]),
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                    "volume": row[5],
                    "close_time": int(row[6]),
                    "is_closed": True,
                    "source": "rest",
                    "price_basis": basis.value,
                }
            )
        return events

    async def list_instruments(self) -> list[dict[str, Any]]:
        now = time.time()
        if self._instrument_cache is not None:
            cached_at, items = self._instrument_cache
            if (now - cached_at) < 900:
                return items

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(f"{self.REST_BASE_URL}/api/v3/exchangeInfo")
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
                "is_spot_trading_allowed": bool(item.get("isSpotTradingAllowed", False)),
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
            raise InvalidSpotSymbolError(f"Unknown Binance spot symbol: {symbol}")
        if instrument["status"] != "TRADING" or not instrument["is_spot_trading_allowed"]:
            raise ValueError(f"Binance spot instrument {symbol.upper()} is not tradable")
