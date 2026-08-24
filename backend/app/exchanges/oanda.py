from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

import httpx

from ..config import settings
from ..services.exchange_limit_service import record_http_error, record_http_response
from .base import ProviderAdapter
from ..price_basis import resolve_price_basis


class OandaConfigurationError(RuntimeError):
    pass


class InvalidOandaInstrumentError(ValueError):
    pass


class OandaAdapter(ProviderAdapter):
    provider_id = "oanda"
    stream_transport = "http_stream"
    native_kline_stream = False
    canonical_interval = "1m"

    _instrument_cache: list[dict[str, Any]] | None = None
    _instrument_cache_at: float = 0.0

    GRANULARITY_MAP = {
        "1m": "M1",
        "3m": "M3",
        "5m": "M5",
        "15m": "M15",
        "30m": "M30",
        "1h": "H1",
        "2h": "H2",
        "4h": "H4",
        "6h": "H6",
        "12h": "H12",
        "1d": "D",
        "1w": "W",
        "1M": "M",
    }

    def get_history_backfill_interval(self, requested_interval: str) -> str:
        if requested_interval in self.GRANULARITY_MAP:
            return requested_interval
        return super().get_history_backfill_interval(requested_interval)

    def build_stream_name(self, *, symbol: str, interval: str) -> str:
        if interval != "1m":
            raise ValueError("OANDA streaming supports only 1m canonical ingestion")
        return symbol.upper()

    def build_combined_url(self, streams: list[str]) -> str:
        instruments = ",".join(sorted(set(streams)))
        return (
            f"{settings.oanda_stream_url}/v3/accounts/"
            f"{settings.oanda_account_id}/pricing/stream?instruments={instruments}"
        )

    def build_headers(self) -> dict[str, str]:
        self._assert_configured()
        return {
            "Authorization": f"Bearer {settings.oanda_api_token}",
            "Accept-Datetime-Format": "UNIX",
        }

    def parse_message(self, raw_message: str) -> dict[str, Any]:
        return json.loads(raw_message)

    def extract_price_event(self, message: dict[str, Any]) -> dict[str, Any] | None:
        if message.get("type") != "PRICE":
            return None

        instrument = message.get("instrument")
        bids = message.get("bids") or []
        asks = message.get("asks") or []
        timestamp = message.get("time")

        if not instrument or not bids or not asks or not timestamp:
            return None

        bid = bids[0].get("price")
        ask = asks[0].get("price")
        if bid is None or ask is None:
            return None

        mid = (float(bid) + float(ask)) / 2.0
        return {
            "symbol": str(instrument).upper(),
            "price": str(mid),
            "bid": str(bid),
            "ask": str(ask),
            "timestamp_ms": self._parse_unix_ms(timestamp),
            "is_closed": False,
            "source": "ws",
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
            exchange="oanda", market=market, requested_price_basis=price_basis
        )
        self._assert_configured()

        if interval not in self.GRANULARITY_MAP:
            raise ValueError(f"OANDA backfill supports only {self.canonical_interval}, got {interval}")

        params: dict[str, Any] = {
            "price": "M",
            "granularity": self.GRANULARITY_MAP[interval],
        }
        if start_time is not None:
            params["from"] = str(int(start_time) / 1000)
        if end_time is not None:
            params["to"] = str(int(end_time) / 1000)
        if limit is not None and start_time is None and end_time is None:
            params["count"] = int(limit)

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{settings.oanda_rest_url}/v3/instruments/{symbol.upper()}/candles",
                    params=params,
                    headers=self.build_headers(),
                )
                record_http_response(exchange="oanda", response=response)
            except Exception as exc:
                record_http_error(exchange="oanda", error=exc)
                raise

        if response.status_code == 404:
            raise InvalidOandaInstrumentError(f"Invalid OANDA instrument: {symbol}")

        response.raise_for_status()
        payload = response.json()
        candles = payload.get("candles")
        if not isinstance(candles, list):
            return []

        events: list[dict[str, Any]] = []
        for item in candles:
            if not isinstance(item, dict):
                continue

            complete = bool(item.get("complete"))
            mid = item.get("mid") or {}
            open_time = self._parse_unix_ms(item.get("time"))
            if open_time is None or not mid:
                continue

            events.append(
                {
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "open_time": open_time,
                    "close_time": open_time + 60_000 - 1,
                    "open": str(mid.get("o")),
                    "high": str(mid.get("h")),
                    "low": str(mid.get("l")),
                    "close": str(mid.get("c")),
                    "volume": str(item.get("volume", 0)),
                    "is_closed": complete,
                    "trades_count": None,
                    "stream": None,
                    "source": "rest",
                    "price_basis": basis.value,
                }
            )

        return events

    async def list_instruments(self) -> list[dict[str, Any]]:
        self._assert_configured()

        cache_ttl = settings.oanda_instruments_cache_ttl_sec
        now = time.time()
        if self._instrument_cache is not None and (now - self._instrument_cache_at) < cache_ttl:
            return self._instrument_cache

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{settings.oanda_rest_url}/v3/accounts/{settings.oanda_account_id}/instruments",
                    headers=self.build_headers(),
                )
                record_http_response(exchange="oanda", response=response)
            except Exception as exc:
                record_http_error(exchange="oanda", error=exc)
                raise

        response.raise_for_status()
        payload = response.json()
        instruments = payload.get("instruments")
        if not isinstance(instruments, list):
            instruments = []

        normalized = [self._normalize_instrument(item) for item in instruments if isinstance(item, dict)]
        self._instrument_cache = normalized
        self._instrument_cache_at = now
        return normalized

    async def validate_symbol(self, *, market: str, symbol: str, interval: str) -> None:
        if interval != self.canonical_interval:
            raise ValueError("OANDA tracked pairs must use interval=1m")

        instruments = await self.list_instruments()
        instrument = next((item for item in instruments if item["symbol"] == symbol.upper()), None)
        if instrument is None:
            raise InvalidOandaInstrumentError(f"Unknown OANDA instrument: {symbol}")

        if instrument["market"] != market:
            raise ValueError(
                f"OANDA instrument {symbol.upper()} belongs to market={instrument['market']}, "
                f"not market={market}"
            )

        if not instrument["tradeable"]:
            raise ValueError(f"OANDA instrument {symbol.upper()} is not tradeable")

    def _normalize_instrument(self, item: dict[str, Any]) -> dict[str, Any]:
        symbol = str(item.get("name", "")).upper()
        display_name = str(item.get("displayName", symbol))
        instrument_type = str(item.get("type", "")).upper()

        return {
            "symbol": symbol,
            "display_name": display_name,
            "market": self._infer_market(symbol=symbol, instrument_type=instrument_type),
            "type": instrument_type,
            "pip_location": item.get("pipLocation"),
            "display_precision": item.get("displayPrecision"),
            "tradeable": bool(item.get("tradeable", True)),
        }

    @staticmethod
    def _infer_market(*, symbol: str, instrument_type: str) -> str:
        if instrument_type in {"CURRENCY", "CURRENCY_PAIR"}:
            return "forex"
        if instrument_type in {"METAL", "CFD_METAL"}:
            return "metals"
        if symbol.startswith("XAU_") or symbol.startswith("XAG_") or symbol.startswith("XPT_"):
            return "metals"
        return "stocks"

    @staticmethod
    def _parse_unix_ms(value: Any) -> int | None:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            if value > 10_000_000_000:
                return int(value)
            return int(float(value) * 1000)

        text = str(value).strip()
        if not text:
            return None

        try:
            numeric = float(text)
        except ValueError:
            try:
                return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
            except ValueError:
                return None

        if numeric > 10_000_000_000:
            return int(numeric)
        return int(numeric * 1000)

    @staticmethod
    def _assert_configured() -> None:
        if not settings.oanda_api_token or not settings.oanda_account_id:
            raise OandaConfigurationError(
                "OANDA support requires OANDA_API_TOKEN and OANDA_ACCOUNT_ID"
            )
