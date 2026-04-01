from __future__ import annotations

import json
from typing import Any

import httpx


class InvalidSpotSymbolError(ValueError):
    pass


class BinanceSpotAdapter:
    REST_BASE_URL = "https://api.binance.com"
    WS_BASE_URL = "wss://stream.binance.com:9443"

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
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
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
            response = await client.get(
                f"{self.REST_BASE_URL}/api/v3/klines",
                params=params,
            )

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
                }
            )
        return events