from __future__ import annotations

from typing import Any

import httpx


class BinanceSpotAdapter:
    REST_BASE_URL = "https://api.binance.com"
    WS_BASE_URL = "wss://stream.binance.com:9443"

    def build_stream_name(self, *, symbol: str, interval: str) -> str:
        return f"{symbol.lower()}@kline_{interval}"

    def build_combined_url(self, streams: list[str]) -> str:
        joined = "/".join(streams)
        return f"{self.WS_BASE_URL}/stream?streams={joined}"

    def parse_message(self, raw_message: str) -> dict[str, Any]:
        import json
        return json.loads(raw_message)

    def extract_kline_event(self, message: dict[str, Any]) -> dict[str, Any] | None:
        data = message.get("data") or message
        event_type = data.get("e")
        if event_type != "kline":
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
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
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
            response.raise_for_status()
            rows = response.json()

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