from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StreamSubscription:
    exchange: str
    market: str
    symbol: str
    interval: str
    stream_key: str


class ProviderAdapter:
    provider_id: str = ""
    stream_transport: str = "websocket"
    native_kline_stream: bool = True
    canonical_interval: str | None = None

    def build_stream_name(self, *, symbol: str, interval: str) -> str:
        raise NotImplementedError

    def build_combined_url(self, streams: list[str]) -> str:
        raise NotImplementedError

    def build_subscribe_messages(self, streams: list[str]) -> list[str]:
        return []

    def parse_message(self, raw_message: str) -> dict[str, Any]:
        raise NotImplementedError

    def extract_kline_event(self, message: dict[str, Any]) -> dict[str, Any] | None:
        return None

    def extract_price_event(self, message: dict[str, Any]) -> dict[str, Any] | None:
        return None

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
        raise NotImplementedError

    async def list_instruments(self) -> list[dict[str, Any]]:
        return []

    async def validate_symbol(self, *, market: str, symbol: str, interval: str) -> None:
        return None
