from __future__ import annotations

from .base import ProviderAdapter
from .binance_futures import BinanceFuturesAdapter
from .binance_spot import BinanceSpotAdapter
from .bybit import BybitAdapter
from .oanda import OandaAdapter


_BINANCE_SPOT = BinanceSpotAdapter()
_BINANCE_FUTURES = BinanceFuturesAdapter()
_BYBIT = BybitAdapter()
_OANDA = OandaAdapter()


def get_adapter(*, exchange: str, market: str) -> ProviderAdapter:
    exchange = exchange.lower()
    market = market.lower()

    if exchange == "binance" and market == "spot":
        return _BINANCE_SPOT
    if exchange == "binance" and market == "futures":
        return _BINANCE_FUTURES
    if exchange == "bybit" and market in {"spot", "futures"}:
        return _BYBIT
    if exchange == "oanda" and market in {"forex", "metals", "stocks"}:
        return _OANDA

    raise ValueError(f"Unsupported exchange/market: {exchange}/{market}")


def get_canonical_interval(*, exchange: str, market: str, requested_interval: str) -> str:
    adapter = get_adapter(exchange=exchange, market=market)
    return adapter.canonical_interval or requested_interval
