import asyncio

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..exchanges.oanda import OandaConfigurationError
from ..exchanges.registry import get_adapter

router = APIRouter(prefix="/api/instruments", tags=["instruments"])

# Keep this list aligned with the combinations accepted by the registry.
MARKETS = {
    "binance": ("spot", "futures", "metals"),
    "bybit": ("spot", "futures"),
    "okx": ("spot", "futures"),
    "oanda": ("forex", "metals", "stocks"),
}


class Instrument(BaseModel):
    exchange: str
    market: str
    symbol: str
    base_asset: str | None
    quote_asset: str | None
    available: bool


class InstrumentsResponse(BaseModel):
    items: list[Instrument]
    count: int


def normalize_instrument(exchange: str, market: str, item: dict) -> Instrument:
    symbol = item["symbol"]
    if exchange == "oanda":
        parts = symbol.rsplit("_", 1)
        base, quote = parts if len(parts) == 2 else (None, None)
        available = item["tradeable"]
    elif exchange == "binance":
        base, quote = item.get("base_asset"), item.get("quote_asset")
        available = item["status"] == "TRADING" and (
            market != "spot" or item.get("is_spot_trading_allowed", False)
        )
    else:
        base, quote = item.get("base_coin"), item.get("quote_coin")
        available = item["status"] == ("Trading" if exchange == "bybit" else "live")
    return Instrument(exchange=exchange, market=market, symbol=symbol,
                      base_asset=base or None, quote_asset=quote or None,
                      available=available)


@router.get("", response_model=InstrumentsResponse)
async def list_instruments(
    exchange: str | None = Query(default=None),
    market: str | None = Query(default=None),
) -> InstrumentsResponse:
    """List provider instruments, including their current availability."""
    exchange = exchange.lower() if exchange else None
    market = market.lower() if market else None
    if exchange and exchange not in MARKETS:
        raise HTTPException(status_code=400, detail="Unsupported exchange")
    combinations = [(e, m) for e, markets in MARKETS.items() for m in markets
                    if (exchange is None or e == exchange) and (market is None or m == market)]
    if not combinations:
        raise HTTPException(status_code=400, detail="Unsupported exchange/market")

    oanda_task = None

    async def fetch(e: str, m: str) -> list[Instrument]:
        nonlocal oanda_task
        adapter = get_adapter(exchange=e, market=m)
        try:
            if e == "oanda":
                if oanda_task is None:
                    oanda_task = asyncio.create_task(adapter.list_instruments())
                rows = await oanda_task
            elif e in {"bybit", "okx"}:
                rows = await adapter.list_instruments(market=m)
            else:
                rows = await adapter.list_instruments()
        except OandaConfigurationError as exc:
            raise HTTPException(status_code=503, detail="OANDA is not configured") from exc
        except (httpx.HTTPError, ValueError, RuntimeError) as exc:
            raise HTTPException(status_code=502, detail=f"Failed to fetch {e}/{m} instruments") from exc
        return [normalize_instrument(e, m, item) for item in rows
                if e != "oanda" or item["market"] == m]

    groups = await asyncio.gather(*(fetch(e, m) for e, m in combinations))
    items = sorted((item for group in groups for item in group),
                   key=lambda item: (item.exchange, item.market, item.symbol))
    return InstrumentsResponse(items=items, count=len(items))
