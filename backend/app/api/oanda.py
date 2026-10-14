from typing import Literal

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..exchanges.oanda import OandaConfigurationError
from ..exchanges.registry import get_adapter

router = APIRouter(prefix="/api/oanda", tags=["oanda"])


class OandaInstrumentResponse(BaseModel):
    symbol: str
    display_name: str
    market: Literal["forex", "metals", "stocks"]
    type: str
    pip_location: int | None = None
    display_precision: int | None = None
    tradeable: bool


class OandaInstrumentsResponse(BaseModel):
    items: list[OandaInstrumentResponse]
    count: int


@router.get("/instruments", response_model=OandaInstrumentsResponse)
async def list_oanda_instruments(
    market: Literal["forex", "metals", "stocks"] | None = Query(default=None),
) -> OandaInstrumentsResponse:
    """Return instruments available to the configured OANDA account."""
    adapter = get_adapter(exchange="oanda", market=market or "forex")
    try:
        instruments = await adapter.list_instruments()
    except OandaConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Failed to fetch OANDA account instruments") from exc

    items = [
        OandaInstrumentResponse.model_validate(item)
        for item in instruments
        if market is None or item["market"] == market
    ]
    return OandaInstrumentsResponse(items=items, count=len(items))
