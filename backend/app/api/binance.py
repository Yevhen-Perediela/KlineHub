import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..exchanges.registry import get_adapter

router = APIRouter(prefix="/api/binance", tags=["binance"])


class BinanceMetalResponse(BaseModel):
    symbol: str
    display_name: str
    market: str
    price_basis: str
    status: str


class BinanceMetalsResponse(BaseModel):
    items: list[BinanceMetalResponse]
    count: int


@router.get("/metals", response_model=BinanceMetalsResponse)
async def list_binance_metals() -> BinanceMetalsResponse:
    """Only listed metals that currently return index-price candles."""
    try:
        instruments = await get_adapter(exchange="binance", market="metals").list_instruments()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="Failed to fetch Binance metal indices") from exc
    items = [BinanceMetalResponse.model_validate(item) for item in instruments]
    return BinanceMetalsResponse(items=items, count=len(items))
