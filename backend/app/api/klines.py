from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db
from ..models import Candle
from ..schemas import KlineHistoryResponse, KlineBarResponse
from ..services.aggregation_service import AggregationService
from ..utils.intervals import is_aggregated_interval, is_supported_interval

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/klines", response_model=KlineHistoryResponse)
async def get_klines(
    exchange: str = Query(...),
    market: str = Query(...),
    symbol: str = Query(...),
    interval: str = Query(...),
    from_ts: int | None = Query(default=None, alias="from"),
    to_ts: int | None = Query(default=None, alias="to"),
    limit: int = Query(default=500, ge=1, le=5000),
    db: AsyncSession = Depends(get_db),
) -> KlineHistoryResponse:
    symbol = symbol.upper()

    if not is_supported_interval(interval):
        return KlineHistoryResponse(bars=[], noData=True)

    if is_aggregated_interval(interval):
        bars = await AggregationService.get_aggregated_bars_from_1h(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            target_interval=interval,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
        )
        return KlineHistoryResponse(
            bars=[KlineBarResponse(**bar) for bar in bars],
            noData=len(bars) == 0,
        )

    stmt = select(Candle).where(
        Candle.exchange == exchange,
        Candle.market == market,
        Candle.symbol == symbol,
        Candle.interval == interval,
        Candle.is_closed.is_(True),
    )

    if from_ts is not None:
        stmt = stmt.where(Candle.open_time >= from_ts)

    if to_ts is not None:
        stmt = stmt.where(Candle.open_time <= to_ts)

    stmt = stmt.order_by(Candle.open_time.asc()).limit(limit)

    result = await db.execute(stmt)
    candles = result.scalars().all()

    bars = [
        KlineBarResponse(
            time=int(item.open_time),
            open=float(item.open),
            high=float(item.high),
            low=float(item.low),
            close=float(item.close),
            volume=float(item.volume),
        )
        for item in candles
    ]

    return KlineHistoryResponse(
        bars=bars,
        noData=len(bars) == 0,
    )