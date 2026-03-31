from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db, SessionLocal
from ..models import Candle
from ..schemas import KlineHistoryResponse, KlineBarResponse
from ..services.aggregation_service import AggregationService
from ..services.backfill_service import BackfillService
from ..utils.intervals import (
    get_interval_ms,
    is_aggregated_interval,
    is_supported_interval,
    latest_closed_open_time,
)

router = APIRouter(prefix="/api", tags=["api"])

backfill_service = BackfillService(SessionLocal)

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
    exchange = exchange.lower()
    market = market.lower()
    symbol = symbol.upper()

    if not is_supported_interval(interval):
        return KlineHistoryResponse(bars=[], noData=True)

    now_ms = __import__("time").time_ns() // 1_000_000

    base_interval = "1h" if is_aggregated_interval(interval) else interval
    base_interval_ms = get_interval_ms(base_interval)

    if to_ts is None:
        to_ts = latest_closed_open_time(now_ms=now_ms, interval=base_interval)
    else:
        to_ts = min(
            (to_ts // base_interval_ms) * base_interval_ms,
            latest_closed_open_time(now_ms=now_ms, interval=base_interval),
        )

    if from_ts is None:
        from_ts = to_ts - ((limit - 1) * base_interval_ms)
    else:
        from_ts = (from_ts // base_interval_ms) * base_interval_ms

    if from_ts > to_ts:
        return KlineHistoryResponse(bars=[], noData=True)

    should_backfill = exchange == "binance" and market == "futures"

    if is_aggregated_interval(interval):
        if should_backfill:
            await backfill_service.ensure_range_loaded(
                db=db,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval="1h",
                from_ts=from_ts,
                to_ts=to_ts,
            )

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

    if should_backfill:
        await backfill_service.ensure_range_loaded(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            from_ts=from_ts,
            to_ts=to_ts,
        )

    stmt = select(Candle).where(
        Candle.exchange == exchange,
        Candle.market == market,
        Candle.symbol == symbol,
        Candle.interval == interval,
        Candle.is_closed.is_(True),
        Candle.open_time >= from_ts,
        Candle.open_time <= to_ts,
    )

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