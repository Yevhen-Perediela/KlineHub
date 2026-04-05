from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db, SessionLocal
from ..schemas import KlineHistoryResponse, KlineBarResponse
from ..services.aggregation_service import AggregationService
from ..services.backfill_service import BackfillService
from ..utils.intervals import (
    floor_to_interval_open,
    is_supported_interval,
    latest_closed_open_time,
    next_interval_open,
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

    if to_ts is None:
        to_ts = latest_closed_open_time(now_ms=now_ms, interval=interval)
    else:
        to_ts = min(floor_to_interval_open(to_ts, interval), latest_closed_open_time(now_ms=now_ms, interval=interval))

    if from_ts is None:
        from_ts = to_ts
        for _ in range(limit - 1):
            previous = floor_to_interval_open(from_ts - 1, interval)
            if previous >= from_ts:
                break
            from_ts = previous
    else:
        from_ts = floor_to_interval_open(from_ts, interval)

    if from_ts > to_ts:
        return KlineHistoryResponse(bars=[], noData=True)

    should_backfill = exchange == "binance" and market in {"spot", "futures"}
    source_interval = await AggregationService.pick_best_source_interval(
        db=db,
        exchange=exchange,
        market=market,
        symbol=symbol,
        target_interval=interval,
    )

    if source_interval is None:
        source_interval = interval

    source_from = floor_to_interval_open(from_ts, source_interval)
    source_to = floor_to_interval_open(
        next_interval_open(to_ts, interval) - 1,
        source_interval,
    )

    if should_backfill:
        await backfill_service.ensure_range_loaded(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=source_interval,
            from_ts=source_from,
            to_ts=source_to,
        )

    if source_interval == interval:
        bars = await AggregationService.get_bars_from_interval(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
        )
    else:
        bars = await AggregationService.get_aggregated_bars(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            source_interval=source_interval,
            target_interval=interval,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
        )

    return KlineHistoryResponse(
        bars=[KlineBarResponse(**bar) for bar in bars],
        noData=len(bars) == 0,
    )
