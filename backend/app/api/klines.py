from fastapi import APIRouter, Depends, Query
from fastapi.exceptions import RequestValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db, SessionLocal
from ..exchanges.registry import get_adapter, get_canonical_interval
from ..schemas import KlineHistoryResponse, KlineBarResponse
from ..services.aggregation_service import AggregationService
from ..services.backfill_service import BackfillService
from ..services.candle_service import CandleService
from ..utils.intervals import (
    floor_to_interval_open,
    latest_closed_open_time,
    is_supported_interval,
    next_interval_open,
)

router = APIRouter(prefix="/api", tags=["api"])

backfill_service = BackfillService(SessionLocal)


def _parse_int_query(
    value: str | int | None,
    *,
    field_name: str,
    default: int | None = None,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    if value is None:
        return default

    if isinstance(value, int):
        parsed = value
    else:
        cleaned = value.strip().strip("'").strip('"')
        if not cleaned:
            return default

        try:
            parsed = int(cleaned)
        except ValueError as exc:
            raise RequestValidationError(
                [
                    {
                        "type": "int_parsing",
                        "loc": ("query", field_name),
                        "msg": "Input should be a valid integer",
                        "input": value,
                    }
                ]
            ) from exc

    if minimum is not None and parsed < minimum:
        raise RequestValidationError(
            [
                {
                    "type": "greater_than_equal",
                    "loc": ("query", field_name),
                    "msg": f"Input should be greater than or equal to {minimum}",
                    "input": parsed,
                    "ctx": {"ge": minimum},
                }
            ]
        )

    if maximum is not None and parsed > maximum:
        raise RequestValidationError(
            [
                {
                    "type": "less_than_equal",
                    "loc": ("query", field_name),
                    "msg": f"Input should be less than or equal to {maximum}",
                    "input": parsed,
                    "ctx": {"le": maximum},
                }
            ]
        )

    return parsed

@router.get("/klines", response_model=KlineHistoryResponse)
async def get_klines(
    exchange: str = Query(...),
    market: str = Query(...),
    symbol: str = Query(...),
    interval: str = Query(...),
    from_ts: str | int | None = Query(default=None, alias="from"),
    to_ts: str | int | None = Query(default=None, alias="to"),
    limit: str | int | None = Query(default=500),
    db: AsyncSession = Depends(get_db),
) -> KlineHistoryResponse:
    exchange = exchange.lower()
    market = market.lower()
    symbol = symbol.upper()
    from_ts = _parse_int_query(from_ts, field_name="from")
    to_ts = _parse_int_query(to_ts, field_name="to")
    limit = _parse_int_query(limit, field_name="limit", default=500, minimum=1, maximum=5000)

    if not is_supported_interval(interval):
        return KlineHistoryResponse(bars=[], noData=True)

    now_ms = __import__("time").time_ns() // 1_000_000
    current_open_ts = floor_to_interval_open(now_ms, interval)
    latest_closed_ts = latest_closed_open_time(now_ms=now_ms, interval=interval)

    if to_ts is None:
        to_ts = current_open_ts
    else:
        to_ts = min(floor_to_interval_open(to_ts, interval), current_open_ts)

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

    should_backfill = True
    adapter = get_adapter(exchange=exchange, market=market)
    preferred_history_interval = adapter.get_history_backfill_interval(interval)

    source_interval = await AggregationService.pick_best_source_interval(
        db=db,
        exchange=exchange,
        market=market,
        symbol=symbol,
        target_interval=interval,
    )

    if source_interval == interval:
        preferred_history_interval = interval
    elif preferred_history_interval == interval:
        # Prefer provider-native history for the requested interval over
        # aggregating from smaller candles that may contain trading-session gaps.
        source_interval = interval
    elif source_interval is None:
        source_interval = get_canonical_interval(
            exchange=exchange,
            market=market,
            requested_interval=interval,
        )

    source_from = floor_to_interval_open(from_ts, source_interval)
    history_to_ts = min(to_ts, latest_closed_ts)
    source_to = floor_to_interval_open(
        next_interval_open(history_to_ts, interval) - 1,
        source_interval,
    )

    if should_backfill and from_ts <= history_to_ts:
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
            to_ts=history_to_ts,
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
            to_ts=history_to_ts,
            limit=limit,
        )

    if to_ts >= current_open_ts:
        open_bar = await CandleService.get_open_candle(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
        )
        if open_bar is not None and from_ts <= open_bar["time"] <= to_ts:
            bars = [bar for bar in bars if bar["time"] != open_bar["time"]]
            bars.append(open_bar)
            bars.sort(key=lambda bar: bar["time"])
            if len(bars) > limit:
                bars = bars[-limit:]

    return KlineHistoryResponse(
        bars=[KlineBarResponse(**bar) for bar in bars],
        noData=len(bars) == 0,
    )
