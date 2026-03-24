from __future__ import annotations

from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Candle
from ..utils.intervals import get_interval_ms, is_aggregated_interval


class AggregationService:
    @staticmethod
    async def get_aggregated_bars_from_1h(
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        target_interval: str,
        from_ts: int | None,
        to_ts: int | None,
        limit: int,
    ) -> list[dict]:
        if not is_aggregated_interval(target_interval):
            raise ValueError(f"Unsupported aggregated interval: {target_interval}")

        target_ms = get_interval_ms(target_interval)
        source_ms = get_interval_ms("1h")

        if target_ms % source_ms != 0:
            raise ValueError(f"Target interval must be divisible by 1h: {target_interval}")

        stmt = select(Candle).where(
            Candle.exchange == exchange,
            Candle.market == market,
            Candle.symbol == symbol.upper(),
            Candle.interval == "1h",
            Candle.is_closed.is_(True),
        )

        if from_ts is not None:
            source_from = from_ts - target_ms
            stmt = stmt.where(Candle.open_time >= source_from)

        if to_ts is not None:
            stmt = stmt.where(Candle.open_time <= to_ts)

        stmt = stmt.order_by(Candle.open_time.asc())

        result = await db.execute(stmt)
        candles = result.scalars().all()

        if not candles:
            return []

        buckets: dict[int, list[Candle]] = {}

        for candle in candles:
            bucket_open = (int(candle.open_time) // target_ms) * target_ms
            buckets.setdefault(bucket_open, []).append(candle)

        bars: list[dict] = []

        for bucket_open in sorted(buckets.keys()):
            items = sorted(buckets[bucket_open], key=lambda x: int(x.open_time))
            if not items:
                continue

            expected_count = target_ms // source_ms

            # Skip incomplete bucket
            if len(items) < expected_count:
                continue

            first = items[0]
            last = items[-1]

            bar = {
                "time": bucket_open,
                "open": float(first.open),
                "high": float(max(Decimal(str(x.high)) for x in items)),
                "low": float(min(Decimal(str(x.low)) for x in items)),
                "close": float(last.close),
                "volume": float(sum(Decimal(str(x.volume)) for x in items)),
            }

            if from_ts is not None and bar["time"] < from_ts:
                continue
            if to_ts is not None and bar["time"] > to_ts:
                continue

            bars.append(bar)

        return bars[:limit]