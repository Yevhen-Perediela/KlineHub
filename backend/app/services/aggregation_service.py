from __future__ import annotations

from decimal import Decimal

from sqlalchemy import distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Candle
from ..utils.intervals import (
    count_interval_steps,
    floor_to_interval_open,
    interval_can_aggregate,
    interval_sort_key,
    is_supported_interval,
    next_interval_open,
)


class AggregationService:
    @staticmethod
    async def get_available_intervals(
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        price_basis: str,
    ) -> list[str]:
        stmt = select(distinct(Candle.interval)).where(
            Candle.exchange == exchange,
            Candle.market == market,
            Candle.symbol == symbol.upper(),
            Candle.price_basis == price_basis,
            Candle.is_closed.is_(True),
        )
        result = await db.execute(stmt)
        intervals = [
            value
            for value in result.scalars().all()
            if value is not None and is_supported_interval(value)
        ]
        return sorted(intervals, key=interval_sort_key)

    @classmethod
    async def pick_best_source_interval(
        cls,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        target_interval: str,
        price_basis: str,
    ) -> str | None:
        available = await cls.get_available_intervals(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            price_basis=price_basis,
        )

        candidates = [
            interval for interval in available if interval_can_aggregate(interval, target_interval)
        ]

        if not candidates:
            return None

        return max(candidates, key=interval_sort_key)

    @staticmethod
    async def get_bars_from_interval(
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str,
        from_ts: int | None,
        to_ts: int | None,
        limit: int,
    ) -> list[dict]:
        stmt = select(Candle).where(
            Candle.exchange == exchange,
            Candle.market == market,
            Candle.symbol == symbol.upper(),
            Candle.interval == interval,
            Candle.price_basis == price_basis,
            Candle.is_closed.is_(True),
        )

        if from_ts is not None:
            stmt = stmt.where(Candle.open_time >= from_ts)

        if to_ts is not None:
            stmt = stmt.where(Candle.open_time <= to_ts)

        stmt = stmt.order_by(Candle.open_time.asc()).limit(limit)

        result = await db.execute(stmt)
        candles = result.scalars().all()

        return [
            {
                "time": int(item.open_time),
                "open": float(item.open),
                "high": float(item.high),
                "low": float(item.low),
                "close": float(item.close),
                "volume": float(item.volume),
            }
            for item in candles
        ]

    @staticmethod
    async def get_aggregated_bars(
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        source_interval: str,
        target_interval: str,
        price_basis: str,
        from_ts: int | None,
        to_ts: int | None,
        limit: int,
    ) -> list[dict]:
        if not interval_can_aggregate(source_interval, target_interval):
            raise ValueError(
                f"Cannot aggregate {source_interval} candles into {target_interval}"
            )

        stmt = select(Candle).where(
            Candle.exchange == exchange,
            Candle.market == market,
            Candle.symbol == symbol.upper(),
            Candle.interval == source_interval,
            Candle.price_basis == price_basis,
            Candle.is_closed.is_(True),
        )

        if from_ts is not None:
            source_from = floor_to_interval_open(from_ts, target_interval)
            stmt = stmt.where(Candle.open_time >= source_from)

        if to_ts is not None:
            source_to = floor_to_interval_open(
                next_interval_open(to_ts, target_interval) - 1,
                source_interval,
            )
            stmt = stmt.where(Candle.open_time <= source_to)

        stmt = stmt.order_by(Candle.open_time.asc())

        result = await db.execute(stmt)
        candles = result.scalars().all()

        if not candles:
            return []

        buckets: dict[int, list[Candle]] = {}

        for candle in candles:
            bucket_open = floor_to_interval_open(int(candle.open_time), target_interval)
            buckets.setdefault(bucket_open, []).append(candle)

        bars: list[dict] = []

        for bucket_open in sorted(buckets.keys()):
            items = sorted(buckets[bucket_open], key=lambda x: int(x.open_time))
            if not items:
                continue

            expected_count = count_interval_steps(
                bucket_open,
                next_interval_open(bucket_open, target_interval) - 1,
                source_interval,
            )
            actual_count = len(items)

            if actual_count < expected_count:
                continue

            complete = True
            expected_open = bucket_open
            for item in items:
                if int(item.open_time) != expected_open:
                    complete = False
                    break
                expected_open = next_interval_open(expected_open, source_interval)

            if not complete:
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

            if len(bars) >= limit:
                break

        return bars
