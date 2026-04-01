from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..exchanges.binance_spot import BinanceSpotAdapter, InvalidSpotSymbolError
from ..models import Candle, TrackedPair
from ..services.candle_service import CandleService
from ..utils.intervals import get_interval_ms, latest_closed_open_time

import asyncio

logger = logging.getLogger(__name__)


@dataclass
class MissingRange:
    start_open_time: int
    end_open_time: int


class BackfillService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self.adapter = BinanceSpotAdapter()

    async def ensure_range_loaded(
        self,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        from_ts: int,
        to_ts: int,
    ) -> int:
        symbol = symbol.upper()
        self._assert_supported(exchange=exchange, market=market)

        interval_ms = get_interval_ms(interval)
        now_ms = int(time.time() * 1000)

        normalized_from = self._floor_open_time(from_ts, interval_ms)
        normalized_to = self._normalize_to_closed_open_time(
            to_ts=to_ts,
            interval=interval,
            now_ms=now_ms,
        )

        if normalized_from > normalized_to:
            logger.info(
                "ensure_range_loaded skipped because normalized_from > normalized_to "
                "for %s %s %s %s (%s > %s)",
                exchange,
                market,
                symbol,
                interval,
                normalized_from,
                normalized_to,
            )
            return 0

        missing_ranges = await self._find_missing_ranges(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            from_ts=normalized_from,
            to_ts=normalized_to,
        )

        if not missing_ranges:
            logger.info(
                "All candles already present in DB for %s %s %s %s [%s..%s]",
                exchange,
                market,
                symbol,
                interval,
                normalized_from,
                normalized_to,
            )
            return 0

        logger.info(
            "Found %s missing ranges for %s %s %s %s",
            len(missing_ranges),
            exchange,
            market,
            symbol,
            interval,
        )

        total_fetched = 0

        for rng in missing_ranges:
            fetched = await self._fetch_and_store_range(
                db=db,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                start_open_time=rng.start_open_time,
                end_open_time=rng.end_open_time,
            )
            total_fetched += fetched

        return total_fetched

    async def backfill_recent_pair(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        limit: int | None = None,
    ) -> int:
        symbol = symbol.upper()
        self._assert_supported(exchange=exchange, market=market)

        limit = min(limit or settings.default_backfill_limit, settings.max_backfill_limit)

        logger.info(
            "Backfill recent pair started: %s %s %s %s limit=%s",
            exchange,
            market,
            symbol,
            interval,
            limit,
        )

        events = await self.adapter.fetch_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
        )

        if not events:
            logger.info("Backfill recent pair got no events for %s %s", symbol, interval)
            return 0

        async with self.session_factory() as session:
            await CandleService.upsert_closed_candles(
                db=session,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                events=events,
            )
            await session.commit()

        await CandleService.write_many_closed_candles_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            events=events,
        )

        logger.info(
            "Backfill recent pair finished: %s %s %s candles=%s",
            symbol,
            interval,
            exchange,
            len(events),
        )

        return len(events)

    async def repair_missing_for_pair(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
    ) -> int:
        symbol = symbol.upper()
        self._assert_supported(exchange=exchange, market=market)

        interval_ms = get_interval_ms(interval)
        now_ms = int(time.time() * 1000)
        target_latest_closed_open = latest_closed_open_time(now_ms=now_ms, interval=interval)

        async with self.session_factory() as session:
            latest = await CandleService.get_latest_closed_candle(
                db=session,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
            )

        if latest is None:
            logger.info("No existing candles for %s %s, doing recent backfill", symbol, interval)
            return await self.backfill_recent_pair(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                limit=settings.default_backfill_limit,
            )

        missing_start = int(latest.open_time) + interval_ms
        missing_end = target_latest_closed_open

        if missing_start > missing_end:
            logger.info("No gap for %s %s", symbol, interval)
            return 0

        logger.info(
            "Repairing tail gap for %s %s start=%s end=%s",
            symbol,
            interval,
            missing_start,
            missing_end,
        )

        async with self.session_factory() as session:
            return await self._fetch_and_store_range(
                db=session,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                start_open_time=missing_start,
                end_open_time=missing_end,
            )

    async def repair_all_active_pairs(self) -> int:
        total_repaired = 0

        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(
                    TrackedPair.exchange == "binance",
                    TrackedPair.market == "spot",
                    TrackedPair.status == "active",
                )
            )
            pairs = result.scalars().all()

        logger.info("repair_all_active_pairs found %s active pairs", len(pairs))

        semaphore = asyncio.Semaphore(10)
        repaired_values: list[int] = []

        async def _repair_one(pair: TrackedPair) -> None:
            nonlocal repaired_values
            async with semaphore:
                try:
                    repaired = await self.repair_missing_for_pair(
                        exchange=pair.exchange,
                        market=pair.market,
                        symbol=pair.symbol,
                        interval=pair.interval,
                    )
                    repaired_values.append(repaired)

                except InvalidSpotSymbolError:
                    logger.warning(
                        "Pair %s %s is invalid on Binance spot. Pausing it.",
                        pair.symbol,
                        pair.interval,
                    )

                    async with self.session_factory() as session:
                        row = await session.execute(
                            select(TrackedPair).where(TrackedPair.id == pair.id)
                        )
                        obj = row.scalar_one_or_none()
                        if obj:
                            obj.status = "paused"
                            await session.commit()

                except Exception as exc:
                    logger.exception(
                        "Failed repairing pair %s %s: %s",
                        pair.symbol,
                        pair.interval,
                        exc,
                    )

        await asyncio.gather(*[_repair_one(pair) for pair in pairs])

        total_repaired = sum(repaired_values)
        logger.info("repair_all_active_pairs finished, total repaired candles=%s", total_repaired)
        return total_repaired

    def _assert_supported(self, *, exchange: str, market: str) -> None:
        if exchange != "binance" or market != "spot":
            raise ValueError(
                f"BackfillService currently supports only binance spot, "
                f"got exchange={exchange} market={market}"
            )

    def _floor_open_time(self, ts: int, interval_ms: int) -> int:
        return (int(ts) // interval_ms) * interval_ms

    def _normalize_to_closed_open_time(self, *, to_ts: int, interval: str, now_ms: int) -> int:
        target = self._floor_open_time(to_ts, get_interval_ms(interval))
        latest_closed = latest_closed_open_time(now_ms=now_ms, interval=interval)
        return min(target, latest_closed)

    async def _find_missing_ranges(
        self,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        from_ts: int,
        to_ts: int,
    ) -> list[MissingRange]:
        interval_ms = get_interval_ms(interval)

        stmt = (
            select(Candle.open_time)
            .where(
                Candle.exchange == exchange,
                Candle.market == market,
                Candle.symbol == symbol,
                Candle.interval == interval,
                Candle.is_closed.is_(True),
                Candle.open_time >= from_ts,
                Candle.open_time <= to_ts,
            )
            .order_by(Candle.open_time.asc())
        )

        result = await db.execute(stmt)
        existing_open_times = [int(x) for x in result.scalars().all()]

        if not existing_open_times:
            return [MissingRange(start_open_time=from_ts, end_open_time=to_ts)]

        existing_set = set(existing_open_times)
        missing_ranges: list[MissingRange] = []

        current = from_ts
        range_start: int | None = None

        while current <= to_ts:
            if current not in existing_set:
                if range_start is None:
                    range_start = current
            else:
                if range_start is not None:
                    missing_ranges.append(
                        MissingRange(
                            start_open_time=range_start,
                            end_open_time=current - interval_ms,
                        )
                    )
                    range_start = None
            current += interval_ms

        if range_start is not None:
            missing_ranges.append(
                MissingRange(
                    start_open_time=range_start,
                    end_open_time=to_ts,
                )
            )

        return missing_ranges

    async def _fetch_and_store_range(
        self,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        start_open_time: int,
        end_open_time: int,
    ) -> int:
        interval_ms = get_interval_ms(interval)

        if start_open_time > end_open_time:
            return 0

        total_saved = 0
        chunk_start = start_open_time

        while chunk_start <= end_open_time:
            remaining_count = ((end_open_time - chunk_start) // interval_ms) + 1
            limit = min(max(1, remaining_count), settings.max_backfill_limit)
            chunk_end = min(
                end_open_time,
                chunk_start + (limit - 1) * interval_ms,
            )

            logger.info(
                "Fetching missing candles: %s %s %s %s start=%s end=%s limit=%s",
                exchange,
                market,
                symbol,
                interval,
                chunk_start,
                chunk_end,
                limit,
            )

            events = await self.adapter.fetch_klines(
                symbol=symbol,
                interval=interval,
                start_time=chunk_start,
                end_time=chunk_end + interval_ms - 1,
                limit=limit,
            )

            if not events:
                logger.info(
                    "Exchange returned no candles for %s %s in range [%s..%s]",
                    symbol,
                    interval,
                    chunk_start,
                    chunk_end,
                )
                break

            await CandleService.upsert_closed_candles(
                db=db,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                events=events,
            )
            await db.commit()

            await CandleService.write_many_closed_candles_to_redis(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                events=events,
            )

            saved_count = len(events)
            total_saved += saved_count

            last_open_time = self._extract_last_open_time(events)
            if last_open_time is None:
                logger.warning(
                    "Could not determine last open_time for %s %s, stopping fetch loop",
                    symbol,
                    interval,
                )
                break

            next_start = last_open_time + interval_ms
            if next_start <= chunk_start:
                logger.warning(
                    "next_start did not advance for %s %s (%s <= %s), stopping fetch loop",
                    symbol,
                    interval,
                    next_start,
                    chunk_start,
                )
                break

            chunk_start = next_start

        logger.info(
            "Finished fetch/store range: %s %s %s %s saved=%s",
            exchange,
            market,
            symbol,
            interval,
            total_saved,
        )

        return total_saved

    def _extract_last_open_time(self, events: list) -> int | None:
        if not events:
            return None

        last = events[-1]

        if hasattr(last, "open_time"):
            try:
                return int(last.open_time)
            except Exception:
                pass

        if isinstance(last, dict):
            value = last.get("open_time")
            if value is not None:
                try:
                    return int(value)
                except Exception:
                    return None

        return None