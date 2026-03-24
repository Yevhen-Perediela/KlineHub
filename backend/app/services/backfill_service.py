from __future__ import annotations

import logging
import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..exchanges.binance_futures import BinanceFuturesAdapter
from ..models import TrackedPair
from .candle_service import CandleService
from ..utils.intervals import get_interval_ms, latest_closed_open_time

logger = logging.getLogger(__name__)


class BackfillService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self.adapter = BinanceFuturesAdapter()

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

        approx_count = ((missing_end - missing_start) // interval_ms) + 1
        limit = min(max(approx_count, 1), settings.max_backfill_limit)

        logger.info(
            "Repairing gap for %s %s start=%s end=%s approx_count=%s",
            symbol,
            interval,
            missing_start,
            missing_end,
            approx_count,
        )

        events = await self.adapter.fetch_klines(
            symbol=symbol,
            interval=interval,
            start_time=missing_start,
            end_time=missing_end + interval_ms - 1,
            limit=limit,
        )

        if not events:
            logger.info("Gap repair got no events for %s %s", symbol, interval)
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

        await CandleService.write_many_closed_candles_to_redis(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            events=events,
        )

        logger.info(
            "Gap repair finished for %s %s candles=%s",
            symbol,
            interval,
            len(events),
        )

        return len(events)

    async def repair_all_active_pairs(self) -> int:
        total_repaired = 0

        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(
                    TrackedPair.exchange == "binance",
                    TrackedPair.market == "futures",
                    TrackedPair.status == "active",
                )
            )
            pairs = result.scalars().all()

        for pair in pairs:
            try:
                repaired = await self.repair_missing_for_pair(
                    exchange=pair.exchange,
                    market=pair.market,
                    symbol=pair.symbol,
                    interval=pair.interval,
                )
                total_repaired += repaired
            except Exception as exc:
                logger.exception(
                    "Failed repairing pair %s %s: %s",
                    pair.symbol,
                    pair.interval,
                    exc,
                )

        return total_repaired