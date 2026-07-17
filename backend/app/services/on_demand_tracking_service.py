from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..config import settings
from ..models import TrackedPair

logger = logging.getLogger(__name__)


class OnDemandTrackingService:
    source = "on_demand"

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        stream_manager,
    ) -> None:
        self.session_factory = session_factory
        self.stream_manager = stream_manager
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_expiration_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def ensure_pair_tracked(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
    ) -> None:
        expires_at = self._new_expiration()
        reload_required = False

        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(
                    TrackedPair.exchange == exchange,
                    TrackedPair.market == market,
                    TrackedPair.symbol == symbol.upper(),
                    TrackedPair.interval == interval,
                )
            )
            item = result.scalar_one_or_none()

            if item is None:
                item = TrackedPair(
                    exchange=exchange,
                    market=market,
                    symbol=symbol.upper(),
                    interval=interval,
                    status="active",
                    source=self.source,
                    priority=500,
                    auto_stop_at=expires_at,
                )
                session.add(item)
                reload_required = True
                logger.info(
                    "Created on-demand tracked pair %s %s %s %s until %s",
                    exchange,
                    market,
                    symbol,
                    interval,
                    expires_at,
                )
            elif item.status != "active":
                item.status = "active"
                item.source = self.source
                item.auto_stop_at = expires_at
                item.updated_at = datetime.utcnow()
                reload_required = True
                logger.info(
                    "Activated on-demand tracked pair %s %s %s %s until %s",
                    exchange,
                    market,
                    symbol,
                    interval,
                    expires_at,
                )
            elif item.source == self.source:
                item.auto_stop_at = expires_at
                item.updated_at = datetime.utcnow()

            await session.commit()

        if reload_required:
            await self.stream_manager.reload()

    async def expire_once(self) -> int:
        now = datetime.utcnow()
        expired_count = 0

        async with self.session_factory() as session:
            result = await session.execute(
                select(TrackedPair).where(
                    TrackedPair.status == "active",
                    TrackedPair.source == self.source,
                    TrackedPair.auto_stop_at.is_not(None),
                    TrackedPair.auto_stop_at <= now,
                )
            )
            expired = result.scalars().all()

            for item in expired:
                item.status = "paused"
                item.updated_at = now
                expired_count += 1

            if expired_count:
                await session.commit()

        if expired_count:
            logger.info("Paused %s expired on-demand tracked pairs", expired_count)
            await self.stream_manager.reload()

        return expired_count

    async def _run_expiration_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.expire_once()
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=settings.on_demand_tracking_cleanup_interval_sec,
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("On-demand tracking expiration loop failed")
                await asyncio.sleep(10)

    @staticmethod
    def _new_expiration() -> datetime:
        return datetime.utcnow() + timedelta(days=settings.on_demand_tracking_ttl_days)
