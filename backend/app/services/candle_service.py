from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..models import Candle
from ..redis_client import get_redis
from ..state import runtime_state


class CandleService:
    @staticmethod
    def _open_key(exchange: str, market: str, symbol: str, interval: str) -> str:
        return f"md:kline:open:{exchange}:{market}:{symbol}:{interval}"

    @staticmethod
    def _last_key(exchange: str, market: str, symbol: str, interval: str) -> str:
        return f"md:kline:last:{exchange}:{market}:{symbol}:{interval}"

    @staticmethod
    def _recent_key(exchange: str, market: str, symbol: str, interval: str) -> str:
        return f"md:kline:recent:{exchange}:{market}:{symbol}:{interval}"

    @classmethod
    async def write_open_candle_to_redis(
        cls,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        event: dict,
    ) -> None:
        redis = get_redis()
        payload = json.dumps(event)
        await redis.set(cls._open_key(exchange, market, symbol, interval), payload)

    @classmethod
    async def write_closed_candle_to_redis(
        cls,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        event: dict,
    ) -> None:
        redis = get_redis()
        payload = json.dumps(event)

        await redis.set(cls._last_key(exchange, market, symbol, interval), payload)

        recent_key = cls._recent_key(exchange, market, symbol, interval)
        score = int(event["open_time"])

        await redis.zadd(recent_key, {payload: score})

        max_keep = settings.recent_bars_limit
        current_count = await redis.zcard(recent_key)
        if current_count > max_keep:
            to_remove = current_count - max_keep
            await redis.zremrangebyrank(recent_key, 0, to_remove - 1)

    @classmethod
    async def write_many_closed_candles_to_redis(
        cls,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        events: list[dict],
    ) -> None:
        if not events:
            return

        redis = get_redis()
        recent_key = cls._recent_key(exchange, market, symbol, interval)

        pipe = redis.pipeline()

        sorted_events = sorted(events, key=lambda x: int(x["open_time"]))

        for event in sorted_events:
            payload = json.dumps(event)
            score = int(event["open_time"])
            pipe.zadd(recent_key, {payload: score})

        last_payload = json.dumps(sorted_events[-1])
        pipe.set(cls._last_key(exchange, market, symbol, interval), last_payload)

        await pipe.execute()

        max_keep = settings.recent_bars_limit
        current_count = await redis.zcard(recent_key)
        if current_count > max_keep:
            to_remove = current_count - max_keep
            await redis.zremrangebyrank(recent_key, 0, to_remove - 1)

    @classmethod
    async def upsert_closed_candle(
        cls,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        event: dict,
    ) -> None:
        await cls.upsert_closed_candles(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            events=[event],
        )

    @classmethod
    async def upsert_closed_candles(
        cls,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        events: list[dict],
    ) -> None:
        if not events:
            return

        values = []
        now = datetime.utcnow()

        for event in events:
            if not bool(event["is_closed"]):
                continue

            values.append(
                {
                    "exchange": exchange,
                    "market": market,
                    "symbol": symbol,
                    "interval": interval,
                    "open_time": int(event["open_time"]),
                    "close_time": int(event["close_time"]),
                    "open": Decimal(str(event["open"])),
                    "high": Decimal(str(event["high"])),
                    "low": Decimal(str(event["low"])),
                    "close": Decimal(str(event["close"])),
                    "volume": Decimal(str(event["volume"])),
                    "trades_count": int(event["trades_count"]) if event.get("trades_count") is not None else None,
                    "is_closed": True,
                    "source": "rest" if event.get("stream") is None else "ws",
                    "created_at": now,
                    "updated_at": now,
                }
            )

        if not values:
            return

        stmt = insert(Candle).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["exchange", "market", "symbol", "interval", "open_time"],
            set_={
                "close_time": stmt.excluded.close_time,
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
                "trades_count": stmt.excluded.trades_count,
                "is_closed": stmt.excluded.is_closed,
                "source": stmt.excluded.source,
                "updated_at": stmt.excluded.updated_at,
            },
        )

        await db.execute(stmt)
        await db.commit()

        last_event = sorted(values, key=lambda x: x["open_time"])[-1]
        runtime_state.last_persisted_candle = {
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "interval": interval,
            "open_time": last_event["open_time"],
            "close_time": last_event["close_time"],
            "close": str(last_event["close"]),
            "volume": str(last_event["volume"]),
        }
        runtime_state.candles_persisted_total += len(values)

    @classmethod
    async def get_latest_closed_candle(
        cls,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
    ) -> Candle | None:
        result = await db.execute(
            select(Candle).where(
                Candle.exchange == exchange,
                Candle.market == market,
                Candle.symbol == symbol,
                Candle.interval == interval,
                Candle.is_closed.is_(True),
            ).order_by(Candle.open_time.desc()).limit(1)
        )
        return result.scalar_one_or_none()