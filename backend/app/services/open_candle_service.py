from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..exchanges.registry import get_adapter
from .exchange_limit_service import record_http_error, record_http_response
from ..models import Candle
from ..utils.intervals import (
    floor_to_interval_open,
    latest_closed_open_time,
    next_interval_open,
)
from .candle_service import CandleService

logger = logging.getLogger(__name__)


class OpenCandleService:
    @classmethod
    async def get_open_bar(
        cls,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str,
        current_open_ts: int,
        now_ms: int,
    ) -> dict[str, float | int] | None:
        # Legacy Bybit futures MARK opens remain REST-backed. TRADE opens may
        # use the native traded-price kline stream cache.
        if not (exchange == "bybit" and market == "futures" and price_basis == "mark"):
            open_bar = await cls._get_exact_redis_open_bar(
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                price_basis=price_basis,
                current_open_ts=current_open_ts,
            )
            if open_bar is not None:
                return open_bar

        if exchange == "oanda" and market in {"forex", "metals", "stocks"}:
            open_bar = await cls._get_oanda_aggregated_open_bar(
                db=db,
                exchange=exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                price_basis=price_basis,
                current_open_ts=current_open_ts,
                now_ms=now_ms,
            )
            if open_bar is not None:
                return open_bar

        return await cls._get_rest_open_bar(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            price_basis=price_basis,
            current_open_ts=current_open_ts,
            now_ms=now_ms,
        )

    @staticmethod
    async def _get_exact_redis_open_bar(
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str,
        current_open_ts: int,
    ) -> dict[str, float | int] | None:
        open_bar = await CandleService.get_open_candle(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval=interval,
            price_basis=price_basis,
        )
        if open_bar is None or int(open_bar["time"]) != current_open_ts:
            return None
        return open_bar

    @classmethod
    async def _get_oanda_aggregated_open_bar(
        cls,
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str,
        current_open_ts: int,
        now_ms: int,
    ) -> dict[str, float | int] | None:
        if interval == "1m":
            return None

        latest_closed_1m = latest_closed_open_time(now_ms=now_ms, interval="1m")
        one_minute_bars = await cls._get_closed_source_bars(
            db=db,
            exchange=exchange,
            market=market,
            symbol=symbol,
            source_interval="1m",
            price_basis=price_basis,
            from_ts=current_open_ts,
            to_ts=latest_closed_1m,
        )

        current_1m_open = floor_to_interval_open(now_ms, "1m")
        redis_1m_open = await cls._get_exact_redis_open_bar(
            exchange=exchange,
            market=market,
            symbol=symbol,
            interval="1m",
            price_basis=price_basis,
            current_open_ts=current_1m_open,
        )
        if redis_1m_open is not None and redis_1m_open["time"] >= current_open_ts:
            one_minute_bars = [
                bar for bar in one_minute_bars if int(bar["time"]) != int(redis_1m_open["time"])
            ]
            one_minute_bars.append(redis_1m_open)

        if not one_minute_bars:
            return None

        return cls._aggregate_open_bars(one_minute_bars, current_open_ts=current_open_ts)

    @staticmethod
    async def _get_closed_source_bars(
        *,
        db: AsyncSession,
        exchange: str,
        market: str,
        symbol: str,
        source_interval: str,
        price_basis: str,
        from_ts: int,
        to_ts: int,
    ) -> list[dict[str, float | int]]:
        if from_ts > to_ts:
            return []

        result = await db.execute(
            select(Candle)
            .where(
                Candle.exchange == exchange,
                Candle.market == market,
                Candle.symbol == symbol.upper(),
                Candle.interval == source_interval,
                Candle.price_basis == price_basis,
                Candle.open_time >= from_ts,
                Candle.open_time <= to_ts,
                Candle.is_closed.is_(True),
            )
            .order_by(Candle.open_time.asc())
        )
        return [
            {
                "time": int(item.open_time),
                "open": float(item.open),
                "high": float(item.high),
                "low": float(item.low),
                "close": float(item.close),
                "volume": float(item.volume),
            }
            for item in result.scalars().all()
        ]

    @staticmethod
    def _aggregate_open_bars(
        bars: list[dict[str, float | int]],
        *,
        current_open_ts: int,
    ) -> dict[str, float | int]:
        ordered = sorted(bars, key=lambda bar: int(bar["time"]))
        return {
            "time": current_open_ts,
            "open": float(ordered[0]["open"]),
            "high": float(max(Decimal(str(bar["high"])) for bar in ordered)),
            "low": float(min(Decimal(str(bar["low"])) for bar in ordered)),
            "close": float(ordered[-1]["close"]),
            "volume": float(sum(Decimal(str(bar["volume"])) for bar in ordered)),
        }

    @classmethod
    async def _get_rest_open_bar(
        cls,
        *,
        exchange: str,
        market: str,
        symbol: str,
        interval: str,
        price_basis: str,
        current_open_ts: int,
        now_ms: int,
    ) -> dict[str, float | int] | None:
        try:
            if exchange == "oanda" and market in {"forex", "metals", "stocks"} and interval == "3d":
                return await cls._get_oanda_rest_aggregated_open_bar(
                    market=market,
                    symbol=symbol,
                    price_basis=price_basis,
                    current_open_ts=current_open_ts,
                    now_ms=now_ms,
                )
            if exchange == "binance" and market == "spot":
                events = await cls._fetch_binance_spot_current(
                    symbol=symbol,
                    interval=interval,
                    current_open_ts=current_open_ts,
                )
            elif exchange == "binance" and market == "futures":
                events = await cls._fetch_binance_futures_current(
                    symbol=symbol,
                    interval=interval,
                    current_open_ts=current_open_ts,
                )
            elif exchange == "bybit" and interval == "3d":
                return await cls._get_rest_aggregated_open_bar(
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    source_interval="1d",
                    price_basis=price_basis,
                    target_interval=interval,
                    current_open_ts=current_open_ts,
                    now_ms=now_ms,
                )
            else:
                adapter = get_adapter(exchange=exchange, market=market)
                events = await adapter.fetch_klines(
                    market=market,
                    symbol=symbol,
                    interval=interval,
                    price_basis=price_basis,
                    start_time=current_open_ts,
                    end_time=min(now_ms, next_interval_open(current_open_ts, interval) - 1),
                    limit=2,
                )
        except Exception:
            logger.exception(
                "Failed to fetch REST open candle for %s %s %s %s",
                exchange,
                market,
                symbol,
                interval,
            )
            return None

        open_event = cls._find_current_open_event(events, current_open_ts=current_open_ts)
        if open_event is None:
            return None

        return cls._event_to_bar(open_event)

    @classmethod
    async def _get_oanda_rest_aggregated_open_bar(
        cls,
        *,
        market: str,
        symbol: str,
        price_basis: str,
        current_open_ts: int,
        now_ms: int,
    ) -> dict[str, float | int] | None:
        adapter = get_adapter(exchange="oanda", market=market)
        events = await adapter.fetch_klines(
            market=market,
            symbol=symbol,
            interval="1m",
            price_basis=price_basis,
            start_time=current_open_ts,
            end_time=min(now_ms, next_interval_open(current_open_ts, "3d") - 1),
            limit=None,
        )
        bars = [
            bar
            for event in events
            if (bar := cls._event_to_bar(event)) is not None and int(bar["time"]) >= current_open_ts
        ]
        if not bars:
            return None
        return cls._aggregate_open_bars(bars, current_open_ts=current_open_ts)

    @classmethod
    async def _get_rest_aggregated_open_bar(
        cls,
        *,
        exchange: str,
        market: str,
        symbol: str,
        source_interval: str,
        price_basis: str,
        target_interval: str,
        current_open_ts: int,
        now_ms: int,
    ) -> dict[str, float | int] | None:
        adapter = get_adapter(exchange=exchange, market=market)
        events = await adapter.fetch_klines(
            market=market,
            symbol=symbol,
            interval=source_interval,
            price_basis=price_basis,
            start_time=current_open_ts,
            end_time=min(now_ms, next_interval_open(current_open_ts, target_interval) - 1),
            limit=1000,
        )
        bars = [
            bar
            for event in events
            if (bar := cls._event_to_bar(event)) is not None and int(bar["time"]) >= current_open_ts
        ]
        if not bars:
            return None
        return cls._aggregate_open_bars(bars, current_open_ts=current_open_ts)

    @staticmethod
    async def _fetch_binance_spot_current(
        *,
        symbol: str,
        interval: str,
        current_open_ts: int,
    ) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    "https://api.binance.com/api/v3/klines",
                    params={
                        "symbol": symbol.upper(),
                        "interval": interval,
                        "startTime": current_open_ts,
                        "limit": 1,
                    },
                )
                record_http_response(exchange="binance", response=response)
            except Exception as exc:
                record_http_error(exchange="binance", error=exc)
                raise
        response.raise_for_status()
        return OpenCandleService._binance_rows_to_events(response.json())

    @staticmethod
    async def _fetch_binance_futures_current(
        *,
        symbol: str,
        interval: str,
        current_open_ts: int,
    ) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{settings.binance_futures_rest_url}/fapi/v1/klines",
                    params={
                        "symbol": symbol.upper(),
                        "interval": interval,
                        "startTime": current_open_ts,
                        "limit": 1,
                    },
                )
                record_http_response(exchange="binance", response=response)
            except Exception as exc:
                record_http_error(exchange="binance", error=exc)
                raise
        response.raise_for_status()
        return OpenCandleService._binance_rows_to_events(response.json())

    @staticmethod
    def _binance_rows_to_events(rows: Any) -> list[dict[str, Any]]:
        if not isinstance(rows, list):
            return []

        events: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 7:
                continue
            events.append(
                {
                    "open_time": int(row[0]),
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                    "volume": row[5],
                    "close_time": int(row[6]),
                    "is_closed": False,
                }
            )
        return events

    @staticmethod
    def _find_current_open_event(
        events: list[dict[str, Any]],
        *,
        current_open_ts: int,
    ) -> dict[str, Any] | None:
        for event in events:
            try:
                if int(event["open_time"]) == current_open_ts:
                    return event
            except (KeyError, TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _event_to_bar(event: dict[str, Any]) -> dict[str, float | int] | None:
        try:
            return {
                "time": int(event["open_time"]),
                "open": float(event["open"]),
                "high": float(event["high"]),
                "low": float(event["low"]),
                "close": float(event["close"]),
                "volume": float(event["volume"]),
            }
        except (KeyError, TypeError, ValueError):
            return None
