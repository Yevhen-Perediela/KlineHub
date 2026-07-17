from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db, engine
from ..models import Candle, TrackedPair
from ..redis_client import get_redis
from ..schemas import StatsResponse
from ..services.candle_service import CandleService
from ..state import runtime_state

router = APIRouter(prefix="/internal", tags=["internal"])


@router.get("/stats", response_model=StatsResponse)
async def internal_stats(db: AsyncSession = Depends(get_db)) -> StatsResponse:
    total_q = await db.execute(select(func.count()).select_from(TrackedPair))
    active_q = await db.execute(
        select(func.count()).select_from(TrackedPair).where(TrackedPair.status == "active")
    )
    paused_q = await db.execute(
        select(func.count()).select_from(TrackedPair).where(TrackedPair.status == "paused")
    )

    total = int(total_q.scalar_one() or 0)
    active = int(active_q.scalar_one() or 0)
    paused = int(paused_q.scalar_one() or 0)

    redis_ok = False
    db_ok = False

    try:
        redis = get_redis()
        redis_ok = bool(await redis.ping())
    except Exception:
        redis_ok = False

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False

    return StatsResponse(
        tracked_pairs_total=total,
        tracked_pairs_active=active,
        tracked_pairs_paused=paused,
        redis_ok=redis_ok,
        db_ok=db_ok,
        active_streams_count=runtime_state.active_streams_count,
        ws_connected=runtime_state.ws_connected,
        ws_reconnect_count=runtime_state.ws_reconnect_count,
        candles_persisted_total=runtime_state.candles_persisted_total,
        chart_ws=runtime_state.chart_ws_metrics(),
    )


@router.get("/ops")
async def internal_ops(db: AsyncSession = Depends(get_db)) -> dict:
    result = await db.execute(
        select(TrackedPair).order_by(TrackedPair.updated_at.desc())
    )
    pairs = list(result.scalars().all())

    latest_rows = await db.execute(
        select(
            Candle.exchange,
            Candle.market,
            Candle.symbol,
            Candle.interval,
            func.max(Candle.open_time),
        ).group_by(
            Candle.exchange,
            Candle.market,
            Candle.symbol,
            Candle.interval,
        )
    )
    latest_by_key = {
        (exchange, market, symbol, interval): int(open_time)
        for exchange, market, symbol, interval, open_time in latest_rows.all()
        if open_time is not None
    }

    redis = None
    try:
        redis = get_redis()
    except Exception:
        redis = None

    now_ms = int(datetime.utcnow().timestamp() * 1000)
    freshness = []
    for item in pairs:
        key = (item.exchange, item.market, item.symbol, item.interval)
        last_closed_open_time = latest_by_key.get(key)
        open_cached = False
        last_cached = False
        if redis is not None:
            try:
                open_cached = bool(
                    await redis.exists(
                        CandleService._open_key(item.exchange, item.market, item.symbol, item.interval)
                    )
                )
                last_cached = bool(
                    await redis.exists(
                        CandleService._last_key(item.exchange, item.market, item.symbol, item.interval)
                    )
                )
            except Exception:
                open_cached = False
                last_cached = False

        last_update_ms = last_closed_open_time
        age_sec = None
        if last_update_ms is not None:
            age_sec = max(0, int((now_ms - int(last_update_ms)) / 1000))

        freshness.append(
            {
                "exchange": item.exchange,
                "market": item.market,
                "symbol": item.symbol,
                "interval": item.interval,
                "status": item.status,
                "source": item.source,
                "last_closed_open_time": last_closed_open_time,
                "age_sec": age_sec,
                "open_cached": open_cached,
                "last_cached": last_cached,
                "stale": item.status == "active" and age_sec is not None and age_sec > 15 * 60,
            }
        )

    by_source: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for item in pairs:
        by_source[item.source] = by_source.get(item.source, 0) + 1
        by_status[item.status] = by_status.get(item.status, 0) + 1

    expiring_before = datetime.utcnow() + timedelta(hours=1)
    on_demand_active = [
        item for item in pairs
        if item.source == "on_demand" and item.status == "active"
    ]

    return {
        "websocket_clients": {
            "legacy_market_clients": runtime_state.internal_ws_clients,
            "legacy_market_subscriptions": runtime_state.internal_ws_subscriptions,
            **runtime_state.chart_ws_metrics(),
        },
        "exchange_limits": runtime_state.exchange_rate_limits,
        "tracked_pair_lifecycle": {
            "by_source": by_source,
            "by_status": by_status,
            "on_demand_active": len(on_demand_active),
            "on_demand_expiring_1h": sum(
                1 for item in on_demand_active
                if item.auto_stop_at is not None and item.auto_stop_at <= expiring_before
            ),
            "recent_changes": [
                {
                    "exchange": item.exchange,
                    "market": item.market,
                    "symbol": item.symbol,
                    "interval": item.interval,
                    "status": item.status,
                    "source": item.source,
                    "auto_stop_at": item.auto_stop_at.isoformat() if item.auto_stop_at else None,
                    "updated_at": item.updated_at.isoformat() if item.updated_at else None,
                }
                for item in pairs[:20]
            ],
        },
        "stream_workers": runtime_state.stream_workers,
        "cold_streams": {
            "warmup_total": runtime_state.chart_ws_warmup_total,
            "warmup_failed_total": runtime_state.chart_ws_warmup_failed_total,
            "active_streams_count": runtime_state.active_streams_count,
            "active_streams": runtime_state.active_streams[:200],
        },
        "data_freshness": freshness,
    }
