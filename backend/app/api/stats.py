from fastapi import APIRouter, Depends
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_db, engine
from ..models import TrackedPair
from ..redis_client import get_redis
from ..schemas import StatsResponse
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
