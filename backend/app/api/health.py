from fastapi import APIRouter
from sqlalchemy import text

from ..db import engine
from ..redis_client import get_redis
from ..schemas import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    redis_status = "down"
    db_status = "down"

    try:
        redis = get_redis()
        pong = await redis.ping()
        redis_status = "ok" if pong else "down"
    except Exception:
        redis_status = "down"

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception:
        db_status = "down"

    overall = "ok" if redis_status == "ok" and db_status == "ok" else "degraded"

    return HealthResponse(
        status=overall,
        service="market-data-worker-api",
        redis=redis_status,
        db=db_status,
    )