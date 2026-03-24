from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from sqlalchemy import text

from .config import settings
from .db import Base, engine, SessionLocal
from .redis_client import init_redis, close_redis
from .api.health import router as health_router
from .api.stats import router as stats_router
from .api.pairs import router as pairs_router
from .api.klines import router as klines_router
from .api.ws import router as ws_router
from .schemas import InternalHealthResponse
from .services.backfill_service import BackfillService
from .services.realtime_service import RealtimeService
from .services.stream_manager import StreamManager
from .state import runtime_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

backfill_service = BackfillService(session_factory=SessionLocal)
realtime_service = RealtimeService()
stream_manager = StreamManager(
    session_factory=SessionLocal,
    backfill_service=backfill_service,
    realtime_service=realtime_service,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_redis()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await backfill_service.repair_all_active_pairs()
    await stream_manager.start()

    yield

    await stream_manager.stop()
    await close_redis()
    await engine.dispose()


app = FastAPI(
    title="Market Data Worker",
    version="0.4.0",
    lifespan=lifespan,
)

app.state.stream_manager = stream_manager
app.state.backfill_service = backfill_service
app.state.realtime_service = realtime_service


@app.get("/")
async def root():
    return {
        "service": settings.app_name,
        "env": settings.app_env,
        "status": "ok",
    }


@app.get("/internal/health", response_model=InternalHealthResponse)
async def internal_health():
    redis_status = "down"
    db_status = "down"

    try:
        from .redis_client import get_redis
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
    ws_status = "connected" if runtime_state.ws_connected else "disconnected"

    return InternalHealthResponse(
        status=overall,
        redis=redis_status,
        db=db_status,
        ws=ws_status,
        ws_connected=runtime_state.ws_connected,
        ws_connecting=runtime_state.ws_connecting,
        ws_reconnect_count=runtime_state.ws_reconnect_count,
        active_streams_count=runtime_state.active_streams_count,
        tracked_pairs_total=runtime_state.tracked_pairs_total,
        tracked_pairs_active=runtime_state.tracked_pairs_active,
        candles_persisted_total=runtime_state.candles_persisted_total,
        ws_last_error=runtime_state.ws_last_error,
        ws_last_message_at=runtime_state.ws_last_message_at,
        ws_connected_at=runtime_state.ws_connected_at,
        last_kline_event=runtime_state.last_kline_event,
        last_persisted_candle=runtime_state.last_persisted_candle,
    )


app.include_router(health_router)
app.include_router(stats_router)
app.include_router(pairs_router)
app.include_router(klines_router)
app.include_router(ws_router)