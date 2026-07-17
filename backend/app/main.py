from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy import text

from .config import settings
from .db import Base, engine, SessionLocal
from .redis_client import init_redis, close_redis
from .api.health import router as health_router
from .api.stats import router as stats_router
from .api.pairs import router as pairs_router
from .api.klines import router as klines_router
from .api.ws import router as ws_router
from .api.refresh_popular_pairs import router as refresh_popular_pairs_router
from .schemas import InternalHealthResponse
from .services.backfill_service import BackfillService
from .services.popular_pairs_service import PopularPairsService
from .services.realtime_service import RealtimeService
from .services.chart_ws_service import ChartWebSocketService
from .services.stream_manager import StreamManager
from .services.on_demand_tracking_service import OnDemandTrackingService
from .state import runtime_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

backfill_service = BackfillService(session_factory=SessionLocal)
realtime_service = RealtimeService()
chart_ws_service = ChartWebSocketService(
    realtime_service=realtime_service,
    session_factory=SessionLocal,
    backfill_service=backfill_service,
)
stream_manager = StreamManager(
    session_factory=SessionLocal,
    backfill_service=backfill_service,
    realtime_service=realtime_service,
)
popular_pairs_service = PopularPairsService(
    session_factory=SessionLocal,
    backfill_service=backfill_service,
    stream_manager=stream_manager,
)
on_demand_tracking_service = OnDemandTrackingService(
    session_factory=SessionLocal,
    stream_manager=stream_manager,
)
chart_ws_service.on_demand_tracking_service = on_demand_tracking_service


async def ensure_schema_upgrades() -> None:
    async with engine.begin() as conn:
        await conn.execute(text("ALTER TABLE tracked_pairs ADD COLUMN IF NOT EXISTS auto_stop_at TIMESTAMP NULL"))
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_tracked_pairs_auto_stop_at ON tracked_pairs (auto_stop_at)")
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_redis()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await ensure_schema_upgrades()
    await backfill_service.repair_all_active_pairs()
    await stream_manager.start()
    await on_demand_tracking_service.start()

    yield

    await on_demand_tracking_service.stop()
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
app.state.chart_ws_service = chart_ws_service
app.state.popular_pairs_service = popular_pairs_service
app.state.on_demand_tracking_service = on_demand_tracking_service


@app.get("/")
async def root():
    return {
        "service": settings.app_name,
        "env": settings.app_env,
        "status": "ok",
    }


@app.exception_handler(SQLAlchemyTimeoutError)
async def sqlalchemy_timeout_handler(request, exc):
    return JSONResponse(
        status_code=503,
        content={"detail": "database connection pool exhausted"},
    )


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
        chart_ws=runtime_state.chart_ws_metrics(),
    )


app.include_router(health_router)
app.include_router(stats_router)
app.include_router(pairs_router)
app.include_router(refresh_popular_pairs_router)
app.include_router(klines_router)
app.include_router(ws_router)
