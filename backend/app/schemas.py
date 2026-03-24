from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict


class HealthResponse(BaseModel):
    status: str
    service: str
    redis: str
    db: str


class StatsResponse(BaseModel):
    tracked_pairs_total: int
    tracked_pairs_active: int
    tracked_pairs_paused: int
    redis_ok: bool
    db_ok: bool
    active_streams_count: int
    ws_connected: bool
    ws_reconnect_count: int
    candles_persisted_total: int


class TrackedPairCreate(BaseModel):
    exchange: str = Field(..., examples=["binance"])
    market: str = Field(..., examples=["futures"])
    symbol: str = Field(..., examples=["BTCUSDT"])
    interval: str = Field(default="1h", examples=["1h"])
    source: str = Field(default="api")
    priority: int = Field(default=100, ge=0)
    backfill_limit: int | None = Field(default=None, ge=1, le=1000)


class TrackedPairResponse(BaseModel):
    id: int
    exchange: str
    market: str
    symbol: str
    interval: str
    status: str
    source: str
    priority: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TrackedPairListResponse(BaseModel):
    items: list[TrackedPairResponse]
    count: int


class DeletePairResponse(BaseModel):
    ok: bool
    deleted: bool


class InternalHealthResponse(BaseModel):
    status: str
    redis: str
    db: str
    ws: str
    ws_connected: bool
    ws_connecting: bool
    ws_reconnect_count: int
    active_streams_count: int
    tracked_pairs_total: int
    tracked_pairs_active: int
    candles_persisted_total: int
    ws_last_error: str | None = None
    ws_last_message_at: datetime | None = None
    ws_connected_at: datetime | None = None
    last_kline_event: dict | None = None
    last_persisted_candle: dict | None = None


class KlineBarResponse(BaseModel):
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class KlineHistoryResponse(BaseModel):
    bars: list[KlineBarResponse]
    noData: bool