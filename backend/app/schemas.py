from datetime import datetime
from typing import Literal

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
    exchange: str = Field(..., examples=["binance", "bybit", "okx", "oanda"])
    market: str = Field(..., examples=["futures", "spot", "forex"])
    symbol: str = Field(..., examples=["BTCUSDT", "BTC-USDT-SWAP"])
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
    auto_stop_at: datetime | None = None
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


class RefreshPopularPairsExchangeConfig(BaseModel):
    quotes: list[str] = Field(default_factory=lambda: ["USDT", "USDC"])


class RefreshPopularPairsBinanceConfig(RefreshPopularPairsExchangeConfig):
    spot_base_limit: int = Field(default=150, ge=1, le=1000)
    futures_base_limit: int = Field(default=150, ge=1, le=1000)


class RefreshPopularPairsBybitConfig(RefreshPopularPairsExchangeConfig):
    spot_base_limit: int = Field(default=100, ge=1, le=1000)
    futures_base_limit: int = Field(default=100, ge=1, le=1000)


class RefreshPopularPairsOkxConfig(RefreshPopularPairsExchangeConfig):
    spot_base_limit: int = Field(default=100, ge=1, le=1000)
    futures_base_limit: int = Field(default=100, ge=1, le=1000)


class RefreshPopularPairsOandaConfig(BaseModel):
    enable_forex: bool = True
    enable_metals: bool = True
    forex_symbols: list[str] = Field(
        default_factory=lambda: [
            "EUR_USD",
            "GBP_USD",
            "USD_JPY",
            "AUD_USD",
            "USD_CAD",
            "USD_CHF",
            "NZD_USD",
            "EUR_CHF",
            "EUR_CAD",
            "EUR_AUD",
            "EUR_NZD",
            "EUR_JPY",
            "GBP_JPY",
            "EUR_GBP",
            "GBP_CHF",
            "GBP_CAD",
            "GBP_AUD",
            "AUD_JPY",
            "AUD_CAD",
            "AUD_CHF",
            "CAD_JPY",
            "CAD_CHF",
            "CHF_JPY",
            "NZD_JPY",
            "USD_SEK",
            "USD_NOK",
            "USD_SGD",
            "EUR_SEK",
            "EUR_NOK",
        ]
    )
    metals_symbols: list[str] = Field(default_factory=lambda: ["XAU_USD", "XAG_USD"])


class RefreshPopularPairsRequest(BaseModel):
    dry_run: bool = False
    mode: Literal["pause", "delete"] = "pause"
    crypto_interval: str = "1h"
    oanda_interval: str = "1m"
    binance: RefreshPopularPairsBinanceConfig = Field(default_factory=RefreshPopularPairsBinanceConfig)
    bybit: RefreshPopularPairsBybitConfig = Field(default_factory=RefreshPopularPairsBybitConfig)
    okx: RefreshPopularPairsOkxConfig = Field(default_factory=RefreshPopularPairsOkxConfig)
    oanda: RefreshPopularPairsOandaConfig = Field(default_factory=RefreshPopularPairsOandaConfig)


class RefreshPopularPairsGroupSummary(BaseModel):
    desired: int = 0
    added: int = 0
    resumed: int = 0
    paused: int = 0
    deleted: int = 0
    unchanged: int = 0


class RefreshPopularPairsItem(BaseModel):
    exchange: str
    market: str
    symbol: str
    interval: str
    action: str
    reason: str | None = None
    status: str | None = None
    error: str | None = None
    group: str | None = None


class RefreshPopularPairsSummary(BaseModel):
    desired_total: int
    current_total: int
    to_add: int
    to_resume: int
    to_pause: int
    to_delete: int
    unchanged: int
    failed: int
    reload_triggered: bool


class RefreshPopularPairsResponse(BaseModel):
    ok: bool
    dry_run: bool
    mode: Literal["pause", "delete"]
    summary: RefreshPopularPairsSummary
    groups: dict[str, RefreshPopularPairsGroupSummary]
    added: list[RefreshPopularPairsItem]
    resumed: list[RefreshPopularPairsItem]
    paused: list[RefreshPopularPairsItem]
    deleted: list[RefreshPopularPairsItem]
    unchanged: list[RefreshPopularPairsItem]
    failed_items: list[RefreshPopularPairsItem]
