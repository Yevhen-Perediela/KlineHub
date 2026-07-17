from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "market-data-worker"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    database_url: str = "postgresql+asyncpg://marketdata:marketdata_pass@postgres:5432/marketdata"
    redis_url: str = "redis://redis:6379/0"

    internal_api_token: str | None = None
    coinmarketcap_api_key: str | None = None
    coinmarketcap_api_url: str = "https://pro-api.coinmarketcap.com"
    coinmarketcap_listings_limit: int = 500

    recent_bars_limit: int = 300
    ws_receive_timeout_sec: int = 60
    ws_ping_interval_sec: int = 20
    ws_ping_timeout_sec: int = 20
    ws_reconnect_min_sec: int = 3
    ws_reconnect_max_sec: int = 30
    chart_ws_ping_interval_sec: int = 20
    chart_ws_idle_timeout_sec: int = 60
    chart_ws_max_subscriptions: int = 20
    chart_ws_max_streams_per_request: int = 20
    chart_ws_outbound_queue_size: int = 100
    chart_ws_max_message_bytes: int = 64 * 1024

    binance_futures_rest_url: str = "https://fapi.binance.com"
    bybit_rest_url: str = "https://api.bybit.com"
    bybit_ws_url: str = "wss://stream.bybit.com"
    bybit_instruments_cache_ttl_sec: int = 900
    okx_rest_url: str = "https://openapi.okx.com"
    okx_ws_business_url: str = "wss://ws.okx.com:8443/ws/v5/business"
    okx_instruments_cache_ttl_sec: int = 900
    oanda_rest_url: str = "https://api-fxtrade.oanda.com"
    oanda_stream_url: str = "https://stream-fxtrade.oanda.com"
    oanda_api_token: str | None = None
    oanda_account_id: str | None = None
    oanda_instruments_cache_ttl_sec: int = 900
    oanda_reconcile_interval_sec: int = 20
    default_backfill_limit: int = 300
    max_backfill_limit: int = 1000
    on_demand_tracking_ttl_days: int = 2
    on_demand_tracking_cleanup_interval_sec: int = 300

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
