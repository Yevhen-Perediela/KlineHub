from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "market-data-worker"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    database_url: str = "postgresql+asyncpg://marketdata:marketdata_pass@postgres:5432/marketdata"
    redis_url: str = "redis://redis:6379/0"

    internal_api_token: str | None = None

    recent_bars_limit: int = 300
    ws_receive_timeout_sec: int = 60
    ws_ping_interval_sec: int = 20
    ws_ping_timeout_sec: int = 20
    ws_reconnect_min_sec: int = 3
    ws_reconnect_max_sec: int = 30

    binance_futures_rest_url: str = "https://fapi.binance.com"
    default_backfill_limit: int = 300
    max_backfill_limit: int = 1000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()