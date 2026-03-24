from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class RuntimeState:
    ws_connected: bool = False
    ws_connecting: bool = False
    ws_last_error: str | None = None
    ws_last_message_at: datetime | None = None
    ws_connected_at: datetime | None = None
    ws_reconnect_count: int = 0

    tracked_pairs_total: int = 0
    tracked_pairs_active: int = 0

    active_streams_count: int = 0
    active_streams: list[str] = field(default_factory=list)

    last_kline_event: dict[str, Any] | None = None
    last_persisted_candle: dict[str, Any] | None = None
    candles_persisted_total: int = 0

    internal_ws_clients: int = 0
    internal_ws_subscriptions: int = 0


runtime_state = RuntimeState()