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
    stream_workers: list[dict[str, Any]] = field(default_factory=list)
    exchange_rate_limits: dict[str, dict[str, Any]] = field(default_factory=dict)

    chart_ws_connections_current: int = 0
    chart_ws_connections_total: int = 0
    chart_ws_subscriptions_current: int = 0
    chart_ws_subscribe_total: int = 0
    chart_ws_unsubscribe_total: int = 0
    chart_ws_switch_total: int = 0
    chart_ws_messages_sent_total: int = 0
    chart_ws_dropped_updates_total: int = 0
    chart_ws_errors_total: int = 0
    chart_ws_warmup_total: int = 0
    chart_ws_warmup_failed_total: int = 0

    def chart_ws_metrics(self) -> dict[str, int]:
        return {
            "chart_ws_connections_current": self.chart_ws_connections_current,
            "chart_ws_connections_total": self.chart_ws_connections_total,
            "chart_ws_subscriptions_current": self.chart_ws_subscriptions_current,
            "chart_ws_subscribe_total": self.chart_ws_subscribe_total,
            "chart_ws_unsubscribe_total": self.chart_ws_unsubscribe_total,
            "chart_ws_switch_total": self.chart_ws_switch_total,
            "chart_ws_messages_sent_total": self.chart_ws_messages_sent_total,
            "chart_ws_dropped_updates_total": self.chart_ws_dropped_updates_total,
            "chart_ws_errors_total": self.chart_ws_errors_total,
            "chart_ws_warmup_total": self.chart_ws_warmup_total,
            "chart_ws_warmup_failed_total": self.chart_ws_warmup_failed_total,
        }

    def record_exchange_http_response(
        self,
        *,
        exchange: str,
        status_code: int,
        headers: dict[str, Any] | None = None,
    ) -> None:
        item = self.exchange_rate_limits.setdefault(
            exchange,
            {
                "requests_total": 0,
                "errors_total": 0,
                "rate_limited_total": 0,
                "last_status_code": None,
                "last_error": None,
                "last_request_at": None,
                "limit": None,
                "remaining": None,
                "used_weight": None,
                "reset_at": None,
            },
        )
        item["requests_total"] = int(item["requests_total"]) + 1
        item["last_status_code"] = int(status_code)
        item["last_request_at"] = datetime.utcnow().isoformat()
        if status_code >= 400:
            item["errors_total"] = int(item["errors_total"]) + 1
        if status_code == 429:
            item["rate_limited_total"] = int(item["rate_limited_total"]) + 1

        normalized_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        item["used_weight"] = (
            normalized_headers.get("x-mbx-used-weight-1m")
            or normalized_headers.get("x-mbx-used-weight")
            or item.get("used_weight")
        )
        item["limit"] = (
            normalized_headers.get("x-ratelimit-limit")
            or normalized_headers.get("x-bapi-limit")
            or item.get("limit")
        )
        item["remaining"] = (
            normalized_headers.get("x-ratelimit-remaining")
            or normalized_headers.get("x-bapi-limit-status")
            or item.get("remaining")
        )
        item["reset_at"] = (
            normalized_headers.get("x-ratelimit-reset")
            or normalized_headers.get("x-bapi-limit-reset-timestamp")
            or item.get("reset_at")
        )

    def record_exchange_http_error(self, *, exchange: str, error: str) -> None:
        item = self.exchange_rate_limits.setdefault(
            exchange,
            {
                "requests_total": 0,
                "errors_total": 0,
                "rate_limited_total": 0,
                "last_status_code": None,
                "last_error": None,
                "last_request_at": None,
                "limit": None,
                "remaining": None,
                "used_weight": None,
                "reset_at": None,
            },
        )
        item["errors_total"] = int(item["errors_total"]) + 1
        item["last_error"] = str(error)
        item["last_request_at"] = datetime.utcnow().isoformat()


runtime_state = RuntimeState()
