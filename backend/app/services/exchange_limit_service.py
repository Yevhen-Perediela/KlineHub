from __future__ import annotations

from typing import Any

from ..state import runtime_state


def record_http_response(*, exchange: str, response: Any) -> None:
    runtime_state.record_exchange_http_response(
        exchange=exchange,
        status_code=int(getattr(response, "status_code", 0) or 0),
        headers=dict(getattr(response, "headers", {}) or {}),
    )


def record_http_error(*, exchange: str, error: Exception) -> None:
    runtime_state.record_exchange_http_error(exchange=exchange, error=str(error))
