from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.services.on_demand_tracking_service import OnDemandTrackingService


@pytest.mark.asyncio
async def test_on_demand_activation_identity_includes_basis():
    service = OnDemandTrackingService(
        session_factory=None,  # type: ignore[arg-type]
        stream_manager=None,
    )
    service._ensure_pair_tracked_once = AsyncMock()  # type: ignore[method-assign]

    common = dict(
        exchange="bybit",
        market="futures",
        symbol="BTCUSDT",
        interval="1d",
    )
    await service.ensure_pair_tracked(price_basis="mark", **common)
    await service.ensure_pair_tracked(price_basis="trade", **common)

    assert service._ensure_pair_tracked_once.await_count == 2
    bases = {
        call.kwargs["price_basis"]
        for call in service._ensure_pair_tracked_once.await_args_list
    }
    assert bases == {"mark", "trade"}
