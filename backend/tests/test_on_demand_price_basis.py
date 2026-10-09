from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.config import settings
from app.models import TrackedPair
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


class TrackingSession:
    def __init__(self, item=None, fail_commit=False):
        self.item = item
        self.fail_commit = fail_commit
        self.commits = 0
        self.lookups = 0
        self.closed = False

    async def execute(self, statement):
        self.lookups += 1
        await asyncio.sleep(0)
        return SimpleNamespace(scalar_one_or_none=lambda: self.item)

    def add(self, item):
        self.item = item

    async def commit(self):
        if self.fail_commit:
            raise RuntimeError('commit failed')
        self.commits += 1

    @asynccontextmanager
    async def factory(self):
        self.closed = False
        try:
            yield self
        finally:
            self.closed = True


TRACKING_ARGS = dict(exchange='bybit', market='futures', symbol='BTCUSDT', interval='1d', price_basis='trade')


@pytest.mark.asyncio
@pytest.mark.parametrize('status', [None, 'paused', 'active'])
async def test_tracking_commit_does_not_wait_for_slow_reload(status):
    item = TrackedPair(**TRACKING_ARGS, status=status, source='api') if status else None
    session = TrackingSession(item)
    entered, release = asyncio.Event(), asyncio.Event()
    async def reload():
        assert session.commits == 1 and session.closed
        entered.set()
        await release.wait()
    manager = SimpleNamespace(reload=AsyncMock(side_effect=reload))
    service = OnDemandTrackingService(session_factory=session.factory, stream_manager=manager)
    try:
        await asyncio.wait_for(service.ensure_pair_tracked(**TRACKING_ARGS), timeout=0.5)
        assert session.item.status == 'active'
        if status != 'active':
            await asyncio.wait_for(entered.wait(), timeout=0.5)
            assert not service._reload_task.done()
            assert session.item.source == 'on_demand'
            assert session.item.auto_stop_at is not None
        else:
            manager.reload.assert_not_awaited()
            assert session.item.source == 'api'
            assert session.item.auto_stop_at is None
    finally:
        release.set()
        await service.stop()


@pytest.mark.asyncio
async def test_concurrent_tracking_shares_activation_and_failed_commit_never_reloads():
    session = TrackingSession()
    manager = SimpleNamespace(reload=AsyncMock())
    service = OnDemandTrackingService(session_factory=session.factory, stream_manager=manager)
    try:
        await asyncio.gather(*[service.ensure_pair_tracked(**TRACKING_ARGS) for _ in range(20)])
        await service._reload_task
        assert session.lookups == session.commits == 1
        manager.reload.assert_awaited_once()
        assert not service._activation_tasks
        session.fail_commit = True
        with pytest.raises(RuntimeError, match='commit failed'):
            await service.ensure_pair_tracked(**{**TRACKING_ARGS, 'symbol': 'ETHUSDT'})
        manager.reload.assert_awaited_once()
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_reload_requests_coalesce_and_shutdown_cancels_background_work():
    entered, release = asyncio.Event(), asyncio.Event()
    async def reload():
        entered.set()
        await release.wait()
    manager = SimpleNamespace(reload=AsyncMock(side_effect=reload))
    service = OnDemandTrackingService(session_factory=None, stream_manager=manager)
    service._request_reload()
    await entered.wait()
    for _ in range(20):
        service._request_reload()
    task = service._reload_task
    release.set()
    await asyncio.wait_for(task, timeout=0.5)
    assert manager.reload.await_count == 2
    release.clear()
    entered.clear()
    service._request_reload()
    await entered.wait()
    task = service._reload_task
    await service.stop()
    assert task.cancelled()
    assert service._reload_task is None
    service._request_reload()
    assert service._reload_task is None


@pytest.mark.asyncio
async def test_background_reload_failure_retries(monkeypatch):
    monkeypatch.setattr(settings, 'ws_reconnect_min_sec', 0.001)
    manager = SimpleNamespace(reload=AsyncMock(side_effect=[RuntimeError('reload failed'), None]))
    service = OnDemandTrackingService(session_factory=None, stream_manager=manager)
    service._request_reload()
    try:
        await asyncio.wait_for(service._reload_task, timeout=0.5)
        assert manager.reload.await_count == 2
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_disconnected_http_waiter_does_not_cancel_shared_activation():
    entered, release = asyncio.Event(), asyncio.Event()
    async def activate(**kwargs):
        entered.set()
        await release.wait()
    service = OnDemandTrackingService(session_factory=None, stream_manager=None)
    service._ensure_pair_tracked_once = AsyncMock(side_effect=activate)
    first = asyncio.create_task(service.ensure_pair_tracked(**TRACKING_ARGS))
    await entered.wait()
    second = asyncio.create_task(service.ensure_pair_tracked(**TRACKING_ARGS))
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    await asyncio.wait_for(second, timeout=0.5)
    service._ensure_pair_tracked_once.assert_awaited_once()
    assert not service._activation_tasks
    await service.stop()
