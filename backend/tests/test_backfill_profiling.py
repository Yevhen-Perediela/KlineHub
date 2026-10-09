import json
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services import backfill_service as backfill


ARGS = dict(exchange='bybit', market='futures', symbol='BTCUSDT', interval='1m', price_basis='mark')


@pytest.fixture
def setup_backfill(monkeypatch):
    monkeypatch.setattr(backfill.settings, 'max_backfill_limit', 2)
    monkeypatch.setattr(backfill.time, 'time', lambda: 600)
    operations = []
    existing = [60_000]
    async def execute(stmt):
        operations.append(('lookup', stmt.compile().params))
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: existing))
    async def rollback():
        operations.append(('rollback',))
    db = SimpleNamespace(execute=execute, rollback=rollback)
    @asynccontextmanager
    async def factory():
        yield SimpleNamespace(commit=AsyncMock())
    async def fetch(**kwargs):
        operations.append(('rest', kwargs))
        return [dict(open_time=t, is_closed=True, price_basis=kwargs['price_basis'])
                for t in range(kwargs['start_time'], kwargs['end_time'] + 1, 60_000)]
    adapter = SimpleNamespace(fetch_klines=AsyncMock(side_effect=fetch))
    monkeypatch.setattr(backfill, 'get_adapter', lambda **kwargs: adapter)
    async def save(**kwargs):
        operations.append(('upsert_commit', [e['open_time'] for e in kwargs['events']]))
        await kwargs['db'].commit()
    async def cache(**kwargs):
        operations.append(('redis', [e['open_time'] for e in kwargs['events']]))
    monkeypatch.setattr(backfill.CandleService, 'upsert_closed_candles', AsyncMock(side_effect=save))
    monkeypatch.setattr(backfill.CandleService, 'write_many_closed_candles_to_redis', AsyncMock(side_effect=cache))
    return backfill.BackfillService(factory), db, adapter, existing, operations


def summaries(caplog):
    return [json.loads(r.getMessage()) for r in caplog.records
            if r.name == backfill.__name__ and r.msg == '%s']


@pytest.mark.asyncio
@pytest.mark.parametrize('profiling', [False, True])
async def test_backfill_profiling_preserves_ranges_pages_and_writes(monkeypatch, caplog, setup_backfill, profiling):
    monkeypatch.setattr(backfill.settings, 'backfill_profiling_enabled', profiling)
    caplog.set_level(logging.INFO, logger=backfill.__name__)
    service, db, adapter, existing, operations = setup_backfill
    assert await service.ensure_range_loaded(db=db, from_ts=0, to_ts=240_000, **ARGS) == 4
    requests = [args for name, *rest in operations if name == 'rest' for args in rest]
    assert [(r['start_time'], r['end_time'], r['limit']) for r in requests] == [
        (0, 59_999, 1), (120_000, 239_999, 2), (240_000, 299_999, 1)]
    assert all(r['price_basis'] == 'mark' for r in requests)
    assert [op[0] for op in operations] == ['lookup', 'rollback',
        'rest', 'upsert_commit', 'redis', 'rest', 'upsert_commit', 'redis', 'rest', 'upsert_commit', 'redis']
    assert [op[1] for op in operations if op[0] == 'upsert_commit'] == [[0], [120_000, 180_000], [240_000]]
    logs = summaries(caplog)
    if not profiling:
        assert not logs
        return
    assert len(logs) == 4
    assert all(log['outcome'] == 'ok' and log['total_ms'] >= 0 for log in logs)
    detection = next(log for log in logs if log['operation'] == '_find_missing_ranges')
    assert set(detection['stages_ms']) == {'database_lookup', 'materialize_open_times', 'compute_missing_ranges'}
    ranges = [log for log in logs if log['operation'] == '_fetch_and_store_range']
    assert sum(log['stage_calls']['rest_fetch'] for log in ranges) == 3
    assert all(set(log['stages_ms']) == {'chunk_planning', 'rest_fetch', 'database_upsert_commit', 'redis_write', 'advance_cursor'} for log in ranges)
    top = next(log for log in logs if log['operation'] == 'ensure_range_loaded')
    assert 'missing_range_detection' in top['stages_ms'] and 'fetch_store_ranges' in top['stages_ms']


@pytest.mark.asyncio
async def test_complete_history_does_not_fetch_or_rollback(monkeypatch, caplog, setup_backfill):
    monkeypatch.setattr(backfill.settings, 'backfill_profiling_enabled', True)
    caplog.set_level(logging.INFO, logger=backfill.__name__)
    service, db, adapter, existing, operations = setup_backfill
    existing[:] = [0, 60_000, 120_000, 180_000, 240_000]
    assert await service.ensure_range_loaded(db=db, from_ts=0, to_ts=240_000, **ARGS) == 0
    adapter.fetch_klines.assert_not_awaited()
    assert [op[0] for op in operations] == ['lookup']
    assert len(summaries(caplog)) == 2


@pytest.mark.asyncio
async def test_profiling_preserves_rest_exception_and_never_logs_its_contents(monkeypatch, caplog, setup_backfill):
    monkeypatch.setattr(backfill.settings, 'backfill_profiling_enabled', True)
    caplog.set_level(logging.INFO, logger=backfill.__name__)
    service, db, adapter, existing, operations = setup_backfill
    error = ValueError('private credential placeholder')
    adapter.fetch_klines.side_effect = error
    with pytest.raises(ValueError) as raised:
        await service.ensure_range_loaded(db=db, from_ts=0, to_ts=240_000, **ARGS)
    assert raised.value is error
    assert [op[0] for op in operations] == ['lookup', 'rollback']
    logs = summaries(caplog)
    assert [log['outcome'] for log in logs] == ['ok', 'ValueError', 'ValueError']
    assert 'private credential placeholder' not in json.dumps(logs)
