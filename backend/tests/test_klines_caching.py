import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError

from app.services import aggregation_service as aggregation
from app.services import open_candle_service as opens
from app import redis_client


class FakeRedis:
    def __init__(self):
        self.now = 0.0
        self.values = {}
        self.expirations = {}
        self.fail = False

    async def get(self, key):
        if self.fail:
            raise ConnectionError('test unavailable')
        if self.expirations.get(key, float('inf')) <= self.now:
            self.values.pop(key, None)
        return self.values.get(key)

    async def set(self, key, value, *, ex=None, px=None):
        if self.fail:
            raise ConnectionError('test unavailable')
        self.values[key] = value
        self.expirations[key] = self.now + (ex if ex is not None else px / 1000)


class FakeDb:
    def __init__(self, intervals):
        self.intervals = intervals
        self.calls = 0
        self.in_transaction = False

    async def execute(self, statement):
        self.calls += 1
        self.in_transaction = True
        await asyncio.sleep(0)
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: list(self.intervals)))

    async def rollback(self):
        self.in_transaction = False



@pytest.fixture
def cache(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(aggregation, 'get_redis', lambda: redis)
    monkeypatch.setattr(opens, 'get_redis', lambda: redis)
    monkeypatch.setattr(aggregation.settings, 'available_intervals_cache_ttl_sec', 60)
    monkeypatch.setattr(opens.settings, 'bybit_mark_open_cache_ttl_sec', 2)
    return redis


INTERVAL_ARGS = dict(exchange='bybit', market='futures', symbol='BTCUSDT', price_basis='mark')
OPEN_ARGS = dict(**INTERVAL_ARGS, interval='1m', current_open_ts=60_000, now_ms=61_000)
BAR = dict(time=60_000, open=100.0, high=102.0, low=99.0, close=101.0, volume=0.0)


@pytest.mark.asyncio
async def test_interval_hit_miss_sorting_and_discovery(cache):
    db = FakeDb(['1h', None, 'invalid', '1m', '1d'])
    first = await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS)
    assert first == ['1m', '1h', '1d']
    assert not db.in_transaction
    db.intervals.append('5m')
    assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == first
    assert db.calls == 1
    cache.now = 60
    assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == ['1m', '5m', '1h', '1d']
    assert db.calls == 2


@pytest.mark.asyncio
async def test_empty_interval_cache_has_short_ttl(cache):
    db = FakeDb([])
    assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == []
    db.intervals.append('1m')
    assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == []
    assert db.calls == 1
    cache.now = 5
    assert await aggregation.AggregationService.pick_best_source_interval(db=db, target_interval='1h', **INTERVAL_ARGS) == '1m'
    assert db.calls == 2


@pytest.mark.asyncio
async def test_interval_concurrency(cache):
    dbs = [FakeDb(['1m']) for _ in range(20)]
    results = await asyncio.gather(*[
        aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) for db in dbs])
    assert results == [['1m']] * 20
    assert sum(db.calls for db in dbs) == 1
    assert len(redis_client._cache_locks) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [('exchange', 'binance'), ('market', 'spot'),
                                        ('symbol', 'ETHUSDT'), ('price_basis', 'trade')])
async def test_interval_identity_isolation(cache, field, value):
    db = FakeDb(['1m'])
    await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS)
    db.intervals = ['1h']
    assert await aggregation.AggregationService.get_available_intervals(db=db, **{**INTERVAL_ARGS, field: value}) == ['1h']
    assert db.calls == 2


@pytest.mark.asyncio
async def test_corrupt_interval_entry_and_case_normalization(cache):
    db = FakeDb(['1h'])
    key = aggregation.AggregationService._interval_cache_key(**INTERVAL_ARGS)
    await cache.set(key, json.dumps(['1d', 'bogus', '1m']), ex=60)
    assert await aggregation.AggregationService.get_available_intervals(db=db, **{**INTERVAL_ARGS, 'symbol': 'btcusdt'}) == ['1m', '1d']
    assert db.calls == 0
    cache.values[key] = '{invalid'
    assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == ['1h']


@pytest.mark.asyncio
async def test_interval_redis_failure_falls_back(cache):
    cache.fail = True
    db = FakeDb(['1h'])
    assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == ['1h']
    assert not db.in_transaction


@pytest.fixture
def mark_rest(monkeypatch, cache):
    monkeypatch.setattr(opens.time, 'time_ns', lambda: int((61 + cache.now) * 1_000_000_000))
    async def fetch_bar(**kwargs):
        await asyncio.sleep(0)
        return BAR.copy()
    fetch = AsyncMock(side_effect=fetch_bar)
    monkeypatch.setattr(opens.OpenCandleService, '_get_rest_open_bar', fetch)
    return fetch


@pytest.mark.asyncio
async def test_mark_open_hit_expiry_and_concurrency(cache, mark_rest):
    results = await asyncio.gather(*[
        opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS) for _ in range(20)])
    assert results == [BAR] * 20
    assert mark_rest.await_count == 1
    cache.now = 1.999
    assert await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS) == BAR
    assert mark_rest.await_count == 1
    cache.now = 2.001
    await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS)
    assert mark_rest.await_count == 2
    assert len(redis_client._cache_locks) == 0


@pytest.mark.asyncio
async def test_open_rollover_and_no_reuse_after_interval_end(cache, mark_rest):
    cache.now = 58
    await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS)
    cache.now = 59
    mark_rest.side_effect = None
    mark_rest.return_value = {**BAR, 'time': 120_000}
    result = await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **{**OPEN_ARGS, 'current_open_ts': 120_000, 'now_ms': 120_000})
    assert result['time'] == 120_000
    assert mark_rest.await_count == 2
    await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS)
    assert mark_rest.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', [None, {**BAR, 'time': 0}, {**BAR, 'close': float('nan')},
                                 {**BAR, 'close': 'invalid'}, {'time': 60_000}])
async def test_no_cache_for_empty_or_malformed_rest(cache, mark_rest, bad):
    mark_rest.side_effect = None
    mark_rest.return_value = bad
    for _ in range(2):
        await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS)
    assert not cache.values
    assert mark_rest.await_count == 2


@pytest.mark.asyncio
async def test_rest_latency_counts_against_staleness(cache, mark_rest):
    async def slow(**kwargs):
        cache.now += 1.5
        return BAR.copy()
    mark_rest.side_effect = slow
    await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS)
    key = opens.OpenCandleService._mark_open_cache_key(**INTERVAL_ARGS, interval='1m')
    assert cache.expirations[key] == 2
    cache.now = 2
    await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS)
    assert mark_rest.await_count == 2


@pytest.mark.asyncio
async def test_mark_cache_does_not_change_trade_path(cache, mark_rest, monkeypatch):
    await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS)
    trade = {**BAR, 'close': 99.5}
    native = AsyncMock(return_value=trade)
    monkeypatch.setattr(opens.OpenCandleService, '_get_exact_redis_open_bar', native)
    assert await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **{**OPEN_ARGS, 'price_basis': 'trade'}) == trade
    native.assert_awaited_once()
    assert mark_rest.await_count == 1


@pytest.mark.asyncio
async def test_mark_redis_failure(cache, mark_rest):
    cache.fail = True
    assert await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS) == BAR
    assert mark_rest.await_count == 1


@pytest.mark.asyncio
async def test_rest_error_is_not_cached(cache, monkeypatch):
    monkeypatch.setattr(opens.time, 'time_ns', lambda: 61_000_000_000)
    adapter = SimpleNamespace(fetch_klines=AsyncMock(side_effect=RuntimeError('REST failure')))
    monkeypatch.setattr(opens, 'get_adapter', lambda **kwargs: adapter)
    for _ in range(2):
        assert await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS) is None
    assert adapter.fetch_klines.await_count == 2
    assert not cache.values


@pytest.mark.asyncio
async def test_corrupt_open_cache_and_mismatched_timestamp(cache, mark_rest):
    key = opens.OpenCandleService._mark_open_cache_key(**INTERVAL_ARGS, interval='1m')
    for raw in ('bad json', json.dumps({'bar': {**BAR, 'time': 0}, 'expires_at_ms': 63_000}),
                json.dumps({'bar': BAR, 'expires_at_ms': 60_999})):
        await cache.set(key, raw, ex=2)
        assert await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS) == BAR
    assert mark_rest.await_count == 3


@pytest.mark.asyncio
async def test_cache_write_failure_does_not_lose_result(cache, mark_rest):
    async def fail_set(*args, **kwargs):
        raise ConnectionError('test write unavailable')
    cache.set = fail_set
    assert await aggregation.AggregationService.get_available_intervals(db=FakeDb(['1h']), **INTERVAL_ARGS) == ['1h']
    assert await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS) == BAR


@pytest.mark.asyncio
async def test_disabled_caches_skip_redis(cache, mark_rest, monkeypatch):
    monkeypatch.setattr(aggregation.settings, 'available_intervals_cache_ttl_sec', 0)
    monkeypatch.setattr(opens.settings, 'bybit_mark_open_cache_ttl_sec', 0)
    def unexpected_redis():
        raise AssertionError('disabled caches must not call Redis')
    monkeypatch.setattr(aggregation, 'get_redis', unexpected_redis)
    monkeypatch.setattr(opens, 'get_redis', unexpected_redis)
    db = FakeDb(['1h'])
    for _ in range(2):
        assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == ['1h']
        assert await opens.OpenCandleService.get_open_bar(db=FakeDb([]), **OPEN_ARGS) == BAR
    assert db.calls == mark_rest.await_count == 2


@pytest.mark.asyncio
async def test_cancelled_interval_fill_releases_lock(cache):
    entered = asyncio.Event()
    db = FakeDb(['1m'])
    original_execute = db.execute
    async def wait_forever(statement):
        entered.set()
        await asyncio.Event().wait()
    db.execute = wait_forever
    task = asyncio.create_task(aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    db.execute = original_execute
    assert await asyncio.wait_for(
        aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS), timeout=1) == ['1m']


class IntervalQueryDb:
    """Run the generated EXISTS predicates against a real in-memory candle table."""
    def __init__(self, rows):
        import sqlite3
        self.conn = sqlite3.connect(':memory:')
        self.conn.execute('CREATE TABLE candles (exchange TEXT, market TEXT, symbol TEXT, interval TEXT, price_basis TEXT, is_closed BOOLEAN)')
        self.conn.executemany('INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?)', rows)
        self.conn.commit()
        self.statements = []

    async def execute(self, statement):
        import re
        from sqlalchemy.dialects import sqlite
        self.statements.append(statement)
        compiled = statement.compile(dialect=sqlite.dialect(paramstyle='named'))
        sql = str(compiled)
        # SQLite requires VALUES in a CTE instead of PostgreSQL's named FROM alias.
        relation = re.search(r'FROM \((VALUES .*?)\) AS candidate_intervals \(interval\)', sql)
        assert relation is not None
        sql = ('WITH candidate_intervals(interval) AS (' + relation[1] + ')\n'
               + sql.replace(relation[0], 'FROM candidate_intervals'))
        rows = [r[0] for r in self.conn.execute(sql, compiled.params)]
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: rows))

    async def rollback(self):
        self.conn.rollback()

    def original_intervals(self, exchange, market, symbol, price_basis):
        from app.utils.intervals import is_supported_interval, interval_sort_key
        rows = self.conn.execute('''SELECT DISTINCT interval FROM candles
            WHERE exchange = ? AND market = ? AND symbol = ? AND price_basis = ? AND is_closed = 1''',
            (exchange, market, symbol.upper(), price_basis))
        return sorted((r[0] for r in rows if r[0] is not None and is_supported_interval(r[0])), key=interval_sort_key)


@pytest.mark.asyncio
@pytest.mark.parametrize('exchange,market,symbol,price_basis', [
    ('bybit', 'futures', 'btcusdt', 'mark'), ('bybit', 'futures', 'BTCUSDT', 'trade'),
    ('bybit', 'spot', 'BTCUSDT', 'trade'), ('binance', 'spot', 'BTCUSDT', 'trade'),
    ('oanda', 'forex', 'EUR_USD', 'mid'), ('okx', 'futures', 'BTCUSDT', 'trade'),
    ('bybit', 'futures', 'MISSING', 'mark'),
])
async def test_exists_query_matches_distinct_results(cache, exchange, market, symbol, price_basis):
    from sqlalchemy.dialects import postgresql
    from app.utils.intervals import list_supported_intervals
    db = IntervalQueryDb([
        ('bybit', 'futures', 'BTCUSDT', interval, 'mark', True)
        for interval in reversed(list_supported_intervals())
    ] + [
        ('bybit', 'futures', 'BTCUSDT', '1m', 'mark', True),  # Duplicate interval
        ('bybit', 'futures', 'BTCUSDT', '1M', 'trade', True),
        ('bybit', 'futures', 'BTCUSDT', '1m', 'trade', False),
        ('bybit', 'futures', 'BTCUSDT', '60', 'mark', True),
        ('bybit', 'futures', 'BTCUSDT', 'D', 'mark', True),
        ('bybit', 'futures', 'BTCUSDT', None, 'mark', True),
        ('bybit', 'spot', 'BTCUSDT', '5m', 'trade', True),
        ('binance', 'spot', 'BTCUSDT', '1h', 'trade', True),
        ('oanda', 'forex', 'EUR_USD', '1m', 'mid', True),
        ('okx', 'futures', 'BTCUSDT', '4h', 'trade', True),
    ])
    args = dict(exchange=exchange, market=market, symbol=symbol, price_basis=price_basis)
    try:
        result = await aggregation.AggregationService.get_available_intervals(db=db, **args)
        assert result == db.original_intervals(**args)
        assert len(db.statements) == 1
        compiled = db.statements[0].compile(dialect=postgresql.dialect())
        sql = str(compiled)
        assert 'DISTINCT' not in sql
        assert 'VALUES' in sql and 'WHERE EXISTS (SELECT 1' in sql
        assert 'candles.interval = candidate_intervals.interval' in sql
        assert 'candles.is_closed IS true' in sql
        assert {v for k, v in compiled.params.items() if k.startswith('param_')} == set(list_supported_intervals())
        # PostgreSQL relation is correlated, not also included inside the EXISTS.
        assert sql.count('AS candidate_intervals') == 1
        assert await aggregation.AggregationService.get_available_intervals(db=db, **args) == result
        assert len(db.statements) == 1  # Redis hit
    finally:
        db.conn.close()


@pytest.mark.asyncio
async def test_exists_fallback_and_source_selection(cache):
    cache.fail = True
    db = IntervalQueryDb([
        ('bybit', 'futures', 'BTCUSDT', interval, 'mark', True)
        for interval in ['1m', '5m', '30m', '1M']
    ])
    try:
        assert await aggregation.AggregationService.pick_best_source_interval(db=db, target_interval='1h', **INTERVAL_ARGS) == '30m'
        assert await aggregation.AggregationService.pick_best_source_interval(db=db, target_interval='1M', **INTERVAL_ARGS) == '1M'
        assert len(db.statements) == 2
    finally:
        db.conn.close()


@pytest.mark.asyncio
async def test_new_authoritative_interval_is_discovered_after_cache_expiry(cache, monkeypatch):
    from app.utils import intervals
    db = IntervalQueryDb([('bybit', 'futures', 'BTCUSDT', '8h', 'mark', True)])
    try:
        assert await aggregation.AggregationService.get_available_intervals(db=db, **INTERVAL_ARGS) == []
        supported = list(intervals.SUPPORTED_INTERVALS)
        supported.insert(supported.index('12h'), '8h')
        monkeypatch.setattr(intervals, 'SUPPORTED_INTERVALS', tuple(supported))
        monkeypatch.setattr(intervals, 'INTERVAL_ORDER', {v: i for i, v in enumerate(supported)})
        monkeypatch.setitem(intervals.FIXED_INTERVAL_MS, '8h', 8 * 3_600_000)
        cache.now = 5
        assert await aggregation.AggregationService.pick_best_source_interval(db=db, target_interval='1d', **INTERVAL_ARGS) == '8h'
        assert len(db.statements) == 2
    finally:
        db.conn.close()
