from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock, Mock
import asyncio
import signal

import pytest

from app import price_basis_migration as migration


class Result:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value

    scalar_one_or_none = scalar_one
    one_or_none = scalar_one

    def mappings(self):
        return self

    def all(self):
        return self.value


class Connection:
    """Transactional checkpoint store with a bounded, immutable application table."""
    def __init__(self, rows=(), locked=True):
        self.rows = list(rows)
        self.states = {}
        self.sql = []
        self.locked = locked
        self.fail_checkpoint = False
        self.commits = 0

    @asynccontextmanager
    async def begin(self):
        before = deepcopy(self.states)
        try:
            yield self
        except BaseException:
            self.states = before
            raise
        else:
            self.commits += 1

    async def execute(self, stmt, params=None):
        sql = ' '.join(str(stmt).split())
        params = params or {}
        self.sql.append((sql, params))
        if 'pg_try_advisory_lock' in sql:
            return Result(self.locked)
        if 'count(*) >=' in sql:
            return Result(False)
        if sql.startswith('SELECT * FROM price_basis_maintenance'):
            return Result(deepcopy(self.states.get(params['task'])))
        if sql.startswith('SELECT completed'):
            state = self.states.get(params['task'])
            return Result(state['completed'] if state else None)
        if sql.startswith('SELECT id FROM'):
            return Result(max((r['id'] for r in self.rows), default=0))
        if sql.startswith('INSERT INTO price_basis_maintenance(task, upper_id)'):
            self.states[params['task']] = dict(last_id=0, upper_id=params['upper'],
                                              completed=False, inspected=0, issues=0)
        if sql.startswith('SELECT id, exchange'):
            assert 'ORDER BY id LIMIT :size' in sql
            return Result([r for r in self.rows if params['last'] < r['id'] <= params['upper']][:params['size']])
        if sql.startswith('UPDATE price_basis_maintenance'):
            if self.fail_checkpoint:
                raise RuntimeError('interrupted before commit')
            state = self.states[params['task']]
            state.update(last_id=params['last'], completed=params['done'])
            state['inspected'] += params['size']
            state['issues'] += params['issues']
        return Result(None)

    async def rollback(self):
        pass

    async def commit(self):
        pass


class Engine:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def connect(self):
        yield self.conn


def row(row_id, basis='trade', exchange='bybit', market='futures'):
    return dict(id=row_id, exchange=exchange, market=market, price_basis=basis)


@pytest.mark.asyncio
async def test_resume_checkpoint_and_preserve_both_bases():
    conn = Connection([row(1), row(2, 'mark'), row(3, None), row(4, 'trade', 'unknown')])
    original = deepcopy(conn.rows)
    options = migration.WorkerOptions(batch_size=2)
    async with conn.begin():
        assert not await migration.audit_batch(conn, 'candles', options)
    assert conn.states['audit-v1:candles']['last_id'] == 2
    # A restarted worker sees the same committed state, and no previous IDs.
    conn.sql.clear()
    async with conn.begin():
        assert await migration.audit_batch(conn, 'candles', options)
    state = conn.states['audit-v1:candles']
    assert (state['inspected'], state['issues'], state['last_id']) == (4, 2, 4)
    select = next(p for s, p in conn.sql if s.startswith('SELECT id, exchange'))
    assert select['last'] == 2
    assert conn.rows == original
    conn.sql.clear()
    async with conn.begin():
        assert await migration.audit_batch(conn, 'candles', options)
    assert not any('FROM candles' in s for s, _ in conn.sql)


@pytest.mark.asyncio
async def test_failed_batch_does_not_advance_checkpoint():
    conn = Connection([row(1), row(2)])
    options = migration.WorkerOptions(batch_size=1)
    async with conn.begin():
        await migration.audit_batch(conn, 'candles', options)
    before = deepcopy(conn.states)
    conn.fail_checkpoint = True
    with pytest.raises(RuntimeError):
        async with conn.begin():
            await migration.audit_batch(conn, 'candles', options)
    assert conn.states == before
    conn.fail_checkpoint = False
    async with conn.begin():
        assert await migration.audit_batch(conn, 'candles', options)
    assert conn.states['audit-v1:candles']['inspected'] == 2


@pytest.mark.parametrize('basis', ['mark', 'trade'])
def test_bybit_supported_labels_are_valid(basis):
    assert migration.audit_issue(row(1, basis)) is None


@pytest.mark.parametrize('record', [row(1, None), row(1, 'trade', 'oanda', 'forex'),
                                   row(1, 'mark', 'unknown')])
def test_ambiguous_or_unverified_rows_reported(record):
    assert migration.audit_issue(record)


@pytest.mark.asyncio
async def test_concurrent_worker_exits_without_processing(monkeypatch):
    conn = Connection(locked=False)
    monkeypatch.setattr(migration, 'engine', Engine(conn))
    monkeypatch.setattr(migration, 'verify_price_basis_schema', AsyncMock())
    await migration.run_worker('audit', asyncio.Event(), migration.WorkerOptions())
    assert not any('price_basis_maintenance' in sql for sql, _ in conn.sql)
    assert not any('FROM candles' in sql for sql, _ in conn.sql)


@pytest.mark.asyncio
async def test_migration_does_not_scan_or_modify_data(monkeypatch):
    conn = Connection([row(1), row(2, 'mark')])
    monkeypatch.setattr(migration, 'engine', Engine(conn))
    monkeypatch.setattr(migration, 'verify_price_basis_schema', AsyncMock())
    await migration.run_worker('migrate', asyncio.Event(), migration.WorkerOptions())
    assert any('legacy-v2-preserve-labels' in s for s, _ in conn.sql)
    assert not any('FROM candles' in s or 'FROM tracked_pairs' in s for s, _ in conn.sql)
    assert any('pg_advisory_unlock' in s for s, _ in conn.sql)


@pytest.mark.asyncio
async def test_sigterm_interrupts_pause_and_stops_next_batch(monkeypatch):
    stop = asyncio.Event()
    handlers = {}
    loop = Mock()
    loop.add_signal_handler.side_effect = lambda sig, fn: handlers.setdefault(sig, fn)
    with monkeypatch.context() as context:
        context.setattr(migration.asyncio, 'get_running_loop', lambda: loop)
        migration.install_stop_handlers(stop)
    handlers[signal.SIGTERM]()
    await asyncio.wait_for(migration.pause(stop, 60), timeout=0.1)
    conn = Connection([row(1)])
    monkeypatch.setattr(migration, 'engine', Engine(conn))
    monkeypatch.setattr(migration, 'verify_price_basis_schema', AsyncMock())
    await migration.run_worker('audit', stop, migration.WorkerOptions())
    assert not any('SELECT id, exchange' in s for s, _ in conn.sql)


@pytest.mark.asyncio
async def test_busy_database_postpones_batch(monkeypatch):
    stop = asyncio.Event()
    conn = Connection([row(1)])
    monkeypatch.setattr(migration, 'engine', Engine(conn))
    monkeypatch.setattr(migration, 'verify_price_basis_schema', AsyncMock())
    monkeypatch.setattr(migration, 'database_busy', AsyncMock(return_value=True))
    delays = []
    async def pause(event, seconds):
        delays.append(seconds)
        stop.set()
    monkeypatch.setattr(migration, 'pause', pause)
    await migration.run_worker('audit', stop, migration.WorkerOptions())
    assert delays == [15]
    assert not any('SELECT id, exchange' in s for s, _ in conn.sql)


@pytest.mark.asyncio
async def test_startup_only_verifies_schema(monkeypatch):
    from app import main
    conn = Connection()
    engine = Mock()
    engine.begin = conn.begin
    verify = AsyncMock()
    monkeypatch.setattr(main, 'engine', engine)
    monkeypatch.setattr(main, 'verify_price_basis_schema', verify)
    await main.ensure_schema_upgrades()
    verify.assert_awaited_once_with(conn)
    assert conn.sql == []
    source = Path(main.__file__).read_text()
    assert 'create_all' not in source
    assert 'migrate_price_basis(' not in source
    assert 'run_worker' not in source


@pytest.mark.asyncio
async def test_ready_schema_verification_and_fail_fast(monkeypatch):
    conn = Mock()
    conn.execute = AsyncMock(return_value=Mock(scalars=lambda: Mock(all=lambda: [])))
    conn.run_sync = AsyncMock(return_value=[])
    await migration.verify_price_basis_schema(conn)
    assert conn.execute.await_count == 3  # timeouts plus catalogs only
    conn.run_sync.return_value = ['candles.price_basis must exist and be NOT NULL']
    with pytest.raises(RuntimeError, match='Startup never migrates data'):
        await migration.verify_price_basis_schema(conn)


def test_migrated_schema_recognized_without_ddl(monkeypatch):
    inspector = Mock()
    inspector.has_table.return_value = True
    inspector.get_columns.return_value = [dict(name='price_basis', nullable=False),
                                          dict(name='auto_stop_at', nullable=True)]
    inspector.get_check_constraints.return_value = [dict(sqltext="price_basis IN ('trade', 'mark', 'mid')")]
    inspector.get_indexes.return_value = []
    inspector.get_unique_constraints.side_effect = [
        [dict(column_names=['exchange', 'market', 'symbol', 'interval', 'price_basis'])],
        [dict(column_names=['exchange', 'market', 'symbol', 'interval', 'price_basis', 'open_time'])],
    ]
    monkeypatch.setattr(migration, 'inspect', lambda conn: inspector)
    assert migration._schema_problems(Mock()) == []


def test_maintenance_has_no_candle_mutations_or_unbounded_updates():
    source = Path(migration.__file__).read_text()
    assert 'DELETE FROM' not in source
    assert 'UPDATE candles' not in source
    assert 'UPDATE tracked_pairs' not in source
    assert 'OFFSET' not in source
    assert 'WHERE task = :task' in source


@pytest.mark.asyncio
async def test_invalid_concurrent_index_fails_schema_verification():
    conn = Mock()
    conn.run_sync = AsyncMock(return_value=[])
    result = Mock()
    result.scalars.return_value.all.return_value = ['candles.invalid_unique']
    conn.execute = AsyncMock(return_value=result)
    with pytest.raises(RuntimeError, match='invalid/unvalidated schema object'):
        await migration.verify_price_basis_schema(conn)
