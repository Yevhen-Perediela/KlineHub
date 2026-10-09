"""Explicit schema setup and independent, resumable price-basis maintenance.

Existing labels are not evidence of original price semantics. In particular,
Bybit futures TRADE is valid; retiring the old migration must not relabel it.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.exc import DBAPIError

from .db import Base, engine
from . import models  # Register metadata for explicit fresh-database setup.
from .price_basis import resolve_price_basis

log = logging.getLogger(__name__)
LOCK_ID = 752019340
TABLES = ("tracked_pairs", "candles")
STATE_DDL = """
CREATE TABLE IF NOT EXISTS price_basis_maintenance (
    task TEXT PRIMARY KEY,
    last_id BIGINT NOT NULL DEFAULT 0,
    upper_id BIGINT NOT NULL DEFAULT 0,
    completed BOOLEAN NOT NULL DEFAULT false,
    inspected BIGINT NOT NULL DEFAULT 0,
    issues BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def _schema_problems(sync_conn) -> list[str]:
    inspector = inspect(sync_conn)
    problems = []
    for table in TABLES:
        if not inspector.has_table(table):
            problems.append(f"missing {table}")
            continue
        columns = {c["name"]: c for c in inspector.get_columns(table)}
        basis = columns.get("price_basis")
        if not basis or basis["nullable"]:
            problems.append(f"{table}.price_basis must exist and be NOT NULL")
        expected = ["exchange", "market", "symbol", "interval", "price_basis"]
        if table == "candles":
            expected.append("open_time")
        unique = inspector.get_unique_constraints(table)
        indexes = inspector.get_indexes(table)
        if not any(u["column_names"] == expected for u in unique) and not any(
            i["unique"] and i["column_names"] == expected
            and not i.get("dialect_options", {}).get("postgresql_where") for i in indexes
        ):
            problems.append(f"missing basis-aware unique key on {table}")
        checks = inspector.get_check_constraints(table)
        if not any(
            "price_basis" in c["sqltext"] and all(
                f"'{v}'" in c["sqltext"] for v in ("trade", "mark", "mid")
            ) for c in checks
        ):
            problems.append(f"missing price_basis check on {table}")
        old_key = [c for c in expected if c != "price_basis"]
        if any(u["column_names"] == old_key for u in unique) or any(
            i["unique"] and i["column_names"] == old_key for i in indexes
        ):
            problems.append(f"obsolete unique key on {table} prevents parallel bases; review explicitly")
        if table == "tracked_pairs" and "auto_stop_at" not in columns:
            problems.append("missing tracked_pairs.auto_stop_at")
    return problems


async def set_timeouts(conn: AsyncConnection) -> None:
    await conn.execute(text("SET LOCAL lock_timeout = '500ms'"))
    await conn.execute(text("SET LOCAL statement_timeout = '3s'"))


async def verify_price_basis_schema(conn: AsyncConnection) -> None:
    await set_timeouts(conn)
    problems = await conn.run_sync(_schema_problems)
    invalid = (await conn.execute(text("""
        SELECT t.relname || '.' || i.relname AS object
        FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        WHERE x.indrelid IN (to_regclass('candles'), to_regclass('tracked_pairs'))
          AND x.indisunique AND (NOT x.indisvalid OR NOT x.indisready)
        UNION ALL
        SELECT conrelid::regclass::text || '.' || conname AS object
        FROM pg_constraint
        WHERE conrelid IN (to_regclass('candles'), to_regclass('tracked_pairs'))
          AND contype = 'c' AND NOT convalidated
          AND pg_get_constraintdef(oid) LIKE '%price_basis%'
    """))).scalars().all()
    problems.extend(f"invalid/unvalidated schema object {name}" for name in invalid)
    if problems:
        raise RuntimeError(
            "Database schema is not ready: " + "; ".join(problems)
            + ". Run explicit schema setup (python -m app.price_basis_migration schema); "
            "existing incomplete schemas require operator review. Startup never migrates data."
        )


async def migrate_price_basis(conn: AsyncConnection) -> None:
    """Compatibility entry point: verification only, never historical migration."""
    await verify_price_basis_schema(conn)


async def setup_schema() -> None:
    # create_all only on an empty application database; never rebuild production indexes.
    async with engine.begin() as conn:
        await set_timeouts(conn)
        existing = await conn.run_sync(lambda c: [inspect(c).has_table(t) for t in TABLES])
        if not any(existing):
            await conn.run_sync(Base.metadata.create_all)
        await verify_price_basis_schema(conn)
        await conn.execute(text(STATE_DDL))


@dataclass(frozen=True)
class WorkerOptions:
    batch_size: int = 1000
    delay: float = 2.0
    busy_delay: float = 15.0
    max_active: int = 4


async def database_busy(conn: AsyncConnection, max_active: int) -> bool:
    return bool((await conn.execute(text("""
        SELECT count(*) >= :max_active OR coalesce(bool_or(wait_event_type = 'Lock'), false)
        FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid()
          AND backend_type = 'client backend' AND state = 'active'
    """), {"max_active": max_active})).scalar_one())


def audit_issue(row) -> str | None:
    if row["price_basis"] is None:
        return "ambiguous: missing basis and no original-price provenance"
    try:
        resolve_price_basis(exchange=row["exchange"], market=row["market"],
                            requested_price_basis=row["price_basis"])
    except ValueError as exc:
        return f"unverified: {exc}"
    return None


async def audit_batch(conn: AsyncConnection, table: str, options: WorkerOptions) -> bool:
    task = f"audit-v1:{table}"
    state = (await conn.execute(text(
        "SELECT * FROM price_basis_maintenance WHERE task = :task FOR UPDATE"
    ), {"task": task})).mappings().one_or_none()
    if state is None:
        # Indexed upper bound fixes the audit horizon while ingestion continues.
        upper = (await conn.execute(text(
            f"SELECT id FROM {table} ORDER BY id DESC LIMIT 1"
        ))).scalar_one_or_none() or 0
        await conn.execute(text("""
            INSERT INTO price_basis_maintenance(task, upper_id) VALUES (:task, :upper)
        """), {"task": task, "upper": upper})
        state = {"last_id": 0, "upper_id": upper, "completed": False}
    if state["completed"]:
        return True
    rows = (await conn.execute(text(f"""
        SELECT id, exchange, market, price_basis FROM {table}
        WHERE id > :last AND id <= :upper ORDER BY id LIMIT :size
    """), {"last": state["last_id"], "upper": state["upper_id"],
           "size": options.batch_size})).mappings().all()
    issues = [(r["id"], reason) for r in rows if (reason := audit_issue(r))]
    last = rows[-1]["id"] if rows else state["upper_id"]
    completed = last >= state["upper_id"] or len(rows) < options.batch_size
    await conn.execute(text("""
        UPDATE price_basis_maintenance SET last_id = :last, completed = :done,
            inspected = inspected + :size, issues = issues + :issues, updated_at = now()
        WHERE task = :task
    """), {"last": last, "done": completed, "size": len(rows),
           "issues": len(issues), "task": task})
    # Only progress metadata changes. Candles and pairs are always read-only.
    if issues or completed or (state.get("inspected", 0) + len(rows)) % 100000 < len(rows):
        log.info("%s checkpoint=%s completed=%s batch_issues=%s samples=%s",
                 task, last, completed, len(issues), issues[:5])
    return completed


async def pause(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def run_worker(mode: str, stop: asyncio.Event, options: WorkerOptions) -> None:
    async with engine.connect() as conn:
        async with conn.begin():
            await verify_price_basis_schema(conn)
            acquired = (await conn.execute(text(
                "SELECT pg_try_advisory_lock(:key)"
            ), {"key": LOCK_ID})).scalar_one()
        if not acquired:
            log.info("Another price-basis worker holds the lock; exiting")
            return
        try:
            async with conn.begin():
                await set_timeouts(conn)
                await conn.execute(text(STATE_DDL))
                # No reliable per-row provenance exists. Retire unsafe legacy classification
                # once, without scanning data or guessing from an exchange default.
                await conn.execute(text("""
                    INSERT INTO price_basis_maintenance(task, completed)
                    VALUES ('legacy-v2-preserve-labels', true) ON CONFLICT (task) DO NOTHING
                """))
            if mode == "migrate":
                log.info("Legacy migration retired; existing labels preserved. Use audit for issues.")
                return
            for table in TABLES:
                while not stop.is_set():
                    try:
                        async with conn.begin():
                            await set_timeouts(conn)
                            completed = (await conn.execute(text(
                                "SELECT completed FROM price_basis_maintenance WHERE task = :task"
                            ), {"task": f"audit-v1:{table}"})).scalar_one_or_none()
                            if completed:
                                break
                            busy = await database_busy(conn, options.max_active)
                            done = False if busy else await audit_batch(conn, table, options)
                    except DBAPIError as exc:
                        code = getattr(exc.orig, "sqlstate", None)
                        if code not in {"57014", "55P03"}:
                            raise
                        log.info("Maintenance timed out; postponing batch (checkpoint unchanged)")
                        busy, done = True, False
                    if done:
                        break
                    await pause(stop, options.busy_delay if busy else options.delay)
        finally:
            await conn.rollback()
            await conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": LOCK_ID})
            await conn.commit()


def install_stop_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("schema", "migrate", "audit"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--delay", type=float, default=2)
    parser.add_argument("--busy-delay", type=float, default=15)
    parser.add_argument("--max-active", type=int, default=4)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 10000 or min(args.delay, args.busy_delay) <= 0 or args.max_active < 1:
        parser.error("batch-size must be 1..10000; delays and max-active must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = asyncio.Event()
    install_stop_handlers(stop)
    try:
        if args.mode == "schema":
            await setup_schema()
        else:
            await run_worker(args.mode, stop, WorkerOptions(
                args.batch_size, args.delay, args.busy_delay, args.max_active))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
