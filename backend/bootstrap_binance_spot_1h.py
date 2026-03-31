import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    DateTime,
    Integer,
    BigInteger,
    Numeric,
    Boolean,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


INTERVAL = "1h"
INTERVAL_MS = 60 * 60 * 1000
BINANCE_BASE_URL = "https://api.binance.com"
DEFAULT_DAYS = 365


metadata = MetaData()

tracked_pairs = Table(
    "tracked_pairs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("exchange", String(32), nullable=False, index=True),
    Column("market", String(32), nullable=False, index=True),
    Column("symbol", String(64), nullable=False, index=True),
    Column("interval", String(16), nullable=False, default="1h"),
    Column("status", String(16), nullable=False, default="active", index=True),
    Column("source", String(32), nullable=False, default="api"),
    Column("priority", Integer, nullable=False, default=100),
    Column("created_at", DateTime(timezone=False), nullable=False),
    Column("updated_at", DateTime(timezone=False), nullable=False),
    UniqueConstraint(
        "exchange",
        "market",
        "symbol",
        "interval",
        name="uq_tracked_pair_exchange_market_symbol_interval",
    ),
)

candles = Table(
    "candles",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("exchange", String(32), nullable=False, index=True),
    Column("market", String(32), nullable=False, index=True),
    Column("symbol", String(64), nullable=False, index=True),
    Column("interval", String(16), nullable=False, index=True),
    Column("open_time", BigInteger, nullable=False, index=True),
    Column("close_time", BigInteger, nullable=False),
    Column("open", Numeric(36, 18), nullable=False),
    Column("high", Numeric(36, 18), nullable=False),
    Column("low", Numeric(36, 18), nullable=False),
    Column("close", Numeric(36, 18), nullable=False),
    Column("volume", Numeric(36, 18), nullable=False),
    Column("trades_count", Integer, nullable=True),
    Column("is_closed", Boolean, nullable=False, default=True),
    Column("source", String(16), nullable=False, default="ws"),
    Column("created_at", DateTime(timezone=False), nullable=False),
    Column("updated_at", DateTime(timezone=False), nullable=False),
    UniqueConstraint(
        "exchange",
        "market",
        "symbol",
        "interval",
        "open_time",
        name="uq_candle_exchange_market_symbol_interval_open_time",
    ),
    Index(
        "ix_candle_lookup",
        "exchange",
        "market",
        "symbol",
        "interval",
        "open_time",
    ),
    Index(
        "ix_candle_lookup_desc",
        "exchange",
        "market",
        "symbol",
        "interval",
        "open_time",
    ),
)


@dataclass
class PairItem:
    symbol: str
    exchange: str
    market: str
    priority: int
    source: str = "bootstrap"


def utc_now_naive() -> datetime:
    return datetime.utcnow()


def now_ms() -> int:
    return int(time.time() * 1000)


def floor_to_hour_ms(ts_ms: int) -> int:
    return ts_ms - (ts_ms % INTERVAL_MS)


def load_pairs(path: str) -> list[PairItem]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    seen: set[tuple[str, str, str]] = set()
    items: list[PairItem] = []

    for row in raw:
        exchange = str(row.get("exchange", "")).strip().lower()
        market = str(row.get("market", "")).strip().lower()
        symbol = str(row.get("symbol", "")).strip().upper()

        if exchange != "binance":
            continue
        if market != "spot":
            continue
        if not symbol:
            continue

        key = (exchange, market, symbol)
        if key in seen:
            continue
        seen.add(key)

        rank = row.get("rank")
        priority = int(rank) if isinstance(rank, int) else 100

        items.append(
            PairItem(
                symbol=symbol,
                exchange=exchange,
                market=market,
                priority=priority,
                source="bootstrap",
            )
        )

    items.sort(key=lambda x: (x.priority, x.symbol))
    return items


async def upsert_tracked_pair(session, pair: PairItem) -> None:
    ts = utc_now_naive()

    stmt = pg_insert(tracked_pairs).values(
        exchange=pair.exchange,
        market=pair.market,
        symbol=pair.symbol,
        interval=INTERVAL,
        status="active",
        source=pair.source,
        priority=pair.priority,
        created_at=ts,
        updated_at=ts,
    )

    stmt = stmt.on_conflict_do_update(
        constraint="uq_tracked_pair_exchange_market_symbol_interval",
        set_={
            "status": "active",
            "source": pair.source,
            "priority": pair.priority,
            "updated_at": ts,
        },
    )

    await session.execute(stmt)


async def insert_candles_chunk(
    session,
    pair: PairItem,
    rows: list[list[Any]],
) -> int:
    if not rows:
        return 0

    ts = utc_now_naive()
    payload = []

    for row in rows:
        payload.append(
            {
                "exchange": pair.exchange,
                "market": pair.market,
                "symbol": pair.symbol,
                "interval": INTERVAL,
                "open_time": int(row[0]),
                "close_time": int(row[6]),
                "open": str(row[1]),
                "high": str(row[2]),
                "low": str(row[3]),
                "close": str(row[4]),
                "volume": str(row[5]),
                "trades_count": int(row[8]) if row[8] is not None else None,
                "is_closed": True,
                "source": "bootstrap",
                "created_at": ts,
                "updated_at": ts,
            }
        )

    stmt = pg_insert(candles).values(payload)
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_candle_exchange_market_symbol_interval_open_time"
    )

    result = await session.execute(stmt)
    # result.rowcount can be None with some drivers, so fallback to len(payload)
    return result.rowcount if result.rowcount is not None else len(payload)


async def fetch_klines_for_pair(
    client: httpx.AsyncClient,
    pair: PairItem,
    start_ms: int,
    end_ms: int,
    session,
    request_sleep: float = 0.03,
) -> int:
    """
    Binance Spot klines:
    GET /api/v3/klines
    weight = 2
    limit max = 1000
    """
    inserted_total = 0
    cursor = start_ms

    while cursor < end_ms:
        params = {
            "symbol": pair.symbol,
            "interval": INTERVAL,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        }

        for attempt in range(6):
            try:
                resp = await client.get("/api/v3/klines", params=params)
                if resp.status_code == 429:
                    wait_s = min(2 ** attempt, 20)
                    print(f"[{pair.symbol}] 429 rate limit, sleep {wait_s}s")
                    await asyncio.sleep(wait_s)
                    continue

                resp.raise_for_status()
                rows = resp.json()
                break
            except httpx.HTTPStatusError as e:
                if e.response is not None and e.response.status_code >= 500 and attempt < 5:
                    wait_s = min(2 ** attempt, 20)
                    print(f"[{pair.symbol}] server error {e.response.status_code}, retry in {wait_s}s")
                    await asyncio.sleep(wait_s)
                    continue
                raise
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                if attempt < 5:
                    wait_s = min(2 ** attempt, 20)
                    print(f"[{pair.symbol}] network retry in {wait_s}s: {e}")
                    await asyncio.sleep(wait_s)
                    continue
                raise
        else:
            raise RuntimeError(f"Failed to fetch klines for {pair.symbol}")

        if not rows:
            break

        inserted = await insert_candles_chunk(session, pair, rows)
        inserted_total += inserted
        await session.commit()

        last_open_time = int(rows[-1][0])
        next_cursor = last_open_time + INTERVAL_MS

        if next_cursor <= cursor:
            break

        cursor = next_cursor
        await asyncio.sleep(request_sleep)

        if len(rows) < 1000:
            break

    return inserted_total


async def start_stream_via_internal_api(
    client: httpx.AsyncClient,
    api_base: str,
    pair: PairItem,
) -> bool:
    url = f"{api_base.rstrip('/')}/internal/pairs"
    payload = {
        "exchange": pair.exchange,
        "market": pair.market,
        "symbol": pair.symbol,
        "interval": INTERVAL,
        "source": "bootstrap",
        "priority": pair.priority,
        "backfill_limit": 1,
    }

    try:
        resp = await client.post(url, json=payload)
        if resp.status_code not in (200, 201):
            print(f"[{pair.symbol}] stream start failed: {resp.status_code} {resp.text[:300]}")
            return False
        return True
    except Exception as e:
        print(f"[{pair.symbol}] stream start error: {e}")
        return False


async def process_pair(
    pair: PairItem,
    session_factory,
    binance_client: httpx.AsyncClient,
    start_ms: int,
    end_ms: int,
    api_base: str | None,
    control_client: httpx.AsyncClient | None,
) -> tuple[str, int, bool]:
    async with session_factory() as session:
        await upsert_tracked_pair(session, pair)
        await session.commit()

        inserted = await fetch_klines_for_pair(
            client=binance_client,
            pair=pair,
            start_ms=start_ms,
            end_ms=end_ms,
            session=session,
        )

    started = False
    if api_base and control_client is not None:
        started = await start_stream_via_internal_api(
            client=control_client,
            api_base=api_base,
            pair=pair,
        )

    return pair.symbol, inserted, started


async def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Binance spot 1h candles into KlineHub DB")
    parser.add_argument(
        "--pairs-json",
        required=True,
        help="Path to JSON with pairs, e.g. backend/main/binance_top300.json",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="Async SQLAlchemy DB url, e.g. postgresql+asyncpg://user:pass@host:5432/dbname",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="How many days of 1h candles to fetch",
    )
    parser.add_argument(
        "--api-base",
        default=None,
        help="Optional running KlineHub base url, e.g. http://127.0.0.1:8000 . "
             "If passed, script will call /internal/pairs to reload/start streams immediately.",
    )
    parser.add_argument(
        "--limit-pairs",
        type=int,
        default=0,
        help="For testing. 0 = all pairs",
    )
    args = parser.parse_args()

    if not args.database_url:
        print("ERROR: --database-url or DATABASE_URL is required")
        sys.exit(1)

    pairs = load_pairs(args.pairs_json)
    if args.limit_pairs > 0:
        pairs = pairs[: args.limit_pairs]

    if not pairs:
        print("No Binance spot pairs found in JSON")
        return

    print(f"Loaded {len(pairs)} Binance spot pairs from {args.pairs_json}")

    end_ms = floor_to_hour_ms(now_ms())
    start_ms = end_ms - (args.days * 24 * INTERVAL_MS)

    engine = create_async_engine(
        args.database_url,
        pool_pre_ping=True,
        future=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    timeout = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=60.0)

    async with httpx.AsyncClient(base_url=BINANCE_BASE_URL, timeout=timeout) as binance_client:
        async with httpx.AsyncClient(timeout=timeout) as control_client:
            total_inserted = 0
            started_count = 0

            for idx, pair in enumerate(pairs, start=1):
                print(f"\n[{idx}/{len(pairs)}] {pair.symbol} ...")

                try:
                    symbol, inserted, started = await process_pair(
                        pair=pair,
                        session_factory=session_factory,
                        binance_client=binance_client,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        api_base=args.api_base,
                        control_client=control_client if args.api_base else None,
                    )
                    total_inserted += inserted
                    started_count += 1 if started else 0
                    print(f"[{symbol}] inserted/upserted ~{inserted} candles")
                    if args.api_base:
                        print(f"[{symbol}] stream {'started/reloaded' if started else 'NOT started'}")
                except Exception as e:
                    print(f"[{pair.symbol}] FAILED: {e}")

    await engine.dispose()

    print("\n=== DONE ===")
    print(f"Pairs processed: {len(pairs)}")
    print(f"Total candles inserted/upserted: ~{total_inserted}")
    if args.api_base:
        print(f"Streams started/reloaded via API: {started_count}/{len(pairs)}")
    else:
        print("Streams were not started via API.")
        print("If your app loads active tracked_pairs on startup, just restart KlineHub.")


if __name__ == "__main__":
    asyncio.run(main())