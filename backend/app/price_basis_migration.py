from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from .price_basis import classify_existing_price_basis


async def migrate_price_basis(conn: AsyncConnection) -> None:
    """Add and classify price_basis without modifying legacy candle values."""
    for table in ("tracked_pairs", "candles"):
        row_count_before = int(
            (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()
        )
        await conn.commit()
        await conn.execute(
            text(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "price_basis VARCHAR(16) NULL DEFAULT 'trade'"
            )
        )
        await conn.execute(
            text(f"ALTER TABLE {table} ALTER COLUMN price_basis SET DEFAULT 'trade'")
        )
        if table == "candles":
            # Both legacy lookup indexes duplicate the old unique constraint's
            # btree exactly. Keep the unique index available while freeing
            # enough space to build its basis-aware replacement.
            await conn.execute(text("DROP INDEX IF EXISTS ix_candle_lookup"))
            await conn.execute(text("DROP INDEX IF EXISTS ix_candle_lookup_desc"))
            for redundant_index in (
                "ix_candles_exchange",
                "ix_candles_market",
                "ix_candles_symbol",
                "ix_candles_interval",
                "ix_candles_open_time",
                "ix_candles_price_basis",
            ):
                await conn.execute(text(f"DROP INDEX IF EXISTS {redundant_index}"))
        await conn.commit()
        legacy_identities = (
            await conn.execute(
                text(f"SELECT DISTINCT exchange, market FROM {table}")
            )
        ).all()
        for exchange, market in legacy_identities:
            basis = classify_existing_price_basis(exchange=exchange, market=market).value
            if basis == "trade":
                continue
            while True:
                result = await conn.execute(
                    text(
                        f"""
                        WITH batch AS (
                            SELECT id
                            FROM {table}
                            WHERE exchange = :exchange
                              AND market = :market
                              AND price_basis IS DISTINCT FROM :basis
                            ORDER BY id
                            LIMIT 10000
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE {table} AS target
                        SET price_basis = :basis
                        FROM batch
                        WHERE target.id = batch.id
                        """
                    ),
                    {"exchange": exchange, "market": market, "basis": basis},
                )
                updated = int(result.rowcount or 0)
                await conn.commit()
                if updated == 0:
                    break
        await conn.commit()

        # Handles a pre-existing nullable column from an interrupted older
        # deployment. Fresh migrations use PostgreSQL's fast DEFAULT path and
        # do not rewrite the large TRADE majority.
        while True:
            result = await conn.execute(
                text(
                    f"""
                    WITH batch AS (
                        SELECT id
                        FROM {table}
                        WHERE price_basis IS NULL
                        ORDER BY id
                        LIMIT 10000
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {table} AS target
                    SET price_basis = 'trade'
                    FROM batch
                    WHERE target.id = batch.id
                    """
                )
            )
            updated = int(result.rowcount or 0)
            await conn.commit()
            if updated == 0:
                break
        null_count = int(
            (await conn.execute(text(f"SELECT count(*) FROM {table} WHERE price_basis IS NULL"))).scalar_one()
        )
        if null_count:
            raise RuntimeError(f"price_basis migration left {null_count} null rows in {table}")
        row_count_after = int(
            (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()
        )
        if row_count_after != row_count_before:
            raise RuntimeError(
                f"price_basis migration changed {table} row count: "
                f"{row_count_before} -> {row_count_after}"
            )
        await conn.commit()

    await conn.execute(
        text(
            """
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'uq_tracked_pair_exchange_market_symbol_interval_price_basis'
                      AND conrelid = 'tracked_pairs'::regclass
                ) THEN
                    ALTER TABLE tracked_pairs ADD CONSTRAINT
                    uq_tracked_pair_exchange_market_symbol_interval_price_basis
                    UNIQUE (exchange, market, symbol, interval, price_basis);
                END IF;
            END $$
            """
        )
    )
    await conn.execute(
        text(
            "ALTER TABLE tracked_pairs "
            "DROP CONSTRAINT IF EXISTS uq_tracked_pair_exchange_market_symbol_interval"
        )
    )
    await conn.commit()

    await conn.execute(
        text(
            """
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'uq_candle_exchange_market_symbol_interval_price_basis_open_time'
                      AND conrelid = 'candles'::regclass
                ) THEN
                    ALTER TABLE candles ADD CONSTRAINT
                    uq_candle_exchange_market_symbol_interval_price_basis_open_time
                    UNIQUE (exchange, market, symbol, interval, price_basis, open_time);
                END IF;
            END $$
            """
        )
    )
    await conn.execute(
        text(
            "ALTER TABLE candles "
            "DROP CONSTRAINT IF EXISTS uq_candle_exchange_market_symbol_interval_open_time"
        )
    )
    await conn.commit()

    await conn.execute(
        text("CREATE INDEX IF NOT EXISTS ix_tracked_pairs_price_basis ON tracked_pairs (price_basis)")
    )
    for table, constraint in (
        ("tracked_pairs", "ck_tracked_pairs_price_basis"),
        ("candles", "ck_candles_price_basis"),
    ):
        await conn.execute(
            text(
                f"""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = '{constraint}' AND conrelid = '{table}'::regclass
                    ) THEN
                        ALTER TABLE {table} ADD CONSTRAINT {constraint}
                        CHECK (price_basis IN ('trade', 'mark', 'mid'));
                    END IF;
                END $$
                """
            )
        )
    await conn.execute(text("ALTER TABLE tracked_pairs ALTER COLUMN price_basis SET NOT NULL"))
    await conn.execute(text("ALTER TABLE candles ALTER COLUMN price_basis SET NOT NULL"))
    await conn.execute(text("ALTER TABLE tracked_pairs ALTER COLUMN price_basis DROP DEFAULT"))
    await conn.execute(text("ALTER TABLE candles ALTER COLUMN price_basis DROP DEFAULT"))
    await conn.commit()
