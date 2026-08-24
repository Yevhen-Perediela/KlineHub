from __future__ import annotations

from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Candle, TrackedPair


def _candle(*, row_id: int, price_basis: str, close: str) -> Candle:
    return Candle(
        id=row_id,
        exchange="bybit",
        market="futures",
        symbol="BTCUSDT",
        interval="1d",
        price_basis=price_basis,
        open_time=1_700_000_000_000,
        close_time=1_700_086_399_999,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=Decimal("0"),
        is_closed=True,
        source="rest",
    )


def test_mark_and_trade_candles_and_pairs_coexist():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                _candle(row_id=1, price_basis="mark", close="100"),
                _candle(row_id=2, price_basis="trade", close="101"),
                TrackedPair(
                    id=1,
                    exchange="bybit",
                    market="futures",
                    symbol="BTCUSDT",
                    interval="1d",
                    price_basis="mark",
                    status="active",
                    source="api",
                    priority=100,
                ),
                TrackedPair(
                    id=2,
                    exchange="bybit",
                    market="futures",
                    symbol="BTCUSDT",
                    interval="1d",
                    price_basis="trade",
                    status="paused",
                    source="on_demand",
                    priority=500,
                ),
            ]
        )
        session.commit()

        mark = session.scalar(select(Candle).where(Candle.price_basis == "mark"))
        trade = session.scalar(select(Candle).where(Candle.price_basis == "trade"))
        pairs = session.scalars(select(TrackedPair).order_by(TrackedPair.id)).all()

        assert mark is not None and mark.close == Decimal("100")
        assert trade is not None and trade.close == Decimal("101")
        assert [(pair.price_basis, pair.status, pair.priority) for pair in pairs] == [
            ("mark", "active", 100),
            ("trade", "paused", 500),
        ]
