from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    String,
    DateTime,
    Integer,
    BigInteger,
    Numeric,
    Boolean,
    CheckConstraint,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class TrackedPair(Base):
    __tablename__ = "tracked_pairs"
    __table_args__ = (
        CheckConstraint(
            "price_basis IN ('trade', 'mark', 'mid')",
            name="ck_tracked_pairs_price_basis",
        ),
        UniqueConstraint(
            "exchange",
            "market",
            "symbol",
            "interval",
            "price_basis",
            name="uq_tracked_pair_exchange_market_symbol_interval_price_basis",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    exchange: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    market: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    interval: Mapped[str] = mapped_column(String(16), nullable=False, default="1h")
    price_basis: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="api")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    auto_stop_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class Candle(Base):
    __tablename__ = "candles"
    __table_args__ = (
        CheckConstraint(
            "price_basis IN ('trade', 'mark', 'mid')",
            name="ck_candles_price_basis",
        ),
        UniqueConstraint(
            "exchange",
            "market",
            "symbol",
            "interval",
            "price_basis",
            "open_time",
            name="uq_candle_exchange_market_symbol_interval_price_basis_open_time",
        ),
        Index(
            "ix_candle_lookup",
            "exchange",
            "market",
            "symbol",
            "interval",
            "price_basis",
            "open_time",
        ),
        Index(
            "ix_candle_lookup_desc",
            "exchange",
            "market",
            "symbol",
            "interval",
            "price_basis",
            "open_time",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    exchange: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    market: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    interval: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    price_basis: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    open_time: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    close_time: Mapped[int] = mapped_column(BigInteger, nullable=False)

    open: Mapped[Decimal] = mapped_column(Numeric(36, 18), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(36, 18), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(36, 18), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(36, 18), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(36, 18), nullable=False)

    trades_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_closed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="ws")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
