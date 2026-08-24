from __future__ import annotations

import pytest

from app.price_basis import PriceBasis, classify_existing_price_basis, resolve_price_basis


@pytest.mark.parametrize(
    ("exchange", "market", "expected"),
    [
        ("bybit", "spot", PriceBasis.TRADE),
        ("bybit", "futures", PriceBasis.MARK),
        ("okx", "spot", PriceBasis.TRADE),
        ("okx", "futures", PriceBasis.TRADE),
        ("binance", "spot", PriceBasis.TRADE),
        ("binance", "futures", PriceBasis.TRADE),
        ("oanda", "forex", PriceBasis.MID),
        ("oanda", "metals", PriceBasis.MID),
        ("oanda", "stocks", PriceBasis.MID),
    ],
)
def test_legacy_defaults_and_migration_classification(exchange, market, expected):
    assert resolve_price_basis(exchange=exchange, market=market) is expected
    assert classify_existing_price_basis(exchange=exchange, market=market) is expected


def test_bybit_futures_supports_parallel_mark_and_trade():
    assert resolve_price_basis(
        exchange="bybit", market="futures", requested_price_basis="mark"
    ) is PriceBasis.MARK
    assert resolve_price_basis(
        exchange="bybit", market="futures", requested_price_basis="trade"
    ) is PriceBasis.TRADE


@pytest.mark.parametrize(
    ("exchange", "market", "basis"),
    [
        ("bybit", "spot", "mark"),
        ("okx", "futures", "mark"),
        ("binance", "futures", "mark"),
        ("oanda", "forex", "trade"),
        ("oanda", "forex", "mark"),
        ("bybit", "futures", "mid"),
    ],
)
def test_unsupported_combinations_are_rejected(exchange, market, basis):
    with pytest.raises(ValueError, match="Unsupported price_basis"):
        resolve_price_basis(
            exchange=exchange,
            market=market,
            requested_price_basis=basis,
        )


def test_unknown_basis_is_rejected_without_fallback():
    with pytest.raises(ValueError, match="Unsupported price_basis"):
        resolve_price_basis(
            exchange="bybit",
            market="futures",
            requested_price_basis="index",
        )
