from __future__ import annotations

from enum import Enum


class PriceBasis(str, Enum):
    TRADE = "trade"
    MARK = "mark"
    MID = "mid"


_SUPPORTED_BASES: dict[tuple[str, str], frozenset[PriceBasis]] = {
    ("bybit", "spot"): frozenset({PriceBasis.TRADE}),
    ("bybit", "futures"): frozenset({PriceBasis.MARK, PriceBasis.TRADE}),
    ("okx", "spot"): frozenset({PriceBasis.TRADE}),
    ("okx", "futures"): frozenset({PriceBasis.TRADE}),
    ("binance", "spot"): frozenset({PriceBasis.TRADE}),
    ("binance", "futures"): frozenset({PriceBasis.TRADE}),
    ("oanda", "forex"): frozenset({PriceBasis.MID}),
    ("oanda", "metals"): frozenset({PriceBasis.MID}),
    ("oanda", "stocks"): frozenset({PriceBasis.MID}),
}

_DEFAULT_BASES: dict[tuple[str, str], PriceBasis] = {
    ("bybit", "spot"): PriceBasis.TRADE,
    ("bybit", "futures"): PriceBasis.MARK,
    ("okx", "spot"): PriceBasis.TRADE,
    ("okx", "futures"): PriceBasis.TRADE,
    ("binance", "spot"): PriceBasis.TRADE,
    ("binance", "futures"): PriceBasis.TRADE,
    ("oanda", "forex"): PriceBasis.MID,
    ("oanda", "metals"): PriceBasis.MID,
    ("oanda", "stocks"): PriceBasis.MID,
}


def resolve_price_basis(
    *,
    exchange: str,
    market: str,
    requested_price_basis: PriceBasis | str | None = None,
) -> PriceBasis:
    key = (exchange.lower().strip(), market.lower().strip())
    supported = _SUPPORTED_BASES.get(key)
    if supported is None:
        raise ValueError(f"Unsupported exchange/market: {key[0]}/{key[1]}")

    if requested_price_basis is None or str(requested_price_basis).strip() == "":
        return _DEFAULT_BASES[key]

    try:
        basis = (
            requested_price_basis
            if isinstance(requested_price_basis, PriceBasis)
            else PriceBasis(str(requested_price_basis).lower().strip())
        )
    except ValueError as exc:
        raise ValueError(f"Unsupported price_basis: {requested_price_basis}") from exc

    if basis not in supported:
        raise ValueError(
            f"Unsupported price_basis={basis.value} for {key[0]}/{key[1]}"
        )
    return basis


def classify_existing_price_basis(*, exchange: str, market: str) -> PriceBasis:
    """Classify legacy rows by their verified exchange/market semantics."""
    return resolve_price_basis(exchange=exchange, market=market)
