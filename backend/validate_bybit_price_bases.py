"""Compare independently stored Bybit futures MARK and TRADE series.

Example:
  python validate_bybit_price_bases.py --symbol BTCUSDT --interval 1d \
    --from-ms 1704067200000 --to-ms 1706745600000
"""

from __future__ import annotations

import argparse
import json

import httpx


def _fetch(base_url: str, args: argparse.Namespace, price_basis: str) -> list[dict]:
    response = httpx.get(
        f"{base_url.rstrip('/')}/api/klines",
        params={
            "exchange": "bybit",
            "market": "futures",
            "symbol": args.symbol.upper(),
            "interval": args.interval,
            "price_basis": price_basis,
            "from": args.from_ms,
            "to": args.to_ms,
            "limit": args.limit,
        },
        timeout=60.0,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("price_basis") != price_basis:
        raise RuntimeError(f"Expected {price_basis} response, got {payload.get('price_basis')}")
    return list(payload.get("bars") or [])


def compare(mark_bars: list[dict], trade_bars: list[dict]) -> dict[str, int]:
    mark_by_time = {int(bar["time"]): bar for bar in mark_bars}
    trade_by_time = {int(bar["time"]): bar for bar in trade_bars}
    overlap = sorted(set(mark_by_time) & set(trade_by_time))
    fields = ("open", "high", "low", "close")
    different = sum(
        any(mark_by_time[ts].get(field) != trade_by_time[ts].get(field) for field in fields)
        for ts in overlap
    )
    return {
        "mark_rows": len(mark_bars),
        "trade_rows": len(trade_bars),
        "timestamp_overlap": len(overlap),
        "ohlc_different_bars": different,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8088")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--from-ms", type=int, required=True)
    parser.add_argument("--to-ms", type=int, required=True)
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()

    mark_bars = _fetch(args.base_url, args, "mark")
    trade_bars = _fetch(args.base_url, args, "trade")
    print(json.dumps(compare(mark_bars, trade_bars), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
