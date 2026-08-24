from validate_bybit_price_bases import compare


def test_side_by_side_report_counts_overlap_and_differences():
    mark = [
        {"time": 1, "open": 1, "high": 2, "low": 0, "close": 1},
        {"time": 2, "open": 2, "high": 3, "low": 1, "close": 2},
    ]
    trade = [
        {"time": 2, "open": 2, "high": 4, "low": 1, "close": 2},
        {"time": 3, "open": 3, "high": 4, "low": 2, "close": 3},
    ]
    assert compare(mark, trade) == {
        "mark_rows": 2,
        "trade_rows": 2,
        "timestamp_overlap": 1,
        "ohlc_different_bars": 1,
    }
