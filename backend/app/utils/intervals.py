INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}

AGGREGATED_FROM_1H = {"2h", "4h", "6h", "12h", "1d", "1w"}


def get_interval_ms(interval: str) -> int:
    if interval not in INTERVAL_MS:
        raise ValueError(f"Unsupported interval: {interval}")
    return INTERVAL_MS[interval]


def is_supported_interval(interval: str) -> bool:
    return interval in INTERVAL_MS


def is_aggregated_interval(interval: str) -> bool:
    return interval in AGGREGATED_FROM_1H


def floor_to_interval_open(ts_ms: int, interval: str) -> int:
    size = get_interval_ms(interval)
    return (ts_ms // size) * size


def latest_closed_open_time(now_ms: int, interval: str) -> int:
    size = get_interval_ms(interval)
    current_open = floor_to_interval_open(now_ms, interval)
    return current_open - size