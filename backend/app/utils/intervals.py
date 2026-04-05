from __future__ import annotations

from datetime import UTC, datetime, timedelta

FIXED_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}

SUPPORTED_INTERVALS: tuple[str, ...] = (
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
)

INTERVAL_ORDER: dict[str, int] = {
    interval: index for index, interval in enumerate(SUPPORTED_INTERVALS)
}


def get_interval_ms(interval: str) -> int:
    if interval == "1M":
        return 30 * FIXED_INTERVAL_MS["1d"]
    if interval not in FIXED_INTERVAL_MS:
        raise ValueError(f"Unsupported interval: {interval}")
    return FIXED_INTERVAL_MS[interval]


def is_supported_interval(interval: str) -> bool:
    return interval in INTERVAL_ORDER


def is_fixed_interval(interval: str) -> bool:
    return interval in FIXED_INTERVAL_MS


def interval_sort_key(interval: str) -> int:
    if interval not in INTERVAL_ORDER:
        raise ValueError(f"Unsupported interval: {interval}")
    return INTERVAL_ORDER[interval]


def floor_to_interval_open(ts_ms: int, interval: str) -> int:
    if interval == "1w":
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        week_start = datetime(dt.year, dt.month, dt.day, tzinfo=UTC) - timedelta(days=dt.weekday())
        return int(week_start.timestamp() * 1000)

    if interval == "1M":
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        month_start = datetime(dt.year, dt.month, 1, tzinfo=UTC)
        return int(month_start.timestamp() * 1000)

    size = get_interval_ms(interval)
    return (ts_ms // size) * size


def next_interval_open(open_time_ms: int, interval: str) -> int:
    if interval == "1M":
        dt = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC)
        if dt.month == 12:
            next_dt = datetime(dt.year + 1, 1, 1, tzinfo=UTC)
        else:
            next_dt = datetime(dt.year, dt.month + 1, 1, tzinfo=UTC)
        return int(next_dt.timestamp() * 1000)

    return open_time_ms + get_interval_ms(interval)


def previous_interval_open(open_time_ms: int, interval: str) -> int:
    if interval == "1M":
        dt = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC)
        if dt.month == 1:
            prev_dt = datetime(dt.year - 1, 12, 1, tzinfo=UTC)
        else:
            prev_dt = datetime(dt.year, dt.month - 1, 1, tzinfo=UTC)
        return int(prev_dt.timestamp() * 1000)

    return open_time_ms - get_interval_ms(interval)


def latest_closed_open_time(now_ms: int, interval: str) -> int:
    current_open = floor_to_interval_open(now_ms, interval)
    return previous_interval_open(current_open, interval)


def count_interval_steps(start_open_ms: int, end_open_ms: int, interval: str) -> int:
    if start_open_ms > end_open_ms:
        return 0

    count = 0
    current = start_open_ms
    while current <= end_open_ms:
        count += 1
        current = next_interval_open(current, interval)
    return count


def interval_can_aggregate(source_interval: str, target_interval: str) -> bool:
    if source_interval == target_interval:
        return True

    if not is_supported_interval(source_interval) or not is_supported_interval(target_interval):
        return False

    if interval_sort_key(source_interval) >= interval_sort_key(target_interval):
        return False

    if target_interval == "1M":
        return is_fixed_interval(source_interval) and (
            get_interval_ms("1d") % get_interval_ms(source_interval) == 0
        )

    if target_interval == "1w":
        return is_fixed_interval(source_interval) and (
            get_interval_ms("1w") % get_interval_ms(source_interval) == 0
        )

    if not is_fixed_interval(source_interval) or not is_fixed_interval(target_interval):
        return False

    return get_interval_ms(target_interval) % get_interval_ms(source_interval) == 0


def list_supported_intervals() -> list[str]:
    return list(SUPPORTED_INTERVALS)
