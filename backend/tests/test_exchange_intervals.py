from app.exchanges.bybit import BybitAdapter
from app.exchanges.okx import OkxAdapter
from app.exchanges.oanda import OandaAdapter
from app.utils.intervals import get_interval_ms, interval_can_aggregate, list_supported_intervals


def test_three_minute_interval_is_available_to_supported_adapters() -> None:
    assert "3m" in list_supported_intervals()
    assert get_interval_ms("3m") == 180_000
    assert interval_can_aggregate("1m", "3m")
    assert BybitAdapter._to_bybit_interval("3m") == "3"
    assert OkxAdapter._to_okx_interval("3m") == "3m"
    assert OandaAdapter.GRANULARITY_MAP["3m"] == "M3"


def test_six_hour_interval_is_available_to_supported_adapters() -> None:
    assert "6h" in list_supported_intervals()
    assert get_interval_ms("6h") == 21_600_000
    assert interval_can_aggregate("1h", "6h")
    assert BybitAdapter._to_bybit_interval("6h") == "360"
    assert OkxAdapter._to_okx_interval("6h") == "6Hutc"
    assert OandaAdapter.GRANULARITY_MAP["6h"] == "H6"
