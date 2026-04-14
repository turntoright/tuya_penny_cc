from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import httpx
import pytest

from tuya_penny_cc.bq.schemas import RAW_ENERGY_HOURLY_SCHEMA
from tuya_penny_cc.jobs.energy_hourly import run


def _make_device(device_id: str, category: str, is_online: bool) -> dict:
    return {"id": device_id, "category": category, "isOnline": is_online}


def _make_stats() -> list[dict]:
    return [{"time": 1700000000000, "value": "1.23"}]


# Fixed "now" for all tests: 2026-04-14 15:30 UTC
_NOW = datetime(2026, 4, 14, 15, 30, 0, tzinfo=UTC)


def test_default_mode_writes_previous_hour():
    """No date params → 1 window: 2026-04-14 14:00 UTC (previous complete hour)."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_energy_stats.return_value = _make_stats()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 1

    run(tuya=fake_tuya, writer=fake_writer, run_id="r1", now=lambda: _NOW)

    fake_writer.load.assert_called_once()
    table_arg, rows = fake_writer.load.call_args.args
    assert table_arg == "raw_energy_hourly"
    assert fake_writer.load.call_args.kwargs["schema"] == RAW_ENERGY_HOURLY_SCHEMA
    assert len(rows) == 1
    assert rows[0]["stat_hour"] == "2026-04-14T14:00:00+00:00"
    assert rows[0]["device_id"] == "d1"
    assert rows[0]["category"] == "znjdq"
    assert isinstance(rows[0]["payload"], list)
    window_start = datetime(2026, 4, 14, 14, 0, 0, tzinfo=UTC)
    fake_tuya.get_energy_stats.assert_called_once_with(
        "d1", "hour",
        int(window_start.timestamp() * 1000),
        int((window_start + timedelta(hours=1)).timestamp() * 1000) - 1,
    )


def test_single_date_writes_24_windows_per_device():
    """--date 2026-04-13 with 2 online devices → 48 rows (24 windows × 2 devices)."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "znjdq", True),
        _make_device("d2", "znjdq", True),
    ])
    fake_tuya.get_energy_stats.return_value = _make_stats()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 48

    run(
        tuya=fake_tuya, writer=fake_writer, run_id="r2",
        now=lambda: _NOW, date=date(2026, 4, 13),
    )

    _, rows = fake_writer.load.call_args.args
    assert len(rows) == 48
    assert fake_tuya.get_energy_stats.call_count == 48
    assert rows[0]["stat_hour"] == "2026-04-13T00:00:00+00:00"
    assert rows[0]["device_id"] == "d1"


def test_date_range_writes_all_windows():
    """--start-date 2026-04-13 --end-date 2026-04-14 with 1 device → 48 rows."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_energy_stats.return_value = _make_stats()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 48

    run(
        tuya=fake_tuya, writer=fake_writer, run_id="r3",
        now=lambda: _NOW,
        start_date=date(2026, 4, 13), end_date=date(2026, 4, 14),
    )

    _, rows = fake_writer.load.call_args.args
    assert len(rows) == 48  # 2 days × 24 hours × 1 device
    assert rows[0]["stat_hour"] == "2026-04-13T00:00:00+00:00"
    assert rows[-1]["stat_hour"] == "2026-04-14T23:00:00+00:00"


def test_offline_device_skipped():
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "znjdq", True),
        _make_device("d2", "dlq", False),
    ])
    fake_tuya.get_energy_stats.return_value = _make_stats()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 1

    run(tuya=fake_tuya, writer=fake_writer, run_id="r4", now=lambda: _NOW)

    _, rows = fake_writer.load.call_args.args
    assert all(r["device_id"] == "d1" for r in rows)
    assert fake_tuya.get_energy_stats.call_count == 1  # only d1, default 1 window


def test_partial_date_range_raises():
    """start_date without end_date (or vice versa) must raise ValueError."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_writer = MagicMock()

    with pytest.raises(ValueError, match="start_date and end_date"):
        run(
            tuya=fake_tuya, writer=fake_writer, run_id="r_partial",
            now=lambda: _NOW, start_date=date(2026, 4, 13),
        )


def test_api_error_propagates():
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_energy_stats.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=MagicMock()
    )
    fake_writer = MagicMock()

    with pytest.raises(httpx.HTTPStatusError):
        run(tuya=fake_tuya, writer=fake_writer, run_id="r5", now=lambda: _NOW)
