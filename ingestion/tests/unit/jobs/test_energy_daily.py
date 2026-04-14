from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import httpx
import pytest

from tuya_penny_cc.bq.schemas import RAW_ENERGY_DAILY_SCHEMA
from tuya_penny_cc.jobs.energy_daily import run


def _make_device(device_id: str, category: str, is_online: bool) -> dict:
    return {"id": device_id, "category": category, "isOnline": is_online}


def _make_stats() -> list[dict]:
    return [{"time": 1700000000000, "value": "1.23"}]


# Fixed "now" for all tests: 2026-04-14 15:30 UTC
_NOW = datetime(2026, 4, 14, 15, 30, 0, tzinfo=UTC)


def test_default_mode_writes_yesterday():
    """No date params → 1 window: yesterday (2026-04-13), stat_date == '2026-04-13'."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_energy_stats.return_value = _make_stats()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 1

    run(tuya=fake_tuya, writer=fake_writer, run_id="r1", now=lambda: _NOW)

    fake_writer.load.assert_called_once()
    table_arg, rows = fake_writer.load.call_args.args
    assert table_arg == "raw_energy_daily"
    assert fake_writer.load.call_args.kwargs["schema"] == RAW_ENERGY_DAILY_SCHEMA
    assert len(rows) == 1
    assert rows[0]["stat_date"] == "2026-04-13"
    assert rows[0]["device_id"] == "d1"
    assert rows[0]["category"] == "znjdq"
    assert isinstance(rows[0]["payload"], list)
    yesterday = date(2026, 4, 13)
    start_ms = int(
        datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=UTC).timestamp() * 1000
    )
    next_day = (
        datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=UTC)
        + timedelta(days=1)
    )
    end_ms = int(next_day.timestamp() * 1000) - 1
    fake_tuya.get_energy_stats.assert_called_once_with("d1", "day", start_ms, end_ms)


def test_single_date_writes_one_window_per_device():
    """--date 2026-04-13 with 2 online devices → 2 rows, stat_date == '2026-04-13'."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "znjdq", True),
        _make_device("d2", "znjdq", True),
    ])
    fake_tuya.get_energy_stats.return_value = _make_stats()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 2

    run(
        tuya=fake_tuya, writer=fake_writer, run_id="r2",
        now=lambda: _NOW, date=date(2026, 4, 13),
    )

    _, rows = fake_writer.load.call_args.args
    assert len(rows) == 2
    assert fake_tuya.get_energy_stats.call_count == 2
    assert rows[0]["stat_date"] == "2026-04-13"
    assert rows[1]["stat_date"] == "2026-04-13"


def test_date_range_writes_one_window_per_day_per_device():
    """--start-date 2026-04-12 --end-date 2026-04-14 with 1 device → 3 rows."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_energy_stats.return_value = _make_stats()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 3

    run(
        tuya=fake_tuya, writer=fake_writer, run_id="r3",
        now=lambda: _NOW,
        start_date=date(2026, 4, 12), end_date=date(2026, 4, 14),
    )

    _, rows = fake_writer.load.call_args.args
    assert len(rows) == 3
    assert [r["stat_date"] for r in rows] == ["2026-04-12", "2026-04-13", "2026-04-14"]


def test_partial_date_range_raises():
    """start_date without end_date must raise ValueError (before device listing)."""
    fake_tuya = MagicMock()
    fake_writer = MagicMock()

    with pytest.raises(ValueError, match="start_date and end_date"):
        run(
            tuya=fake_tuya, writer=fake_writer, run_id="r_partial",
            now=lambda: _NOW, start_date=date(2026, 4, 13),
        )


def test_offline_device_skipped():
    """Offline device is skipped; only online device's row is written."""
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


def test_api_error_propagates():
    """HTTPStatusError from get_energy_stats must not be swallowed."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_energy_stats.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=MagicMock()
    )
    fake_writer = MagicMock()

    with pytest.raises(httpx.HTTPStatusError):
        run(tuya=fake_tuya, writer=fake_writer, run_id="r5", now=lambda: _NOW)
