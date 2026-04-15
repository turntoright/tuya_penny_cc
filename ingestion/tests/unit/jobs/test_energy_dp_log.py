from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import httpx
import pytest

from tuya_penny_cc.bq.schemas import RAW_ENERGY_DP_LOG_SCHEMA
from tuya_penny_cc.jobs.energy_dp_log import run


def _make_device(device_id: str, category: str, is_online: bool) -> dict:
    return {"id": device_id, "category": category, "isOnline": is_online}


def _make_events() -> list[dict]:
    return [
        {"code": "add_ele", "value": 100, "event_time": 1_700_000_000_000},
        {"code": "cur_power", "value": 50, "event_time": 1_700_000_060_000},
    ]


_NOW = datetime(2026, 4, 15, 10, 0, 0, tzinfo=UTC)


def test_default_mode_writes_yesterday():
    """No date params → 1 window: yesterday, log_date == '2026-04-14'."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_dp_log.return_value = _make_events()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 1

    run(tuya=fake_tuya, writer=fake_writer, run_id="r1", now=lambda: _NOW)

    fake_writer.load.assert_called_once()
    table_arg, rows = fake_writer.load.call_args.args
    assert table_arg == "raw_energy_dp_log"
    assert fake_writer.load.call_args.kwargs["schema"] == RAW_ENERGY_DP_LOG_SCHEMA
    assert len(rows) == 1
    assert rows[0]["log_date"] == "2026-04-14"
    assert rows[0]["device_id"] == "d1"
    assert rows[0]["category"] == "znjdq"
    assert isinstance(rows[0]["payload"], list)
    yesterday = date(2026, 4, 14)
    start_ms = int(
        datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=UTC).timestamp() * 1000
    )
    next_day = datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=UTC) + timedelta(days=1)
    end_ms = int(next_day.timestamp() * 1000) - 1
    fake_tuya.get_dp_log.assert_called_once_with("d1", ["add_ele", "cur_power", "cur_current", "cur_voltage"], start_ms, end_ms)


def test_single_date_writes_one_row_per_device():
    """--date 2026-04-14 with 2 online devices → writer called once per device, 1 row each."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "znjdq", True),
        _make_device("d2", "znjdq", True),
    ])
    fake_tuya.get_dp_log.return_value = _make_events()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 1

    run(tuya=fake_tuya, writer=fake_writer, run_id="r2", now=lambda: _NOW, date=date(2026, 4, 14))

    assert fake_writer.load.call_count == 2
    all_rows = [call.args[1] for call in fake_writer.load.call_args_list]
    assert all(len(rows) == 1 for rows in all_rows)
    assert all(rows[0]["log_date"] == "2026-04-14" for rows in all_rows)
    device_ids = [rows[0]["device_id"] for rows in all_rows]
    assert sorted(device_ids) == ["d1", "d2"]


def test_date_range_writes_one_row_per_device_per_day():
    """--start-date 2026-04-12 --end-date 2026-04-14 with 1 device → 3 rows."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_dp_log.return_value = _make_events()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 3

    run(
        tuya=fake_tuya, writer=fake_writer, run_id="r3",
        now=lambda: _NOW,
        start_date=date(2026, 4, 12), end_date=date(2026, 4, 14),
    )

    _, rows = fake_writer.load.call_args.args
    assert len(rows) == 3
    assert [r["log_date"] for r in rows] == ["2026-04-12", "2026-04-13", "2026-04-14"]


def test_offline_device_skipped():
    """Offline device is skipped; only online device produces a row."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "znjdq", True),
        _make_device("d2", "dlq", False),
    ])
    fake_tuya.get_dp_log.return_value = _make_events()
    fake_writer = MagicMock()
    fake_writer.load.return_value = 1

    run(tuya=fake_tuya, writer=fake_writer, run_id="r4", now=lambda: _NOW)

    _, rows = fake_writer.load.call_args.args
    assert all(r["device_id"] == "d1" for r in rows)
    assert fake_tuya.get_dp_log.call_count == 1


def test_api_error_propagates():
    """HTTPStatusError from get_dp_log must not be swallowed."""
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_dp_log.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=MagicMock()
    )
    fake_writer = MagicMock()

    with pytest.raises(httpx.HTTPStatusError):
        run(tuya=fake_tuya, writer=fake_writer, run_id="r5", now=lambda: _NOW)
