from datetime import UTC, datetime
from unittest.mock import MagicMock

import httpx
import pytest

from tuya_penny_cc.bq.schemas import RAW_ENERGY_REALTIME_SCHEMA
from tuya_penny_cc.jobs.energy_realtime import run


def _make_device(device_id: str, category: str, is_online: bool) -> dict:
    return {"id": device_id, "category": category, "isOnline": is_online}


def _make_dps(device_id: str) -> list[dict]:
    return [{"code": "cur_power", "value": 100 + ord(device_id[-1])}]


def test_run_writes_one_row_per_online_device():
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "znjdq", True),
        _make_device("d2", "znjdq", True),
    ])
    fake_tuya.get_device_dps.side_effect = [_make_dps("d1"), _make_dps("d2")]
    fake_writer = MagicMock()
    fake_writer.load.return_value = 2

    fixed_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)
    result = run(tuya=fake_tuya, writer=fake_writer, run_id="run-1", now=lambda: fixed_now)

    assert result == 2
    fake_writer.load.assert_called_once()
    table_arg, rows_arg = fake_writer.load.call_args.args
    assert table_arg == "raw_energy_realtime"
    assert fake_writer.load.call_args.kwargs["schema"] == RAW_ENERGY_REALTIME_SCHEMA
    assert len(rows_arg) == 2

    first = rows_arg[0]
    assert first["device_id"] == "d1"
    assert first["category"] == "znjdq"
    assert first["ingest_run_id"] == "run-1"
    assert first["ingest_ts"] == "2026-04-14T12:00:00+00:00"
    assert first["ingest_date"] == "2026-04-14"
    assert first["source_endpoint"] == "/v1.0/iot-03/devices/d1/status"
    assert isinstance(first["payload"], list)
    assert first["payload"][0]["code"] == "cur_power"


def test_run_skips_offline_devices():
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "znjdq", True),
        _make_device("d2", "dlq", False),   # offline — should be skipped
    ])
    fake_tuya.get_device_dps.return_value = _make_dps("d1")
    fake_writer = MagicMock()
    fake_writer.load.return_value = 1

    run(tuya=fake_tuya, writer=fake_writer, run_id="run-2",
        now=lambda: datetime(2026, 4, 14, tzinfo=UTC))

    _, rows_arg = fake_writer.load.call_args.args
    assert len(rows_arg) == 1
    assert rows_arg[0]["device_id"] == "d1"
    # get_device_dps must NOT be called for offline device
    assert fake_tuya.get_device_dps.call_count == 1
    fake_tuya.get_device_dps.assert_called_once_with("d1")


def test_run_all_devices_offline_writes_zero_rows():
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([
        _make_device("d1", "dlq", False),
        _make_device("d2", "dlq", False),
    ])
    fake_writer = MagicMock()
    fake_writer.load.return_value = 0

    result = run(tuya=fake_tuya, writer=fake_writer, run_id="run-3",
                 now=lambda: datetime(2026, 4, 14, tzinfo=UTC))

    assert result == 0
    fake_tuya.get_device_dps.assert_not_called()
    fake_writer.load.assert_called_once()
    _, rows_arg = fake_writer.load.call_args.args
    assert rows_arg == []


def test_run_propagates_dp_error():
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_device_dps.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=MagicMock()
    )
    fake_writer = MagicMock()

    with pytest.raises(httpx.HTTPStatusError):
        run(tuya=fake_tuya, writer=fake_writer, run_id="run-4",
            now=lambda: datetime(2026, 4, 14, tzinfo=UTC))
