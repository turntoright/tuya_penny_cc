import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

from tuya_penny_cc.bq.schemas import RAW_DEVICES_SCHEMA
from tuya_penny_cc.jobs.device_sync import run


def test_run_writes_one_row_per_device_with_lineage():
    fake_client = MagicMock()
    fake_client.list_devices.return_value = iter(
        [
            {"id": "d1", "name": "Switch A", "category": "kg"},
            {"id": "d2", "name": "Switch B", "category": "kg"},
        ]
    )
    fake_writer = MagicMock()
    fake_writer.load.return_value = 2

    fixed_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)
    rows_written = run(
        tuya=fake_client,
        writer=fake_writer,
        run_id="run-uuid-1",
        now=lambda: fixed_now,
    )

    assert rows_written == 2
    fake_writer.load.assert_called_once()
    table_arg, rows_arg = fake_writer.load.call_args.args
    assert table_arg == "raw_devices"
    assert fake_writer.load.call_args.kwargs["schema"] == RAW_DEVICES_SCHEMA
    assert len(rows_arg) == 2

    first = rows_arg[0]
    assert first["device_id"] == "d1"
    assert first["ingest_run_id"] == "run-uuid-1"
    assert first["ingest_ts"] == "2026-04-14T12:00:00+00:00"
    assert first["ingest_date"] == "2026-04-14"
    assert first["source_endpoint"] == "/v1.3/iot-03/devices"
    assert json.loads(first["payload"])["name"] == "Switch A"


def test_run_with_no_devices_writes_nothing_but_returns_zero():
    fake_client = MagicMock()
    fake_client.list_devices.return_value = iter([])
    fake_writer = MagicMock()
    fake_writer.load.return_value = 0

    n = run(
        tuya=fake_client,
        writer=fake_writer,
        run_id="r",
        now=lambda: datetime(2026, 4, 14, tzinfo=UTC),
    )
    assert n == 0
    # Writer is still called with [] so behavior is consistent; writer no-ops.
    fake_writer.load.assert_called_once()
