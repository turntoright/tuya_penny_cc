"""Energy DP log job.

Fetches Tuya device DP change history per online device and appends
one row per (device, date) to `raw_energy_dp_log`.

Offline devices are skipped silently.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from datetime import date as _date
from typing import Any, Protocol

from tuya_penny_cc.bq.schemas import RAW_ENERGY_DP_LOG_SCHEMA
from tuya_penny_cc.tuya.client import TuyaClient

TABLE = "raw_energy_dp_log"
DP_CODES = ["add_ele"]


class _TuyaLike(Protocol):
    def list_devices(self) -> Any: ...
    def get_dp_log(
        self,
        device_id: str,
        codes: list[str],
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict]: ...


class _WriterLike(Protocol):
    def load(self, table: str, rows: list[dict[str, Any]], *, schema: list) -> int: ...


def _date_windows(
    now: datetime,
    date: _date | None,
    start_date: _date | None,
    end_date: _date | None,
) -> list[_date]:
    """Return list of dates to fetch DP logs for."""
    if date is not None:
        return [date]
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must both be provided or both omitted")
    if start_date is not None and end_date is not None:
        windows: list[_date] = []
        current = start_date
        while current <= end_date:
            windows.append(current)
            current += timedelta(days=1)
        return windows
    return [(now - timedelta(days=1)).date()]


def run(
    *,
    tuya: _TuyaLike,
    writer: _WriterLike,
    run_id: str,
    now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    date: _date | None = None,
    start_date: _date | None = None,
    end_date: _date | None = None,
) -> int:
    """Run the energy DP log job. Returns the number of rows written."""
    ts = now()
    ts_iso = ts.isoformat()
    ingest_date_iso = ts.date().isoformat()
    windows = _date_windows(ts, date, start_date, end_date)

    total_written = 0
    for device in tuya.list_devices():
        if not device.get("isOnline"):
            continue
        device_id = device["id"]
        category = device["category"]
        device_rows: list[dict[str, Any]] = []
        for d in windows:
            start_ms = int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)
            next_day = datetime(d.year, d.month, d.day, tzinfo=UTC) + timedelta(days=1)
            end_ms = int(next_day.timestamp() * 1000) - 1
            events = tuya.get_dp_log(device_id, DP_CODES, start_ms, end_ms)
            device_rows.append(
                {
                    "device_id": device_id,
                    "category": category,
                    "log_date": d.isoformat(),
                    "ingest_ts": ts_iso,
                    "ingest_run_id": run_id,
                    "ingest_date": ingest_date_iso,
                    "source_endpoint": TuyaClient.DP_LOG_PATH.format(
                        device_id=device_id
                    ),
                    "payload": events,
                }
            )
        # Write after each device so partial progress is preserved on failure.
        total_written += writer.load(TABLE, device_rows, schema=RAW_ENERGY_DP_LOG_SCHEMA)
    return total_written
