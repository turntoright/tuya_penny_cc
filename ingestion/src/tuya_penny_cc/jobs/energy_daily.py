"""Energy daily job.

Fetches Tuya server-side daily energy aggregations per online device
and appends one row per (device, day window) to `raw_energy_daily`.

Offline devices are skipped silently.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from datetime import date as _date
from typing import Any, Protocol

from tuya_penny_cc.bq.schemas import RAW_ENERGY_DAILY_SCHEMA
from tuya_penny_cc.tuya.client import TuyaClient

TABLE = "raw_energy_daily"


class _TuyaLike(Protocol):
    def list_devices(self) -> Any: ...
    def get_energy_stats(
        self, device_id: str, granularity: str, start_ts_ms: int, end_ts_ms: int
    ) -> list[dict]: ...


class _WriterLike(Protocol):
    def load(self, table: str, rows: list[dict[str, Any]], *, schema: list) -> int: ...


def _date_windows(
    now: datetime,
    date: _date | None,
    start_date: _date | None,
    end_date: _date | None,
) -> list[_date]:
    """Return list of dates representing day windows."""
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
    # Default: yesterday
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
    """Run the energy daily job. Returns the number of rows written."""
    ts = now()
    ts_iso = ts.isoformat()
    ingest_date_iso = ts.date().isoformat()
    windows = _date_windows(ts, date, start_date, end_date)

    rows: list[dict[str, Any]] = []
    for device in tuya.list_devices():
        if not device.get("isOnline"):
            continue
        device_id = device["id"]
        category = device["category"]
        for d in windows:
            start_ms = int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)
            next_day = datetime(d.year, d.month, d.day, tzinfo=UTC) + timedelta(days=1)
            end_ms = int(next_day.timestamp() * 1000) - 1
            stats = tuya.get_energy_stats(device_id, "day", start_ms, end_ms)
            rows.append(
                {
                    "device_id": device_id,
                    "category": category,
                    "stat_date": d.isoformat(),
                    "ingest_ts": ts_iso,
                    "ingest_run_id": run_id,
                    "ingest_date": ingest_date_iso,
                    "source_endpoint": TuyaClient.ENERGY_STATS_PATH.format(
                        device_id=device_id
                    ),
                    "payload": stats,
                }
            )
    return writer.load(TABLE, rows, schema=RAW_ENERGY_DAILY_SCHEMA)
