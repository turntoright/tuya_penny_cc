"""Energy hourly job.

Fetches Tuya server-side hourly energy aggregations per online device
and appends one row per (device, hour window) to `raw_energy_hourly`.

Offline devices are skipped silently.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from datetime import date as _date
from typing import Any, Protocol

from tuya_penny_cc.bq.schemas import RAW_ENERGY_HOURLY_SCHEMA
from tuya_penny_cc.tuya.client import TuyaClient

TABLE = "raw_energy_hourly"


class _TuyaLike(Protocol):
    def list_devices(self, *, page_size: int = 20) -> Any: ...
    def get_energy_stats(
        self, device_id: str, granularity: str, start_ts_ms: int, end_ts_ms: int
    ) -> list[dict]: ...


class _WriterLike(Protocol):
    def load(self, table: str, rows: list[dict[str, Any]], *, schema: list) -> int: ...


def _hour_windows(
    now: datetime,
    date: _date | None,
    start_date: _date | None,
    end_date: _date | None,
) -> list[datetime]:
    """Return list of UTC hour-window start datetimes."""
    if date is not None:
        base = datetime(date.year, date.month, date.day, tzinfo=UTC)
        return [base + timedelta(hours=h) for h in range(24)]
    if start_date is not None and end_date is not None:
        windows: list[datetime] = []
        current = start_date
        while current <= end_date:
            base = datetime(current.year, current.month, current.day, tzinfo=UTC)
            windows.extend([base + timedelta(hours=h) for h in range(24)])
            current += timedelta(days=1)
        return windows
    # Default: previous complete hour
    return [now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)]


def run(
    *,
    tuya: _TuyaLike,
    writer: _WriterLike,
    run_id: str,
    now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    page_size: int = 20,
    date: _date | None = None,
    start_date: _date | None = None,
    end_date: _date | None = None,
) -> int:
    """Run the energy hourly job. Returns the number of rows written."""
    ts = now()
    ts_iso = ts.isoformat()
    ingest_date_iso = ts.date().isoformat()
    windows = _hour_windows(ts, date, start_date, end_date)

    rows: list[dict[str, Any]] = []
    for device in tuya.list_devices(page_size=page_size):
        if not device.get("isOnline"):
            continue
        device_id = device["id"]
        category = device["category"]
        for window in windows:
            start_ms = int(window.timestamp() * 1000)
            end_ms = int((window + timedelta(hours=1)).timestamp() * 1000) - 1
            stats = tuya.get_energy_stats(device_id, "hour", start_ms, end_ms)
            rows.append(
                {
                    "device_id": device_id,
                    "category": category,
                    "stat_hour": window.isoformat(),
                    "ingest_ts": ts_iso,
                    "ingest_run_id": run_id,
                    "ingest_date": ingest_date_iso,
                    "source_endpoint": TuyaClient.ENERGY_STATS_PATH.format(
                        device_id=device_id
                    ),
                    "payload": stats,
                }
            )
    return writer.load(TABLE, rows, schema=RAW_ENERGY_HOURLY_SCHEMA)
