"""Energy realtime job.

Reads current DP values from every *online* device and appends one row
per device to `raw_energy_realtime`.

Offline devices are skipped silently — no row is written for them.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from tuya_penny_cc.bq.schemas import RAW_ENERGY_REALTIME_SCHEMA

_DPS_PATH = "/v1.0/iot-03/devices/{device_id}/status"
TABLE = "raw_energy_realtime"


class _TuyaLike(Protocol):
    def list_devices(self, *, page_size: int = 20) -> Any: ...
    def get_device_dps(self, device_id: str) -> list[dict]: ...


class _WriterLike(Protocol):
    def load(self, table: str, rows: list[dict[str, Any]], *, schema: list) -> int: ...


def run(
    *,
    tuya: _TuyaLike,
    writer: _WriterLike,
    run_id: str,
    now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    page_size: int = 20,
) -> int:
    """Run the energy realtime job. Returns the number of rows written."""
    ts = now()
    ts_iso = ts.isoformat()
    date_iso = ts.date().isoformat()

    rows: list[dict[str, Any]] = []
    for device in tuya.list_devices(page_size=page_size):
        if not device.get("isOnline"):
            continue
        device_id = device["id"]
        category = device.get("category", "")
        dps = tuya.get_device_dps(device_id)
        rows.append(
            {
                "ingest_ts": ts_iso,
                "ingest_run_id": run_id,
                "source_endpoint": _DPS_PATH.format(device_id=device_id),
                "payload": dps,
                "device_id": device_id,
                "category": category,
                "ingest_date": date_iso,
            }
        )
    return writer.load(TABLE, rows, schema=RAW_ENERGY_REALTIME_SCHEMA)
