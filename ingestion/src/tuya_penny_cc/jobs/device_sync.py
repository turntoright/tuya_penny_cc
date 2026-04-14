"""Device-list sync job.

Reads the full device list from Tuya, wraps each device dict into a raw
row with lineage columns, and batch-loads to `raw_devices`.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from tuya_penny_cc.bq.schemas import RAW_DEVICES_SCHEMA

SOURCE_ENDPOINT = "/v1.3/iot-03/devices"
TABLE = "raw_devices"


class _TuyaLike(Protocol):
    def list_devices(self, *, page_size: int = 100) -> Any: ...


class _WriterLike(Protocol):
    def load(self, table: str, rows: list[dict[str, Any]], *, schema: list) -> int: ...


def run(
    *,
    tuya: _TuyaLike,
    writer: _WriterLike,
    run_id: str,
    now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    page_size: int = 100,
) -> int:
    """Run the device sync. Returns number of rows written."""
    ts = now()
    ts_iso = ts.isoformat()
    date_iso = ts.date().isoformat()

    rows: list[dict[str, Any]] = []
    for device in tuya.list_devices(page_size=page_size):
        rows.append(
            {
                "ingest_ts": ts_iso,
                "ingest_run_id": run_id,
                "source_endpoint": SOURCE_ENDPOINT,
                "payload": json.dumps(device, ensure_ascii=False),
                "device_id": device["id"],
                "ingest_date": date_iso,
            }
        )
    return writer.load(TABLE, rows, schema=RAW_DEVICES_SCHEMA)
