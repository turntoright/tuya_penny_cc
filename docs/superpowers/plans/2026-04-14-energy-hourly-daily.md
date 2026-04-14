# Energy Hourly & Daily Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `energy_hourly` and `energy_daily` ingestion jobs that fetch Tuya server-side aggregated energy stats per online device and append rows to `tuya_raw.raw_energy_hourly` / `tuya_raw.raw_energy_daily`, with optional backfill via `--date` or `--start-date/--end-date`.

**Architecture:** Extend `TuyaClient` with `get_energy_stats()`, add two BQ schemas, implement two job modules (`energy_hourly.py`, `energy_daily.py`) following the same `run(*, tuya, writer, run_id, now, date, start_date, end_date)` protocol as `energy_realtime`, and wire both into `main.py` with new optional date CLI params.

**Tech Stack:** Python 3.11, httpx, tenacity, pydantic-settings, typer, google-cloud-bigquery, pytest, respx, ruff.

---

## File Map

| Action | File |
|--------|------|
| Modify | `ingestion/src/tuya_penny_cc/tuya/client.py` — add `get_energy_stats()` |
| Modify | `ingestion/src/tuya_penny_cc/bq/schemas.py` — add `RAW_ENERGY_HOURLY_SCHEMA`, `RAW_ENERGY_DAILY_SCHEMA` |
| Create | `ingestion/src/tuya_penny_cc/jobs/energy_hourly.py` |
| Create | `ingestion/src/tuya_penny_cc/jobs/energy_daily.py` |
| Modify | `ingestion/src/tuya_penny_cc/main.py` — add two tasks + date CLI params |
| Modify | `ingestion/tests/unit/tuya/test_client.py` — add `get_energy_stats` tests |
| Create | `ingestion/tests/unit/jobs/test_energy_hourly.py` |
| Create | `ingestion/tests/unit/jobs/test_energy_daily.py` |

---

## Task 1: `TuyaClient.get_energy_stats()`

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/tuya/client.py`
- Test: `ingestion/tests/unit/tuya/test_client.py`

The starting endpoint is `/v1.0/iot-03/devices/{device_id}/statistics-month` with query params `type`, `start_time`, `end_time`. **During smoke testing against the real API, the path and param names may need adjustment** — the test mocks will need updating to match.

- [ ] **Step 1: Write the failing tests**

Append to `ingestion/tests/unit/tuya/test_client.py`:

```python
def test_get_energy_stats_returns_list(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(
        return_value=_token_response()
    )
    mock_router.get("/v1.0/iot-03/devices/d1/statistics-month").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": [{"time": 1700000000000, "value": "1.23"}],
                "success": True,
                "t": 1700000000000,
            },
        )
    )
    c = make_client()
    stats = c.get_energy_stats("d1", "hour", 1700000000000, 1700003599000)
    assert stats == [{"time": 1700000000000, "value": "1.23"}]


def test_get_energy_stats_raises_on_api_error(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(
        return_value=_token_response()
    )
    mock_router.get("/v1.0/iot-03/devices/d1/statistics-month").mock(
        return_value=httpx.Response(
            200,
            json={"success": False, "code": 40000001, "msg": "device not found"},
        )
    )
    c = make_client()
    with pytest.raises(httpx.HTTPStatusError):
        c.get_energy_stats("d1", "hour", 1700000000000, 1700003599000)


def test_get_energy_stats_returns_empty_list_when_no_result(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(
        return_value=_token_response()
    )
    mock_router.get("/v1.0/iot-03/devices/d1/statistics-month").mock(
        return_value=httpx.Response(
            200,
            json={"success": True, "t": 1700000000000},
        )
    )
    c = make_client()
    stats = c.get_energy_stats("d1", "hour", 1700000000000, 1700003599000)
    assert stats == []
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/unit/tuya/test_client.py::test_get_energy_stats_returns_list tests/unit/tuya/test_client.py::test_get_energy_stats_raises_on_api_error tests/unit/tuya/test_client.py::test_get_energy_stats_returns_empty_list_when_no_result -v
```

Expected: `FAILED` — `AttributeError: 'TuyaClient' object has no attribute 'get_energy_stats'`

- [ ] **Step 3: Implement `get_energy_stats()` in `client.py`**

Add after the `# ---- device DPs` block, before `# ---- lifecycle`:

```python
# ---- energy stats -----------------------------------------------------------

ENERGY_STATS_PATH = "/v1.0/iot-03/devices/{device_id}/statistics-month"

def get_energy_stats(
    self,
    device_id: str,
    granularity: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> list[dict]:
    """Return aggregated energy stats for a single device.

    NOTE: Endpoint path and query param names are best-guess starting points
    based on Tuya OpenAPI conventions. Verify against actual API response
    during smoke testing and adjust path/params as needed.

    Args:
        device_id: Tuya device ID.
        granularity: ``"hour"`` or ``"day"``.
        start_ts_ms: Window start as Unix milliseconds.
        end_ts_ms: Window end as Unix milliseconds.
    """
    access_token = self._get_access_token()
    path = self.ENERGY_STATS_PATH.format(device_id=quote(device_id, safe=""))
    payload = self._signed_request(
        method="GET",
        path=path,
        query={
            "type": granularity,
            "start_time": str(start_ts_ms),
            "end_time": str(end_ts_ms),
        },
        access_token=access_token,
    )
    return payload.get("result") or []
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/unit/tuya/test_client.py::test_get_energy_stats_returns_list tests/unit/tuya/test_client.py::test_get_energy_stats_raises_on_api_error tests/unit/tuya/test_client.py::test_get_energy_stats_returns_empty_list_when_no_result -v
```

Expected: `3 passed`

- [ ] **Step 5: Run full test suite**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/ -v
```

Expected: 30 passed (27 existing + 3 new).

- [ ] **Step 6: Lint**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m ruff check src/ tests/
```

Expected: no output.

- [ ] **Step 7: Commit**

```bash
cd D:/git/dbt_projects/tuya_penny_cc && git add ingestion/src/tuya_penny_cc/tuya/client.py ingestion/tests/unit/tuya/test_client.py && git commit -m "feat(tuya): add get_energy_stats() to TuyaClient"
```

---

## Task 2: BQ Schemas

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/bq/schemas.py`

- [ ] **Step 1: Add both schemas to `schemas.py`**

Append after `RAW_ENERGY_REALTIME_SCHEMA`:

```python
RAW_ENERGY_HOURLY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("stat_hour", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingest_run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingest_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
]

RAW_ENERGY_DAILY_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("stat_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingest_run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingest_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
]
```

- [ ] **Step 2: Lint**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m ruff check src/tuya_penny_cc/bq/schemas.py
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
cd D:/git/dbt_projects/tuya_penny_cc && git add ingestion/src/tuya_penny_cc/bq/schemas.py && git commit -m "feat(bq): add RAW_ENERGY_HOURLY_SCHEMA and RAW_ENERGY_DAILY_SCHEMA"
```

---

## Task 3: `energy_hourly.py` Job

**Files:**
- Create: `ingestion/src/tuya_penny_cc/jobs/energy_hourly.py`
- Create: `ingestion/tests/unit/jobs/test_energy_hourly.py`

- [ ] **Step 1: Write the failing tests**

Create `ingestion/tests/unit/jobs/test_energy_hourly.py`:

```python
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


def test_api_error_propagates():
    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_energy_stats.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=MagicMock()
    )
    fake_writer = MagicMock()

    with pytest.raises(httpx.HTTPStatusError):
        run(tuya=fake_tuya, writer=fake_writer, run_id="r5", now=lambda: _NOW)
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/unit/jobs/test_energy_hourly.py -v
```

Expected: `ERROR` — `ModuleNotFoundError: No module named 'tuya_penny_cc.jobs.energy_hourly'`

- [ ] **Step 3: Create `energy_hourly.py`**

Create `ingestion/src/tuya_penny_cc/jobs/energy_hourly.py`:

```python
"""Energy hourly job.

Fetches Tuya server-side hourly energy aggregations per online device
and appends one row per (device, hour window) to `raw_energy_hourly`.

Offline devices are skipped silently.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date as _date, datetime, timedelta
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/unit/jobs/test_energy_hourly.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Run full test suite**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/ -v
```

Expected: 35 passed (30 + 5 new).

- [ ] **Step 6: Lint**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m ruff check src/ tests/
```

Expected: no output.

- [ ] **Step 7: Commit**

```bash
cd D:/git/dbt_projects/tuya_penny_cc && git add ingestion/src/tuya_penny_cc/jobs/energy_hourly.py ingestion/tests/unit/jobs/test_energy_hourly.py && git commit -m "feat(jobs): add energy_hourly job"
```

---

## Task 4: `energy_daily.py` Job

**Files:**
- Create: `ingestion/src/tuya_penny_cc/jobs/energy_daily.py`
- Create: `ingestion/tests/unit/jobs/test_energy_daily.py`

- [ ] **Step 1: Write the failing tests**

Create `ingestion/tests/unit/jobs/test_energy_daily.py`:

```python
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import httpx
import pytest

from tuya_penny_cc.bq.schemas import RAW_ENERGY_DAILY_SCHEMA
from tuya_penny_cc.jobs.energy_daily import run


def _make_device(device_id: str, category: str, is_online: bool) -> dict:
    return {"id": device_id, "category": category, "isOnline": is_online}


def _make_stats() -> list[dict]:
    return [{"time": 1700000000000, "value": "25.6"}]


# Fixed "now" for all tests: 2026-04-14 15:30 UTC
_NOW = datetime(2026, 4, 14, 15, 30, 0, tzinfo=UTC)


def test_default_mode_writes_yesterday():
    """No date params → 1 window: 2026-04-13 (yesterday)."""
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
    fake_tuya.get_energy_stats.assert_called_once_with(
        "d1", "day",
        int(datetime(2026, 4, 13, 0, 0, 0, tzinfo=UTC).timestamp() * 1000),
        int(datetime(2026, 4, 13, 23, 59, 59, tzinfo=UTC).timestamp() * 1000),
    )


def test_single_date_writes_one_window_per_device():
    """--date 2026-04-13 with 2 online devices → 2 rows."""
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
    assert rows[0]["stat_date"] == "2026-04-13"
    assert fake_tuya.get_energy_stats.call_count == 2


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
    assert len(rows) == 1
    assert rows[0]["device_id"] == "d1"
    fake_tuya.get_energy_stats.assert_called_once_with(
        "d1", "day",
        int(datetime(2026, 4, 13, 0, 0, 0, tzinfo=UTC).timestamp() * 1000),
        int(datetime(2026, 4, 13, 23, 59, 59, tzinfo=UTC).timestamp() * 1000),
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
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/unit/jobs/test_energy_daily.py -v
```

Expected: `ERROR` — `ModuleNotFoundError: No module named 'tuya_penny_cc.jobs.energy_daily'`

- [ ] **Step 3: Create `energy_daily.py`**

Create `ingestion/src/tuya_penny_cc/jobs/energy_daily.py`:

```python
"""Energy daily job.

Fetches Tuya server-side daily energy aggregations per online device
and appends one row per (device, date window) to `raw_energy_daily`.

Offline devices are skipped silently.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date as _date, datetime, timedelta
from typing import Any, Protocol

from tuya_penny_cc.bq.schemas import RAW_ENERGY_DAILY_SCHEMA
from tuya_penny_cc.tuya.client import TuyaClient

TABLE = "raw_energy_daily"


class _TuyaLike(Protocol):
    def list_devices(self, *, page_size: int = 20) -> Any: ...
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
    """Return list of UTC dates to fetch."""
    if date is not None:
        return [date]
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
    page_size: int = 20,
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
    for device in tuya.list_devices(page_size=page_size):
        if not device.get("isOnline"):
            continue
        device_id = device["id"]
        category = device["category"]
        for window in windows:
            day_start = datetime(window.year, window.month, window.day, tzinfo=UTC)
            day_end = datetime(window.year, window.month, window.day, 23, 59, 59, tzinfo=UTC)
            start_ms = int(day_start.timestamp() * 1000)
            end_ms = int(day_end.timestamp() * 1000)
            stats = tuya.get_energy_stats(device_id, "day", start_ms, end_ms)
            rows.append(
                {
                    "device_id": device_id,
                    "category": category,
                    "stat_date": window.isoformat(),
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/unit/jobs/test_energy_daily.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Run full test suite**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/ -v
```

Expected: 40 passed (35 + 5 new).

- [ ] **Step 6: Lint**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m ruff check src/ tests/
```

Expected: no output.

- [ ] **Step 7: Commit**

```bash
cd D:/git/dbt_projects/tuya_penny_cc && git add ingestion/src/tuya_penny_cc/jobs/energy_daily.py ingestion/tests/unit/jobs/test_energy_daily.py && git commit -m "feat(jobs): add energy_daily job"
```

---

## Task 5: Wire into CLI (`main.py`)

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/main.py`

- [ ] **Step 1: Replace the entire content of `main.py`**

```python
"""CLI entry point. Dispatches to per-task modules based on --task."""

from __future__ import annotations

import logging
import uuid
from datetime import date as _date
from enum import StrEnum
from typing import assert_never

import typer
from google.cloud import bigquery

from tuya_penny_cc.bq.writer import BigQueryWriter
from tuya_penny_cc.config import Settings
from tuya_penny_cc.jobs import device_sync, energy_daily, energy_hourly, energy_realtime
from tuya_penny_cc.tuya.client import TuyaClient

logger = logging.getLogger("tuya_penny_cc")


class Task(StrEnum):
    device_sync = "device_sync"
    energy_realtime = "energy_realtime"
    energy_hourly = "energy_hourly"
    energy_daily = "energy_daily"


def app() -> None:
    typer.run(_main)


def _main(
    task: Task = typer.Option(..., "--task", help="Which ingestion task to run."),  # noqa: B008
    log_level: str = typer.Option("INFO", "--log-level"),  # noqa: B008
    date_str: str | None = typer.Option(None, "--date", help="Backfill single date YYYY-MM-DD."),  # noqa: B008
    start_date_str: str | None = typer.Option(None, "--start-date", help="Range start YYYY-MM-DD."),  # noqa: B008
    end_date_str: str | None = typer.Option(None, "--end-date", help="Range end YYYY-MM-DD."),  # noqa: B008
) -> None:
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # Parse and validate date params
    parsed_date = _date.fromisoformat(date_str) if date_str else None
    parsed_start = _date.fromisoformat(start_date_str) if start_date_str else None
    parsed_end = _date.fromisoformat(end_date_str) if end_date_str else None
    if parsed_date and (parsed_start or parsed_end):
        raise typer.BadParameter("--date and --start-date/--end-date are mutually exclusive.")
    if bool(parsed_start) != bool(parsed_end):
        raise typer.BadParameter("--start-date and --end-date must be used together.")

    settings = Settings()
    run_id = uuid.uuid4().hex
    logger.info("starting task=%s run_id=%s", task.value, run_id)

    bq_client = bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)
    writer = BigQueryWriter(
        client=bq_client,
        project=settings.gcp_project_id,
        dataset=settings.bq_dataset_raw,
    )
    tuya = TuyaClient(
        base_url=settings.tuya_base_url,
        access_id=settings.tuya_access_id,
        access_secret=settings.tuya_access_secret.get_secret_value(),
        user_uid=settings.tuya_user_uid,
    )
    try:
        match task:
            case Task.device_sync:
                written = device_sync.run(tuya=tuya, writer=writer, run_id=run_id)
            case Task.energy_realtime:
                written = energy_realtime.run(tuya=tuya, writer=writer, run_id=run_id)
            case Task.energy_hourly:
                written = energy_hourly.run(
                    tuya=tuya, writer=writer, run_id=run_id,
                    date=parsed_date, start_date=parsed_start, end_date=parsed_end,
                )
            case Task.energy_daily:
                written = energy_daily.run(
                    tuya=tuya, writer=writer, run_id=run_id,
                    date=parsed_date, start_date=parsed_start, end_date=parsed_end,
                )
            case _ as unreachable:
                assert_never(unreachable)
        logger.info("task=%s wrote %d rows", task.value, written)
    finally:
        tuya.close()


if __name__ == "__main__":
    app()
```

- [ ] **Step 2: Run full test suite**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m pytest tests/ -v
```

Expected: all 40 tests pass.

- [ ] **Step 3: Verify CLI help shows all four tasks**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m tuya_penny_cc --help
```

Expected: output includes `--task [device_sync|energy_realtime|energy_hourly|energy_daily]` and `--date`, `--start-date`, `--end-date` options.

- [ ] **Step 4: Lint**

```bash
cd D:/git/dbt_projects/tuya_penny_cc/ingestion && python -m ruff check src/ tests/
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
cd D:/git/dbt_projects/tuya_penny_cc && git add ingestion/src/tuya_penny_cc/main.py && git commit -m "feat(cli): add energy_hourly and energy_daily tasks with date backfill params"
```
