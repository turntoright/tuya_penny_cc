# Energy Realtime Job Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `energy_realtime` ingestion job that reads current DP values from every online Tuya device and appends one row per device to `tuya_raw.raw_energy_realtime` in BigQuery.

**Architecture:** Extend `TuyaClient` with a `get_device_dps()` method, define a new BQ schema, implement `energy_realtime.py` following the same `run(tuya, writer, run_id, now)` protocol as `device_sync`, and wire it into the existing CLI dispatcher in `main.py`.

**Tech Stack:** Python 3.11, httpx, tenacity, pydantic-settings, typer, google-cloud-bigquery, pytest, respx, ruff.

---

## File Map

| Action | File |
|--------|------|
| Modify | `ingestion/src/tuya_penny_cc/tuya/client.py` — add `get_device_dps()` |
| Modify | `ingestion/src/tuya_penny_cc/bq/schemas.py` — add `RAW_ENERGY_REALTIME_SCHEMA` |
| Create | `ingestion/src/tuya_penny_cc/jobs/energy_realtime.py` |
| Modify | `ingestion/src/tuya_penny_cc/main.py` — add `Task.energy_realtime` and dispatch |
| Modify | `ingestion/tests/unit/tuya/test_client.py` — add `get_device_dps` tests |
| Create | `ingestion/tests/unit/jobs/test_energy_realtime.py` |

---

## Task 1: `TuyaClient.get_device_dps()`

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/tuya/client.py`
- Test: `ingestion/tests/unit/tuya/test_client.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/tuya/test_client.py`:

```python
def test_get_device_dps_returns_dp_list(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(
        return_value=_token_response()
    )
    mock_router.get("/v1.0/iot-03/devices/d1/status").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": [
                    {"code": "switch_1", "value": True},
                    {"code": "cur_power", "value": 120},
                ],
                "success": True,
                "t": 1700000000000,
            },
        )
    )
    c = make_client()
    dps = c.get_device_dps("d1")
    assert dps == [{"code": "switch_1", "value": True}, {"code": "cur_power", "value": 120}]


def test_get_device_dps_raises_on_api_error(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(
        return_value=_token_response()
    )
    mock_router.get("/v1.0/iot-03/devices/d1/status").mock(
        return_value=httpx.Response(
            200,
            json={"success": False, "code": 40000001, "msg": "not found"},
        )
    )
    c = make_client()
    with pytest.raises(httpx.HTTPStatusError):
        c.get_device_dps("d1")
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd ingestion
python -m pytest tests/unit/tuya/test_client.py::test_get_device_dps_returns_dp_list tests/unit/tuya/test_client.py::test_get_device_dps_raises_on_api_error -v
```

Expected: `FAILED` — `AttributeError: 'TuyaClient' object has no attribute 'get_device_dps'`

- [ ] **Step 3: Implement `get_device_dps()` in `client.py`**

Add after the `list_devices` method, before `# ---- lifecycle`:

```python
# ---- device DPs -------------------------------------------------------------

DPS_PATH = "/v1.0/iot-03/devices/{device_id}/status"

def get_device_dps(self, device_id: str) -> list[dict]:
    """Return current DP values for a single device.

    Calls /v1.0/iot-03/devices/{device_id}/status and returns the
    ``result`` list of {code, value} dicts. Returns an empty list if
    the endpoint returns no result.
    """
    access_token = self._get_access_token()
    path = self.DPS_PATH.format(device_id=device_id)
    payload = self._signed_request(
        method="GET",
        path=path,
        query=None,
        access_token=access_token,
    )
    return payload.get("result") or []
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ingestion
python -m pytest tests/unit/tuya/test_client.py::test_get_device_dps_returns_dp_list tests/unit/tuya/test_client.py::test_get_device_dps_raises_on_api_error -v
```

Expected: `2 passed`

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
cd ingestion
python -m pytest tests/ -v
```

Expected: all previously passing tests still pass, 2 new ones added.

- [ ] **Step 6: Lint**

```bash
cd ingestion
python -m ruff check src/ tests/
```

Expected: no output (no errors).

- [ ] **Step 7: Commit**

```bash
cd ingestion
git add src/tuya_penny_cc/tuya/client.py tests/unit/tuya/test_client.py
git commit -m "feat(tuya): add get_device_dps() to TuyaClient"
```

---

## Task 2: `RAW_ENERGY_REALTIME_SCHEMA`

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/bq/schemas.py`

- [ ] **Step 1: Add the schema to `schemas.py`**

Append after `RAW_DEVICES_SCHEMA`:

```python
RAW_ENERGY_REALTIME_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingest_run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
    bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingest_date", "DATE", mode="REQUIRED"),
]
```

- [ ] **Step 2: Lint**

```bash
cd ingestion
python -m ruff check src/tuya_penny_cc/bq/schemas.py
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
cd ingestion
git add src/tuya_penny_cc/bq/schemas.py
git commit -m "feat(bq): add RAW_ENERGY_REALTIME_SCHEMA"
```

---

## Task 3: `energy_realtime.py` Job

**Files:**
- Create: `ingestion/src/tuya_penny_cc/jobs/energy_realtime.py`
- Create: `ingestion/tests/unit/jobs/test_energy_realtime.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/jobs/test_energy_realtime.py`:

```python
from datetime import UTC, datetime
from unittest.mock import MagicMock

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
    import httpx

    fake_tuya = MagicMock()
    fake_tuya.list_devices.return_value = iter([_make_device("d1", "znjdq", True)])
    fake_tuya.get_device_dps.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=MagicMock()
    )
    fake_writer = MagicMock()

    import pytest
    with pytest.raises(httpx.HTTPStatusError):
        run(tuya=fake_tuya, writer=fake_writer, run_id="run-4",
            now=lambda: datetime(2026, 4, 14, tzinfo=UTC))
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd ingestion
python -m pytest tests/unit/jobs/test_energy_realtime.py -v
```

Expected: `ERROR` — `ModuleNotFoundError: No module named 'tuya_penny_cc.jobs.energy_realtime'`

- [ ] **Step 3: Create `energy_realtime.py`**

Create `src/tuya_penny_cc/jobs/energy_realtime.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ingestion
python -m pytest tests/unit/jobs/test_energy_realtime.py -v
```

Expected: `4 passed`

- [ ] **Step 5: Run full test suite**

```bash
cd ingestion
python -m pytest tests/ -v
```

Expected: all tests pass (previously 21 + 2 from Task 1 + 4 new = 27 total).

- [ ] **Step 6: Lint**

```bash
cd ingestion
python -m ruff check src/ tests/
```

Expected: no output.

- [ ] **Step 7: Commit**

```bash
cd ingestion
git add src/tuya_penny_cc/jobs/energy_realtime.py tests/unit/jobs/test_energy_realtime.py
git commit -m "feat(jobs): add energy_realtime job"
```

---

## Task 4: Wire into CLI (`main.py`)

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/main.py`

- [ ] **Step 1: Update `main.py`**

Replace the entire file content with:

```python
"""CLI entry point. Dispatches to per-task modules based on --task."""

from __future__ import annotations

import logging
import uuid
from enum import StrEnum

import typer
from google.cloud import bigquery

from tuya_penny_cc.bq.writer import BigQueryWriter
from tuya_penny_cc.config import Settings
from tuya_penny_cc.jobs import device_sync, energy_realtime
from tuya_penny_cc.tuya.client import TuyaClient

logger = logging.getLogger("tuya_penny_cc")


class Task(StrEnum):
    device_sync = "device_sync"
    energy_realtime = "energy_realtime"


def app() -> None:
    typer.run(_main)


def _main(
    task: Task = typer.Option(..., "--task", help="Which ingestion task to run."),  # noqa: B008
    log_level: str = typer.Option("INFO", "--log-level"),  # noqa: B008
) -> None:
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
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
            case _:
                raise typer.BadParameter(f"Unknown task: {task}")
        logger.info("task=%s wrote %d rows", task.value, written)
    finally:
        tuya.close()


if __name__ == "__main__":
    app()
```

- [ ] **Step 2: Run full test suite**

```bash
cd ingestion
python -m pytest tests/ -v
```

Expected: all 27 tests pass.

- [ ] **Step 3: Verify CLI help shows new task**

```bash
cd ingestion
python -m tuya_penny_cc --help
```

Expected: output shows `--task [device_sync|energy_realtime]`

- [ ] **Step 4: Lint**

```bash
cd ingestion
python -m ruff check src/ tests/
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
cd ingestion
git add src/tuya_penny_cc/main.py
git commit -m "feat(cli): wire energy_realtime task into CLI dispatcher"
```
