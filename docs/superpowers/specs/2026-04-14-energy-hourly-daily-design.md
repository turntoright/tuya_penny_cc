# Plan B — Energy Hourly & Daily Jobs Design

**Date:** 2026-04-14  
**Branch:** `feat/plan-b-energy-hourly`  
**Scope:** `energy_hourly` and `energy_daily` jobs (both designed together; backfill support included)

---

## Background

Plan B `energy_realtime` is complete. This spec covers the two remaining aggregated energy jobs:

- `energy_hourly` — Tuya server-side hourly aggregation, one row per device per hour window
- `energy_daily` — Tuya server-side daily aggregation, one row per device per day

The Tuya aggregation endpoint is unknown at design time. Starting point: `/v1.0/iot-03/devices/{device_id}/statistics-month`. Actual path and response structure will be confirmed during implementation via inline smoke testing (same approach used for v2.0 device list endpoint).

Device inventory:
- 12 × `znjdq` A5 smart relay switches — all online
- 5 × `dlq` WIFI Smart Meter Pro W — all offline

---

## Goals

- Capture Tuya server-side hourly and daily energy aggregations per online device
- Support default mode (previous complete period) and backfill via `--date` or `--start-date/--end-date`
- Append-only raw storage; deduplication delegated to dbt staging layer
- Skip offline devices (consistent with `energy_realtime`)

---

## Architecture

### Data Flow

```
Tuya API (energy aggregation endpoint)
    ↓  per online device, per time window
TuyaClient.get_energy_stats(device_id, granularity, start_ts_ms, end_ts_ms)
    ↓
energy_hourly.run() / energy_daily.run()
    ↓  lineage field wrapping
BigQueryWriter → raw_energy_hourly / raw_energy_daily
```

### CLI

```bash
# Default: previous complete period
python -m tuya_penny_cc --task energy_hourly
python -m tuya_penny_cc --task energy_daily

# Single-day backfill
python -m tuya_penny_cc --task energy_hourly --date 2026-04-13
python -m tuya_penny_cc --task energy_daily --date 2026-04-13

# Date-range backfill
python -m tuya_penny_cc --task energy_hourly --start-date 2026-04-01 --end-date 2026-04-13
python -m tuya_penny_cc --task energy_daily --start-date 2026-04-01 --end-date 2026-04-13
```

`--date` and `--start-date/--end-date` are mutually exclusive. Passing both exits with an error.

---

## Components

### New / Modified Files

```
src/tuya_penny_cc/
├── tuya/
│   └── client.py             # add get_energy_stats()
├── bq/
│   └── schemas.py            # add RAW_ENERGY_HOURLY_SCHEMA, RAW_ENERGY_DAILY_SCHEMA
└── jobs/
    ├── energy_hourly.py      # new job module
    └── energy_daily.py       # new job module

main.py                       # add Task.energy_hourly, Task.energy_daily; add --date/--start-date/--end-date options

tests/unit/tuya/test_client.py         # add get_energy_stats() cases
tests/unit/jobs/test_energy_hourly.py  # new
tests/unit/jobs/test_energy_daily.py   # new
```

### `TuyaClient.get_energy_stats(device_id, granularity, start_ts_ms, end_ts_ms)`

- `granularity`: `"hour"` or `"day"`
- `start_ts_ms`, `end_ts_ms`: Unix millisecond timestamps (Tuya convention)
- Returns raw aggregation list (`list[dict]`) from the API `result` field
- Reuses existing token cache and tenacity retry logic
- Starting endpoint: `/v1.0/iot-03/devices/{device_id}/statistics-month` — adjust based on smoke test

### `energy_hourly.run()` / `energy_daily.run()`

Signature (both jobs):

```python
def run(
    *,
    tuya: _TuyaLike,
    writer: _WriterLike,
    run_id: str,
    now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> int:
```

Logic:
1. Resolve time windows from parameters (see Time Window section below)
2. Fetch all devices via `list_devices()`; filter to `isOnline == True`
3. For each online device × time window: call `get_energy_stats()`, build row, append
4. Batch-write all rows to BQ; return row count

### `main.py`

- Add `energy_hourly = "energy_hourly"` and `energy_daily = "energy_daily"` to `Task` StrEnum
- Add optional CLI options: `--date`, `--start-date`, `--end-date` (all `Optional[str]`, parsed to `datetime.date`)
- Validate mutual exclusivity of `--date` vs `--start-date/--end-date` before dispatch
- Pass resolved date arguments to each job's `run()`

---

## BigQuery Schemas

### `tuya_raw.raw_energy_hourly`

| Column | Type | Notes |
|--------|------|-------|
| `device_id` | STRING REQUIRED | Tuya device ID |
| `category` | STRING REQUIRED | `znjdq` or `dlq` |
| `stat_hour` | TIMESTAMP REQUIRED | Hour window start (UTC, whole hour) |
| `ingest_ts` | TIMESTAMP REQUIRED | Write time (UTC) |
| `ingest_run_id` | STRING REQUIRED | UUID for this run batch |
| `ingest_date` | DATE REQUIRED | Partition key, derived from `ingest_ts` |
| `source_endpoint` | STRING REQUIRED | Actual Tuya API path called |
| `payload` | JSON REQUIRED | Full raw aggregation response (not `json.dumps()`) |

### `tuya_raw.raw_energy_daily`

Same as above, replacing `stat_hour TIMESTAMP` with `stat_date DATE`.

**Write mode:** append-only. dbt staging deduplicates with:
```sql
ROW_NUMBER() OVER (PARTITION BY device_id, stat_hour ORDER BY ingest_ts DESC) = 1
```

---

## Time Window Logic

### Parameter resolution

| Input | `energy_hourly` behaviour | `energy_daily` behaviour |
|-------|--------------------------|--------------------------|
| No params | Previous complete hour (UTC) | Yesterday (UTC) |
| `--date 2026-04-13` | 24 windows: 00:00–23:00 UTC | 1 window: full day |
| `--start-date X --end-date Y` | Expand each day in range → 24 windows each | Expand each day in range → 1 window each |
| `--date` + `--start-date` | Error: mutually exclusive | same |

### Time window → Tuya millisecond timestamps

`stat_hour` (e.g., `2026-04-13 14:00 UTC`) maps to:
- `start_ts_ms = int(stat_hour.timestamp() * 1000)`
- `end_ts_ms = int((stat_hour + timedelta(hours=1)).timestamp() * 1000) - 1`

`stat_date` (e.g., `2026-04-13`) maps to:
- `start_ts_ms = int(datetime(2026, 4, 13, 0, 0, tzinfo=UTC).timestamp() * 1000)`
- `end_ts_ms = int(datetime(2026, 4, 13, 23, 59, 59, tzinfo=UTC).timestamp() * 1000)`

The job module handles this conversion. `get_energy_stats()` receives raw millisecond timestamps.

---

## Testing

Framework: `pytest + MagicMock` (job tests), `respx` (client tests). Target: ~13 new tests (total 40).

### `test_client.py` additions (~3 cases)

| Scenario | Assertion |
|----------|-----------|
| Normal response | Returns aggregation list |
| API error code | Raises `httpx.HTTPStatusError` |
| Path includes device_id | Correct URL format |

### `test_energy_hourly.py` (~5 cases)

| Scenario | Assertion |
|----------|-----------|
| Default (no params) | Writes 1 window; `stat_hour` = previous complete hour |
| `--date` single day | 24 windows × N online devices rows written |
| `--start-date/--end-date` range (2 days) | 48 windows × N online devices rows written |
| Offline device skipped | `get_energy_stats` not called for offline device |
| API error propagates | `httpx.HTTPStatusError` not swallowed |

### `test_energy_daily.py` (~5 cases)

Same structure as hourly, replacing 24-window expansion with 1-window-per-day logic.

---

## Out of Scope

- `energy_realtime` (already complete)
- dbt staging/marts for energy data (Plan C)
- Cloud deployment (Plan D)
- Timezone-aware window calculation (all windows are UTC; local time conversion is a dbt concern)
