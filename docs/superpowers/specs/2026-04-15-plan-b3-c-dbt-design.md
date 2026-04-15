# Plan B.3 + C — DP History Log Ingestion & dbt Project Design

**Date:** 2026-04-15
**Branch:** `feat/plan-c-dbt` → merged to `main` 2026-04-15
**Scope:** Plan B.3 (energy_dp_log ingestion job) + Plan C (dbt staging, snapshots, seeds, marts)
**Status:** ✅ Complete

---

## Background

Plans A and B are complete. Raw tables in `tuya_raw`:
- `raw_devices` — daily device inventory snapshots
- `raw_energy_realtime` — daily snapshot of current DP values
- `raw_energy_hourly` — Tuya server-side hourly energy aggregations
- `raw_energy_daily` — Tuya server-side daily energy aggregations

Realtime DP payload analysis revealed that `add_ele` (cumulative energy counter, unit: 0.01 kWh) is the key field for fine-grained energy measurement. Fetching its change history via Tuya's DP log endpoint gives sub-hour interval consumption without requiring frequent polling.

Device inventory:
- 12 × `znjdq` A5 smart relay switches (online)
- 5 × `dlq` WIFI Smart Meter Pro W (offline)
- All devices currently flat (no parent-child topology); cascading relationships possible in future

---

## Goals

- Capture Tuya DP change history at the finest available granularity (event-level `add_ele` increments)
- Build a dbt project that transforms raw data into clean staging models and analytical mart tables
- Expose energy consumption per device at three granularities: event-interval, hourly, daily
- Preserve topology extensibility without implementing topology calculations now

---

## Plan B.3 — DP History Log Ingestion

### Tuya Endpoint

```
GET /v2.0/cloud/thing/{device_id}/report-logs   ✅ confirmed working (2026-04-15)

Query params:
  codes         — comma-separated DP codes: add_ele,cur_power,cur_current,cur_voltage
  start_time    — Unix milliseconds (window start)
  end_time      — Unix milliseconds (window end)
  size          — page size (max 50 confirmed)
  last_row_key  — cursor for pagination (returned in response)
```

Response structure (confirmed):
```jsonc
{
  "result": {
    "device_id": "...",
    "has_more": true,
    "last_row_key": "E134...",
    "logs": [
      {"code": "add_ele", "event_time": 1776210807000, "value": "91"},
      {"code": "cur_current", "event_time": 1776207992390, "value": "462"}
    ],
    "total": 1000
  }
}
```

**Confirmed facts (2026-04-15 smoke test):**
- `event_time` unit: **Unix milliseconds** ✅
- `value` type: **string** (must cast to FLOAT64 in dbt)
- `add_ele` unit: **0.01 kWh** (divide by 100)
- `add_ele` counter resets periodically (~100 kWh max); negative deltas preserved in staging
- Empty window response: `result: {"has_more": false}` only (no `logs` key)
- Tuya retains approximately 10 days of DP history

**Auth signing bug (fixed 2026-04-15):** Multi-code queries (`codes=add_ele,cur_power,...`) returned `code=1004 sign invalid` due to commas being percent-encoded as `%2C` in the canonical signing string. Tuya verifies signatures against the decoded URL, so commas must remain literal. Fixed by using `safe=','` in `urllib.parse.quote()` inside `build_string_to_sign`.

### New BQ Table: `tuya_raw.raw_energy_dp_log`

| Column | Type | Notes |
|--------|------|-------|
| `device_id` | STRING REQUIRED | Tuya device ID |
| `category` | STRING REQUIRED | `znjdq` or `dlq` |
| `log_date` | DATE REQUIRED | UTC date this row covers; partition key |
| `ingest_ts` | TIMESTAMP REQUIRED | Write time (UTC) |
| `ingest_run_id` | STRING REQUIRED | UUID for this run batch |
| `ingest_date` | DATE REQUIRED | Derived from `ingest_ts` |
| `source_endpoint` | STRING REQUIRED | Actual Tuya API path called |
| `payload` | JSON REQUIRED | Full list of DP events for this device on this date |

**One row per (device_id, log_date).** `payload` contains all DP change events for that device on that date: `[{code, value, event_time}, ...]`.

### New `TuyaClient` method: `get_dp_log()`

```python
def get_dp_log(
    self,
    device_id: str,
    codes: list[str],
    start_ts_ms: int,
    end_ts_ms: int,
) -> list[dict]:
```

- Paginates using `last_row_key` cursor until exhausted
- Merges all pages into a single flat list
- Returns `[]` if endpoint returns no result
- Reuses existing token cache and tenacity retry logic

### New Job: `energy_dp_log.run()`

- Default: yesterday's data
- `--date` / `--start-date + --end-date` backfill (consistent with hourly/daily CLI)
- Maximum lookback: 10 days (Tuya retention limit)
- Skips offline devices (consistent with other jobs)
- Fetches codes: `add_ele`, `cur_power`, `cur_current`, `cur_voltage`

### CLI

```bash
python -m tuya_penny_cc --task energy_dp_log
python -m tuya_penny_cc --task energy_dp_log --date 2026-04-14
python -m tuya_penny_cc --task energy_dp_log --start-date 2026-04-05 --end-date 2026-04-14
```

### Testing (~8 new tests)

| Scope | Scenarios |
|-------|-----------|
| `TuyaClient.get_dp_log()` | Normal response; multi-page pagination merges correctly; API error propagates |
| `energy_dp_log.run()` | Default (yesterday); `--date` single day; offline device skipped; pagination merged into one payload row |

### Smoke Test Gate

✅ **Passed 2026-04-15.** All three checks confirmed:
1. Endpoint `/v2.0/cloud/thing/{device_id}/report-logs` — confirmed working
2. `event_time` is milliseconds; `add_ele` unit is 0.01 kWh — confirmed
3. `add_ele` counter resets at ~100 kWh; negative deltas preserved in staging, flagged in marts

---

## Plan C — dbt Project

### Directory Layout

```
dbt/
├── dbt_project.yml
├── profiles.yml
├── requirements.txt                  # dbt-bigquery>=1.8,<2.0
├── packages.yml                      # dbt_utils>=1.0 for composite unique tests
├── seeds/
│   └── device_relationships.csv      # stub — header only; filled when topology exists
├── snapshots/
│   └── snap_devices.sql              # SCD2: track name/category/product_name/is_online changes
├── models/
│   ├── staging/                      # dataset: tuya_staging
│   │   ├── schema.yml
│   │   ├── stg_devices_latest.sql
│   │   ├── stg_energy_dp_log.sql
│   │   ├── stg_energy_hourly.sql     # derived from stg_energy_dp_log (not raw table)
│   │   └── stg_energy_daily.sql      # derived from stg_energy_dp_log (not raw table)
│   └── marts/                        # dataset: tuya_marts
│       ├── schema.yml
│       ├── dim_devices.sql
│       ├── fct_energy_intervals.sql
│       ├── fct_energy_hourly.sql
│       └── fct_energy_daily.sql
└── tests/
    └── (schema tests via schema.yml)
```

### Dataset Plan

| Dataset | Purpose | Materialization |
|---------|---------|-----------------|
| `tuya_raw` | Raw landing (existing) | ingestion jobs |
| `tuya_staging` | Clean, deduplicate, type-cast, unnest | view / table |
| `tuya_marts` | Analytical facts and dimensions | table / incremental |

---

## Staging Models

### `stg_devices_latest`

- Source: `tuya_raw.raw_devices`
- Logic: `ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY ingest_ts DESC) = 1`
- Renames camelCase → snake_case (`isOnline` → `is_online`, etc.)
- Fields: `device_id`, `name`, `category`, `product_name`, `is_online`, `active_time`, `ingest_ts`
- Materialization: **view**

### `stg_energy_dp_log`

- Source: `tuya_raw.raw_energy_dp_log`
- Dedup: `ROW_NUMBER() OVER (PARTITION BY device_id, log_date ORDER BY ingest_ts DESC) = 1`
- Unnests `payload` array → one row per DP event
- Filters to energy-relevant codes: `add_ele`, `cur_power`, `cur_current`, `cur_voltage`
- Converts `event_time` (ms) → `event_ts` (TIMESTAMP)
- Computes `add_ele` delta:
  ```sql
  LAG(dp_value) OVER (PARTITION BY device_id, dp_code ORDER BY event_ts) AS prev_value
  -- interval_kwh = (dp_value - prev_value) / 100.0
  ```
- Fields: `device_id`, `category`, `dp_code`, `dp_value`, `prev_value`, `event_ts`, `prev_event_ts`, `interval_kwh`, `log_date`
- Materialization: **table**

### `stg_energy_hourly`

- **Source: `stg_energy_dp_log`** (not `raw_energy_hourly` — statistics-month endpoint unavailable)
- Filters `dp_code = 'add_ele'` and `interval_kwh > 0` (excludes counter resets)
- Buckets `event_ts` via `TIMESTAMP_TRUNC(event_ts, HOUR)` → `stat_hour`
- Aggregates `SUM(interval_kwh)` per `(device_id, category, stat_hour)`
- Fields: `device_id`, `category`, `stat_hour`, `energy_kwh`
- Materialization: **table**

### `stg_energy_daily`

- **Source: `stg_energy_dp_log`** (not `raw_energy_daily` — statistics-month endpoint unavailable)
- Filters `dp_code = 'add_ele'` and `interval_kwh > 0` (excludes counter resets)
- Extracts `DATE(event_ts)` → `stat_date`
- Aggregates `SUM(interval_kwh)` per `(device_id, category, stat_date)`
- Fields: `device_id`, `category`, `stat_date`, `energy_kwh`
- Materialization: **table**

> **Note:** The Tuya `statistics-month` endpoint (`/v1.0/iot-03/devices/{id}/statistics-month`) returns `1108 uri path invalid` on this account. All v1/v2 variants tested are unavailable. `raw_energy_hourly` and `raw_energy_daily` tables remain empty. Hourly/daily aggregations are derived entirely from DP event intervals.

---

## Snapshots

### `snap_devices` (SCD2)

- Source: `stg_devices_latest`
- Unique key: `device_id`
- Tracked fields: `name`, `category`, `product_name`, `is_online`
- On change: closes previous row (`dbt_valid_to`), opens new row
- Use case: "what was this device called at time T?"

---

## Seeds

### `device_relationships.csv`

```csv
parent_device_id,child_device_id,relationship_type,valid_from,note
```

Current: header only (stub). Filled manually when parent-child topology is established. `dim_devices` LEFT JOINs this seed and exposes `parent_device_id` as a nullable field for future topology queries.

---

## Marts Models

### `dim_devices`

- Source: `stg_devices_latest` LEFT JOIN `device_relationships`
- One row per device (current state)
- Fields: `device_id`, `name`, `category`, `product_name`, `is_online`, `parent_device_id`
- Materialization: **table**

### `fct_energy_intervals` (finest granularity)

- Source: `stg_energy_dp_log` WHERE `dp_code = 'add_ele'` AND `interval_kwh IS NOT NULL`, JOIN `dim_devices`
- One row per `add_ele` change event per device
- Fields:

| Column | Description |
|--------|-------------|
| `device_id` | Device ID |
| `name` | Device name (from dim_devices) |
| `category` | Device category |
| `event_ts` | Event timestamp (UTC) |
| `prev_event_ts` | Previous event timestamp |
| `interval_minutes` | Duration of interval in minutes |
| `interval_kwh` | Energy consumed in this interval (kWh); negative = counter reset |
| `cumulative_kwh` | `add_ele` cumulative value / 100.0 |
| `is_counter_reset` | TRUE when `interval_kwh < 0` |

- Materialization: **incremental**, unique_key: `(device_id, event_ts)`

### `fct_energy_hourly`

- Source: `stg_energy_hourly` JOIN `dim_devices`
- One row per (device, hour)
- Fields: `device_id`, `name`, `category`, `stat_hour`, `energy_kwh`
- Materialization: **incremental**, unique_key: `(device_id, stat_hour)`
- Incremental filter: `stat_hour >= MAX(stat_hour) - INTERVAL 2 HOUR` (2-hour lookback for current-hour updates)

### `fct_energy_daily`

- Source: `stg_energy_daily` JOIN `dim_devices`
- One row per (device, date)
- Fields: `device_id`, `name`, `category`, `stat_date`, `energy_kwh`
- Materialization: **incremental**, unique_key: `(device_id, stat_date)`
- Incremental filter: `stat_date >= MAX(stat_date) - INTERVAL 2 DAY` (2-day lookback for current-day updates)

---

## Testing

### Schema Tests (via `schema.yml`)

| Model | Tests |
|-------|-------|
| All staging | `not_null` on key fields |
| `stg_energy_dp_log` | `accepted_values` for `dp_code` |
| `dim_devices` | `unique` + `not_null` on `device_id` |
| `fct_energy_intervals` | `not_null` on `device_id`, `event_ts`, `interval_kwh`, `is_counter_reset`; composite unique on `(device_id, event_ts)` via `dbt_utils` |
| `fct_energy_hourly` | `not_null`; composite unique on `(device_id, stat_hour)` via `dbt_utils` |
| `fct_energy_daily` | `not_null`; composite unique on `(device_id, stat_date)` via `dbt_utils` |
| All categories | `accepted_values`: `znjdq`, `dlq` |

Requires `packages.yml` with `dbt-labs/dbt_utils >=1.0.0`.

### Data Tests

- `interval_kwh >= 0` — negative delta signals counter reset; flag rather than silently drop
- `interval_minutes > 0`

---

## Out of Scope

- Topology net-load calculation (`parent - sum(children)`) — deferred until cascading devices exist
- `snap_device_relationships` SCD2 — deferred until topology seed has data
- `raw_energy_realtime` dbt models — redundant once `fct_energy_intervals` is available
- Cloud deployment (Plan D)
- Timezone conversion (all timestamps UTC; local time conversion is a consumer concern)
