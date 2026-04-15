# Plan B.3 + C — DP History Log Ingestion & dbt Project Design

**Date:** 2026-04-15
**Branch:** `feat/plan-c-dbt`
**Scope:** Plan B.3 (energy_dp_log ingestion job) + Plan C (dbt staging, snapshots, seeds, marts)

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
GET /v2.0/cloud/thing/{device_id}/report-logs   (latest API version — confirm path during smoke test)

Query params:
  codes         — comma-separated DP codes: add_ele,cur_power,cur_current,cur_voltage
  start_time    — Unix milliseconds (window start)
  end_time      — Unix milliseconds (window end)
  size          — page size (max 50)
  last_row_key  — cursor for pagination (returned in response)
```

Response record format: `{code, value, event_time}` where `event_time` is Unix milliseconds.

**Note:** Endpoint path and query parameter names are best-guess based on Tuya OpenAPI v2 conventions. Verify against actual API response during smoke testing and adjust as needed. Tuya retains approximately 10 days of DP history.

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

After B.3 implementation, run against real account and confirm:
1. Endpoint path and query params are correct
2. `event_time` precision (milliseconds) and `add_ele` unit (0.01 kWh)
3. Whether `add_ele` counter resets (negative delta handling strategy)

**Do not start dbt models until smoke test passes.**

---

## Plan C — dbt Project

### Directory Layout

```
dbt/
├── dbt_project.yml
├── profiles.yml
├── seeds/
│   └── device_relationships.csv      # stub — header only; filled when topology exists
├── snapshots/
│   └── snap_devices.sql              # SCD2: track name/category/product_name/is_online changes
├── models/
│   ├── staging/                      # dataset: tuya_staging
│   │   ├── schema.yml
│   │   ├── stg_devices_latest.sql
│   │   ├── stg_energy_dp_log.sql
│   │   ├── stg_energy_hourly.sql
│   │   └── stg_energy_daily.sql
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

- Source: `tuya_raw.raw_energy_hourly`
- Dedup: `ROW_NUMBER() OVER (PARTITION BY device_id, stat_hour ORDER BY ingest_ts DESC) = 1`
- Unnests payload; extracts Tuya kWh value
- Fields: `device_id`, `category`, `stat_hour`, `energy_kwh`
- Materialization: **table**

### `stg_energy_daily`

- Source: `tuya_raw.raw_energy_daily`
- Dedup: `ROW_NUMBER() OVER (PARTITION BY device_id, stat_date ORDER BY ingest_ts DESC) = 1`
- Unnests payload; extracts Tuya kWh value
- Fields: `device_id`, `category`, `stat_date`, `energy_kwh`
- Materialization: **table**

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

- Source: `stg_energy_dp_log` WHERE `dp_code = 'add_ele'`, JOIN `dim_devices`
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
| `interval_kwh` | Energy consumed in this interval (kWh) |
| `cumulative_kwh` | `add_ele` cumulative value / 100.0 |

- Materialization: **incremental** (append by `event_ts`)

### `fct_energy_hourly`

- Source: `stg_energy_hourly` JOIN `dim_devices`
- One row per (device, hour)
- Fields: `device_id`, `name`, `category`, `stat_hour`, `energy_kwh`
- Materialization: **incremental**

### `fct_energy_daily`

- Source: `stg_energy_daily` JOIN `dim_devices`
- One row per (device, date)
- Fields: `device_id`, `name`, `category`, `stat_date`, `energy_kwh`
- Materialization: **incremental**

---

## Testing

### Schema Tests (via `schema.yml`)

| Model | Tests |
|-------|-------|
| All staging | `not_null` on all fields |
| `stg_energy_dp_log` | `accepted_values` for `dp_code` |
| `dim_devices` | `unique` + `not_null` on `device_id` |
| `fct_energy_intervals` | `not_null` on `device_id`, `event_ts`, `interval_kwh`; `relationships` to `dim_devices` |
| `fct_energy_hourly` | `not_null`; `relationships` to `dim_devices` |
| `fct_energy_daily` | `not_null`; `relationships` to `dim_devices` |
| All categories | `accepted_values`: `znjdq`, `dlq` |

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
