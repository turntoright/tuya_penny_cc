# Plan B — Energy Realtime Job Design

**Date:** 2026-04-14  
**Scope:** `energy_realtime` job only (hourly/daily left for future Plan B iterations)

---

## Background

Plan A delivered the `device_sync` job: 17 devices synced to `tuya_raw.devices` via the `/v2.0/cloud/thing/device` endpoint. The Tuya DP endpoint for energy readings is currently unknown and will be discovered inline during implementation.

Device inventory:
- 12 × `znjdq` A5 smart relay switches — all online
- 5 × `dlq` WIFI Smart Meter Pro W — all offline

---

## Goals

- Capture real-time DP readings (power, energy accumulation, etc.) for each online device
- Support both near-realtime monitoring and historical trend accumulation
- Skip offline devices silently (no row written)
- Store raw DP payload as-is; defer field normalization to dbt staging layer

---

## Architecture

### Data Flow

```
Tuya API (DP endpoint)
    ↓  per online device
TuyaClient.get_device_dps(device_id)
    ↓
EnergyRealtimeJob
    ↓  lineage field wrapping
BigQueryWriter → tuya_raw.energy_realtime
```

### CLI Entry Point

Added to the existing `Task` StrEnum in `main.py`:

```bash
python -m tuya_penny_cc energy_realtime
```

### API Exploration Strategy

The DP endpoint is unknown at design time. The implementation will start with the known Tuya DP status path (`/v1.0/iot-03/devices/{device_id}/status`) and adjust based on actual responses — following the same inline validation approach used for the v2.0 device list endpoint.

---

## Components

### New Files

```
src/tuya_penny_cc/
├── tuya/
│   └── client.py          # add get_device_dps() method
├── bq/
│   └── schemas.py         # add RAW_ENERGY_REALTIME_SCHEMA
└── jobs/
    └── energy_realtime.py # new job module
```

### `TuyaClient.get_device_dps(device_id: str) -> list[dict]`

- Calls the Tuya DP status endpoint for a single device
- Returns the raw DP list as returned by the API
- Reuses existing token cache and tenacity retry logic
- No changes to auth layer

### `RAW_ENERGY_REALTIME_SCHEMA`

Mirrors `RAW_DEVICES_SCHEMA` structure:

| Column | Type | Notes |
|--------|------|-------|
| `device_id` | STRING | Tuya device ID |
| `category` | STRING | `znjdq` or `dlq` |
| `ingest_ts` | TIMESTAMP | Write time (UTC) |
| `ingest_run_id` | STRING | UUID for this run batch |
| `ingest_date` | DATE | Partition key, derived from `ingest_ts` |
| `source_endpoint` | STRING | Actual Tuya API path called |
| `payload` | JSON | Full raw DP response object (not `json.dumps()`) |

### `energy_realtime.py` — `run(client, writer)`

Implements the same `run(client, writer)` protocol as `device_sync`:

1. Call `list_devices()` to fetch all devices
2. Filter to `isOnline == True` only
3. For each online device, call `get_device_dps(device_id)`
4. Wrap lineage fields, batch-write to `tuya_raw.energy_realtime`

`category` is taken from the `list_devices()` response — no extra API call needed.

### `main.py`

- Add `energy_realtime = "energy_realtime"` to `Task` StrEnum
- Add dispatch `case` calling `energy_realtime.run(client, writer)`

---

## BigQuery Table

**Table:** `tuya_raw.energy_realtime`  
**Write mode:** append-only (never modify raw data)  
**Partitioning:** by `ingest_date`

---

## Testing

Framework: `pytest + respx` (HTTP mocked, no real API calls). Target: ~8 new unit tests.

### New Test Files

```
tests/unit/tuya/test_client.py         # add get_device_dps() cases
tests/unit/jobs/test_energy_realtime.py  # new
```

### Test Cases

**`test_energy_realtime.py`:**

| Scenario | Assertion |
|----------|-----------|
| 2 online devices | 2 rows written; payload is dict not string |
| 1 online + 1 offline | 1 row written; offline device skipped |
| All devices offline | 0 rows written; job exits cleanly |
| DP endpoint error | tenacity retries; exception raised after exhaustion |

**`test_client.py` additions:**

| Scenario | Assertion |
|----------|-----------|
| Normal DP response | Returns DP list |
| API error code | Raises expected exception |

---

## Out of Scope

- `energy_hourly` and `energy_daily` jobs (future Plan B iterations)
- Backfill support (future)
- dbt staging/marts for energy data (Plan C)
- Cloud deployment (Plan D)
