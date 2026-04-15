# tuya_penny_cc — CLAUDE.md

Codebase guide for AI assistants. Read this before making any changes.

---

## Repository layout

```
tuya_penny_cc/
├── ingestion/          # Python ingestion package (uv-managed)
│   ├── src/tuya_penny_cc/
│   │   ├── main.py          # CLI entry point (typer + StrEnum dispatch)
│   │   ├── config.py        # pydantic-settings: reads .env
│   │   ├── tuya/
│   │   │   ├── auth.py      # HMAC-SHA256 request signing
│   │   │   ├── client.py    # TuyaClient (token cache, pagination, retries)
│   │   │   └── models.py    # TuyaToken dataclass
│   │   ├── bq/
│   │   │   ├── schemas.py   # BigQuery table schemas
│   │   │   └── writer.py    # BigQueryWriter (append-only)
│   │   └── jobs/
│   │       ├── device_sync.py
│   │       ├── energy_realtime.py
│   │       ├── energy_hourly.py
│   │       ├── energy_daily.py
│   │       └── energy_dp_log.py
│   └── tests/unit/          # pytest tests (respx for HTTP mocking)
├── dbt/                # dbt project (BigQuery)
│   ├── models/
│   │   ├── staging/         # dataset: tuya_staging
│   │   └── marts/           # dataset: tuya_marts
│   ├── snapshots/           # SCD Type 2 (snap_devices)
│   └── seeds/               # device_relationships.csv (topology stub)
└── docs/
    ├── progress.md          # implementation history and decisions
    └── superpowers/
        └── specs/           # design documents
```

---

## Ingestion package

### Setup

```bash
cd ingestion
# install deps
uv sync
# copy and fill in credentials
cp .env.example .env
```

### Running tests

```bash
cd ingestion
uv run python -m pytest tests/ -q          # all tests (currently 55)
uv run python -m pytest tests/ -q -k name  # filter by name
```

### Lint

```bash
cd ingestion
uv run ruff check src/ tests/
```

Rules: `E`, `F`, `I`, `B`, `UP`, `SIM`. Line length: 100. Python target: 3.11.

### Running a task

```bash
cd ingestion

# Default (yesterday / last complete hour):
uv run python -m tuya_penny_cc --task device_sync
uv run python -m tuya_penny_cc --task energy_realtime
uv run python -m tuya_penny_cc --task energy_dp_log

# Single date backfill:
uv run python -m tuya_penny_cc --task energy_dp_log --date 2026-04-14

# Date range backfill (max 7 days — Tuya retention limit):
uv run python -m tuya_penny_cc --task energy_dp_log --start-date 2026-04-08 --end-date 2026-04-14
```

Available tasks: `device_sync`, `energy_realtime`, `energy_hourly`, `energy_daily`, `energy_dp_log`.

`--date` and `--start-date/--end-date` are mutually exclusive. `--start-date` and `--end-date` must be used together.

---

## dbt project

```bash
cd dbt

# install packages (first time or after packages.yml change)
dbt deps

# run all models
dbt run

# run staging only
dbt run --select staging

# run tests
dbt test

# run a specific model
dbt run --select stg_energy_dp_log
```

Requires `GOOGLE_APPLICATION_DEFAULT_CREDENTIALS` (or `gcloud auth application-default login`) and `GCP_PROJECT_ID` env var.

---

## BigQuery datasets

| Dataset | Contents |
|---------|----------|
| `tuya_raw` | Raw landing tables (written by ingestion jobs) |
| `tuya_staging` | Cleaned / unnested views and tables |
| `tuya_marts` | Analytical facts and dimensions |

Raw tables are append-only. Never modify or delete raw data.

---

## Key conventions

### Ingestion jobs

- **Protocol-typed dependencies**: jobs accept `_TuyaLike` and `_WriterLike` protocols, never concrete classes. This makes them trivially testable without mocking.
- **Offline devices skipped silently**: `if not device.get("isOnline"): continue`
- **Per-device BQ writes**: `writer.load()` is called after each device, not after all devices. This preserves partial progress on failure.
- **`payload` is a raw list/dict** — never call `json.dumps()` before passing to BigQueryWriter. BigQuery's JSON column type accepts native Python objects.
- **All timestamps UTC** — timezone conversion is a consumer concern.

### TuyaClient

- Token is cached in-memory and refreshed when <5 min from expiry.
- `_fetch_dp_log_page()` calls `_get_access_token()` on every attempt (not once at the start of a multi-page fetch), so long paginations survive token expiry.
- Rate limit (`code=40000309`) is retried with exponential backoff: 10 s, 20 s, 40 s, 60 s, up to 5 retries.

### dbt models

- `stg_energy_dp_log` is the single source of truth for energy data. It unnests the `payload` JSON array and computes `interval_kwh` via `LAG(dp_value) OVER (PARTITION BY device_id ORDER BY event_ts)`.
- `interval_kwh` can be negative (counter reset). Negative deltas are preserved in staging and flagged as `is_counter_reset = TRUE` in `fct_energy_intervals`. They are excluded (filtered to `> 0`) in `stg_energy_hourly` and `stg_energy_daily`.
- Incremental fact tables use a lookback window (`>= MAX - INTERVAL 2 HOUR/DAY`) so the current in-progress period is always refreshed on re-run.

### Tests

- HTTP mocking: `respx` via `mock_router` fixture (defined in `tests/conftest.py`).
- Job tests use `MagicMock` for `tuya` and `writer` arguments — no network calls.
- No database mocking — there is no DB in this project; BQ writes are tested via writer mock.

---

## Tuya API — critical gotchas

**Device list:** Use `/v2.0/cloud/thing/device` (NOT `/v1.3/iot-03/devices` — returns empty list silently on this account). Response `result` is a flat list, NOT `result.list`. Field names are camelCase. `page_size` max = 20.

**DP log endpoint:** `GET /v2.0/cloud/thing/{device_id}/report-logs`
- `event_time` is **Unix milliseconds** (not seconds)
- `value` is a **string** (cast to FLOAT64 in dbt)
- Only `add_ele` is fetched — `cur_power`/`cur_current`/`cur_voltage` are not needed
- **Tuya retains ~7 days of history** — daily runs must not lag more than a week

**Statistics-month endpoint:** `/v1.0/iot-03/devices/{id}/statistics-month` returns `1108` on this account (not subscribed). All v1/v2 variants are unavailable. Hourly/daily aggregations are derived from `stg_energy_dp_log` instead.

**Auth signing:** Commas in query param values (e.g. `codes=add_ele,cur_other`) must remain as literal commas in the canonical signing string — not percent-encoded. Fixed in `auth.py` with `quote(v, safe=',')`.

Full API notes: `ingestion/docs/tuya_api_notes.md`

---

## Environment variables (`.env`)

```
TUYA_BASE_URL=https://openapi.tuyaus.com
TUYA_ACCESS_ID=...
TUYA_ACCESS_SECRET=...
TUYA_USER_UID=...
GCP_PROJECT_ID=...
BQ_DATASET_RAW=tuya_raw
BQ_LOCATION=US
```

---

## Branch / PR workflow

- Each plan phase uses a feature branch + PR. Never commit directly to `main`.
- Current branch: `feat/plan-b-energy-hourly` (Plan B work; merge to `main` when complete).
- Next phase: Plan D — Cloud Run deployment.

## Document language

- Formal technical docs (design specs, API notes): **English**
- Progress notes and conversational docs: Chinese is fine
