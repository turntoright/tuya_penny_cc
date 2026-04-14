# tuya_penny_cc

IoT energy monitoring pipeline for a Tuya developer account.
Ingests device metadata and energy-consumption data from the Tuya OpenAPI
and stores everything in BigQuery for analysis.

## Repository layout

```
tuya_penny_cc/
├── ingestion/          # Python: Cloud Run Job (Tuya → BigQuery raw)
├── dbt/                # dbt project: raw → staging → marts  [Plan C, not started]
├── infra/              # Terraform / deployment scripts        [Plan D, not started]
└── docs/
    ├── progress.md                         # project progress & roadmap (Chinese)
    └── superpowers/
        ├── specs/
        │   ├── 2026-04-14-tuya-iot-design.md          # system design spec
        │   └── 2026-04-14-tuya-iot-design-zh-notes.md # design rationale (Chinese)
        └── plans/
            └── 2026-04-14-ingestion-mvp.md             # Plan A implementation plan
```

## System overview

```
Tuya Cloud OpenAPI
        │  HTTPS (signed requests)
        ▼
Cloud Run Job  ──── Secret Manager (Tuya credentials)
        │  batch load
        ▼
BigQuery: tuya_raw          ← append-only raw tables
        │  dbt
        ▼
BigQuery: tuya_staging      ← cleaned, typed, deduplicated
        │  dbt
        ▼
BigQuery: tuya_marts        ← analytics-ready facts & dimensions
```

Data types collected:

| Table | Content | Frequency |
|---|---|---|
| `raw_devices` | Full device list snapshot | 1× / day |
| `raw_energy_realtime` | Live DP readings (cur_power, add_ele…) | 1–6× / day |
| `raw_energy_hourly` | Tuya hourly aggregates | 1× / day |
| `raw_energy_daily` | Tuya daily aggregates | 1× / day |

Device topology (parent/child cascade relationships) is maintained in
`dbt/seeds/device_relationships.csv` and tracked with dbt SCD Type 2 snapshots.

## Quickstart (ingestion)

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) — `pip install uv`
- Google Cloud SDK — `gcloud auth application-default login`
- BigQuery dataset: `bq mk --dataset --location=US <PROJECT_ID>:tuya_raw`
- Tuya IoT Cloud project with `access_id` / `access_secret`

### Setup

```bash
cd ingestion
cp .env.example .env   # fill in Tuya and GCP credentials
uv sync
```

### Run jobs

```bash
# Sync device list → tuya_raw.raw_devices
uv run python -m tuya_penny_cc --task=device_sync
```

### Tests

```bash
cd ingestion
uv run pytest -v       # 21 unit tests
uv run ruff check .    # linting
```

## Tuya API notes

Key findings from initial integration (see `ingestion/docs/tuya_api_notes.md`
for full details):

- Device list: use `GET /v2.0/cloud/thing/device` (v1.3 endpoint returns empty
  on this account)
- Pagination: `page_no` based, `page_size` max **20**
- BQ `payload` column: pass dict directly — do **not** `json.dumps()`
- Token TTL ~6700 s; client refreshes automatically at <300 s remaining

## Roadmap

| Plan | Status | Description |
|---|---|---|
| **A — Ingestion MVP** | ✅ Complete | device_sync job, BQ writer, CLI, 21 tests |
| **B — Energy jobs** | ⬜ Not started | energy_realtime, hourly, daily, backfill |
| **C — dbt project** | ⬜ Not started | staging, snapshots (SCD2), marts, topology |
| **D — Cloud deployment** | ⬜ Not started | Cloud Run, Scheduler, Secret Manager, CI/CD |

## Design documents

- **System design spec:** `docs/superpowers/specs/2026-04-14-tuya-iot-design.md`
- **Plan A implementation:** `docs/superpowers/plans/2026-04-14-ingestion-mvp.md`
- **Tuya API gotchas:** `ingestion/docs/tuya_api_notes.md`
