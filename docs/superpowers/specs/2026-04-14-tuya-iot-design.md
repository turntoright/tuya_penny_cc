# Tuya IoT Ingestion & Analytics — Design Spec

- **Date:** 2026-04-14
- **Status:** Draft (pending implementation plan)
- **Owner:** harveycao-lincoln
- **Repo:** `dbt_projects/tuya_penny_cc`

## 1. Goal

Build an extensible system that:

1. Ingests device metadata and energy-consumption data from a Tuya developer
   account.
2. Persists all raw data in BigQuery for analysis.
3. Tracks changes in device metadata over time (e.g., device renaming).
4. Tracks user-maintained device topology (parent/child cascade
   relationships) and its history.
5. Models the data with dbt for analytics consumption.
6. Is structured so future requirements (new device categories, new data
   sources, new analytics) can be added without invasive changes.

## 2. Scope

| In scope | Out of scope (initially) |
|---|---|
| Device list sync | Real-time alerting / push notifications |
| Energy data: real-time cumulative reading, hourly aggregate, daily aggregate | Web UI |
| Historical backfill (one-shot, manual) | Multi-tenant support |
| dbt models: staging → marts | Cost / billing calculation models (mart-layer follow-up) |
| dbt snapshots for device & topology change tracking | Non-Tuya vendors |
| Cloud Scheduler-driven daily runs | Streaming ingestion |
| BigQuery as warehouse | Other warehouses |

## 3. Non-Goals

- High-frequency (sub-minute) polling. Tuya API quotas and the analytics
  use case do not require it.
- Bidirectional control (sending commands to devices). This is read-only
  ingestion.
- A general-purpose IoT platform. The system is scoped to the user's own
  Tuya account (~100 devices, primarily smart relays/switches).

## 4. Constraints & Assumptions

- Device count is below 100. Device list updates 1×/day; energy data 1–6×/day.
- Devices are mostly smart relays/switches that expose
  `cur_power`, `cur_voltage`, `cur_current`, `add_ele` (cumulative kWh), etc.
- Tuya retains aggregate energy statistics for ~1 year; backfill should
  pull as much as the API allows.
- Runtime environment is GCP. BigQuery is the system of record.
- Single maintainer; small-team conventions apply (mono-repo, light tooling).
- Topology is user-maintained metadata; Tuya does not expose it.
- A parent device's measured consumption equals the sum of its children's
  consumption plus the parent's own load (the parent meters the whole
  downstream branch).

## 5. High-Level Architecture

```
┌─────────────────┐    HTTPS    ┌──────────────────────┐
│   Tuya Cloud    │ ◄─────────► │  Cloud Run Job       │
│  OpenAPI v2.0   │             │  (Python ingestion)  │
└─────────────────┘             └──────────┬───────────┘
                                           │ batch load
                                           ▼
                                ┌──────────────────────┐
                                │   BigQuery: raw      │  (append-only)
                                └──────────┬───────────┘
                                           │ dbt seed/snapshot/run
                                           ▼
                                ┌──────────────────────┐
                                │   BigQuery: staging  │
                                └──────────┬───────────┘
                                           │
                                           ▼
                                ┌──────────────────────┐
                                │   BigQuery: marts    │
                                └──────────────────────┘

  Cloud Scheduler  ──triggers──►  Cloud Run Job
  Secret Manager   ──provides──►  Tuya credentials
```

### Key architectural decisions

- **Cloud Run Job** (not Cloud Function): supports longer-running tasks
  required for backfill and for sequencing across many devices.
- **Cloud Scheduler with multiple schedules** triggers the same Job
  with different `--task` arguments.
- **Secret Manager** holds Tuya `access_id` / `access_secret`; the Job's
  service account reads them at startup.
- **dbt orchestration** (where dbt actually runs — dbt Cloud, Cloud Run,
  GitHub Actions) is decided in the implementation plan; the design here
  is independent of that choice.

## 6. Repository Layout

```
tuya_penny_cc/
├── ingestion/                    # Python: Cloud Run Job
│   ├── pyproject.toml
│   ├── Dockerfile
│   ├── src/
│   │   └── tuya_penny_cc/
│   │       ├── main.py           # CLI entry point, --task dispatch
│   │       ├── config.py         # env vars / Secret Manager
│   │       ├── tuya/
│   │       │   ├── client.py     # OpenAPI HTTP client (auth, retry)
│   │       │   └── models.py     # pydantic models
│   │       ├── bq/
│   │       │   ├── writer.py     # BQ batch loader
│   │       │   └── schemas.py    # raw table schemas
│   │       └── jobs/
│   │           ├── device_sync.py
│   │           ├── energy_realtime.py
│   │           ├── energy_hourly.py
│   │           ├── energy_daily.py
│   │           └── backfill.py
│   └── tests/
│       ├── unit/
│       └── integration/
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── seeds/
│   │   └── device_relationships.csv
│   ├── snapshots/
│   │   ├── snap_devices.sql
│   │   └── snap_device_relationships.sql
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_devices.sql
│   │   │   ├── stg_devices_latest.sql
│   │   │   ├── stg_energy_realtime.sql
│   │   │   ├── stg_energy_hourly.sql
│   │   │   └── stg_energy_daily.sql
│   │   └── marts/
│   │       ├── dim_devices.sql
│   │       ├── dim_devices_history.sql
│   │       ├── dim_device_topology.sql
│   │       ├── dim_device_topology_history.sql
│   │       ├── fct_device_tree.sql
│   │       ├── fct_energy_consumption.sql
│   │       └── agg_device_daily.sql
│   ├── macros/
│   └── tests/
│
├── infra/
│   ├── terraform/                # placeholder for IaC
│   └── README.md
│
├── docs/
│   └── superpowers/
│       └── specs/
│           ├── 2026-04-14-tuya-iot-design.md
│           └── 2026-04-14-tuya-iot-design-zh-notes.md
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── deploy.yml
│
├── .gitignore
└── README.md
```

### Module boundaries

- `tuya/client.py` is the **only** module that talks HTTP to Tuya. All
  jobs go through it.
- `bq/writer.py` is the **only** module that writes to BigQuery from
  ingestion. Swapping warehouses touches one file.
- Each `jobs/*.py` file represents one ingestion task. New tasks add a
  new file plus a new Cloud Scheduler entry; they do not modify existing
  files.

## 7. BigQuery Layout

### 7.1 Datasets

| Dataset | Purpose | Writer | Write mode |
|---|---|---|---|
| `tuya_raw` | Raw API payloads + ops table | ingestion job | append-only |
| `tuya_snapshots` | dbt SCD Type 2 outputs | dbt | snapshot |
| `tuya_staging` | Cleaned, typed, deduplicated | dbt | view / incremental |
| `tuya_marts` | Analytics-friendly facts/dimensions | dbt | table / incremental |

### 7.2 Raw table conventions

Every `raw_*` table includes the following lineage columns:

| Column | Type | Notes |
|---|---|---|
| `ingest_ts` | TIMESTAMP NOT NULL | When this row was written to BQ |
| `ingest_run_id` | STRING NOT NULL | UUID of the Job run |
| `source_endpoint` | STRING NOT NULL | Tuya endpoint path used |
| `payload` | JSON NOT NULL | Raw response body (per device or per stat) |
| `device_id` | STRING | Promoted from payload, used for partition-pruning queries |
| `ingest_date` | DATE | `DATE(ingest_ts)`, partition column |
| `stat_date` | DATE | Aggregate tables only |

### 7.3 Raw tables

| Table | Partition | Dedup key (used in staging) | Source |
|---|---|---|---|
| `raw_devices` | `ingest_date` | `(ingest_ts, device_id)` | Device list snapshot per run |
| `raw_energy_realtime` | `ingest_date` | `(ingest_ts, device_id)` | DP values at fetch time |
| `raw_energy_hourly` | `stat_date` | `(device_id, stat_hour)` | Tuya hourly aggregate API |
| `raw_energy_daily` | `stat_date` | `(device_id, stat_date)` | Tuya daily aggregate API |
| `ops_runs` | `DATE(started_at)` | `run_id` | Written at end of every Job run |

### 7.4 `ops_runs` schema

| Column | Type | Notes |
|---|---|---|
| `run_id` | STRING | UUID |
| `task` | STRING | `device_sync` / `energy_realtime` / etc. |
| `started_at` | TIMESTAMP | |
| `ended_at` | TIMESTAMP | |
| `status` | STRING | `success` / `failed` / `partial` |
| `rows_written` | INT64 | |
| `tuya_api_calls` | INT64 | |
| `error_message` | STRING | Short description on failure |
| `error_stack` | STRING | Traceback on failure |
| `params` | JSON | Job invocation parameters |

### 7.5 Why `payload JSON` for raw data

- Tuya may add new DP fields without notice; JSON absorbs them without
  schema migration.
- Raw remains a faithful, replayable record of the API response.
- All field-level parsing happens in dbt staging, where changes are
  reviewable and version-controlled SQL.

## 8. Device Topology

### 8.1 Source of truth

`dbt/seeds/device_relationships.csv` — manually edited, version-controlled.

Schema:

| Column | Notes |
|---|---|
| `child_device_id` | Tuya device ID (immutable) |
| `parent_device_id` | Tuya device ID (immutable) |
| `relationship_type` | Enum, currently `downstream`; reserved for future types |
| `port` | Optional, e.g. relay channel number |
| `note` | Free-form description |

Implicit unique key: `(child_device_id, parent_device_id, relationship_type)`.

### 8.2 Why a dbt seed (not a BQ table or external sheet)

- Every change is a `git commit` with author, timestamp, and message.
- CI can validate the CSV before it lands.
- Combined with `dbt snapshot`, history is captured automatically.
- Small data, technical owner: heavier alternatives add no value.

### 8.3 Validation

dbt tests on the seed (and the topology mart):

- `child_device_id` and `parent_device_id` must exist in `dim_devices`.
- `child_device_id != parent_device_id` (no self-loop).
- No cycles. Recursive resolution capped at a sensible depth.

## 9. Change Tracking (SCD Type 2)

### 9.1 `snap_devices`

- Source: `stg_devices_latest` (one row per `device_id`, latest by `ingest_ts`).
- Strategy: `check` on `name`, `category`, `category_code`, `product_id`,
  `product_name`, `icon`, `local_key_present`, `online`.
- `invalidate_hard_deletes=True` — devices removed from the Tuya account
  are closed off in the snapshot.
- Output: `tuya_snapshots.snap_devices` with `dbt_valid_from`,
  `dbt_valid_to`, `dbt_scd_id`.

### 9.2 `snap_device_relationships`

- Source: `ref('device_relationships')` (the seed).
- Unique key: `child_device_id || '|' || parent_device_id || '|' || relationship_type`.
- Strategy: `check` on `port`, `note`.
- `invalidate_hard_deletes=True` — removed rows in the CSV close off in
  the snapshot.

### 9.3 dbt run order

```
dbt seed       # publish latest CSV to BQ
dbt snapshot   # record diffs into SCD2 tables
dbt run        # staging → marts
dbt test       # data quality + topology validation
```

Snapshot must run before `run`, otherwise that day's marts would not see
today's topology changes.

## 10. Marts

| Table | Type | Purpose |
|---|---|---|
| `dim_devices` | dimension | Current devices (`WHERE dbt_valid_to IS NULL` from snapshot) |
| `dim_devices_history` | SCD2 view | Full device-attribute history |
| `dim_device_topology` | dimension | Current topology, joined with device names |
| `dim_device_topology_history` | SCD2 view | Topology change history |
| `fct_device_tree` | fact | Recursive expansion: `device_id`, `root_id`, `depth`, `path_array` |
| `fct_energy_consumption` | fact | Unified consumption fact across granularities (raw / hourly / daily) |
| `agg_device_daily` | aggregate | Per-device daily totals, peak power, etc. |

### 10.1 Parent net load (follow-up mart)

When parent and children both meter consumption, the parent's measured
value already includes its children. The relationship is:

```
parent_measured = Σ(children_measured) + parent_own_load
```

A future mart (`fct_device_net_load` or similar) can derive
`parent_own_load` per period using `fct_device_tree`. Not built in v1.

## 11. Scheduling

| Schedule | Cron (UTC) | Job parameters | Rationale |
|---|---|---|---|
| `tuya-device-sync-daily` | `0 2 * * *` | `--task=device_sync` | Capture renames / additions / removals |
| `tuya-energy-realtime` | `0 */4 * * *` | `--task=energy_realtime` | 6×/day, tunable |
| `tuya-energy-hourly` | `30 1 * * *` | `--task=energy_hourly --date=yesterday` | After Tuya finalizes hourly stats |
| `tuya-energy-daily` | `45 1 * * *` | `--task=energy_daily --date=yesterday` | After Tuya finalizes daily stats |
| `dbt-build-daily` | `0 3 * * *` | (triggers dbt seed/snapshot/run/test) | After all ingestion completes |

Backfill is invoked manually:
`--task=backfill --from=YYYY-MM-DD --to=YYYY-MM-DD`.

## 12. Job Execution Model

Each Cloud Run Job execution:

1. Generates a `run_id` (UUID) and records `started_at`.
2. Reads Tuya credentials from Secret Manager.
3. Obtains / refreshes the Tuya access token.
4. Fetches data appropriate to `--task`.
5. Batch-loads to the target raw table (`load_table_from_json`),
   tagging each row with `ingest_run_id`.
6. Writes a row to `ops_runs` with status, counts, and timing.
7. Exits non-zero on failure to surface the failure to Cloud Run / Monitoring.

Batch load (not streaming insert) is preferred: it is free, has no
streaming buffer side effects, and easily handles this volume.

## 13. Reliability

### 13.1 Retries

- HTTP layer: exponential backoff on 5xx and 429, max 3 attempts.
- Job layer: Cloud Run Job `task-timeout=30m`, `max-retries=2`.

### 13.2 Idempotency

- Raw tables are append-only. Re-running a task only adds rows with
  different `ingest_ts` values.
- dbt staging deduplicates by business key, taking the row with
  `MAX(ingest_ts)`.

### 13.3 Alerting

- Cloud Run Job failure → Cloud Monitoring alert → email/webhook.
- Freshness check: a Cloud Monitoring metric verifies that
  `device_sync` has at least one `success` row in `ops_runs` within the
  last 24h.

## 14. Security

- Tuya credentials live in Secret Manager. The Job's service account is
  granted `roles/secretmanager.secretAccessor` for those secrets only.
- The Job's service account has `roles/bigquery.dataEditor` scoped to
  `tuya_raw` and `roles/bigquery.jobUser` at the project level.
- dbt's service account has read on `tuya_raw` and write on
  `tuya_snapshots`, `tuya_staging`, `tuya_marts`.
- No secrets in code, env files, or logs. `.env` is gitignored.

## 15. Testing

| Layer | Type | Tooling | Coverage |
|---|---|---|---|
| `tuya/client.py` | unit | pytest + `responses` | Signature, token refresh, retry on 429/5xx |
| `bq/writer.py` | unit | pytest + mocked `bigquery.Client` | Schema mapping, JSON serialization, error propagation |
| `jobs/*.py` | unit | pytest + mocked client + writer | Pagination, filtering, error handling |
| `jobs/*.py` | integration (opt-in) | Real Tuya + BQ sandbox dataset | Gated by env var, not run by default |
| dbt models | data tests | dbt built-in + `dbt_utils` | Uniqueness, not-null, FK, value ranges |
| dbt marts | unit tests | dbt 1.8+ unit tests | Recursive topology, parent-net-load logic |
| Topology | data tests | custom dbt tests | No self-loops, no cycles, depth bound |

## 16. CI / CD

- **`ci.yml`** on PR: ruff lint, ingestion unit tests, `dbt parse`, then
  `dbt seed/snapshot/run/test` against a sandbox dataset.
- **`deploy.yml`** on push to `main`: build & push container to Artifact
  Registry, deploy Cloud Run Job, trigger one full dbt build against
  production datasets.

## 17. Extensibility

| Future change | Where it lands |
|---|---|
| New device category | New `stg_<category>_<metric>.sql` in dbt; raw schema unchanged thanks to `payload JSON` |
| New Tuya endpoint | New `jobs/*.py` file + new Cloud Scheduler entry + new raw table; existing files untouched |
| New analytics | New mart model only |
| Topology becomes more complex | `relationship_type` and `port` already reserved; recursive `fct_device_tree` is depth-agnostic |
| Replace warehouse | dbt is portable; ingestion change is confined to `bq/writer.py` |
| Add other vendors | Abstract `tuya/client.py` behind an `IngestionSource` interface; add `vendor` column to raw |
| Real-time alerting | Mart-layer aggregate + Cloud Monitoring metric; ingestion unchanged |
| Reporting UI | BigQuery → Looker Studio / Metabase; no code |

YAGNI is applied: none of these are pre-built, but module boundaries
allow each to be added without disturbing unrelated code.

## 18. Out-of-Scope Follow-ups

- Cost / tariff calculation mart (time-of-use pricing).
- Parent net-load mart (`parent_measured - Σ(children_measured)`).
- Anomaly detection (sudden spikes / drops).
- Multi-vendor abstraction.

These are explicitly deferred to keep the v1 implementation focused.

## 19. Open Questions / TBD

- GCP project ID and BigQuery region — confirm during implementation.
- Where dbt actually runs (dbt Cloud vs Cloud Run vs GitHub Actions) —
  decide before writing the deploy workflow.
- Alert destination (email address, Slack webhook).
- Initial set of device IDs for the topology seed CSV.
