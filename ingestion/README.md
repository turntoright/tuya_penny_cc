# tuya_penny_cc ingestion

Python ingestion pipeline that reads from Tuya OpenAPI and writes to BigQuery.

## Local setup

1. Install [uv](https://docs.astral.sh/uv/).
2. From this directory:
   ```bash
   uv sync
   ```
3. Authenticate with GCP:
   ```bash
   gcloud auth application-default login
   ```
4. Copy the env template and fill in values:
   ```bash
   cp .env.example .env
   # edit .env
   ```

## Run a job

```bash
uv run python -m tuya_penny_cc --task=device_sync
```

## Tests

```bash
uv run pytest
```
