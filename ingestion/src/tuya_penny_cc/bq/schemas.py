"""BigQuery schemas for raw landing tables.

Conventions (per design spec §7.2):
- Every raw_* table includes lineage columns: ingest_ts, ingest_run_id,
  source_endpoint, payload, plus a partition column.
- Business keys (device_id, stat_date) are promoted from payload to top
  level for partition pruning and join performance.
"""

from google.cloud import bigquery

RAW_DEVICES_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingest_run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
    bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingest_date", "DATE", mode="REQUIRED"),
]

RAW_ENERGY_REALTIME_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingest_run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
    bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingest_date", "DATE", mode="REQUIRED"),
]
