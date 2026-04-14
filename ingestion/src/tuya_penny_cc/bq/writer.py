"""BigQuery batch writer.

Wraps `Client.load_table_from_json` because at our scale (≤100 devices,
a few runs per day) batch loads are free and avoid streaming-buffer
side effects on partitioned tables.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from google.cloud import bigquery


class BigQueryWriter:
    def __init__(self, *, client: bigquery.Client, project: str, dataset: str) -> None:
        self._client = client
        self._project = project
        self._dataset = dataset

    def load(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        *,
        schema: list[bigquery.SchemaField],
    ) -> int:
        """Append `rows` to `<project>.<dataset>.<table>` and return rows written."""
        if not rows:
            return 0
        table_id = f"{self._project}.{self._dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = self._client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()  # raises on failure
        return int(job.output_rows or len(rows))
