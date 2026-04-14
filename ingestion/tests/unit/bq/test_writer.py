from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from tuya_penny_cc.bq.schemas import RAW_DEVICES_SCHEMA
from tuya_penny_cc.bq.writer import BigQueryWriter


def test_raw_devices_schema_has_lineage_columns():
    names = {f.name for f in RAW_DEVICES_SCHEMA}
    required = {
        "ingest_ts",
        "ingest_run_id",
        "source_endpoint",
        "payload",
        "device_id",
        "ingest_date",
    }
    assert required.issubset(names)


def test_raw_devices_payload_is_json_type():
    payload_field = next(f for f in RAW_DEVICES_SCHEMA if f.name == "payload")
    assert payload_field.field_type == "JSON"
    assert payload_field.mode == "REQUIRED"


def test_writer_load_calls_bigquery_load_table_from_json():
    mock_client = MagicMock(spec=bigquery.Client)
    mock_load_job = MagicMock()
    mock_load_job.output_rows = 2
    mock_client.load_table_from_json.return_value = mock_load_job

    writer = BigQueryWriter(client=mock_client, project="proj", dataset="tuya_raw")
    rows = [{"device_id": "d1"}, {"device_id": "d2"}]
    written = writer.load("raw_devices", rows, schema=RAW_DEVICES_SCHEMA)

    assert written == 2
    assert mock_client.load_table_from_json.called
    kwargs = mock_client.load_table_from_json.call_args.kwargs
    assert mock_client.load_table_from_json.call_args.args[0] == rows
    assert mock_client.load_table_from_json.call_args.args[1] == "proj.tuya_raw.raw_devices"
    job_config = kwargs["job_config"]
    assert job_config.schema == RAW_DEVICES_SCHEMA
    assert job_config.write_disposition == "WRITE_APPEND"
    mock_load_job.result.assert_called_once()


def test_writer_load_empty_rows_is_noop():
    mock_client = MagicMock(spec=bigquery.Client)
    writer = BigQueryWriter(client=mock_client, project="proj", dataset="tuya_raw")
    written = writer.load("raw_devices", [], schema=RAW_DEVICES_SCHEMA)
    assert written == 0
    mock_client.load_table_from_json.assert_not_called()


def test_writer_load_propagates_load_job_failure():
    mock_client = MagicMock(spec=bigquery.Client)
    mock_load_job = MagicMock()
    mock_load_job.result.side_effect = RuntimeError("BQ load failed")
    mock_client.load_table_from_json.return_value = mock_load_job

    writer = BigQueryWriter(client=mock_client, project="proj", dataset="tuya_raw")
    with pytest.raises(RuntimeError, match="BQ load failed"):
        writer.load("raw_devices", [{"x": 1}], schema=RAW_DEVICES_SCHEMA)
