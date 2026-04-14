"""CLI entry point. Dispatches to per-task modules based on --task."""

from __future__ import annotations

import logging
import uuid
from enum import StrEnum

import typer
from google.cloud import bigquery

from tuya_penny_cc.bq.writer import BigQueryWriter
from tuya_penny_cc.config import Settings
from tuya_penny_cc.jobs import device_sync, energy_realtime
from tuya_penny_cc.tuya.client import TuyaClient

logger = logging.getLogger("tuya_penny_cc")


class Task(StrEnum):
    device_sync = "device_sync"
    energy_realtime = "energy_realtime"


def app() -> None:
    typer.run(_main)


def _main(
    task: Task = typer.Option(..., "--task", help="Which ingestion task to run."),  # noqa: B008
    log_level: str = typer.Option("INFO", "--log-level"),  # noqa: B008
) -> None:
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings()
    run_id = uuid.uuid4().hex
    logger.info("starting task=%s run_id=%s", task.value, run_id)

    bq_client = bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)
    writer = BigQueryWriter(
        client=bq_client,
        project=settings.gcp_project_id,
        dataset=settings.bq_dataset_raw,
    )
    tuya = TuyaClient(
        base_url=settings.tuya_base_url,
        access_id=settings.tuya_access_id,
        access_secret=settings.tuya_access_secret.get_secret_value(),
        user_uid=settings.tuya_user_uid,
    )
    try:
        match task:
            case Task.device_sync:
                written = device_sync.run(tuya=tuya, writer=writer, run_id=run_id)
            case Task.energy_realtime:
                written = energy_realtime.run(tuya=tuya, writer=writer, run_id=run_id)
            case _:
                raise typer.BadParameter(f"Unknown task: {task}")
        logger.info("task=%s wrote %d rows", task.value, written)
    finally:
        tuya.close()


if __name__ == "__main__":
    app()
