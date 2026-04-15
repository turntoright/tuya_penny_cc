from unittest.mock import MagicMock, patch

from tuya_penny_cc.main import Task, _main


def _run_main(task_name: str, **kwargs):
    """Call _main() with all external dependencies patched out."""
    # Provide defaults for all optional parameters to avoid typer.Option handling
    defaults = {
        "log_level": "INFO",
        "date_str": None,
        "start_date_str": None,
        "end_date_str": None,
    }
    defaults.update(kwargs)

    with (
        patch("tuya_penny_cc.main.Settings"),
        patch("tuya_penny_cc.main.bigquery"),
        patch("tuya_penny_cc.main.BigQueryWriter"),
        patch("tuya_penny_cc.main.TuyaClient") as MockTuya,
    ):
        mock_tuya = MagicMock()
        MockTuya.return_value = mock_tuya
        _main(task=Task(task_name), **defaults)


def test_energy_dp_log_dispatched():
    with patch("tuya_penny_cc.main.energy_dp_log") as mock_job:
        mock_job.run.return_value = 0
        _run_main("energy_dp_log")
        mock_job.run.assert_called_once()
