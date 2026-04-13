from tuya_penny_cc.config import Settings


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("TUYA_ACCESS_ID", "id123")
    monkeypatch.setenv("TUYA_ACCESS_SECRET", "secret456")
    monkeypatch.setenv("TUYA_BASE_URL", "https://openapi.tuyacn.com")
    monkeypatch.setenv("TUYA_USER_UID", "uid789")
    monkeypatch.setenv("GCP_PROJECT_ID", "my-proj")
    monkeypatch.setenv("BQ_DATASET_RAW", "tuya_raw")

    s = Settings()

    assert s.tuya_access_id == "id123"
    assert s.tuya_access_secret.get_secret_value() == "secret456"
    assert s.tuya_base_url == "https://openapi.tuyacn.com"
    assert s.tuya_user_uid == "uid789"
    assert s.gcp_project_id == "my-proj"
    assert s.bq_dataset_raw == "tuya_raw"
    assert s.bq_location == "US"  # default


def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("TUYA_ACCESS_ID", raising=False)
    monkeypatch.delenv("TUYA_ACCESS_SECRET", raising=False)
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(_env_file=None)
