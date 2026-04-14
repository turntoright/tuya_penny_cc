from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables (or .env)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Tuya
    tuya_access_id: str
    tuya_access_secret: SecretStr
    tuya_base_url: str
    tuya_user_uid: str

    # BigQuery
    gcp_project_id: str
    bq_dataset_raw: str = "tuya_raw"
    bq_location: str = "US"
