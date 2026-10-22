from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://imports:imports@localhost:5437/imports"
    broker_url: str = "amqp://imports:imports@localhost:5673//"
    result_backend: str = "redis://localhost:6380/0"

    # "key:seller,key2:seller2". Each API key belongs to one seller.
    api_keys: str = "dev-key:seller-1"
    webhook_secret: str = "dev-webhook-secret"  # noqa: S105

    max_upload_bytes: int = 50 * 1024 * 1024
    chunk_rows: int = 1000
    # QUEUED/PROCESSING imports untouched for this long are picked up again by the sweeper.
    stuck_after_seconds: int = 120
    webhook_max_attempts: int = 10
    # Share of chunk attempts that fail on purpose (tests and chaos runs only).
    fault_rate: float = Field(default=0.0, ge=0, le=1)

    def sellers_by_key(self) -> dict[str, str]:
        pairs = (p.split(":", 1) for p in self.api_keys.split(",") if ":" in p)
        return {k.strip(): s.strip() for k, s in pairs}


@lru_cache
def settings() -> Settings:
    return Settings()
